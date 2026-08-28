use std::collections::{HashMap, HashSet};

use river_data_core::chrono::{DateTime, Duration, Utc};
use river_data_core::client::{
    BackendError, SourceBackend, StreamDescriptor, StreamFetchRequest, StreamReadings,
    StreamStatusEvents,
};
use river_data_core::models::{DataStream, IngestReading, IngestStatusEvent};
use river_data_core::serde_json::json;
use river_data_core::tracing;

use crate::config::VaisalaConfig;
use crate::models::LocationsHistoryResponse;
use crate::vaisala_client::VaisalaClient;

pub struct VaisalaBackend {
    config: VaisalaConfig,
    client: VaisalaClient,
}

fn parse_hierarchy(path: &str) -> river_data_core::serde_json::Value {
    let segments: Vec<&str> = path.split('/').collect();
    json!({
        "project": segments.get(1).unwrap_or(&""),
        "site": segments.get(2).unwrap_or(&""),
        "parameter": segments.get(3).unwrap_or(&""),
    })
}

impl VaisalaBackend {
    pub fn new(config: VaisalaConfig, client: VaisalaClient) -> Self {
        Self { config, client }
    }

    /// Map one history response onto its requesting streams, filtering each
    /// stream's points by its own cursor.
    fn history_to_readings(
        history: LocationsHistoryResponse,
        requests: &HashMap<i32, &StreamFetchRequest>,
    ) -> Vec<StreamReadings> {
        let mut out = Vec::new();
        for resource in history.data {
            let attrs = resource.attributes;
            let Some(req) = requests.get(&attrs.id) else {
                continue;
            };

            let since_epoch = req.since.map(|t| t.timestamp());
            let readings: Vec<IngestReading> = attrs
                .data_points
                .into_iter()
                .filter(|dp| since_epoch.is_none_or(|s| dp.timestamp > s))
                .filter_map(|dp| {
                    let value = dp.value?;
                    // Clamp to a representable datetime before arithmetic so a
                    // malformed epoch cannot overflow the snap below.
                    let epoch = DateTime::from_timestamp(dp.timestamp, 0)
                        .unwrap_or_else(Utc::now)
                        .timestamp();
                    // viewLinc loggers report on a 10-minute cadence with second-level
                    // jitter; snapping keeps one canonical timestamp per interval.
                    let rounded_epoch = ((epoch + 300) / 600) * 600;
                    let time = DateTime::from_timestamp(rounded_epoch, 0)
                        .or_else(|| DateTime::from_timestamp(epoch, 0))
                        .unwrap_or_else(Utc::now);
                    Some(IngestReading::new(time, value))
                })
                .collect();

            if readings.is_empty() {
                continue;
            }
            out.push(StreamReadings::new(
                req.stream_id,
                req.source_key.clone(),
                readings,
            ));
        }
        out
    }
}

#[async_trait::async_trait]
impl SourceBackend for VaisalaBackend {
    fn source_system(&self) -> &str {
        "vaisala"
    }

    async fn discover_streams(&self) -> Result<Vec<StreamDescriptor>, BackendError> {
        let locations = self.client.get_locations().await?;

        let leaf_ids: Vec<i32> = locations
            .data
            .iter()
            .filter(|r| !r.attributes.deleted && r.attributes.leaf)
            .map(|r| r.attributes.node_id)
            .collect();

        let location_data: HashMap<i32, _> = if leaf_ids.is_empty() {
            HashMap::new()
        } else {
            match self.client.get_locations_data(&leaf_ids).await {
                Ok(data) => data
                    .data
                    .into_iter()
                    .map(|r| (r.attributes.id, r.attributes))
                    .collect(),
                Err(e) => {
                    tracing::warn!(error = %e, "Failed to fetch locations_data, proceeding without device metadata");
                    HashMap::new()
                }
            }
        };

        let mut descriptors = Vec::new();
        for resource in &locations.data {
            let attrs = &resource.attributes;
            if attrs.deleted || !attrs.leaf {
                continue;
            }

            let leaf_name = attrs.path.split('/').next_back().unwrap_or(&attrs.text);
            let mut metadata = json!({
                "vaisala_node_id": attrs.node_id,
                "hierarchy": parse_hierarchy(&attrs.path),
            });
            if let Some(ld) = location_data.get(&attrs.node_id) {
                metadata["device"] = json!({
                    "logger_serial": &ld.logger_serial_number,
                    "probe_serial": &ld.probe_serial_number,
                    "logger_device": &ld.logger_device,
                    "device_class": &ld.device_class,
                });
                metadata["units"] = json!(&ld.display_units);
                metadata["sample_interval_sec"] = json!(ld.sample_interval_sec);
                metadata["channel_id"] = json!(ld.channel_id);
            }

            descriptors.push(StreamDescriptor {
                source_key: attrs.node_id.to_string(),
                source_name: leaf_name.to_string(),
                source_path: attrs.path.clone(),
                metadata,
                measurement_type: Some("continuous".to_string()),
                sensor_id: None,
                replicates: None,
            });
        }

        Ok(descriptors)
    }

    /// Splits streams into two groups so one new stream cannot drag all others
    /// back to the max history window: no cursor fetches from `max_history_days`
    /// ago, the rest fetch from the earliest cursor in the group.
    async fn fetch_readings(
        &self,
        requests: &[StreamFetchRequest],
    ) -> Result<Vec<StreamReadings>, BackendError> {
        let by_location: HashMap<i32, &StreamFetchRequest> = requests
            .iter()
            .filter_map(|r| match r.source_key.parse::<i32>() {
                Ok(id) => Some((id, r)),
                Err(_) => {
                    tracing::warn!(source_key = %r.source_key, "Skipping stream: source_key is not a viewLinc location id");
                    None
                }
            })
            .collect();

        let backfill: Vec<i32> = by_location
            .iter()
            .filter(|(_, r)| r.since.is_none())
            .map(|(id, _)| *id)
            .collect();
        let incremental: Vec<i32> = by_location
            .iter()
            .filter(|(_, r)| r.since.is_some())
            .map(|(id, _)| *id)
            .collect();

        let now = Utc::now();
        let mut out = Vec::new();

        if !backfill.is_empty() {
            let from = now - Duration::days(self.config.max_history_days);
            tracing::info!(count = backfill.len(), from = %from, "Backfilling streams with no prior data");
            let history = self
                .client
                .get_locations_history(&backfill, from, Some(now))
                .await?;
            out.extend(Self::history_to_readings(history, &by_location));
        }

        if !incremental.is_empty() {
            let earliest = incremental
                .iter()
                .filter_map(|id| by_location.get(id).and_then(|r| r.since))
                .min()
                .unwrap_or(now);
            tracing::info!(count = incremental.len(), from = %earliest, "Incremental sync from last known data");
            let history = self
                .client
                .get_locations_history(&incremental, earliest, Some(now))
                .await?;
            out.extend(Self::history_to_readings(history, &by_location));
        }

        Ok(out)
    }

    async fn fetch_status_events(
        &self,
        streams: &[DataStream],
    ) -> Result<Vec<StreamStatusEvents>, BackendError> {
        let by_location: HashMap<i32, &DataStream> = streams
            .iter()
            .filter_map(|s| match s.source_key.parse::<i32>() {
                Ok(id) => Some((id, s)),
                Err(_) => {
                    tracing::warn!(source_key = %s.source_key, "Skipping stream: source_key is not a viewLinc location id");
                    None
                }
            })
            .collect();
        if by_location.is_empty() {
            return Ok(Vec::new());
        }

        let location_ids: Vec<i32> = by_location.keys().copied().collect();
        let data = self.client.get_locations_data(&location_ids).await?;
        let now = Utc::now();

        let mut seen: HashSet<i32> = HashSet::new();
        let mut out = Vec::new();
        for resource in data.data {
            let attrs = resource.attributes;
            if !seen.insert(attrs.id) {
                continue;
            }
            let Some(stream) = by_location.get(&attrs.id) else {
                continue;
            };
            out.push(StreamStatusEvents {
                stream_id: stream.id,
                source_key: stream.source_key.clone(),
                events: vec![IngestStatusEvent {
                    time: now,
                    value: format!(
                        "status={} battery={} signal={} powered={} unreachable={}",
                        attrs.device_status,
                        attrs.battery_level,
                        attrs.signal_quality,
                        attrs.line_powered,
                        attrs.unreachable
                    ),
                }],
            });
        }
        Ok(out)
    }
}

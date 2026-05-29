use serde::{Deserialize, Deserializer, Serialize};

/// Deserialize a number that may arrive as float into i64.
fn deserialize_timestamp<'de, D: Deserializer<'de>>(d: D) -> Result<i64, D::Error> {
    let v: f64 = Deserialize::deserialize(d)?;
    Ok(v as i64)
}

fn deserialize_timestamp_opt<'de, D: Deserializer<'de>>(d: D) -> Result<Option<i64>, D::Error> {
    let v: Option<f64> = Deserialize::deserialize(d)?;
    Ok(v.map(|f| f as i64))
}

// ============================================================================
// Vaisala API response types (from connectors/vaisala/models.rs)
// ============================================================================

/// JSON API wrapper for responses
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JsonApiResponse<T> {
    pub jsonapi: JsonApiVersion,
    pub data: Vec<JsonApiResource<T>>,
    #[serde(default)]
    pub links: Option<serde_json::Value>,
    #[serde(default)]
    pub meta: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JsonApiVersion {
    pub version: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JsonApiResource<T> {
    #[serde(rename = "type")]
    pub resource_type: String,
    pub id: String,
    pub attributes: T,
}

/// Response from `/rest/v1/locations_history`
pub type LocationsHistoryResponse = JsonApiResponse<LocationHistoryAttributes>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LocationHistoryAttributes {
    pub id: i32,
    pub name: String,
    pub zone: String,
    #[serde(default, deserialize_with = "deserialize_timestamp_opt")]
    pub timestamp: Option<i64>,
    #[serde(default)]
    pub value: Option<f64>,
    #[serde(default)]
    pub current_units: Option<String>,
    #[serde(default)]
    pub display_units: Option<String>,
    #[serde(default)]
    pub max: Option<f64>,
    #[serde(default, deserialize_with = "deserialize_timestamp_opt")]
    pub max_time: Option<i64>,
    #[serde(default)]
    pub avg: Option<f64>,
    #[serde(default)]
    pub min: Option<f64>,
    #[serde(default, deserialize_with = "deserialize_timestamp_opt")]
    pub min_time: Option<i64>,
    #[serde(default, deserialize_with = "deserialize_timestamp_opt")]
    pub seconds: Option<i64>,
    #[serde(default)]
    pub decimal_places: Option<i16>,
    #[serde(default)]
    #[serde(rename = "std")]
    pub std_dev: Option<f64>,
    #[serde(default)]
    pub mkt: Option<serde_json::Value>,
    #[serde(default)]
    pub samples: Option<i32>,
    #[serde(default)]
    pub realtime_samples: Option<i32>,
    #[serde(default)]
    pub data_points: Vec<DataPoint>,
    #[serde(default)]
    pub thresholds: Vec<serde_json::Value>,
}

/// A single data point: [timestamp_epoch, value, logged_bool]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(from = "RawDataPoint")]
pub struct DataPoint {
    pub timestamp: i64,
    pub value: f64,
    pub logged: bool,
}

#[derive(Debug, Clone, Deserialize)]
struct RawDataPoint(f64, Option<f64>, bool);

impl From<RawDataPoint> for DataPoint {
    fn from(raw: RawDataPoint) -> Self {
        Self {
            timestamp: raw.0 as i64,
            value: raw.1.unwrap_or(0.0),
            logged: raw.2,
        }
    }
}

/// Response from `/rest/v1/locations`
pub type LocationsResponse = JsonApiResponse<LocationAttributes>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LocationAttributes {
    #[serde(default)]
    pub type_name: String,
    #[serde(default)]
    pub description: String,
    #[serde(default)]
    pub path: String,
    #[serde(default)]
    pub text: String,
    #[serde(default)]
    pub pos: i32,
    #[serde(default)]
    pub node_id: i32,
    #[serde(default)]
    pub pause: bool,
    #[serde(default)]
    pub leaf: bool,
    #[serde(default)]
    pub type_id: i32,
    #[serde(default)]
    pub node_type: i32,
    #[serde(default)]
    pub deleted: bool,
}

/// Response from `/rest/v1/locations_data`
pub type LocationsDataResponse = JsonApiResponse<LocationDataAttributes>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LocationDataAttributes {
    pub id: i32,
    #[serde(default)]
    pub zone: String,
    #[serde(default)]
    pub location_name: String,
    #[serde(default)]
    pub location_description: String,
    #[serde(default)]
    pub location_path: String,
    #[serde(default)]
    pub location_type: String,
    #[serde(default)]
    pub permission: i32,
    #[serde(default)]
    pub value: f64,
    #[serde(default)]
    pub decimal_places: i16,
    #[serde(default)]
    pub display_units: String,
    #[serde(default)]
    pub channel_id: i32,
    #[serde(default)]
    pub logger_id: i32,
    #[serde(default)]
    pub logger_description: String,
    #[serde(default)]
    pub logger_serial_number: String,
    #[serde(default)]
    pub probe_serial_number: String,
    #[serde(default)]
    pub sample_interval_sec: i32,
    #[serde(default)]
    pub chindex: i32,
    #[serde(default)]
    pub description: String,
    #[serde(default)]
    pub logger_device: String,
    #[serde(default, deserialize_with = "deserialize_timestamp")]
    pub timestamp: i64,
    #[serde(default)]
    pub device_status: String,
    #[serde(default)]
    pub deleted: i32,
    #[serde(default)]
    pub device_class: String,
    #[serde(default)]
    pub battery_level: i16,
    #[serde(default)]
    pub battery_state: i16,
    #[serde(default)]
    pub line_powered: i16,
    #[serde(default)]
    pub signal_quality: i16,
    #[serde(default)]
    pub unreachable: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    // viewLinc 5.2.1.859 serialises some integer epoch fields as floats (e.g. `1780007546.0`).
    // The timestamp fields must parse from either int or float.
    #[test]
    fn locations_history_parses_float_timestamps() {
        let json = r#"{
            "jsonapi": {"version": "5.2.1.859"},
            "data": [{"type": "locations_history", "id": "1312", "attributes": {
                "id": 1312, "name": "SDOdegC", "zone": "viewLinc/BREATHE/Saxon",
                "timestamp": 1780007546.0, "value": 10.97,
                "max": 14.83, "max_time": 1779547080.0, "avg": 10.6,
                "min": 6.76, "min_time": 1774678080.0,
                "seconds": 7748220.0, "decimal_places": 2, "std": 1.6,
                "mkt": "N/A", "samples": 12915, "realtime_samples": 0,
                "data_points": [[1772259060.0, 11.97, true]]
            }}]
        }"#;

        let resp: LocationsHistoryResponse =
            serde_json::from_str(json).expect("float timestamps must deserialize");
        let attrs = &resp.data[0].attributes;
        assert_eq!(attrs.timestamp, Some(1_780_007_546));
        assert_eq!(attrs.max_time, Some(1_779_547_080));
        assert_eq!(attrs.min_time, Some(1_774_678_080));
        assert_eq!(attrs.seconds, Some(7_748_220));
        assert_eq!(attrs.data_points[0].timestamp, 1_772_259_060);
    }

    // Integer epochs (the historical format) must keep working.
    #[test]
    fn locations_history_parses_integer_timestamps() {
        let json = r#"{
            "jsonapi": {"version": "5.0.0"},
            "data": [{"type": "locations_history", "id": "1", "attributes": {
                "id": 1, "name": "x", "zone": "z",
                "timestamp": 1780007546, "max_time": 1779547080, "min_time": 1774678080,
                "seconds": 7748220, "data_points": [[1772259060, 1.0, false]]
            }}]
        }"#;

        let resp: LocationsHistoryResponse =
            serde_json::from_str(json).expect("integer timestamps must deserialize");
        let attrs = &resp.data[0].attributes;
        assert_eq!(attrs.timestamp, Some(1_780_007_546));
        assert_eq!(attrs.seconds, Some(7_748_220));
        assert_eq!(attrs.data_points[0].timestamp, 1_772_259_060);
    }
}

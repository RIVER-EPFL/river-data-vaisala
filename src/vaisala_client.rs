use std::time::Duration;

use reqwest::Client;
use river_data_core::chrono::{DateTime, Utc};

use crate::models::{LocationsDataResponse, LocationsHistoryResponse, LocationsResponse};

/// How far after the requested start a location's first point may sit before the response is
/// reported as short of it. viewLinc reports on a 10-minute cadence, so an exact match is never
/// expected.
const COVERAGE_SLACK_SECONDS: i64 = 3600;

#[derive(Debug, thiserror::Error)]
#[error("Vaisala API error: {0}")]
pub struct VaisalaError(pub String);

pub struct VaisalaClient {
    http_client: Client,
    base_url: String,
    bearer_token: String,
}

impl VaisalaClient {
    pub fn new(
        base_url: &str,
        bearer_token: &str,
        skip_tls_verify: bool,
    ) -> Result<Self, reqwest::Error> {
        let http_client = Client::builder()
            .danger_accept_invalid_certs(skip_tls_verify)
            .timeout(Duration::from_secs(300))
            .build()?;

        Ok(Self {
            http_client,
            base_url: base_url.to_string(),
            bearer_token: bearer_token.to_string(),
        })
    }

    async fn get(&self, url: String) -> Result<reqwest::Response, VaisalaError> {
        let response = self
            .http_client
            .get(&url)
            .bearer_auth(&self.bearer_token)
            .send()
            .await
            .map_err(|e| VaisalaError(format!("Request failed: {e}")))?;

        if response.status() == reqwest::StatusCode::TOO_MANY_REQUESTS {
            return Err(VaisalaError("Rate limited (429)".to_string()));
        }
        if !response.status().is_success() {
            return Err(VaisalaError(format!(
                "HTTP {}: {}",
                response.status(),
                response.text().await.unwrap_or_default()
            )));
        }
        Ok(response)
    }

    pub async fn get_locations(&self) -> Result<LocationsResponse, VaisalaError> {
        let response = self
            .get(format!("{}/locations?flatten=true", self.base_url))
            .await?;
        response
            .json()
            .await
            .map_err(|e| VaisalaError(format!("Failed to parse response: {e}")))
    }

    pub async fn get_locations_history(
        &self,
        location_ids: &[i32],
        date_from: DateTime<Utc>,
        date_to: Option<DateTime<Utc>>,
    ) -> Result<LocationsHistoryResponse, VaisalaError> {
        let mut url = format!(
            "{}/locations_history?location_ids={}&date_from={}",
            self.base_url,
            ids_param(location_ids),
            date_from.timestamp()
        );
        if let Some(to) = date_to {
            url.push_str(&format!("&date_to={}", to.timestamp()));
        }

        let response = self.get(url).await?;
        let text = response
            .text()
            .await
            .map_err(|e| VaisalaError(format!("Failed to get response text: {e}")))?;

        let history: LocationsHistoryResponse = river_data_core::serde_json::from_str(&text)
            .map_err(|e| {
                river_data_core::tracing::error!(
                    error = %e,
                    body_preview = %text.chars().take(500).collect::<String>(),
                    "Failed to parse locations_history response"
                );
                VaisalaError(format!("Failed to parse response: {e}"))
            })?;

        let from_epoch = date_from.timestamp();
        let to_epoch = date_to.map(|t| t.timestamp());
        let cov = coverage(&history, location_ids, from_epoch, to_epoch);

        let ignored: Vec<i32> = cov
            .iter()
            .filter(|c| c.out_of_range > 0)
            .map(|c| c.location_id)
            .collect();
        if !ignored.is_empty() {
            return Err(VaisalaError(format!(
                "locations_history answered outside the requested range [{from_epoch}, {}] for locations {ignored:?}",
                to_epoch.map_or_else(|| "now".to_string(), |t| t.to_string())
            )));
        }

        // Short of the requested start is ambiguous: a channel installed part way through the
        // window looks the same as a truncated page, so it is reported and not refused.
        let short = shortfall(&cov, from_epoch, COVERAGE_SLACK_SECONDS);
        if !short.is_empty() {
            river_data_core::tracing::warn!(
                locations = ?short,
                requested_from = from_epoch,
                "locations_history returned no point near the requested start"
            );
        }
        river_data_core::tracing::info!(
            locations = location_ids.len(),
            points = cov.iter().map(|c| c.points).sum::<usize>(),
            requested_from = from_epoch,
            "locations_history fetched"
        );

        Ok(history)
    }

    pub async fn get_locations_data(
        &self,
        location_ids: &[i32],
    ) -> Result<LocationsDataResponse, VaisalaError> {
        let response = self
            .get(format!(
                "{}/locations_data?location_ids={}",
                self.base_url,
                ids_param(location_ids)
            ))
            .await?;
        response
            .json()
            .await
            .map_err(|e| VaisalaError(format!("Failed to parse response: {e}")))
    }
}

/// What one `locations_history` response returned for a location, against the range asked for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HistoryCoverage {
    pub location_id: i32,
    pub points: usize,
    pub oldest: Option<i64>,
    pub newest: Option<i64>,
    pub out_of_range: usize,
}

/// Coverage per requested location, in request order. A location the response omits entirely is
/// reported with no points rather than dropped, and a location the response volunteers is ignored.
pub fn coverage(
    history: &LocationsHistoryResponse,
    location_ids: &[i32],
    from: i64,
    to: Option<i64>,
) -> Vec<HistoryCoverage> {
    location_ids
        .iter()
        .map(|&location_id| {
            let stamps: Vec<i64> = history
                .data
                .iter()
                .filter(|r| r.attributes.id == location_id)
                .flat_map(|r| r.attributes.data_points.iter().map(|dp| dp.timestamp))
                .collect();
            HistoryCoverage {
                location_id,
                points: stamps.len(),
                oldest: stamps.iter().min().copied(),
                newest: stamps.iter().max().copied(),
                out_of_range: stamps
                    .iter()
                    .filter(|&&t| t < from || to.is_some_and(|end| t > end))
                    .count(),
            }
        })
        .collect()
}

/// Locations whose oldest returned point sits more than `slack` seconds after the requested
/// start. A location that returned nothing is silent, not short, and is never named.
pub fn shortfall(coverage: &[HistoryCoverage], from: i64, slack: i64) -> Vec<i32> {
    coverage
        .iter()
        .filter(|c| c.oldest.is_some_and(|oldest| oldest - from > slack))
        .map(|c| c.location_id)
        .collect()
}

fn ids_param(location_ids: &[i32]) -> String {
    format!(
        "[{}]",
        location_ids
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join(",")
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::{DataPoint, JsonApiResource, LocationHistoryAttributes};

    fn history(entries: &[(i32, &[i64])]) -> LocationsHistoryResponse {
        LocationsHistoryResponse {
            data: entries
                .iter()
                .map(|(id, stamps)| JsonApiResource {
                    attributes: LocationHistoryAttributes {
                        id: *id,
                        data_points: stamps
                            .iter()
                            .map(|&timestamp| DataPoint {
                                timestamp,
                                value: Some(1.0),
                            })
                            .collect(),
                    },
                })
                .collect(),
        }
    }

    #[test]
    fn test_coverage_counts_points_and_span_per_location() {
        let cov = coverage(
            &history(&[(1270, &[100, 300, 200])]),
            &[1270],
            100,
            Some(400),
        );
        assert_eq!(
            cov,
            vec![HistoryCoverage {
                location_id: 1270,
                points: 3,
                oldest: Some(100),
                newest: Some(300),
                out_of_range: 0,
            }]
        );
    }

    #[test]
    fn test_coverage_reports_a_requested_location_the_response_omits() {
        let cov = coverage(&history(&[(1270, &[100])]), &[1270, 1272], 100, Some(400));
        assert_eq!(cov[1].location_id, 1272);
        assert_eq!(cov[1].points, 0);
        assert_eq!(cov[1].oldest, None);
    }

    // Scenario: the appliance ignores the range filter and answers with points outside it.
    // Expected behaviour: they are counted, on both edges, so the caller can refuse the response.
    #[test]
    fn test_coverage_counts_points_outside_the_requested_range() {
        let cov = coverage(
            &history(&[(1270, &[99, 100, 400, 401])]),
            &[1270],
            100,
            Some(400),
        );
        assert_eq!(cov[0].out_of_range, 2);
        assert_eq!(cov[0].points, 4);
    }

    #[test]
    fn test_coverage_with_no_upper_bound_only_checks_the_lower_edge() {
        let cov = coverage(
            &history(&[(1270, &[99, 100, 1_000_000])]),
            &[1270],
            100,
            None,
        );
        assert_eq!(cov[0].out_of_range, 1);
    }

    #[test]
    fn test_coverage_of_an_empty_response_is_empty_per_requested_location() {
        let cov = coverage(&history(&[]), &[1270], 100, Some(400));
        assert_eq!(cov[0].points, 0);
        assert_eq!(cov[0].out_of_range, 0);
        assert_eq!(cov[0].newest, None);
    }

    #[test]
    fn test_shortfall_names_only_locations_short_of_the_requested_start() {
        let cov = coverage(
            &history(&[(1270, &[100]), (1272, &[100_000])]),
            &[1270, 1272],
            100,
            Some(200_000),
        );
        assert_eq!(shortfall(&cov, 100, 3600), vec![1272]);
    }

    // A location whose first point sits inside the tolerance of the requested start has not
    // fallen short: viewLinc reports on a 10-minute cadence, so an exact match is not expected.
    #[test]
    fn test_shortfall_allows_one_cadence_of_slack() {
        let cov = coverage(&history(&[(1270, &[3_699])]), &[1270], 100, Some(200_000));
        assert!(shortfall(&cov, 100, 3600).is_empty());
        assert_eq!(shortfall(&cov, 100, 3598), vec![1270]);
    }

    // A location that returned nothing has not fallen short of anything: it is silent, which
    // is what a decommissioned channel looks like, and erroring on it would stop every sync.
    #[test]
    fn test_shortfall_ignores_a_location_with_no_points() {
        let cov = coverage(&history(&[]), &[1270], 100, Some(200_000));
        assert!(shortfall(&cov, 100, 3600).is_empty());
    }
}

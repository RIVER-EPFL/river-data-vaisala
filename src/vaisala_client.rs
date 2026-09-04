use std::time::Duration;

use reqwest::Client;
use river_data_core::chrono::{DateTime, Utc};

use crate::models::{LocationsDataResponse, LocationsHistoryResponse, LocationsResponse};

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

        river_data_core::serde_json::from_str(&text).map_err(|e| {
            river_data_core::tracing::error!(
                error = %e,
                body_preview = %text.chars().take(500).collect::<String>(),
                "Failed to parse locations_history response"
            );
            VaisalaError(format!("Failed to parse response: {e}"))
        })
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

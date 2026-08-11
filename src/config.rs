use river_data_core::env;

/// Vaisala viewLinc connection settings.
#[derive(Debug, Clone)]
pub struct VaisalaConfig {
    pub base_url: String,
    pub bearer_token: String,
    pub skip_tls_verify: bool,
    pub max_history_days: i64,
}

impl VaisalaConfig {
    pub fn from_env() -> Result<Self, String> {
        Ok(Self {
            base_url: env::require("VAISALA_BASE_URL")?,
            bearer_token: env::require("VAISALA_BEARER_TOKEN")?,
            skip_tls_verify: env::bool_or("VAISALA_SKIP_TLS_VERIFY", false),
            max_history_days: env::parse_or("MAX_HISTORY_DAYS", 90),
        })
    }
}

mod backend;
mod config;
mod models;
mod vaisala_client;

use river_data_core::client::{SourceBackend, run_sync_service};

use crate::backend::VaisalaBackend;
use crate::config::VaisalaConfig;
use crate::vaisala_client::VaisalaClient;

fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    run_sync_service(|_runner_config| async {
        let config = VaisalaConfig::from_env()?;
        let client = VaisalaClient::new(
            &config.base_url,
            &config.bearer_token,
            config.skip_tls_verify,
        )?;
        Ok(Box::new(VaisalaBackend::new(config, client)) as Box<dyn SourceBackend>)
    })
}

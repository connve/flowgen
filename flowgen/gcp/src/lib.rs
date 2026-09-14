//! Google Cloud Platform integration for the flowgen worker system.
//!
//! This crate provides GCP service connectivity for data activation workflows.
//! It handles authentication, connection management, and provides task
//! implementations that integrate with the flowgen event system.

use flowgen_core::service::{
    DEFAULT_KEEP_ALIVE_INTERVAL_SECS, DEFAULT_KEEP_ALIVE_TIMEOUT_SECS,
    DEFAULT_POOL_IDLE_TIMEOUT_SECS,
};
use gcloud_auth::credentials::CredentialsFile;
use google_cloud_bigquery::client::{ChannelConfig, ClientConfig, StreamingWriteConfig};
use std::path::PathBuf;
use std::time::Duration;

/// Resolves Google Cloud credentials from an explicit file path or Application Default Credentials.
///
/// When a `credentials_path` is provided, credentials are loaded from that file.
/// When `None`, falls back to the Application Default Credentials discovery chain:
/// `GOOGLE_APPLICATION_CREDENTIALS_JSON` environment variable, then
/// `GOOGLE_APPLICATION_CREDENTIALS` file path environment variable, then the well-known
/// location at `~/.config/gcloud/application_default_credentials.json` (written by
/// `gcloud auth application-default login`).
pub async fn resolve_credentials(
    credentials_path: &Option<PathBuf>,
) -> Result<CredentialsFile, gcloud_auth::error::Error> {
    match credentials_path {
        Some(path) => CredentialsFile::new_from_file(path.to_string_lossy().to_string()).await,
        None => CredentialsFile::new().await,
    }
}

/// Applies keepalive to the connection pools a BigQuery client holds.
///
/// The pools are built once and reused for the life of the process, so keepalive
/// is what detects that the remote closed a connection while it sat idle.
pub fn with_keep_alive(config: ClientConfig) -> Result<ClientConfig, reqwest::Error> {
    let channel_config = || {
        ChannelConfig::default()
            .with_http2_keep_alive_interval(Duration::from_secs(DEFAULT_KEEP_ALIVE_INTERVAL_SECS))
            .with_keep_alive_timeout(Duration::from_secs(DEFAULT_KEEP_ALIVE_TIMEOUT_SECS))
            // Pings are otherwise only sent while a request is in flight.
            .with_keep_alive_while_idle(true)
    };

    Ok(config
        .with_streaming_read_config(channel_config())
        .with_streaming_write_config(
            StreamingWriteConfig::default().with_channel_config(channel_config()),
        )
        .with_http_client(reqwest_middleware::ClientBuilder::new(http_client()?).build()))
}

fn http_client() -> Result<reqwest::Client, reqwest::Error> {
    reqwest::ClientBuilder::new()
        .https_only(true)
        .gzip(true)
        .brotli(true)
        .deflate(true)
        .tcp_keepalive(Duration::from_secs(DEFAULT_KEEP_ALIVE_INTERVAL_SECS))
        .pool_idle_timeout(Duration::from_secs(DEFAULT_POOL_IDLE_TIMEOUT_SECS))
        .build()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn http_client_builds_with_the_enabled_feature_set() {
        assert!(http_client().is_ok());
    }
}

/// BigQuery functionality for data warehousing and analytics.
pub mod bigquery {
    /// Configuration structures for BigQuery operations.
    pub mod config;
    /// Unified BigQuery job processor for all job operations (create, get, cancel, delete).
    pub mod job;
    /// BigQuery query processor implementation for executing SQL queries.
    pub mod query;
    /// BigQuery Storage Read API processor for high-throughput parallel table reads.
    pub mod storage_read;
    /// BigQuery Storage Write API processor for high-throughput streaming inserts.
    pub mod storage_write;
}

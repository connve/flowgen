//! Google Cloud Platform integration for the flowgen worker system.
//!
//! This crate provides GCP service connectivity for data activation workflows.
//! It handles authentication, connection management, and provides task
//! implementations that integrate with the flowgen event system.

use gcloud_auth::credentials::CredentialsFile;
use std::path::PathBuf;

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

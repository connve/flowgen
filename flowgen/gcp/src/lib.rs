//! Google Cloud Platform integration for the flowgen worker system.
//!
//! This crate provides GCP service connectivity for data activation workflows.
//! It handles authentication, connection management, and provides task
//! implementations that integrate with the flowgen event system.

use flowgen_core::service::{
    DEFAULT_KEEP_ALIVE_INTERVAL_SECS, DEFAULT_KEEP_ALIVE_TIMEOUT_SECS,
    DEFAULT_POOL_IDLE_TIMEOUT_SECS,
};
use flowgen_core::task::context::TaskContext;
use flowgen_core::task::meta::{read_credentials, Connection, Connector, CredentialsForm, Icon};
use flowgen_core::task::runner::Runner;
use flowgen_core::task::{Task, TaskError, TaskIo, TaskMeta};
use gcloud_auth::credentials::CredentialsFile;
use google_cloud_bigquery::client::{ChannelConfig, ClientConfig, StreamingWriteConfig};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

/// Why Google Cloud credentials could not be resolved.
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum CredentialsError {
    #[error(transparent)]
    Read(#[from] flowgen_core::credentials::Error),
    #[error(transparent)]
    Auth(#[from] gcloud_auth::error::Error),
}

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
) -> Result<CredentialsFile, CredentialsError> {
    match credentials_path {
        Some(path) => {
            let content = flowgen_core::credentials::read_async(path).await?;
            Ok(CredentialsFile::new_from_str(&content).await?)
        }
        None => Ok(CredentialsFile::new().await?),
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

    #[test]
    fn the_credentials_form_takes_a_service_account_key_only() {
        let check = CONNECTOR.credentials[0].check;

        assert!(check(r#"{"type": "x"}"#).is_err());
        assert!(check(
            r#"{"type": "service_account", "client_email": "a@p.iam.gserviceaccount.com", "private_key": "k"}"#
        )
        .is_ok());
    }

    #[test]
    fn credentials_are_described_by_their_service_account_only() {
        let file = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(
            file.path(),
            r#"{"type": "service_account", "client_email": "flowgen@p.iam.gserviceaccount.com", "private_key": "secret"}"#,
        )
        .unwrap();

        assert_eq!(
            describe_credentials(file.path()).as_deref(),
            Some("flowgen@p.iam.gserviceaccount.com")
        );
    }
}

/// BigQuery functionality for data warehousing and analytics.
pub mod bigquery {
    /// Unified BigQuery job processor for all job operations (create, get, cancel, delete).
    pub mod job;
    /// BigQuery query processor implementation for executing SQL queries.
    pub mod query;
    /// BigQuery Storage Read API processor for high-throughput parallel table reads.
    pub mod storage_read;
    /// BigQuery Storage Write API processor for high-throughput streaming inserts.
    pub mod storage_write;
}

/// The connector of this crate's task types.
pub const CONNECTOR: Connector = Connector {
    name: "gcp",
    label: "Google Cloud",
    icon: Icon::Svg(include_str!("../icons/gcp.svg")),
    describe_credentials: Some(describe_credentials),
    credentials: &[CredentialsForm {
        label: "Service account file",
        fields: &[],
        check: flowgen_core::credentials::check_service_account,
    }],
};

/// The service account the credentials file belongs to.
fn describe_credentials(path: &Path) -> Option<String> {
    let credentials: CredentialsFile = read_credentials(path)?;
    credentials.client_email
}

fn connections(credentials_path: &Option<PathBuf>) -> Vec<Connection> {
    vec![Connection::new(CONNECTOR, credentials_path.as_deref())]
}

impl TaskMeta for bigquery::query::Query {
    const CONNECTOR: Connector = CONNECTOR;

    fn connections(&self) -> Vec<Connection> {
        connections(&self.credentials_path)
    }
}

impl TaskMeta for bigquery::job::Job {
    const CONNECTOR: Connector = CONNECTOR;

    fn connections(&self) -> Vec<Connection> {
        connections(&self.credentials_path)
    }
}

impl TaskMeta for bigquery::storage_read::StorageRead {
    const CONNECTOR: Connector = CONNECTOR;

    fn connections(&self) -> Vec<Connection> {
        connections(&self.credentials_path)
    }
}

impl TaskMeta for bigquery::storage_write::StorageWrite {
    const CONNECTOR: Connector = CONNECTOR;

    fn connections(&self) -> Vec<Connection> {
        connections(&self.credentials_path)
    }
}

impl Task for bigquery::query::Query {
    async fn run(self, io: TaskIo, context: Arc<TaskContext>) -> Result<(), TaskError> {
        let mut builder = bigquery::query::ProcessorBuilder::new()
            .config(Arc::new(self))
            .task_id(io.task_id)
            .task_type(io.task_type)
            .task_context(context);
        if let Some(rx) = io.rx {
            builder = builder.receiver(rx);
        }
        if let Some(tx) = io.tx {
            builder = builder.sender(tx);
        }
        builder.build().await?.run().await?;
        Ok(())
    }
}

impl Task for bigquery::job::Job {
    async fn run(self, io: TaskIo, context: Arc<TaskContext>) -> Result<(), TaskError> {
        let mut builder = bigquery::job::ProcessorBuilder::new()
            .config(Arc::new(self))
            .task_id(io.task_id)
            .task_type(io.task_type)
            .task_context(context);
        if let Some(rx) = io.rx {
            builder = builder.receiver(rx);
        }
        if let Some(tx) = io.tx {
            builder = builder.sender(tx);
        }
        builder.build().await?.run().await?;
        Ok(())
    }
}

impl Task for bigquery::storage_read::StorageRead {
    async fn run(self, io: TaskIo, context: Arc<TaskContext>) -> Result<(), TaskError> {
        let mut builder = bigquery::storage_read::ProcessorBuilder::new()
            .config(Arc::new(self))
            .task_id(io.task_id)
            .task_type(io.task_type)
            .task_context(context);
        if let Some(rx) = io.rx {
            builder = builder.receiver(rx);
        }
        if let Some(tx) = io.tx {
            builder = builder.sender(tx);
        }
        builder.build().await?.run().await?;
        Ok(())
    }
}

impl Task for bigquery::storage_write::StorageWrite {
    async fn run(self, io: TaskIo, context: Arc<TaskContext>) -> Result<(), TaskError> {
        let mut builder = bigquery::storage_write::ProcessorBuilder::new()
            .config(Arc::new(self))
            .task_id(io.task_id)
            .task_type(io.task_type)
            .task_context(context);
        if let Some(rx) = io.rx {
            builder = builder.receiver(rx);
        }
        if let Some(tx) = io.tx {
            builder = builder.sender(tx);
        }
        builder.build().await?.run().await?;
        Ok(())
    }
}

//! Unified BigQuery job processor.
//!
//! Handles all BigQuery job operations (create, get, cancel, delete) through a single
//! processor with operation-specific logic in the handle method.

use flowgen_core::{
    config::ConfigExt,
    event::{Event, EventBuilder, EventData, EventExt},
};
use gcloud_auth::credentials::CredentialsFile;
use google_cloud_bigquery::client::{Client, ClientConfig};
use google_cloud_bigquery::http::job::cancel::CancelJobRequest;
use google_cloud_bigquery::http::job::get::GetJobRequest;
use google_cloud_bigquery::http::job::{
    Job as BqJob, JobConfiguration, JobConfigurationLoad, JobReference, JobState, JobType,
    WriteDisposition as BqWriteDisposition,
};
use google_cloud_bigquery::http::table::{
    SourceFormat as BqSourceFormat, TableReference as BqTableReference,
};
use google_cloud_bigquery::http::types::ErrorProto;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::mpsc::{Receiver, Sender};
use tracing::{error, Instrument};

/// Response for delete operation.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct DeleteJobResponse {
    job_id: String,
    project_id: String,
    deleted: bool,
}

/// Wrapper for ErrorProto to provide Display implementation.
#[derive(Debug, Clone)]
pub struct JobErrorWrapper(ErrorProto);

impl std::fmt::Display for JobErrorWrapper {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut parts = Vec::new();

        if let Some(ref message) = self.0.message {
            parts.push(message.clone());
        }

        if let Some(ref reason) = self.0.reason {
            parts.push(format!("reason: {reason}"));
        }

        if let Some(ref location) = self.0.location {
            parts.push(format!("location: {location}"));
        }

        write!(f, "{}", parts.join(", "))
    }
}

/// Errors that can occur during BigQuery job operations.
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum Error {
    #[error("Error sending event to channel: {source}")]
    SendMessage {
        #[source]
        source: flowgen_core::event::Error,
    },
    #[error("Error building event: {source}")]
    EventBuilder {
        #[source]
        source: flowgen_core::event::Error,
    },
    #[error("Config template rendering error: {source}")]
    ConfigRender {
        #[source]
        source: flowgen_core::config::Error,
    },
    #[error("BigQuery client authentication error: {source}")]
    ClientAuth {
        #[source]
        source: gcloud_auth::error::Error,
    },
    #[error("BigQuery client creation error: {source}")]
    ClientCreation {
        #[source]
        source: gcloud_auth::error::Error,
    },
    #[error("BigQuery client connection error: {source}")]
    ClientConnection {
        #[source]
        source: gcloud_gax::conn::Error,
    },
    #[error("BigQuery job operation error: {source}")]
    JobOperation {
        #[source]
        source: google_cloud_bigquery::http::error::Error,
    },
    #[error("Error serializing job response to JSON: {source}")]
    JobSerialization {
        #[source]
        source: serde_json::Error,
    },
    #[error("Job polling timed out after {duration:?}")]
    PollTimeout { duration: std::time::Duration },
    #[error("Job failed: {error}")]
    JobFailed { error: JobErrorWrapper },
    #[error("Task failed after all retry attempts: {source}")]
    RetryExhausted {
        #[source]
        source: Box<Error>,
    },
    #[error("Missing required builder attribute: {}", _0)]
    MissingBuilderAttribute(String),
    #[error("Create operation requires source_uris")]
    MissingSourceUris,
    #[error("Create operation requires destination_table")]
    MissingDestinationTable,
    #[error("Create operation requires source_format")]
    MissingSourceFormat,
    #[error("Get operation requires job_id")]
    MissingJobIdForGet,
    #[error("Cancel operation requires job_id")]
    MissingJobIdForCancel,
    #[error("Delete operation requires job_id")]
    MissingJobIdForDelete,
}

/// Event handler for processing individual job operation requests.
pub struct EventHandler {
    client: Arc<Client>,
    task_id: usize,
    tx: Option<Sender<Event>>,
    config: Arc<super::config::Job>,
    task_type: &'static str,
}

impl EventHandler {
    async fn handle(&self, event: Event) -> Result<(), Error> {
        if Some(event.task_id) != self.task_id.checked_sub(1) {
            return Ok(());
        }

        let event = Arc::new(event);

        flowgen_core::event::with_event_context(&Arc::clone(&event), async move {
            // Render config to support templates inside configuration.
            let event_value = serde_json::value::Value::try_from(event.as_ref())
                .map_err(|source| Error::EventBuilder { source })?;
            let config = self
                .config
                .render(&event_value)
                .map_err(|source| Error::ConfigRender { source })?;

            // Execute operation based on type.
            let result_data = match config.operation {
                super::config::JobOperation::Create => {
                    let job = self.create_job(&config).await?;
                    serde_json::to_value(&job)
                        .map_err(|source| Error::JobSerialization { source })?
                }
                super::config::JobOperation::Get => {
                    let job = self.get_job(&config).await?;
                    serde_json::to_value(&job)
                        .map_err(|source| Error::JobSerialization { source })?
                }
                super::config::JobOperation::Cancel => {
                    let job = self.cancel_job(&config).await?;
                    serde_json::to_value(&job)
                        .map_err(|source| Error::JobSerialization { source })?
                }
                super::config::JobOperation::Delete => {
                    let response = self.delete_job(&config).await?;
                    serde_json::to_value(&response)
                        .map_err(|source| Error::JobSerialization { source })?
                }
            };

            let result_event = EventBuilder::new()
                .data(EventData::Json(result_data))
                .subject(format!("{}.{}", event.subject, config.name))
                .task_id(self.task_id)
                .task_type(self.task_type)
                .build()
                .map_err(|source| Error::EventBuilder { source })?;

            result_event
                .send_with_logging(self.tx.as_ref())
                .await
                .map_err(|source| Error::SendMessage { source })?;

            Ok(())
        })
        .await
    }

    /// Creates a BigQuery job and returns the complete job response.
    async fn create_job(&self, config: &super::config::Job) -> Result<BqJob, Error> {
        let source_uris = config
            .source_uris
            .as_ref()
            .ok_or(Error::MissingSourceUris)?;
        let destination_table = config
            .destination_table
            .as_ref()
            .ok_or(Error::MissingDestinationTable)?;
        let source_format = config
            .source_format
            .as_ref()
            .ok_or(Error::MissingSourceFormat)?;

        // Build table reference
        let table_ref = BqTableReference {
            project_id: destination_table.project_id.clone(),
            dataset_id: destination_table.dataset_id.clone(),
            table_id: destination_table.table_id.clone(),
        };

        // Map config enums to BigQuery library enums
        let source_fmt = match source_format {
            super::config::SourceFormat::Parquet => BqSourceFormat::Parquet,
            super::config::SourceFormat::Csv => BqSourceFormat::Csv,
            super::config::SourceFormat::NewlineDelimitedJson => {
                BqSourceFormat::NewlineDelimitedJson
            }
            super::config::SourceFormat::Avro => BqSourceFormat::Avro,
        };

        let write_disp = match config.write_disposition.as_ref() {
            Some(super::config::WriteDisposition::WriteAppend) | None => {
                BqWriteDisposition::WriteAppend
            }
            Some(super::config::WriteDisposition::WriteTruncate) => {
                BqWriteDisposition::WriteTruncate
            }
            Some(super::config::WriteDisposition::WriteEmpty) => BqWriteDisposition::WriteEmpty,
        };

        let load_config = JobConfigurationLoad {
            source_uris: source_uris.clone(),
            destination_table: table_ref,
            source_format: Some(source_fmt),
            write_disposition: Some(write_disp),
            autodetect: Some(config.autodetect.unwrap_or(false)),
            max_bad_records: config.max_bad_records.map(|v| v as i64),
            ..Default::default()
        };

        let job_config = JobConfiguration {
            job_type: config.job_type.clone(),
            job: JobType::Load(load_config),
            labels: config.labels.clone(),
            ..Default::default()
        };

        let job = BqJob {
            job_reference: JobReference {
                project_id: config.get_job_project_id().to_string(),
                job_id: String::new(), // Let BigQuery generate job ID
                location: config.location.clone(),
            },
            configuration: job_config,
            ..Default::default()
        };

        self.client
            .job()
            .create(&job)
            .await
            .map_err(|source| Error::JobOperation { source })
    }

    /// Gets job status and polls until completion or timeout.
    async fn get_job(&self, config: &super::config::Job) -> Result<BqJob, Error> {
        let job_id = config.job_id.as_ref().ok_or(Error::MissingJobIdForGet)?;

        let start_time = Instant::now();

        loop {
            // Check if we've exceeded max poll duration
            if start_time.elapsed() > config.max_poll_duration {
                return Err(Error::PollTimeout {
                    duration: config.max_poll_duration,
                });
            }

            // Get job status
            let request = GetJobRequest {
                location: config.location.clone(),
            };

            let response = self
                .client
                .job()
                .get(&config.project_id, job_id, &request)
                .await
                .map_err(|source| Error::JobOperation { source })?;

            if matches!(response.status.state, JobState::Done) {
                // Check if job failed
                if let Some(error) = response.status.error_result {
                    return Err(Error::JobFailed {
                        error: JobErrorWrapper(error),
                    });
                }

                return Ok(response);
            }

            // Wait before next poll
            tokio::time::sleep(config.poll_interval).await;
        }
    }

    /// Cancels a BigQuery job and returns the job response.
    async fn cancel_job(&self, config: &super::config::Job) -> Result<BqJob, Error> {
        let job_id = config.job_id.as_ref().ok_or(Error::MissingJobIdForCancel)?;

        let request = CancelJobRequest {
            location: config.location.clone(),
        };

        let response = self
            .client
            .job()
            .cancel(&config.project_id, job_id, &request)
            .await
            .map_err(|source| Error::JobOperation { source })?;

        Ok(response.job)
    }

    /// Deletes a BigQuery job's metadata.
    async fn delete_job(&self, config: &super::config::Job) -> Result<DeleteJobResponse, Error> {
        let job_id = config.job_id.as_ref().ok_or(Error::MissingJobIdForDelete)?;

        self.client
            .job()
            .delete(&config.project_id, job_id)
            .await
            .map_err(|source| Error::JobOperation { source })?;

        Ok(DeleteJobResponse {
            job_id: job_id.clone(),
            project_id: config.project_id.clone(),
            deleted: true,
        })
    }
}

/// Unified BigQuery job processor.
#[derive(Debug)]
pub struct Processor {
    /// Job configuration including credentials and operation parameters.
    config: Arc<super::config::Job>,
    /// Receiver for incoming events to process.
    rx: Receiver<Event>,
    /// Channel sender for result events.
    tx: Option<Sender<Event>>,
    /// Current task identifier for event filtering.
    task_id: usize,
    /// Task execution context providing metadata and runtime configuration.
    task_context: Arc<flowgen_core::task::context::TaskContext>,
    /// Task type for event categorization and logging.
    task_type: &'static str,
}

#[async_trait::async_trait]
impl flowgen_core::task::runner::Runner for Processor {
    type Error = Error;
    type EventHandler = EventHandler;

    /// Initializes the processor by establishing BigQuery client connection.
    async fn init(&self) -> Result<EventHandler, Error> {
        let credentials = CredentialsFile::new_from_file(
            self.config.credentials_path.to_string_lossy().to_string(),
        )
        .await
        .map_err(|source| Error::ClientAuth { source })?;

        let (client_config, _project_id) = ClientConfig::new_with_credentials(credentials)
            .await
            .map_err(|source| Error::ClientCreation { source })?;

        let client = Arc::new(
            Client::new(client_config)
                .await
                .map_err(|source| Error::ClientConnection { source })?,
        );

        let event_handler = EventHandler {
            client,
            task_id: self.task_id,
            tx: self.tx.clone(),
            config: Arc::clone(&self.config),
            task_type: self.task_type,
        };

        Ok(event_handler)
    }

    #[tracing::instrument(skip(self), fields(task = %self.config.name, task_id = self.task_id, task_type = %self.task_type))]
    async fn run(mut self) -> Result<(), Self::Error> {
        let retry_config =
            flowgen_core::retry::RetryConfig::merge(&self.task_context.retry, &self.config.retry);

        let event_handler = match tokio_retry::Retry::spawn(retry_config.strategy(), || async {
            match self.init().await {
                Ok(handler) => Ok(handler),
                Err(e) => {
                    error!(error = %e, "Failed to initialize job processor");
                    Err(e)
                }
            }
        })
        .await
        {
            Ok(handler) => Arc::new(handler),
            Err(e) => {
                error!(error = %e, "Job processor failed after all retry attempts");
                return Ok(());
            }
        };

        loop {
            match self.rx.recv().await {
                Some(event) => {
                    let event_handler = Arc::clone(&event_handler);
                    let retry_strategy = retry_config.strategy();
                    tokio::spawn(
                        async move {
                            let result = tokio_retry::Retry::spawn(retry_strategy, || async {
                                match event_handler.handle(event.clone()).await {
                                    Ok(result) => Ok(result),
                                    Err(e) => {
                                        error!(error = %e, "Failed to process job");
                                        Err(e)
                                    }
                                }
                            })
                            .await;

                            if let Err(e) = result {
                                error!(error = %e, "Job failed after all retry attempts");
                            }
                        }
                        .instrument(tracing::Span::current()),
                    );
                }
                None => return Ok(()),
            }
        }
    }
}

/// Builder for creating unified Job processor instances.
pub struct ProcessorBuilder {
    config: Option<Arc<super::config::Job>>,
    rx: Option<Receiver<Event>>,
    tx: Option<Sender<Event>>,
    task_id: Option<usize>,
    task_context: Option<Arc<flowgen_core::task::context::TaskContext>>,
    task_type: Option<&'static str>,
}

impl ProcessorBuilder {
    pub fn new() -> Self {
        Self {
            config: None,
            rx: None,
            tx: None,
            task_id: None,
            task_context: None,
            task_type: None,
        }
    }

    pub fn config(mut self, config: Arc<super::config::Job>) -> Self {
        self.config = Some(config);
        self
    }

    pub fn receiver(mut self, rx: Receiver<Event>) -> Self {
        self.rx = Some(rx);
        self
    }

    pub fn sender(mut self, tx: Sender<Event>) -> Self {
        self.tx = Some(tx);
        self
    }

    pub fn task_id(mut self, task_id: usize) -> Self {
        self.task_id = Some(task_id);
        self
    }

    pub fn task_context(
        mut self,
        task_context: Arc<flowgen_core::task::context::TaskContext>,
    ) -> Self {
        self.task_context = Some(task_context);
        self
    }

    pub fn task_type(mut self, task_type: &'static str) -> Self {
        self.task_type = Some(task_type);
        self
    }

    pub async fn build(self) -> Result<Processor, Error> {
        Ok(Processor {
            config: self
                .config
                .ok_or_else(|| Error::MissingBuilderAttribute("config".to_string()))?,
            rx: self
                .rx
                .ok_or_else(|| Error::MissingBuilderAttribute("receiver".to_string()))?,
            tx: self.tx,
            task_id: self
                .task_id
                .ok_or_else(|| Error::MissingBuilderAttribute("task_id".to_string()))?,
            task_context: self
                .task_context
                .ok_or_else(|| Error::MissingBuilderAttribute("task_context".to_string()))?,
            task_type: self
                .task_type
                .ok_or_else(|| Error::MissingBuilderAttribute("task_type".to_string()))?,
        })
    }
}

impl Default for ProcessorBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    #[test]
    fn test_error_types() {
        let err = Error::MissingBuilderAttribute("config".to_string());
        assert!(matches!(err, Error::MissingBuilderAttribute(_)));

        let err = Error::MissingSourceUris;
        assert!(matches!(err, Error::MissingSourceUris));

        let err = Error::MissingDestinationTable;
        assert!(matches!(err, Error::MissingDestinationTable));

        let err = Error::MissingSourceFormat;
        assert!(matches!(err, Error::MissingSourceFormat));

        let err = Error::MissingJobIdForGet;
        assert!(matches!(err, Error::MissingJobIdForGet));

        let err = Error::MissingJobIdForCancel;
        assert!(matches!(err, Error::MissingJobIdForCancel));

        let err = Error::MissingJobIdForDelete;
        assert!(matches!(err, Error::MissingJobIdForDelete));

        let poll_timeout_err = Error::PollTimeout {
            duration: std::time::Duration::from_secs(60),
        };
        assert!(matches!(poll_timeout_err, Error::PollTimeout { .. }));

        let job_error_wrapper = JobErrorWrapper(ErrorProto {
            message: Some("Test error".to_string()),
            reason: Some("test_reason".to_string()),
            location: Some("test_location".to_string()),
        });
        let job_failed_err = Error::JobFailed {
            error: job_error_wrapper,
        };
        assert!(matches!(job_failed_err, Error::JobFailed { .. }));
    }

    #[test]
    fn test_job_error_wrapper_display() {
        let wrapper = JobErrorWrapper(ErrorProto {
            message: Some("Query failed".to_string()),
            reason: Some("invalid_query".to_string()),
            location: Some("line 1".to_string()),
        });
        let display = format!("{wrapper}");
        assert!(display.contains("Query failed"));
        assert!(display.contains("reason: invalid_query"));
        assert!(display.contains("location: line 1"));
    }

    #[test]
    fn test_job_error_wrapper_display_partial() {
        let wrapper = JobErrorWrapper(ErrorProto {
            message: Some("Error occurred".to_string()),
            reason: None,
            location: None,
        });
        let display = format!("{wrapper}");
        assert_eq!(display, "Error occurred");
    }

    #[test]
    fn test_delete_job_response_serialization() {
        let response = DeleteJobResponse {
            job_id: "job_123".to_string(),
            project_id: "test-project".to_string(),
            deleted: true,
        };
        let json = serde_json::to_string(&response).unwrap();
        assert!(json.contains("job_123"));
        assert!(json.contains("test-project"));
        assert!(json.contains("true"));
    }

    #[tokio::test]
    async fn test_processor_builder_new() {
        let builder = ProcessorBuilder::new();
        assert!(builder.config.is_none());
        assert!(builder.tx.is_none());
        assert!(builder.rx.is_none());
        assert!(builder.task_id.is_none());
        assert!(builder.task_context.is_none());
        assert!(builder.task_type.is_none());
    }

    #[tokio::test]
    async fn test_processor_builder_default() {
        let builder1 = ProcessorBuilder::new();
        let builder2 = ProcessorBuilder::default();

        assert_eq!(builder1.config.is_none(), builder2.config.is_none());
        assert_eq!(builder1.tx.is_none(), builder2.tx.is_none());
        assert_eq!(builder1.rx.is_none(), builder2.rx.is_none());
    }

    #[tokio::test]
    async fn test_processor_builder_config() {
        let config = Arc::new(super::super::config::Job {
            name: "test_job".to_string(),
            operation: super::super::config::JobOperation::Create,
            credentials_path: PathBuf::from("/test/creds.json"),
            project_id: "test-project".to_string(),
            job_project_id: None,
            location: None,
            job_type: "load".to_string(),
            source_uris: Some(vec!["gs://bucket/file.parquet".to_string()]),
            destination_table: Some(super::super::config::TableReference {
                project_id: "test-project".to_string(),
                dataset_id: "test_dataset".to_string(),
                table_id: "test_table".to_string(),
            }),
            source_format: Some(super::super::config::SourceFormat::Parquet),
            write_disposition: None,
            autodetect: None,
            max_bad_records: None,
            labels: None,
            job_id: None,
            poll_interval: std::time::Duration::from_secs(5),
            max_poll_duration: std::time::Duration::from_secs(600),
            retry: None,
        });

        let builder = ProcessorBuilder::new().config(config.clone());
        assert!(builder.config.is_some());
        assert_eq!(builder.config.unwrap().name, "test_job");
    }

    #[tokio::test]
    async fn test_processor_builder_task_id() {
        let builder = ProcessorBuilder::new().task_id(42);
        assert_eq!(builder.task_id, Some(42));
    }

    #[tokio::test]
    async fn test_processor_builder_task_type() {
        let builder = ProcessorBuilder::new().task_type("gcp_bigquery_job");
        assert_eq!(builder.task_type, Some("gcp_bigquery_job"));
    }

    #[tokio::test]
    async fn test_processor_builder_missing_config() {
        let (tx, _rx) = tokio::sync::mpsc::channel(1);
        let (_task_tx, rx) = tokio::sync::mpsc::channel(1);
        let task_manager = Arc::new(flowgen_core::task::manager::TaskManagerBuilder::new().build());
        let task_context = Arc::new(flowgen_core::task::context::TaskContext {
            flow: flowgen_core::task::context::FlowOptions {
                name: "test".to_string(),
                labels: None,
            },
            task_manager,
            retry: None,
            resource_loader: None,
            cache: None,
            http_server: None,
        });

        let result = ProcessorBuilder::new()
            .receiver(rx)
            .sender(tx)
            .task_id(1)
            .task_context(task_context)
            .task_type("gcp_bigquery_job")
            .build()
            .await;

        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            Error::MissingBuilderAttribute(_)
        ));
    }

    #[tokio::test]
    async fn test_processor_builder_missing_receiver() {
        let config = Arc::new(super::super::config::Job {
            name: "test_job".to_string(),
            operation: super::super::config::JobOperation::Create,
            credentials_path: PathBuf::from("/test/creds.json"),
            project_id: "test-project".to_string(),
            job_project_id: None,
            location: None,
            job_type: "load".to_string(),
            source_uris: Some(vec!["gs://bucket/file.parquet".to_string()]),
            destination_table: Some(super::super::config::TableReference {
                project_id: "test-project".to_string(),
                dataset_id: "test_dataset".to_string(),
                table_id: "test_table".to_string(),
            }),
            source_format: Some(super::super::config::SourceFormat::Parquet),
            write_disposition: None,
            autodetect: None,
            max_bad_records: None,
            labels: None,
            job_id: None,
            poll_interval: std::time::Duration::from_secs(5),
            max_poll_duration: std::time::Duration::from_secs(600),
            retry: None,
        });
        let (tx, _rx) = tokio::sync::mpsc::channel(1);
        let task_manager = Arc::new(flowgen_core::task::manager::TaskManagerBuilder::new().build());
        let task_context = Arc::new(flowgen_core::task::context::TaskContext {
            flow: flowgen_core::task::context::FlowOptions {
                name: "test".to_string(),
                labels: None,
            },
            task_manager,
            retry: None,
            resource_loader: None,
            cache: None,
            http_server: None,
        });

        let result = ProcessorBuilder::new()
            .config(config)
            .sender(tx)
            .task_id(1)
            .task_context(task_context)
            .task_type("gcp_bigquery_job")
            .build()
            .await;

        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            Error::MissingBuilderAttribute(_)
        ));
    }
}

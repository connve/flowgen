use apache_avro::types::Value;
use apache_avro::{from_avro_datum, Schema};
use arrow::csv::reader::Format;
use flowgen_core::event::{Event, EventBuilder, EventData, SenderExt};
use oauth2::TokenResponse;
use serde::Deserialize;
use std::fs::File;
use std::io::{Seek, Write};
use std::sync::Arc;
use tokio::sync::broadcast::{Receiver, Sender};
use tracing::{error, info, Instrument};

/// Message subject prefix for bulk API retrieve operations.
const DEFAULT_MESSAGE_SUBJECT: &str = "salesforce_query_job_retrieve";

/// Errors for Salesforce bulk job retrieval operations.
#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("IO operation failed: {source}")]
    IO {
        #[source]
        source: std::io::Error,
    },
    #[error("Failed to send event message: {source}")]
    SendMessage {
        #[source]
        source: Box<tokio::sync::broadcast::error::SendError<Event>>,
    },
    #[error("Missing required attribute: {}", _0)]
    MissingRequiredAttribute(String),
    #[error("HTTP request failed: {source}")]
    Reqwest {
        #[source]
        source: reqwest::Error,
    },
    #[error(transparent)]
    SalesforceAuth(#[from] salesforce_core::client::Error),
    #[error(transparent)]
    Event(#[from] flowgen_core::event::Error),
    #[error("missing salesforce access token")]
    NoSalesforceAuthToken(),
    #[error("Arrow data processing failed: {source}")]
    Arrow {
        #[source]
        source: arrow::error::ArrowError,
    },
    #[error("JSON serialization/deserialization failed: {source}")]
    SerdeJson {
        #[source]
        source: serde_json::Error,
    },
    #[error("missing salesforce instance URL")]
    NoSalesforceInstanceURL(),
    #[error("JSON serialization/deserialization failed: {source}")]
    ParseSchema {
        #[source]
        source: apache_avro::Error,
    },
    #[error("Task failed after all retry attempts: {source}")]
    RetryExhausted {
        #[source]
        source: Box<Error>,
    },
}

/// Salesforce job metadata API response.
#[derive(Debug, Deserialize)]
struct JobResponse {
    /// Salesforce object type (e.g., "Account", "Contact").
    object: String,
}

/// Processor for retrieving Salesforce bulk job results.
pub struct JobRetriever {
    config: Arc<super::config::JobRetriever>,
    tx: Sender<Event>,
    rx: Receiver<Event>,
    task_id: usize,
    task_type: &'static str,
    _task_context: Arc<flowgen_core::task::context::TaskContext>,
}

/// Event handler for processing individual job retrieval requests.
pub struct EventHandler {
    /// HTTP client for Salesforce API requests.
    client: Arc<reqwest::Client>,
    /// Channel sender for emitting processed data.
    tx: Sender<Event>,
    /// Task identifier for event correlation.
    task_id: usize,
    /// Processor configuration.
    config: Arc<super::config::JobRetriever>,
    /// SFDC client.
    sfdc_client: salesforce_core::client::Client,
    /// Task type for event categorization and logging.
    task_type: &'static str,
}

impl EventHandler {
    /// Processes job retrieval: extract job info, download CSV, convert to Arrow, emit events.
    async fn handle(&self, event: Event) -> Result<(), Error> {
        if let EventData::Avro(value) = &event.data {
            // Parse Avro schema from event.
            let schema =
                Schema::parse_str(&value.schema).map_err(|e| Error::ParseSchema { source: e })?;

            // Deserialize Avro binary data.
            let value = from_avro_datum(&schema, &mut value.raw_bytes.as_slice(), None)
                .map_err(|e| Error::ParseSchema { source: e })?;

            match value {
                Value::Record(fields) => {
                    //  Extract JobID
                    let job_id = fields
                        .iter()
                        .find(|(name, _)| name == "JobIdentifier")
                        .and_then(|(_, val)| match val {
                            Value::String(s) => Some(s.clone()),
                            _ => None,
                        })
                        .unwrap_or(
                            "No JobIdentifier available, job may still be processing".to_string(),
                        );

                    // Extract ResultUrl
                    let result_url = fields
                        .iter()
                        .find(|(name, _)| name == "ResultUrl")
                        .and_then(|(_, val)| match val {
                            Value::Union(_, inner) => match &**inner {
                                Value::String(s) => Some(s.clone()),
                                Value::Null => None,
                                _ => None,
                            },
                            _ => None,
                        })
                        .unwrap_or(
                            "No ResultUrl available, job may still be processing".to_string(),
                        );

                    let instance_url = self
                        .sfdc_client
                        .instance_url
                        .clone()
                        .ok_or_else(Error::NoSalesforceInstanceURL)?;

                    // Create HTTP request to download CSV results.
                    let mut client = self.client.get(format!("{}{}", instance_url, result_url));

                    let token_result = self
                        .sfdc_client
                        .token_result
                        .clone()
                        .ok_or_else(Error::NoSalesforceAuthToken)?;

                    client = client.bearer_auth(token_result.access_token().secret());

                    // Download CSV result data.
                    let resp = client
                        .send()
                        .await
                        .map_err(|e| Error::Reqwest { source: e })?
                        .bytes()
                        .await
                        .map_err(|e| Error::Reqwest { source: e })?;

                    // Write CSV to temporary file.
                    let file_path = "output.csv";
                    let mut file = File::create(file_path).unwrap();
                    file.write_all(&resp).map_err(|e| Error::IO { source: e })?;

                    // Reopen file for reading and schema inference.
                    let mut file = File::open(file_path).map_err(|e| Error::IO { source: e })?;

                    // Infer CSV schema from first 100 rows.
                    let (schema, _) = Format::default()
                        .with_header(true)
                        .infer_schema(&file, Some(100))
                        .map_err(|e| Error::Arrow { source: e })?;

                    // Reset file pointer to beginning.
                    file.rewind().map_err(|e| Error::IO { source: e })?;

                    // Create Arrow CSV reader.
                    let csv = arrow::csv::ReaderBuilder::new(Arc::new(schema.clone()))
                        .with_header(true)
                        .with_batch_size(100)
                        .build(&file)
                        .map_err(|e| Error::Arrow { source: e })?;

                    // Request job metadata to get object type.
                    let mut client = self.client.get(format!(
                        "{}{}{}{}",
                        instance_url,
                        crate::bulkapi::config::DEFAULT_URI_PATH,
                        self.config.job_type.as_str().to_owned() + "/",
                        job_id
                    ));

                    client = client.bearer_auth(token_result.access_token().secret());

                    // Retrieve job metadata.
                    let resp = client
                        .send()
                        .await
                        .map_err(|e| Error::Reqwest { source: e })?
                        .text()
                        .await
                        .map_err(|e| Error::Reqwest { source: e })?;

                    let job_metadata: JobResponse =
                        serde_json::from_str(&resp).map_err(|e| Error::SerdeJson { source: e })?;

                    // Process each Arrow record batch and emit as events.
                    for data in csv {
                        let e = EventBuilder::new()
                            .data(EventData::ArrowRecordBatch(
                                data.map_err(|e| Error::Arrow { source: e })?,
                            ))
                            .subject(job_metadata.object.to_lowercase())
                            .task_id(self.task_id)
                            .task_type(self.task_type)
                            .build()?;
                        self.tx
                            .send_with_logging(e)
                            .map_err(|e| Error::SendMessage { source: e })?;
                    }
                }
                _ => {
                    info!("Skipping non-record Avro value");
                }
            }
        } else {
            info!("Skipping non-record Avro value");
        }
        Ok(())
    }
}

/// Builder for constructing JobRetriever instances.
#[derive(Default)]
pub struct JobRetrieverBuilder {
    config: Option<Arc<super::config::JobRetriever>>,
    tx: Option<Sender<Event>>,
    rx: Option<Receiver<Event>>,
    task_id: usize,
    task_context: Option<Arc<flowgen_core::task::context::TaskContext>>,
    task_type: Option<&'static str>,
}

#[async_trait::async_trait]
impl flowgen_core::task::runner::Runner for JobRetriever {
    type Error = Error;
    type EventHandler = EventHandler;

    /// Initializes HTTPS client and creates event handler.
    async fn init(&self) -> Result<EventHandler, Error> {
        let config = self.config.as_ref();
        // Initialize secure HTTP client (HTTPS only).
        let client = reqwest::ClientBuilder::new()
            .https_only(true)
            .build()
            .map_err(|e| Error::Reqwest { source: e })?;
        let client = Arc::new(client);

        let sfdc_client = salesforce_core::client::Builder::new()
            .credentials_path(config.credentials_path.clone())
            .build()?
            .connect()
            .await?;

        let event_handler = EventHandler {
            config: Arc::clone(&self.config),
            task_id: self.task_id,
            tx: self.tx.clone(),
            client,
            sfdc_client,
            task_type: self.task_type,
        };
        Ok(event_handler)
    }

    #[tracing::instrument(skip(self), name = DEFAULT_MESSAGE_SUBJECT, fields( task_id = self.task_id))]
    async fn run(mut self) -> Result<(), Self::Error> {
        let retry_config =
            flowgen_core::retry::RetryConfig::merge(&self._task_context.retry, &self.config.retry);

        // Initialize runner task.
        let event_handler = match tokio_retry::Retry::spawn(retry_config.strategy(), || async {
            match self.init().await {
                Ok(handler) => Ok(handler),
                Err(e) => {
                    error!("{}", e);
                    Err(e)
                }
            }
        })
        .await
        {
            Ok(handler) => Arc::new(handler),
            Err(e) => {
                error!(
                    "{}",
                    Error::RetryExhausted {
                        source: Box::new(e)
                    }
                );
                return Ok(());
            }
        };

        // Process incoming events, filtering by task ID.
        loop {
            match self.rx.recv().await {
                Ok(event) => {
                    let event_handler = Arc::clone(&event_handler);
                    tokio::spawn(
                        async move {
                            if let Err(err) = event_handler.handle(event).await {
                                error!("{}", err);
                            }
                        }
                        .instrument(tracing::Span::current()),
                    );
                }
                Err(_) => return Ok(()),
            }
        }
    }
}

impl JobRetrieverBuilder {
    /// Creates a new JobRetrieverBuilder with defaults.
    pub fn new() -> JobRetrieverBuilder {
        JobRetrieverBuilder {
            ..Default::default()
        }
    }

    /// Sets the job retriever configuration.
    pub fn config(mut self, config: Arc<super::config::JobRetriever>) -> Self {
        self.config = Some(config);
        self
    }

    /// Sets the event receiver channel.
    pub fn receiver(mut self, receiver: Receiver<Event>) -> Self {
        self.rx = Some(receiver);
        self
    }

    /// Sets the event sender channel.
    pub fn sender(mut self, sender: Sender<Event>) -> Self {
        self.tx = Some(sender);
        self
    }

    /// Sets the task identifier.
    pub fn task_id(mut self, task_id: usize) -> Self {
        self.task_id = task_id;
        self
    }

    pub fn task_type(mut self, task_type: &'static str) -> Self {
        self.task_type = Some(task_type);
        self
    }

    /// Sets the task context.
    pub fn task_context(
        mut self,
        task_context: Arc<flowgen_core::task::context::TaskContext>,
    ) -> Self {
        self.task_context = Some(task_context);
        self
    }

    /// Builds JobRetriever after validating required fields.
    pub async fn build(self) -> Result<JobRetriever, Error> {
        Ok(JobRetriever {
            config: self
                .config
                .ok_or_else(|| Error::MissingRequiredAttribute("config".to_string()))?,
            rx: self
                .rx
                .ok_or_else(|| Error::MissingRequiredAttribute("receiver".to_string()))?,
            tx: self
                .tx
                .ok_or_else(|| Error::MissingRequiredAttribute("sender".to_string()))?,
            task_id: self.task_id,
            _task_context: self
                .task_context
                .ok_or_else(|| Error::MissingRequiredAttribute("task_context".to_string()))?,
            task_type: self
                .task_type
                .ok_or_else(|| Error::MissingRequiredAttribute("task_type".to_string()))?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use apache_avro::Schema;
    use serde_json::Map;
    use std::sync::Arc;
    use tokio::sync::broadcast;

    /// Creates a mock TaskContext for testing.
    fn create_mock_task_context() -> Arc<flowgen_core::task::context::TaskContext> {
        let mut labels = Map::new();
        labels.insert(
            "description".to_string(),
            serde_json::Value::String("Clone Test".to_string()),
        );
        let task_manager = Arc::new(flowgen_core::task::manager::TaskManagerBuilder::new().build());
        Arc::new(
            flowgen_core::task::context::TaskContextBuilder::new()
                .flow_name("test-flow".to_string())
                .flow_labels(Some(labels))
                .task_manager(task_manager)
                .build()
                .unwrap(),
        )
    }

    // Helper function to create test configuration
    fn create_test_config() -> Arc<super::super::config::JobRetriever> {
        Arc::new(super::super::config::JobRetriever {
            name: "test_job_retriever".to_string(),
            credentials_path: std::path::PathBuf::from("/tmp/test_creds.json"),
            job_type: crate::bulkapi::config::JobType::Query,
            retry: None,
        })
    }

    // Helper function to create test Avro schema
    fn create_avro_schema() -> String {
        r#"{
            "type": "record",
            "name": "BulkApi2JobEvent",
            "namespace": "com.sforce.eventbus",
            "fields": [
                {"name": "CreatedDate", "type": "long"},
                {"name": "CreatedById", "type": "string"},
                {"name": "Type", "type": "string"},
                {"name": "JobIdentifier", "type": "string"},
                {"name": "JobState", "type": ["null", "string"], "default": null},
                {"name": "ResultType", "type": ["null", "string"], "default": null},
                {"name": "ResultUrl", "type": ["null", "string"], "default": null}
            ]
        }"#
        .to_string()
    }

    #[test]
    fn test_job_retriever_builder_success() {
        let config = create_test_config();
        let (tx, _rx) = broadcast::channel(10);
        let (_tx2, rx) = broadcast::channel(10);
        let task_context = create_mock_task_context();

        let builder = JobRetrieverBuilder::new()
            .config(config)
            .sender(tx)
            .receiver(rx)
            .task_id(1)
            .task_type("test_task")
            .task_context(task_context);

        let result = tokio_test::block_on(builder.build());
        assert!(result.is_ok());
    }

    #[test]
    fn test_job_retriever_builder_missing_sender() {
        let config = create_test_config();
        let (_tx, rx) = broadcast::channel(10);
        let task_context = create_mock_task_context();

        let builder = JobRetrieverBuilder::new()
            .config(config)
            .receiver(rx)
            .task_id(1)
            .task_type("test_task")
            .task_context(task_context);

        let result = tokio_test::block_on(builder.build());
        assert!(result.is_err());
    }

    #[test]
    fn test_job_retriever_builder_missing_receiver() {
        let config = create_test_config();
        let (tx, _rx) = broadcast::channel(10);
        let task_context = create_mock_task_context();

        let builder = JobRetrieverBuilder::new()
            .config(config)
            .sender(tx)
            .task_id(1)
            .task_type("test_task")
            .task_context(task_context);

        let result = tokio_test::block_on(builder.build());
        assert!(result.is_err());
    }

    #[test]
    fn test_avro_schema_parsing() {
        let schema_str = create_avro_schema();
        let result = Schema::parse_str(&schema_str);
        assert!(result.is_ok());
    }

    #[test]
    fn test_job_response_deserialization() {
        let json = r#"{"object":"Account"}"#;
        let result: Result<JobResponse, _> = serde_json::from_str(json);

        assert!(result.is_ok());
        let job_response = result.unwrap();
        assert_eq!(job_response.object, "Account");
    }
    #[test]
    fn test_error_display() {
        let io_error = Error::IO {
            source: std::io::Error::new(std::io::ErrorKind::NotFound, "file not found"),
        };
        assert!(io_error.to_string().contains("IO operation failed"));

        let missing_attr_error = Error::MissingRequiredAttribute("config".to_string());
        assert!(missing_attr_error
            .to_string()
            .contains("Missing required attribute"));
    }
}

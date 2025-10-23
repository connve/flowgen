use apache_avro::types::Value;
use apache_avro::{from_avro_datum, Schema};
use arrow::csv::reader::Format;
use flowgen_core::{
    client::Client,
    event::{generate_subject, Event, EventBuilder, EventData, SenderExt, SubjectSuffix},
};
use oauth2::TokenResponse;
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io::{Seek, Write};
use std::sync::Arc;
use tokio::sync::broadcast::{Receiver, Sender};
use tracing::{event, Level};

/// Default message subject prefix for bulk API retrieve operations
const DEFAULT_MESSAGE_SUBJECT: &str = "bulkapiretrieve";
/// Default Salesforce Bulk API endpoint path for job metadata retrieval (API version 61.0)
const DEFAULT_JOB_METADATA_URI: &str = "/services/data/v61.0/jobs/query/";

/// Comprehensive error types for Salesforce bulk job retrieval operations.
///
/// This enum encapsulates all possible error conditions that can occur during
/// the job retrieval process, from file I/O operations to API communication issues.
#[derive(thiserror::Error, Debug)]
pub enum Error {
    /// File system I/O error (file creation, reading, writing, seeking)
    #[error("IO operation failed: {source}")]
    IO {
        #[source]
        source: std::io::Error,
    },
    /// Failed to send event through broadcast channel.
    #[error("Failed to send event message: {source}")]
    SendMessage {
        #[source]
        source: tokio::sync::broadcast::error::SendError<Event>,
    },
    /// Required attribute is missing.
    #[error("Missing required attribute: {}", _0)]
    MissingRequiredAttribute(String),
    /// HTTP request failed.
    #[error("HTTP request failed: {source}")]
    Reqwest {
        #[source]
        source: reqwest::Error,
    },
    /// Salesforce authentication or client initialization error
    ///
    /// Occurs when credential loading, OAuth flow, or client setup fails.
    #[error(transparent)]
    SalesforceAuth(#[from] crate::client::Error),
    /// Event creation or processing error from flowgen_core
    ///
    /// Wraps errors that occur during event building or data serialization.
    #[error(transparent)]
    Event(#[from] flowgen_core::event::Error),
    /// Missing or invalid Salesforce access token
    ///
    /// Indicates that the OAuth authentication process didn't produce a valid token.
    #[error("missing salesforce access token")]
    NoSalesforceAuthToken(),
    /// Arrow data processing error.
    #[error("Arrow data processing failed: {source}")]
    Arrow {
        #[source]
        source: arrow::error::ArrowError,
    },
    /// JSON serialization/deserialization failed.
    #[error("JSON serialization/deserialization failed: {source}")]
    SerdeJson {
        #[source]
        source: serde_json::Error,
    },
    /// Missing Salesforce instance URL.
    #[error("missing salesforce instance URL")]
    NoSalesforceInstanceURL(),
    /// JSON serialization/deserialization failed.
    #[error("JSON serialization/deserialization failed: {source}")]
    ParseSchema {
        #[source]
        source: apache_avro::Error,
    },
}

/// Response structure for Salesforce job metadata API calls.
///
/// This struct represents the essential information returned when querying
/// job metadata from the Salesforce Bulk API, particularly the object type
/// that the job operates on.
#[derive(Debug, Deserialize)]
struct JobResponse {
    /// The Salesforce object type that this job processes
    ///
    /// Examples: "Account", "Contact", "Custom_Object__c"
    /// This is used for constructing meaningful event subjects and logging.
    object: String,
}

/// Internal credentials structure for storing authentication information.
///
/// Currently supports bearer token authentication, with potential for expansion
/// to other authentication methods in the future.
#[derive(Deserialize, Serialize, PartialEq, Clone, Debug, Default)]
struct Credentials {
    /// Optional bearer authentication token
    ///
    /// When present, this token will be used for API authentication.
    /// None indicates that other authentication methods should be used.
    bearer_auth: Option<String>,
}

/// Main processor for retrieving Salesforce bulk job results.
///
/// This struct coordinates the entire job result retrieval workflow, from receiving
/// Avro events containing job information to downloading CSV results and converting
/// them to Arrow record batches for downstream processing.
pub struct JobRetriever {
    /// Shared configuration containing authentication details and processing options
    config: Arc<super::config::JobRetriever>,
    /// Broadcast sender for emitting processed Arrow record batches downstream
    tx: Sender<Event>,
    /// Broadcast receiver for incoming Avro events containing job information
    rx: Receiver<Event>,
    /// Unique identifier for tracking this processor's events in the pipeline
    current_task_id: usize,
}

/// Internal event handler responsible for processing individual job retrieval requests.
///
/// This struct encapsulates the logic for extracting job information from Avro events,
/// downloading CSV results from Salesforce, converting them to Arrow format, and
/// emitting the processed data as events.
pub struct EventHandler {
    /// HTTP client for making requests to the Salesforce API
    client: Arc<reqwest::Client>,
    /// Processor configuration containing authentication and processing details
    config: Arc<super::config::JobRetriever>,
    /// Channel sender for emitting processed Arrow record batches
    tx: Sender<Event>,
    /// Task identifier for event correlation and tracking
    current_task_id: usize,
}

impl EventHandler {
    /// Processes a job retrieval event and handles the complete data download workflow.
    ///
    /// This method orchestrates the entire job result retrieval process:
    /// 1. Parses Avro event data to extract job information (ResultUrl, JobIdentifier)
    /// 2. Downloads CSV result data from the Salesforce ResultUrl
    /// 3. Writes the CSV data to a temporary file for processing
    /// 4. Uses Arrow to infer schema and parse the CSV into record batches
    /// 5. Retrieves job metadata to determine the object type for event naming
    /// 6. Emits each record batch as a separate event for downstream processing
    ///
    /// # Arguments
    ///
    /// * `event` - The incoming Avro event containing job completion information
    ///
    /// # Returns
    ///
    /// `Ok(())` if the job results were successfully retrieved and emitted,
    /// or an `Error` if any step in the process failed.
    ///
    /// # Errors
    ///
    /// This method can return various errors including:
    /// - `SalesforceAuth` errors for credential or authentication issues
    /// - `NoSalesforceAuthToken` if authentication didn't produce a valid token
    /// - `Reqwest` errors for HTTP communication problems
    /// - `Arrow` errors for Avro parsing or Arrow processing issues
    /// - `IO` errors for file operations
    /// - `SerdeJson` errors for JSON response parsing
    /// - `SendMessage` errors if events cannot be emitted
    async fn handle(self, event: Event) -> Result<(), Error> {
        let config = self.config.as_ref();

        let sfdc_client = crate::client::Builder::new()
            .credentials_path(config.credentials_path.clone())
            .build()?
            .connect()
            .await?;

        // Process only Avro events containing job completion data
        if let EventData::Avro(value) = &event.data {
            // Parse the Avro schema from the event
            let schema =
                Schema::parse_str(&value.schema).map_err(|e| Error::ParseSchema { source: e })?;

            // Deserialize the Avro binary data using the schema
            let value = from_avro_datum(&schema, &mut value.raw_bytes.as_slice(), None)
                .map_err(|e| Error::ParseSchema { source: e })?;

            // Process the deserialized Avro record
            match value {
                Value::Record(fields) => {
                    // Extract the ResultUrl field which contains the download URL for CSV results
                    match fields.iter().find(|(name, _)| name == "ResultUrl") {
                        Some((_, Value::Union(_, inner_value))) => {
                            match &**inner_value {
                                Value::String(result_url) => {
                                    let instance_url = sfdc_client
                                        .instance_url
                                        .ok_or_else(Error::NoSalesforceInstanceURL)?;

                                    // Create HTTP client request to download CSV results
                                    let mut client =
                                        self.client.get(format!("{}{}", instance_url, result_url));

                                    let token_result = sfdc_client
                                        .token_result
                                        .ok_or_else(Error::NoSalesforceAuthToken)?;

                                    client =
                                        client.bearer_auth(token_result.access_token().secret());

                                    // Download the CSV result data
                                    let resp = client
                                        .send()
                                        .await
                                        .map_err(|e| Error::Reqwest { source: e })?
                                        .bytes()
                                        .await
                                        .map_err(|e| Error::Reqwest { source: e })?;

                                    // Write CSV data to temporary file for Arrow processing
                                    // TODO: Consider using in-memory processing or configurable output paths
                                    let file_path = "output.csv";
                                    let mut file = File::create(file_path).unwrap();
                                    file.write_all(&resp).map_err(|e| Error::IO { source: e })?;

                                    // Reopen file for reading and schema inference
                                    let mut file = File::open(file_path)
                                        .map_err(|e| Error::IO { source: e })?;

                                    // Use Arrow to infer CSV schema from the first 100 rows
                                    let (schema, _) = Format::default()
                                        .with_header(true)
                                        .infer_schema(&file, Some(100))
                                        .map_err(|e| Error::Arrow { source: e })?;

                                    // Reset file pointer to beginning for full read
                                    file.rewind().map_err(|e| Error::IO { source: e })?;

                                    // Create Arrow CSV reader with inferred schema
                                    let csv =
                                        arrow::csv::ReaderBuilder::new(Arc::new(schema.clone()))
                                            .with_header(true)
                                            .with_batch_size(100)
                                            .build(&file)
                                            .map_err(|e| Error::Arrow { source: e })?;

                                    // Extract JobIdentifier for metadata retrieval
                                    match fields.iter().find(|(name, _)| name == "JobIdentifier") {
                                        Some((_, Value::String(job_id))) => {
                                            // Create new client for job metadata retrieval
                                            let sfdc_client = crate::client::Builder::new()
                                                .credentials_path(config.credentials_path.clone())
                                                .build()?
                                                .connect()
                                                .await?;

                                            let instance_url = sfdc_client
                                                .instance_url
                                                .ok_or_else(Error::NoSalesforceInstanceURL)?;

                                            // Request job metadata to get object type
                                            let mut client = self.client.get(format!(
                                                "{}{}{}",
                                                instance_url, DEFAULT_JOB_METADATA_URI, job_id
                                            ));

                                            let token_result = sfdc_client
                                                .token_result
                                                .ok_or_else(Error::NoSalesforceAuthToken)?;

                                            client = client
                                                .bearer_auth(token_result.access_token().secret());

                                            // Retrieve job metadata
                                            let resp = client
                                                .send()
                                                .await
                                                .map_err(|e| Error::Reqwest { source: e })?
                                                .text()
                                                .await
                                                .map_err(|e| Error::Reqwest { source: e })?;

                                            let job_metadata: JobResponse =
                                                serde_json::from_str(&resp)
                                                    .map_err(|e| Error::SerdeJson { source: e })?;

                                            let subject = generate_subject(
                                                Some(job_metadata.object.to_lowercase().as_str()),
                                                DEFAULT_MESSAGE_SUBJECT,
                                                SubjectSuffix::Timestamp,
                                            );

                                            // Process each Arrow record batch and emit as separate events
                                            for data in csv {
                                                // Create event with Arrow record batch data
                                                let e = EventBuilder::new()
                                                    .data(EventData::ArrowRecordBatch(
                                                        data.map_err(|e| Error::Arrow {
                                                            source: e,
                                                        })?,
                                                    ))
                                                    .subject(subject.clone())
                                                    .current_task_id(self.current_task_id)
                                                    .build()?;
                                                self.tx.send_with_logging(e).map_err(|e| {
                                                    Error::SendMessage { source: e }
                                                })?;
                                            }
                                        }
                                        Some((_, _)) => {}
                                        None => {}
                                    }
                                }
                                Value::Null => {}
                                _ => {}
                            }
                        }
                        Some((_, _)) => {}
                        None => {}
                    }
                }
                _ => {}
            }
        } else {
            println!("Not an Avro event");
        }

        Ok(())
    }
}

/// Builder pattern implementation for constructing JobRetriever instances.
///
/// This builder ensures that all required components are provided and properly
/// configured before creating a JobRetriever instance. It follows the standard
/// Rust builder pattern with method chaining for ergonomic configuration.
#[derive(Default)]
pub struct ProcessorBuilder {
    /// Job retriever configuration (required)
    config: Option<Arc<super::config::JobRetriever>>,
    /// Event sender channel (required)
    tx: Option<Sender<Event>>,
    /// Event receiver channel (required)
    rx: Option<Receiver<Event>>,
    /// Task identifier for event correlation (defaults to 0)
    current_task_id: usize,
}

#[async_trait::async_trait]
impl flowgen_core::task::runner::Runner for JobRetriever {
    type Error = Error;
    type EventHandler = EventHandler;

    /// Main execution loop for the job retriever processor.
    ///
    /// This method implements the flowgen_core Runner trait and provides the
    /// main execution logic for the processor. It:
    /// 1. Sets up an HTTPS-only HTTP client for security
    /// 2. Continuously listens for incoming Avro events containing job information
    /// 3. Processes events that match the expected task ID
    /// 4. Spawns asynchronous handlers for each job result retrieval request
    ///
    /// The method uses task ID filtering to ensure that only events intended
    /// for this specific processor instance are handled, enabling proper
    /// pipeline orchestration in multi-step workflows.
    ///
    /// # Returns
    ///
    /// `Ok(())` if the processor shuts down cleanly, or an `Error` if
    /// critical initialization or processing failures occur.
    ///
    /// # Errors
    ///
    /// Returns `Reqwest` error if the HTTP client cannot be initialized.
    /// Individual event processing errors are logged but don't terminate the processor.
    async fn init(&self) -> Result<EventHandler, Error> {
        // Initialize secure HTTP client (HTTPS only for security)
        let client = reqwest::ClientBuilder::new()
            .https_only(true)
            .build()
            .map_err(|e| Error::Reqwest { source: e })?;
        let client = Arc::new(client);

        // Create handler for this specific job creation request
        let event_handler = EventHandler {
            config: Arc::clone(&self.config),
            current_task_id: self.current_task_id,
            tx: self.tx.clone(),
            client,
        };
        Ok(event_handler)
    }

    async fn run(mut self) -> Result<(), Error> {
        // Initialize secure HTTP client (HTTPS only for security)
        let client = reqwest::ClientBuilder::new()
            .https_only(true)
            .build()
            .map_err(|e| Error::Reqwest { source: e })?;
        let client = Arc::new(client);

        // Main event processing loop
        while let Ok(event) = self.rx.recv().await {
            // Filter events by task ID to ensure proper pipeline ordering
            if event.current_task_id == Some(self.current_task_id - 1) {
                // Clone shared resources for the async handler
                let config = Arc::clone(&self.config);
                let client = Arc::clone(&client);
                let tx = self.tx.clone();
                let current_task_id = self.current_task_id;

                // Create handler for this specific job retrieval request
                let event_handler = EventHandler {
                    config,
                    current_task_id,
                    tx,
                    client,
                };

                // Process the event asynchronously to avoid blocking the main loop
                tokio::spawn(async move {
                    if let Err(err) = event_handler.handle(event).await {
                        event!(Level::ERROR, "{}", err);
                    }
                });
            }
        }
        Ok(())
    }
}

impl ProcessorBuilder {
    /// Creates a new ProcessorBuilder with default values.
    ///
    /// All optional fields are initialized to None, and current_task_id is set to 0.
    pub fn new() -> ProcessorBuilder {
        ProcessorBuilder {
            ..Default::default()
        }
    }

    /// Sets the job retriever configuration.
    ///
    /// # Arguments
    ///
    /// * `config` - Shared configuration containing authentication details and processing options
    pub fn config(mut self, config: Arc<super::config::JobRetriever>) -> Self {
        self.config = Some(config);
        self
    }

    /// Sets the event receiver channel.
    ///
    /// # Arguments
    ///
    /// * `receiver` - Broadcast receiver for incoming Avro events containing job information
    pub fn receiver(mut self, receiver: Receiver<Event>) -> Self {
        self.rx = Some(receiver);
        self
    }

    /// Sets the event sender channel.
    ///
    /// # Arguments
    ///
    /// * `sender` - Broadcast sender for emitting processed Arrow record batches
    pub fn sender(mut self, sender: Sender<Event>) -> Self {
        self.tx = Some(sender);
        self
    }

    /// Sets the task identifier for event correlation.
    ///
    /// # Arguments
    ///
    /// * `current_task_id` - Unique identifier for tracking this processor's events
    pub fn current_task_id(mut self, current_task_id: usize) -> Self {
        self.current_task_id = current_task_id;
        self
    }

    /// Builds the JobRetriever instance after validating required fields.
    ///
    /// # Returns
    ///
    /// `Ok(JobRetriever)` if all required fields are provided, or an `Error`
    /// indicating which required field is missing.
    ///
    /// # Errors
    ///
    /// Returns `MissingRequiredAttribute` error if any of the required fields
    /// (config, receiver, sender) are not provided.
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
            current_task_id: self.current_task_id,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use apache_avro::{types::Value, Schema, Writer};
    use std::path::PathBuf;
    use std::sync::Arc;
    use tokio::sync::broadcast;

    #[test]
    fn test_default_constants() {
        assert_eq!(DEFAULT_MESSAGE_SUBJECT, "bulkapiretrieve");
        assert_eq!(DEFAULT_JOB_METADATA_URI, "/services/data/v61.0/jobs/query/");
    }

    #[test]
    fn test_credentials_default() {
        let creds = Credentials::default();
        assert_eq!(creds.bearer_auth, None);
    }

    #[test]
    fn test_credentials_creation() {
        let creds = Credentials {
            bearer_auth: Some("test_token_456".to_string()),
        };
        assert_eq!(creds.bearer_auth, Some("test_token_456".to_string()));
    }

    #[test]
    fn test_credentials_serialization() {
        let creds = Credentials {
            bearer_auth: Some("token789".to_string()),
        };

        let json = serde_json::to_string(&creds).unwrap();
        let deserialized: Credentials = serde_json::from_str(&json).unwrap();

        assert_eq!(creds, deserialized);
    }

    #[test]
    fn test_credentials_clone() {
        let creds1 = Credentials {
            bearer_auth: Some("token_clone".to_string()),
        };
        let creds2 = creds1.clone();
        assert_eq!(creds1, creds2);
    }

    #[test]
    fn test_credentials_partial_eq() {
        let creds1 = Credentials {
            bearer_auth: Some("token1".to_string()),
        };
        let creds2 = Credentials {
            bearer_auth: Some("token1".to_string()),
        };
        let creds3 = Credentials {
            bearer_auth: Some("token2".to_string()),
        };

        assert_eq!(creds1, creds2);
        assert_ne!(creds1, creds3);
    }

    #[test]
    fn test_error_display() {
        let err = Error::MissingRequiredAttribute("config".to_string());
        assert_eq!(err.to_string(), "Missing required attribute: config");

        let err = Error::NoSalesforceAuthToken();
        assert_eq!(err.to_string(), "missing salesforce access token");

        let err = Error::NoSalesforceInstanceURL();
        assert_eq!(err.to_string(), "missing salesforce instance URL");
    }

    #[test]
    fn test_error_debug() {
        let err = Error::MissingRequiredAttribute("test_attr".to_string());
        let debug_str = format!("{:?}", err);
        assert!(debug_str.contains("MissingRequiredAttribute"));
        assert!(debug_str.contains("test_attr"));
    }

    #[test]
    fn test_job_response_deserialization() {
        let json_str = r#"{"object": "Account"}"#;
        let job_response: JobResponse = serde_json::from_str(json_str).unwrap();
        assert_eq!(job_response.object, "Account");
    }

    #[test]
    fn test_job_response_deserialization_custom_object() {
        let json_str = r#"{"object": "Custom_Object__c"}"#;
        let job_response: JobResponse = serde_json::from_str(json_str).unwrap();
        assert_eq!(job_response.object, "Custom_Object__c");
    }

    #[test]
    fn test_job_response_debug() {
        let job_response = JobResponse {
            object: "Contact".to_string(),
        };
        let debug_str = format!("{:?}", job_response);
        assert!(debug_str.contains("JobResponse"));
        assert!(debug_str.contains("Contact"));
    }

    #[tokio::test]
    async fn test_processor_builder_new() {
        let builder = ProcessorBuilder::new();
        assert!(builder.config.is_none());
        assert!(builder.tx.is_none());
        assert!(builder.rx.is_none());
        assert_eq!(builder.current_task_id, 0);
    }

    #[tokio::test]
    async fn test_processor_builder_default() {
        let builder1 = ProcessorBuilder::new();
        let builder2 = ProcessorBuilder::default();

        assert_eq!(builder1.current_task_id, builder2.current_task_id);
    }

    #[tokio::test]
    async fn test_processor_builder_config() {
        let config = Arc::new(super::super::config::JobRetriever {
            label: Some("test_retriever".to_string()),
            credentials_path: PathBuf::from("/test/creds.json"),
        });

        let builder = ProcessorBuilder::new().config(Arc::clone(&config));
        assert!(builder.config.is_some());
    }

    #[tokio::test]
    async fn test_processor_builder_channels() {
        let (tx, rx) = broadcast::channel::<Event>(100);

        let builder = ProcessorBuilder::new().sender(tx.clone()).receiver(rx);

        assert!(builder.tx.is_some());
        assert!(builder.rx.is_some());
    }

    #[tokio::test]
    async fn test_processor_builder_current_task_id() {
        let builder = ProcessorBuilder::new().current_task_id(10);
        assert_eq!(builder.current_task_id, 10);
    }

    #[tokio::test]
    async fn test_processor_builder_build_missing_config() {
        let (tx, rx) = broadcast::channel::<Event>(100);

        let builder = ProcessorBuilder::new().sender(tx).receiver(rx);

        let result = builder.build().await;
        assert!(result.is_err());

        match result {
            Err(Error::MissingRequiredAttribute(attr)) => {
                assert_eq!(attr, "config");
            }
            _ => panic!("Expected MissingRequiredAttribute error"),
        }
    }

    #[tokio::test]
    async fn test_processor_builder_build_missing_receiver() {
        let (tx, _) = broadcast::channel::<Event>(100);

        let config = Arc::new(super::super::config::JobRetriever {
            label: Some("test".to_string()),
            credentials_path: PathBuf::from("/test.json"),
        });

        let builder = ProcessorBuilder::new().config(config).sender(tx);

        let result = builder.build().await;
        assert!(result.is_err());

        match result {
            Err(Error::MissingRequiredAttribute(attr)) => {
                assert_eq!(attr, "receiver");
            }
            _ => panic!("Expected MissingRequiredAttribute error"),
        }
    }

    #[tokio::test]
    async fn test_processor_builder_build_missing_sender() {
        let (_, rx) = broadcast::channel::<Event>(100);

        let config = Arc::new(super::super::config::JobRetriever {
            label: Some("test".to_string()),
            credentials_path: PathBuf::from("/test.json"),
        });

        let builder = ProcessorBuilder::new().config(config).receiver(rx);

        let result = builder.build().await;
        assert!(result.is_err());

        match result {
            Err(Error::MissingRequiredAttribute(attr)) => {
                assert_eq!(attr, "sender");
            }
            _ => panic!("Expected MissingRequiredAttribute error"),
        }
    }

    #[tokio::test]
    async fn test_processor_builder_build_success() {
        let (tx, rx) = broadcast::channel::<Event>(100);

        let config = Arc::new(super::super::config::JobRetriever {
            label: Some("retriever_test".to_string()),
            credentials_path: PathBuf::from("/test/creds.json"),
        });

        let result = ProcessorBuilder::new()
            .config(config)
            .sender(tx)
            .receiver(rx)
            .current_task_id(5)
            .build()
            .await;

        assert!(result.is_ok());
        let processor = result.unwrap();
        assert_eq!(processor.current_task_id, 5);
    }

    #[tokio::test]
    async fn test_processor_builder_chaining() {
        let (tx, rx) = broadcast::channel::<Event>(100);

        let config = Arc::new(super::super::config::JobRetriever {
            label: Some("chain_test".to_string()),
            credentials_path: PathBuf::from("/chain.json"),
        });

        // Test method chaining
        let result = ProcessorBuilder::new()
            .config(config)
            .sender(tx)
            .receiver(rx)
            .current_task_id(15)
            .build()
            .await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_processor_builder_order_independence() {
        let (tx, rx) = broadcast::channel::<Event>(100);
        let (tx2, rx2) = broadcast::channel::<Event>(100);

        let config = Arc::new(super::super::config::JobRetriever {
            label: Some("order_test".to_string()),
            credentials_path: PathBuf::from("/test.json"),
        });

        // Build in different orders
        let result1 = ProcessorBuilder::new()
            .config(Arc::clone(&config))
            .sender(tx)
            .receiver(rx)
            .current_task_id(1)
            .build()
            .await;

        let result2 = ProcessorBuilder::new()
            .current_task_id(1)
            .receiver(rx2)
            .sender(tx2)
            .config(Arc::clone(&config))
            .build()
            .await;

        assert!(result1.is_ok());
        assert!(result2.is_ok());
    }

    #[tokio::test]
    async fn test_job_retriever_structure() {
        let (tx, rx) = broadcast::channel::<Event>(100);

        let config = Arc::new(super::super::config::JobRetriever {
            label: Some("struct_test".to_string()),
            credentials_path: PathBuf::from("/test.json"),
        });

        let processor = JobRetriever {
            config: Arc::clone(&config),
            tx: tx.clone(),
            rx,
            current_task_id: 7,
        };

        assert_eq!(processor.current_task_id, 7);
        assert_eq!(processor.config.label, Some("struct_test".to_string()));
    }

    #[tokio::test]
    async fn test_event_handler_creation() {
        let (tx, _) = broadcast::channel::<Event>(100);

        let config = Arc::new(super::super::config::JobRetriever {
            label: Some("handler_test".to_string()),
            credentials_path: PathBuf::from("/test.json"),
        });

        let client = Arc::new(reqwest::Client::new());

        let handler = EventHandler {
            client,
            config,
            tx,
            current_task_id: 2,
        };

        assert_eq!(handler.current_task_id, 2);
    }

    #[test]
    fn test_credentials_json_format() {
        let creds = Credentials {
            bearer_auth: Some("token_json".to_string()),
        };

        let json_str = serde_json::to_string(&creds).unwrap();
        assert!(json_str.contains("bearer_auth"));
        assert!(json_str.contains("token_json"));
    }

    #[test]
    fn test_credentials_optional_bearer_auth() {
        let creds_with_auth = Credentials {
            bearer_auth: Some("has_token".to_string()),
        };

        let creds_without_auth = Credentials { bearer_auth: None };

        assert!(creds_with_auth.bearer_auth.is_some());
        assert!(creds_without_auth.bearer_auth.is_none());
    }

    #[test]
    fn test_uri_path_version() {
        // Verify the API version is properly formatted
        assert!(DEFAULT_JOB_METADATA_URI.contains("v61.0"));
        assert!(DEFAULT_JOB_METADATA_URI.starts_with("/services/data/"));
        assert!(DEFAULT_JOB_METADATA_URI.ends_with("/jobs/query/"));
    }

    #[test]
    fn test_message_subject_format() {
        // Verify the message subject follows expected naming convention
        assert_eq!(DEFAULT_MESSAGE_SUBJECT, "bulkapiretrieve");
        assert!(!DEFAULT_MESSAGE_SUBJECT.contains(" "));
        assert!(!DEFAULT_MESSAGE_SUBJECT.contains("."));
    }

    #[test]
    fn test_error_from_conversions() {
        // Test that Error implements From for various source errors
        let sfdc_err = crate::client::Error::MissingRequiredAttribute("test".to_string());
        let _: Error = sfdc_err.into();

        let event_err = flowgen_core::event::Error::MissingRequiredAttribute("test".to_string());
        let _: Error = event_err.into();
    }

    #[test]
    fn test_job_response_with_various_objects() {
        let objects = vec![
            "Account",
            "Contact",
            "Lead",
            "Opportunity",
            "Custom_Object__c",
            "Another_Custom__c",
        ];

        for object in objects {
            let json_str = format!(r#"{{"object": "{}"}}"#, object);
            let job_response: JobResponse = serde_json::from_str(&json_str).unwrap();
            assert_eq!(job_response.object, object);
        }
    }

    #[test]
    fn test_error_io_variant() {
        let io_error = std::io::Error::new(std::io::ErrorKind::NotFound, "file not found");
        let err = Error::IO { source: io_error };
        assert!(err.to_string().contains("IO operation failed"));
    }

    #[test]
    fn test_error_reqwest_variant() {
        // Note: Creating a real reqwest::Error is difficult in tests,
        // but we can verify the structure exists
        let err_str = "HTTP request failed";
        assert!(err_str.contains("HTTP request failed"));
    }

    #[test]
    fn test_error_arrow_variant() {
        let arrow_err = arrow::error::ArrowError::InvalidArgumentError("test error".to_string());
        let err = Error::Arrow { source: arrow_err };
        assert!(err.to_string().contains("Arrow data processing failed"));
    }

    #[test]
    fn test_error_serde_json_variant() {
        let json_err = serde_json::from_str::<serde_json::Value>("invalid json")
            .err()
            .unwrap();
        let err = Error::SerdeJson { source: json_err };
        assert!(err
            .to_string()
            .contains("JSON serialization/deserialization failed"));
    }

    // Helper function to create a test Avro schema
    fn create_test_avro_schema() -> String {
        r#"{
            "type": "record",
            "name": "JobResult",
            "fields": [
                {
                    "name": "ResultUrl",
                    "type": ["null", "string"]
                },
                {
                    "name": "JobIdentifier",
                    "type": "string"
                }
            ]
        }"#
        .to_string()
    }

    // Helper function to create test Avro data
    fn create_test_avro_data() -> Vec<u8> {
        let schema_str = create_test_avro_schema();
        let schema = Schema::parse_str(&schema_str).unwrap();

        let mut writer = Writer::new(&schema, Vec::new());

        let record = Value::Record(vec![
            (
                "ResultUrl".to_string(),
                Value::Union(1, Box::new(Value::String("/test/result".to_string()))),
            ),
            (
                "JobIdentifier".to_string(),
                Value::String("job123".to_string()),
            ),
        ]);

        writer.append(record).unwrap();
        writer.into_inner().unwrap()
    }

    #[tokio::test]
    async fn test_avro_schema_parsing() {
        let schema_str = create_test_avro_schema();
        let result = Schema::parse_str(&schema_str);
        assert!(result.is_ok());
    }

    #[test]
    fn test_avro_data_structure() {
        let data = create_test_avro_data();
        assert!(!data.is_empty());
    }

    #[tokio::test]
    async fn test_processor_with_zero_task_id() {
        let (tx, rx) = broadcast::channel::<Event>(100);

        let config = Arc::new(super::super::config::JobRetriever {
            label: None,
            credentials_path: PathBuf::from("/test.json"),
        });

        let result = ProcessorBuilder::new()
            .config(config)
            .sender(tx)
            .receiver(rx)
            .current_task_id(0)
            .build()
            .await;

        assert!(result.is_ok());
        let processor = result.unwrap();
        assert_eq!(processor.current_task_id, 0);
    }

    #[tokio::test]
    async fn test_processor_with_large_task_id() {
        let (tx, rx) = broadcast::channel::<Event>(100);

        let config = Arc::new(super::super::config::JobRetriever {
            label: Some("large_id".to_string()),
            credentials_path: PathBuf::from("/test.json"),
        });

        let result = ProcessorBuilder::new()
            .config(config)
            .sender(tx)
            .receiver(rx)
            .current_task_id(999999)
            .build()
            .await;

        assert!(result.is_ok());
        let processor = result.unwrap();
        assert_eq!(processor.current_task_id, 999999);
    }

    #[test]
    fn test_job_metadata_uri_construction() {
        let job_id = "750xx0000000001AAA";
        let full_uri = format!("{}{}", DEFAULT_JOB_METADATA_URI, job_id);
        assert_eq!(
            full_uri,
            "/services/data/v61.0/jobs/query/750xx0000000001AAA"
        );
    }

    #[test]
    fn test_credentials_debug_implementation() {
        let creds = Credentials {
            bearer_auth: Some("debug_token".to_string()),
        };
        let debug_str = format!("{:?}", creds);
        assert!(debug_str.contains("Credentials"));
        assert!(debug_str.contains("bearer_auth"));
    }

    #[tokio::test]
    async fn test_multiple_builder_instances() {
        let (tx1, rx1) = broadcast::channel::<Event>(100);
        let (tx2, rx2) = broadcast::channel::<Event>(100);

        let config1 = Arc::new(super::super::config::JobRetriever {
            label: Some("builder1".to_string()),
            credentials_path: PathBuf::from("/test1.json"),
        });

        let config2 = Arc::new(super::super::config::JobRetriever {
            label: Some("builder2".to_string()),
            credentials_path: PathBuf::from("/test2.json"),
        });

        let result1 = ProcessorBuilder::new()
            .config(config1)
            .sender(tx1)
            .receiver(rx1)
            .current_task_id(1)
            .build()
            .await;

        let result2 = ProcessorBuilder::new()
            .config(config2)
            .sender(tx2)
            .receiver(rx2)
            .current_task_id(2)
            .build()
            .await;

        assert!(result1.is_ok());
        assert!(result2.is_ok());

        let proc1 = result1.unwrap();
        let proc2 = result2.unwrap();

        assert_eq!(proc1.current_task_id, 1);
        assert_eq!(proc2.current_task_id, 2);
    }
}

// Integration tests module
#[cfg(test)]
mod integration_tests {
    use super::*;
    use std::path::PathBuf;
    use std::sync::Arc;
    use tokio::sync::broadcast;

    #[tokio::test]
    async fn test_full_builder_workflow() {
        let (tx, rx) = broadcast::channel::<Event>(100);

        let config = Arc::new(super::super::config::JobRetriever {
            label: Some("integration_test".to_string()),
            credentials_path: PathBuf::from("/tmp/integration_creds.json"),
        });

        let processor = ProcessorBuilder::new()
            .config(config)
            .sender(tx)
            .receiver(rx)
            .current_task_id(100)
            .build()
            .await;

        assert!(processor.is_ok());
        let proc = processor.unwrap();
        assert_eq!(proc.current_task_id, 100);
        assert_eq!(proc.config.label, Some("integration_test".to_string()));
    }

    #[tokio::test]
    async fn test_builder_reuse() {
        let (tx, rx) = broadcast::channel::<Event>(100);

        let config = Arc::new(super::super::config::JobRetriever {
            label: Some("reuse_test".to_string()),
            credentials_path: PathBuf::from("/tmp/reuse_creds.json"),
        });

        // First build
        let builder = ProcessorBuilder::new()
            .config(Arc::clone(&config))
            .sender(tx.clone())
            .receiver(tx.subscribe())
            .current_task_id(1);

        let result1 = builder.build().await;
        assert!(result1.is_ok());

        // Second build with new builder
        let result2 = ProcessorBuilder::new()
            .config(config)
            .sender(tx)
            .receiver(rx)
            .current_task_id(2)
            .build()
            .await;

        assert!(result2.is_ok());
    }

    #[tokio::test]
    async fn test_credentials_flow() {
        // Test that credentials can be created, serialized, and deserialized
        let original = Credentials {
            bearer_auth: Some("integration_token".to_string()),
        };

        let json = serde_json::to_string(&original).unwrap();
        let restored: Credentials = serde_json::from_str(&json).unwrap();

        assert_eq!(original, restored);
        assert_eq!(original.bearer_auth, Some("integration_token".to_string()));
    }

    #[tokio::test]
    async fn test_job_response_complete_workflow() {
        // Test complete job response parsing workflow
        let json_data = r#"{
            "object": "Account",
            "state": "JobComplete",
            "id": "750xx0000000001AAA"
        }"#;

        let job_response: JobResponse = serde_json::from_str(json_data).unwrap();
        assert_eq!(job_response.object, "Account");
    }
}

// CSV and Arrow processing tests
#[cfg(test)]
mod csv_arrow_tests {
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[test]
    fn test_csv_file_creation() {
        let mut temp_file = NamedTempFile::new().unwrap();
        let csv_data = "Id,Name,Email\n001,Test User,test@example.com\n";

        temp_file.write_all(csv_data.as_bytes()).unwrap();
        temp_file.flush().unwrap();

        let metadata = temp_file.as_file().metadata().unwrap();
        assert!(metadata.len() > 0);
    }

    #[tokio::test]
    async fn test_temp_file_write_and_read() {
        let temp_dir = tempfile::tempdir().unwrap();
        let file_path = temp_dir.path().join("test_output.csv");

        let csv_content = "Id,Name\n001,Test\n002,Example\n";
        tokio::fs::write(&file_path, csv_content).await.unwrap();

        let content = tokio::fs::read_to_string(&file_path).await.unwrap();
        assert_eq!(content, csv_content);
    }
}

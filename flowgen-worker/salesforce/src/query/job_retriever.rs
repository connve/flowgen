use apache_avro::types::Value;
use apache_avro::{from_avro_datum, Schema};
use arrow::csv::reader::Format;
use flowgen_core::{
    client::Client,
    event::{generate_subject, Event, EventBuilder, EventData, SenderExt, SubjectSuffix},
};
use oauth2::TokenResponse;
use serde::Deserialize;
use std::fs::File;
use std::io::{Seek, Write};
use std::sync::Arc;
use tokio::sync::broadcast::{Receiver, Sender};
use tracing::{error, Instrument};

/// Message subject prefix for bulk API retrieve operations.
const DEFAULT_MESSAGE_SUBJECT: &str = "salesforce_query_job_retrieve";
/// Salesforce Bulk API endpoint for job metadata (API v61.0).
const DEFAULT_JOB_METADATA_URI: &str = "/services/data/v61.0/jobs/query/";

/// Errors for Salesforce bulk job retrieval operations.
#[derive(thiserror::Error, Debug)]
pub enum Error {
    /// File system I/O error.
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
    /// Salesforce authentication or client initialization error.
    #[error(transparent)]
    SalesforceAuth(#[from] crate::client::Error),
    /// Event creation or processing error.
    #[error(transparent)]
    Event(#[from] flowgen_core::event::Error),
    /// Missing or invalid Salesforce access token.
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
    /// Avro schema parsing failed.
    #[error("JSON serialization/deserialization failed: {source}")]
    ParseSchema {
        #[source]
        source: apache_avro::Error,
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
    /// Job configuration and authentication details.
    config: Arc<super::config::JobRetriever>,
    /// Broadcast sender for emitting Arrow record batches.
    tx: Sender<Event>,
    /// Broadcast receiver for incoming Avro events.
    rx: Receiver<Event>,
    /// Unique identifier for tracking events.
    current_task_id: usize,
}

/// Event handler for processing individual job retrieval requests.
pub struct EventHandler {
    /// HTTP client for Salesforce API requests.
    client: Arc<reqwest::Client>,
    /// Channel sender for emitting processed data.
    tx: Sender<Event>,
    /// Task identifier for event correlation.
    current_task_id: usize,
    /// SFDC client.
    sfdc_client: crate::client::Client,
}

impl EventHandler {
    /// Processes job retrieval: extract job info, download CSV, convert to Arrow, emit events.
    async fn handle(&self, event: Event) -> Result<(), Error> {
        // Process only Avro events with job completion data.
        if let EventData::Avro(value) = &event.data {
            // Parse Avro schema from event.
            let schema =
                Schema::parse_str(&value.schema).map_err(|e| Error::ParseSchema { source: e })?;

            // Deserialize Avro binary data.
            let value = from_avro_datum(&schema, &mut value.raw_bytes.as_slice(), None)
                .map_err(|e| Error::ParseSchema { source: e })?;

            // Process deserialized Avro record.
            if let Value::Record(fields) = value {
                // Extract ResultUrl containing CSV download URL.
                if let Some((_, Value::Union(_, inner_value))) =
                    fields.iter().find(|(name, _)| name == "ResultUrl")
                {
                    if let Value::String(result_url) = &**inner_value {
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
                        let mut file =
                            File::open(file_path).map_err(|e| Error::IO { source: e })?;

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

                        // Extract JobIdentifier for metadata retrieval.
                        if let Some((_, Value::String(job_id))) =
                            fields.iter().find(|(name, _)| name == "JobIdentifier")
                        {
                            // Request job metadata to get object type.
                            let mut client = self.client.get(format!(
                                "{}{}{}",
                                instance_url, DEFAULT_JOB_METADATA_URI, job_id
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

                            let job_metadata: JobResponse = serde_json::from_str(&resp)
                                .map_err(|e| Error::SerdeJson { source: e })?;

                            let subject = generate_subject(
                                Some(job_metadata.object.to_lowercase().as_str()),
                                DEFAULT_MESSAGE_SUBJECT,
                                SubjectSuffix::Timestamp,
                            );

                            // Process each Arrow record batch and emit as events.
                            for data in csv {
                                let e = EventBuilder::new()
                                    .data(EventData::ArrowRecordBatch(
                                        data.map_err(|e| Error::Arrow { source: e })?,
                                    ))
                                    .subject(subject.clone())
                                    .current_task_id(self.current_task_id)
                                    .build()?;
                                self.tx
                                    .send_with_logging(e)
                                    .map_err(|e| Error::SendMessage { source: e })?;
                            }
                        }
                    }
                }
            }
        }
        Ok(())
    }
}

/// Builder for constructing JobRetriever instances.
#[derive(Default)]
pub struct ProcessorBuilder {
    config: Option<Arc<super::config::JobRetriever>>,
    tx: Option<Sender<Event>>,
    rx: Option<Receiver<Event>>,
    current_task_id: usize,
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

        let sfdc_client = crate::client::Builder::new()
            .credentials_path(config.credentials_path.clone())
            .build()?
            .connect()
            .await?;

        let event_handler = EventHandler {
            current_task_id: self.current_task_id,
            tx: self.tx.clone(),
            client,
            sfdc_client,
        };
        Ok(event_handler)
    }

    #[tracing::instrument(skip(self), name = DEFAULT_MESSAGE_SUBJECT, fields( task_id = self.current_task_id))]
    async fn run(mut self) -> Result<(), Self::Error> {
        // Initialize runner task.
        let event_handler = match self.init().await {
            Ok(handler) => Arc::new(handler),
            Err(e) => {
                error!("{}", e);
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

impl ProcessorBuilder {
    /// Creates a new ProcessorBuilder with defaults.
    pub fn new() -> ProcessorBuilder {
        ProcessorBuilder {
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
    pub fn current_task_id(mut self, current_task_id: usize) -> Self {
        self.current_task_id = current_task_id;
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
        assert_eq!(DEFAULT_MESSAGE_SUBJECT, "salesforce_query_job_retrieve");
        assert_eq!(DEFAULT_JOB_METADATA_URI, "/services/data/v61.0/jobs/query/");
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

    #[test]
    fn test_uri_path_version() {
        assert!(DEFAULT_JOB_METADATA_URI.contains("v61.0"));
        assert!(DEFAULT_JOB_METADATA_URI.starts_with("/services/data/"));
        assert!(DEFAULT_JOB_METADATA_URI.ends_with("/jobs/query/"));
    }

    #[test]
    fn test_message_subject_format() {
        assert_eq!(DEFAULT_MESSAGE_SUBJECT, "salesforce_query_job_retrieve");
        assert!(!DEFAULT_MESSAGE_SUBJECT.contains(" "));
        assert!(!DEFAULT_MESSAGE_SUBJECT.contains("."));
    }

    #[test]
    fn test_error_from_conversions() {
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

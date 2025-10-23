use chrono::Utc;
use flowgen_core::{
    client::Client,
    event::{Event, EventBuilder, EventData, SenderExt},
};
use oauth2::TokenResponse;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::sync::Arc;
use tokio::sync::broadcast::{Receiver, Sender};
use tracing::{error, Instrument};

/// Default message subject prefix for bulk API create operations
const DEFAULT_MESSAGE_SUBJECT: &str = "bulkapicreate";
/// Default Salesforce Bulk API endpoint path for query jobs (API version 61.0)
const DEFAULT_URI_PATH: &str = "/services/data/v61.0/jobs/query";

/// Comprehensive error types for Salesforce bulk job creation operations.
/// This enum encapsulates all possible error conditions that can occur during
/// the job creation process, from authentication failures to API communication issues.
#[derive(thiserror::Error, Debug)]
pub enum Error {
    /// Failed to send event through broadcast channel.
    #[error("Failed to send event message: {source}")]
    SendMessage {
        #[source]
        source: tokio::sync::broadcast::error::SendError<Event>,
    },
    /// Required attribute is missing.
    #[error("Missing required attribute: {}", _0)]
    MissingRequiredAttribute(String),
    /// Flowgen core service error.
    #[error(transparent)]
    Service(#[from] flowgen_core::service::Error),

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

    #[error("missing salesforce instance URL")]
    NoSalesforceInstanceURL(),
}

/// Internal credentials structure for storing authentication information.
#[derive(Deserialize, Serialize, PartialEq, Clone, Debug, Default)]
struct Credentials {
    /// Optional bearer authentication token
    /// When present, this token will be used for API authentication.
    bearer_auth: Option<String>,
}

/// Main processor for creating Salesforce bulk API jobs.
/// This struct coordinates the entire job creation workflow, from receiving
/// trigger events to authenticating with Salesforce and submitting job requests.
/// It implements the flowgen_core Runner trait for integration with the task system.
pub struct JobCreator {
    /// Shared configuration containing job parameters and authentication details
    config: Arc<super::config::JobCreator>,
    /// Broadcast sender for emitting processed events downstream
    tx: Sender<Event>,
    /// Broadcast receiver for incoming trigger events
    rx: Receiver<Event>,
    /// Unique identifier for tracking this processor's events in the pipeline
    current_task_id: usize,
}

/// Internal event handler responsible for processing individual job creation requests.
/// This struct encapsulates the logic for authenticating with Salesforce,
/// constructing the appropriate API payload, and making the HTTP request to create a bulk job.
pub struct EventHandler {
    /// HTTP client for making requests to the Salesforce API
    client: Arc<reqwest::Client>,
    /// Processor configuration containing job parameters and credentials
    config: Arc<super::config::JobCreator>,
    /// Channel sender for emitting the job creation response
    tx: Sender<Event>,
    /// Task identifier for event correlation and tracking
    current_task_id: usize,
}

impl EventHandler {
    /// Processes a job creation request and handles the complete workflow.
    ///
    /// This method orchestrates the entire job creation process:
    /// 1. Loads and validates Salesforce credentials
    /// 2. Authenticates with Salesforce to obtain access token
    /// 3. Constructs the appropriate API payload based on operation type
    /// 4. Makes the HTTP request to create the bulk job
    /// 5. Processes the response and emits it as an event
    ///
    /// # Returns
    ///
    /// `Ok(())` if the job was successfully created and the response was emitted,
    /// or an `Error` if any step in the process failed.
    ///
    /// # Errors
    ///
    /// This method can return various errors including:
    /// - `SalesforceAuth` errors for credential or authentication issues
    /// - `NoSalesforceAuthToken` if authentication didn't produce a valid token
    /// - `Reqwest` errors for HTTP communication problems
    /// - `Event` errors for response processing issues
    /// - `SendMessage` errors if the response cannot be emitted
    async fn handle(&self) -> Result<(), Error> {
        let config = self.config.as_ref();

        let sfdc_client = crate::client::Builder::new()
            .credentials_path(config.credentials_path.clone())
            .build()?
            .connect()
            .await?;

        // Extract access token from authentication result
        let token_result = sfdc_client
            .token_result
            .ok_or_else(Error::NoSalesforceAuthToken)?;

        // Construct API payload based on the requested operation type
        let payload = match self.config.operation {
            super::config::Operation::Query | super::config::Operation::QueryAll => {
                // Create payload for query or queryall job.
                // These operations require a SOQL query and output format specifications.
                json!({
                    "operation": self.config.operation,
                    "query": self.config.query,
                    "contentType": self.config.content_type,
                    "columnDelimiter": self.config.column_delimiter,
                    "lineEnding": self.config.line_ending,
                })
            }
            _ => {
                // TODO: Implement payload construction for data manipulation operations
                // (Insert, Update, Upsert, Delete, HardDelete)
                // These operations will require object name and potentially external ID fields
                todo!("Implement other operations like Insert, Update, Upsert, Delete, HardDelete");
            }
        };

        let instance_url = sfdc_client
            .instance_url
            .ok_or_else(Error::NoSalesforceInstanceURL)?;

        // Configure HTTP client with Salesforce endpoint and authentication
        let mut client = self.client.post(instance_url + DEFAULT_URI_PATH);

        client = client.bearer_auth(token_result.access_token().secret());
        client = client.json(&payload);

        // Execute API request and retrieve response
        let resp = client
            .send()
            .await
            .map_err(|e| Error::Reqwest { source: e })?
            .text()
            .await
            .map_err(|e| Error::Reqwest { source: e })?;

        // Prepare the response data for event emission
        let data = json!(resp);

        // Generate unique event subject for tracking and routing
        let timestamp = Utc::now().timestamp_micros();
        let subject = match &self.config.label {
            Some(label) => format!(
                "{}.{}.{}",
                DEFAULT_MESSAGE_SUBJECT,
                label.to_lowercase(),
                timestamp
            ),
            None => format!("{DEFAULT_MESSAGE_SUBJECT}.{timestamp}"),
        };

        // Create and emit the response event
        let e = EventBuilder::new()
            .data(EventData::Json(data))
            .subject(subject.clone())
            .current_task_id(self.current_task_id)
            .build()?;
        self.tx
            .send_with_logging(e)
            .map_err(|e| Error::SendMessage { source: e })?;
        Ok(())
    }
}

#[async_trait::async_trait]
impl flowgen_core::task::runner::Runner for JobCreator {
    type Error = Error;
    type EventHandler = EventHandler;

    /// Main execution loop for the job creator processor.
    ///
    /// This method implements the flowgen_core Runner trait and provides the
    /// main execution logic for the processor. It:
    /// 1. Sets up an HTTPS-only HTTP client for security
    /// 2. Continuously listens for incoming events
    /// 3. Processes events that match the expected task ID
    /// 4. Spawns asynchronous handlers for each job creation request
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

    async fn run(mut self) -> Result<(), Self::Error> {
        // Initialize runner task.
        let event_handler = match self.init().await {
            Ok(handler) => Arc::new(handler),
            Err(e) => {
                error!("{}", e);
                return Ok(());
            }
        };

        loop {
            match self.rx.recv().await {
                Ok(event) => {
                    if event.current_task_id == event_handler.current_task_id.checked_sub(1) {
                        let event_handler = Arc::clone(&event_handler);
                        tokio::spawn(
                            async move {
                                if let Err(err) = event_handler.handle().await {
                                    error!("{}", err);
                                }
                            }
                            .instrument(tracing::Span::current()),
                        );
                    }
                }
                Err(_) => return Ok(()),
            }
        }
    }
}

/// Builder pattern implementation for constructing JobCreator instances.
/// This builder ensures that all required components are provided and properly
/// configured before creating a JobCreator instance.
#[derive(Default)]
pub struct ProcessorBuilder {
    /// Job configuration (required)
    config: Option<Arc<super::config::JobCreator>>,
    /// Event sender channel (required)
    tx: Option<Sender<Event>>,
    /// Event receiver channel (required)
    rx: Option<Receiver<Event>>,
    /// Task identifier for event correlation (defaults to 0)
    current_task_id: usize,
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

    /// Sets the job configuration.
    ///
    /// # Arguments
    ///
    /// * `config` - Shared configuration containing job parameters, credentials, and operation details
    pub fn config(mut self, config: Arc<super::config::JobCreator>) -> Self {
        self.config = Some(config);
        self
    }

    /// Sets the event receiver channel.
    ///
    /// # Arguments
    ///
    /// * `receiver` - Broadcast receiver for incoming trigger events
    pub fn receiver(mut self, receiver: Receiver<Event>) -> Self {
        self.rx = Some(receiver);
        self
    }

    /// Sets the event sender channel.
    ///
    /// # Arguments
    ///
    /// * `sender` - Broadcast sender for emitting processed events
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

    /// Builds the JobCreator instance after validating required fields.
    ///
    /// # Returns
    ///
    /// `Ok(JobCreator)` if all required fields are provided, or an `Error`
    /// indicating which required field is missing.
    ///
    /// # Errors
    ///
    /// Returns `MissingRequiredAttribute` error if any of the required fields
    /// (config, receiver, sender) are not provided.
    pub async fn build(self) -> Result<JobCreator, Error> {
        Ok(JobCreator {
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
    use serde_json::json;
    use std::path::PathBuf;
    use std::sync::Arc;
    use tokio::sync::broadcast;

    #[test]
    fn test_default_constants() {
        assert_eq!(DEFAULT_MESSAGE_SUBJECT, "bulkapicreate");
        assert_eq!(DEFAULT_URI_PATH, "/services/data/v61.0/jobs/query");
    }

    #[test]
    fn test_credentials_default() {
        let creds = Credentials::default();
        assert_eq!(creds.bearer_auth, None);
    }

    #[test]
    fn test_credentials_creation() {
        let creds = Credentials {
            bearer_auth: Some("test_token_123".to_string()),
        };
        assert_eq!(creds.bearer_auth, Some("test_token_123".to_string()));
    }

    #[test]
    fn test_credentials_serialization() {
        let creds = Credentials {
            bearer_auth: Some("token123".to_string()),
        };

        let json = serde_json::to_string(&creds).unwrap();
        let deserialized: Credentials = serde_json::from_str(&json).unwrap();

        assert_eq!(creds, deserialized);
    }

    #[test]
    fn test_credentials_clone() {
        let creds1 = Credentials {
            bearer_auth: Some("token".to_string()),
        };
        let creds2 = creds1.clone();
        assert_eq!(creds1, creds2);
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
        let err = Error::MissingRequiredAttribute("test".to_string());
        let debug_str = format!("{:?}", err);
        assert!(debug_str.contains("MissingRequiredAttribute"));
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
    async fn test_processor_builder_config() {
        let config = Arc::new(super::super::config::JobCreator {
            name: "test_job".to_string(),
            label: Some("test_label".to_string()),
            credentials_path: PathBuf::from("/test/creds.json"),
            query: Some("SELECT Id FROM Account".to_string()),
            object: None,
            operation: super::super::config::Operation::Query,
            content_type: Some(super::super::config::ContentType::Csv),
            column_delimiter: Some(super::super::config::ColumnDelimiter::Comma),
            line_ending: Some(super::super::config::LineEnding::Lf),
            assignment_rule_id: None,
            external_id_field_name: None,
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
        let builder = ProcessorBuilder::new().current_task_id(5);
        assert_eq!(builder.current_task_id, 5);
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

        let config = Arc::new(super::super::config::JobCreator {
            name: "test".to_string(),
            label: None,
            credentials_path: PathBuf::from("/test.json"),
            query: Some("SELECT Id FROM Account".to_string()),
            object: None,
            operation: super::super::config::Operation::Query,
            content_type: None,
            column_delimiter: None,
            line_ending: None,
            assignment_rule_id: None,
            external_id_field_name: None,
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

        let config = Arc::new(super::super::config::JobCreator {
            name: "test".to_string(),
            label: None,
            credentials_path: PathBuf::from("/test.json"),
            query: Some("SELECT Id FROM Account".to_string()),
            object: None,
            operation: super::super::config::Operation::Query,
            content_type: None,
            column_delimiter: None,
            line_ending: None,
            assignment_rule_id: None,
            external_id_field_name: None,
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

        let config = Arc::new(super::super::config::JobCreator {
            name: "test_job".to_string(),
            label: Some("test".to_string()),
            credentials_path: PathBuf::from("/test.json"),
            query: Some("SELECT Id FROM Account".to_string()),
            object: None,
            operation: super::super::config::Operation::Query,
            content_type: Some(super::super::config::ContentType::Csv),
            column_delimiter: Some(super::super::config::ColumnDelimiter::Comma),
            line_ending: Some(super::super::config::LineEnding::Lf),
            assignment_rule_id: None,
            external_id_field_name: None,
        });

        let result = ProcessorBuilder::new()
            .config(config)
            .sender(tx)
            .receiver(rx)
            .current_task_id(3)
            .build()
            .await;

        assert!(result.is_ok());
        let processor = result.unwrap();
        assert_eq!(processor.current_task_id, 3);
    }

    #[tokio::test]
    async fn test_processor_builder_chaining() {
        let (tx, rx) = broadcast::channel::<Event>(100);

        let config = Arc::new(super::super::config::JobCreator {
            name: "chain_test".to_string(),
            label: Some("chained".to_string()),
            credentials_path: PathBuf::from("/chain.json"),
            query: Some("SELECT Id FROM Contact".to_string()),
            object: None,
            operation: super::super::config::Operation::QueryAll,
            content_type: Some(super::super::config::ContentType::Csv),
            column_delimiter: Some(super::super::config::ColumnDelimiter::Tab),
            line_ending: Some(super::super::config::LineEnding::Crlf),
            assignment_rule_id: None,
            external_id_field_name: None,
        });

        // Test method chaining
        let result = ProcessorBuilder::new()
            .config(config)
            .sender(tx)
            .receiver(rx)
            .current_task_id(10)
            .build()
            .await;

        assert!(result.is_ok());
    }

    #[test]
    fn test_event_subject_generation_with_label() {
        let label = Some("test_label".to_string());
        let timestamp = 1234567890123456i64;

        let subject = match &label {
            Some(l) => format!(
                "{}.{}.{}",
                DEFAULT_MESSAGE_SUBJECT,
                l.to_lowercase(),
                timestamp
            ),
            None => format!("{}.{}", DEFAULT_MESSAGE_SUBJECT, timestamp),
        };

        assert_eq!(subject, "bulkapicreate.test_label.1234567890123456");
    }

    #[test]
    fn test_event_subject_generation_without_label() {
        let label: Option<String> = None;
        let timestamp = 1234567890123456i64;

        let subject = match &label {
            Some(l) => format!(
                "{}.{}.{}",
                DEFAULT_MESSAGE_SUBJECT,
                l.to_lowercase(),
                timestamp
            ),
            None => format!("{}.{}", DEFAULT_MESSAGE_SUBJECT, timestamp),
        };

        assert_eq!(subject, "bulkapicreate.1234567890123456");
    }

    #[test]
    fn test_query_operation_payload_structure() {
        let operation = super::super::config::Operation::Query;
        let query = Some("SELECT Id, Name FROM Account".to_string());
        let content_type = Some(super::super::config::ContentType::Csv);
        let column_delimiter = Some(super::super::config::ColumnDelimiter::Comma);
        let line_ending = Some(super::super::config::LineEnding::Lf);

        let payload = json!({
            "operation": operation,
            "query": query,
            "contentType": content_type,
            "columnDelimiter": column_delimiter,
            "lineEnding": line_ending,
        });

        assert!(payload.get("operation").is_some());
        assert!(payload.get("query").is_some());
        assert!(payload.get("contentType").is_some());
        assert!(payload.get("columnDelimiter").is_some());
        assert!(payload.get("lineEnding").is_some());
    }

    #[test]
    fn test_query_all_operation_payload_structure() {
        let operation = super::super::config::Operation::QueryAll;
        let query = Some("SELECT Id FROM Account".to_string());

        let payload = json!({
            "operation": operation,
            "query": query,
        });

        assert!(payload.get("operation").is_some());
        assert!(payload.get("query").is_some());
    }

    #[tokio::test]
    async fn test_event_handler_creation() {
        let (tx, _) = broadcast::channel::<Event>(100);

        let config = Arc::new(super::super::config::JobCreator {
            name: "handler_test".to_string(),
            label: Some("test".to_string()),
            credentials_path: PathBuf::from("/test.json"),
            query: Some("SELECT Id FROM Account".to_string()),
            object: None,
            operation: super::super::config::Operation::Query,
            content_type: Some(super::super::config::ContentType::Csv),
            column_delimiter: Some(super::super::config::ColumnDelimiter::Comma),
            line_ending: Some(super::super::config::LineEnding::Lf),
            assignment_rule_id: None,
            external_id_field_name: None,
        });

        let client = Arc::new(reqwest::Client::new());

        let handler = EventHandler {
            client,
            config,
            tx,
            current_task_id: 1,
        };

        assert_eq!(handler.current_task_id, 1);
    }

    #[test]
    fn test_error_from_conversions() {
        // Test that Error implements From for various source errors
        let service_err = Error::MissingRequiredAttribute("test".to_string());
        let _: Error = service_err.into();

        // These conversions should compile
        fn _test_conversions() {
            let _: Error = Error::MissingRequiredAttribute("x".to_string()).into();
        }
    }

    #[tokio::test]
    async fn test_builder_default_trait() {
        let builder1 = ProcessorBuilder::new();
        let builder2 = ProcessorBuilder::default();

        assert_eq!(builder1.current_task_id, builder2.current_task_id);
    }

    #[test]
    fn test_multiple_operations_distinct() {
        let ops = vec![
            super::super::config::Operation::Query,
            super::super::config::Operation::QueryAll,
            super::super::config::Operation::Insert,
            super::super::config::Operation::Update,
            super::super::config::Operation::Delete,
            super::super::config::Operation::HardDelete,
            super::super::config::Operation::Upsert,
        ];

        // Serialize all operations and verify they're distinct
        let serialized: Vec<String> = ops
            .iter()
            .map(|op| serde_json::to_string(op).unwrap())
            .collect();

        let unique_count = serialized
            .iter()
            .collect::<std::collections::HashSet<_>>()
            .len();

        assert_eq!(unique_count, ops.len());
    }

    #[tokio::test]
    async fn test_job_creator_structure() {
        let (tx, rx) = broadcast::channel::<Event>(100);

        let config = Arc::new(super::super::config::JobCreator {
            name: "struct_test".to_string(),
            label: None,
            credentials_path: PathBuf::from("/test.json"),
            query: Some("SELECT Id FROM Account".to_string()),
            object: None,
            operation: super::super::config::Operation::Query,
            content_type: None,
            column_delimiter: None,
            line_ending: None,
            assignment_rule_id: None,
            external_id_field_name: None,
        });

        let processor = JobCreator {
            config: Arc::clone(&config),
            tx: tx.clone(),
            rx,
            current_task_id: 5,
        };

        assert_eq!(processor.current_task_id, 5);
        assert_eq!(processor.config.name, "struct_test");
    }

    #[test]
    fn test_uri_path_version() {
        // Verify the API version is properly formatted
        assert!(DEFAULT_URI_PATH.contains("v61.0"));
        assert!(DEFAULT_URI_PATH.starts_with("/services/data/"));
        assert!(DEFAULT_URI_PATH.ends_with("/jobs/query"));
    }

    #[test]
    fn test_message_subject_format() {
        // Verify the message subject follows expected naming convention
        assert_eq!(DEFAULT_MESSAGE_SUBJECT, "bulkapicreate");
        assert!(!DEFAULT_MESSAGE_SUBJECT.contains(" "));
        assert!(!DEFAULT_MESSAGE_SUBJECT.contains("."));
    }

    #[tokio::test]
    async fn test_processor_builder_order_independence() {
        let (tx, rx) = broadcast::channel::<Event>(100);
        let (tx2, rx2) = broadcast::channel::<Event>(100);

        let config = Arc::new(super::super::config::JobCreator {
            name: "order_test".to_string(),
            label: None,
            credentials_path: PathBuf::from("/test.json"),
            query: Some("SELECT Id FROM Account".to_string()),
            object: None,
            operation: super::super::config::Operation::Query,
            content_type: None,
            column_delimiter: None,
            line_ending: None,
            assignment_rule_id: None,
            external_id_field_name: None,
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

    #[test]
    fn test_credentials_json_format() {
        let creds = Credentials {
            bearer_auth: Some("token123".to_string()),
        };

        let json_str = serde_json::to_string(&creds).unwrap();
        assert!(json_str.contains("bearer_auth"));
        assert!(json_str.contains("token123"));
    }

    #[test]
    fn test_credentials_optional_bearer_auth() {
        let creds_with_auth = Credentials {
            bearer_auth: Some("token".to_string()),
        };

        let creds_without_auth = Credentials { bearer_auth: None };

        assert!(creds_with_auth.bearer_auth.is_some());
        assert!(creds_without_auth.bearer_auth.is_none());
    }
}

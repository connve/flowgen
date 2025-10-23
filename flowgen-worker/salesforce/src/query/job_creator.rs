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

/// Message subject prefix for bulk API create operations.
const DEFAULT_MESSAGE_SUBJECT: &str = "salesforce_query_job_create";
/// Salesforce Bulk API endpoint for query jobs (API v61.0).
const DEFAULT_URI_PATH: &str = "/services/data/v61.0/jobs/query";

/// Errors for Salesforce bulk job creation operations.
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

    /// Salesforce authentication or client initialization error.
    #[error(transparent)]
    SalesforceAuth(#[from] crate::client::Error),

    /// Event creation or processing error.
    #[error(transparent)]
    Event(#[from] flowgen_core::event::Error),

    /// Missing or invalid Salesforce access token.
    #[error("missing salesforce access token")]
    NoSalesforceAuthToken(),

    #[error("missing salesforce instance URL")]
    NoSalesforceInstanceURL(),
}

/// Request payload for Salesforce bulk query job creation.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct QueryJobPayload {
    /// Type of operation (query or queryAll).
    operation: super::config::Operation,
    /// SOQL query string.
    query: Option<String>,
    /// Output file format.
    content_type: Option<super::config::ContentType>,
    /// CSV column delimiter.
    column_delimiter: Option<super::config::ColumnDelimiter>,
    /// Line ending style.
    line_ending: Option<super::config::LineEnding>,
}

/// Processor for creating Salesforce bulk API jobs.
pub struct JobCreator {
    /// Job configuration and authentication details.
    config: Arc<super::config::JobCreator>,
    /// Broadcast sender for emitting processed events.
    tx: Sender<Event>,
    /// Broadcast receiver for incoming trigger events.
    rx: Receiver<Event>,
    /// Unique identifier for tracking events.
    current_task_id: usize,
}

/// Event handler for processing individual job creation requests.
pub struct EventHandler {
    /// HTTP client for Salesforce API requests.
    client: Arc<reqwest::Client>,
    /// Processor configuration.
    config: Arc<super::config::JobCreator>,
    /// Channel sender for emitting job creation responses.
    tx: Sender<Event>,
    /// Task identifier for event correlation.
    current_task_id: usize,
    /// SFDC client
    sfdc_client: crate::client::Client,
}

impl EventHandler {
    /// Processes a job creation request: authenticate, build payload, create job, emit response.
    async fn handle(&self) -> Result<(), Error> {
        // Extract access token from authentication result.
        let token_result = self
            .sfdc_client
            .token_result
            .clone()
            .ok_or_else(Error::NoSalesforceAuthToken)?;

        // Build API payload based on operation type.
        let payload = match self.config.operation {
            super::config::Operation::Query | super::config::Operation::QueryAll => {
                // Query operations require SOQL query and output format specs.
                QueryJobPayload {
                    operation: self.config.operation.clone(),
                    query: self.config.query.clone(),
                    content_type: self.config.content_type.clone(),
                    column_delimiter: self.config.column_delimiter.clone(),
                    line_ending: self.config.line_ending.clone(),
                }
            }
            _ => {
                // TODO: Implement Insert, Update, Upsert, Delete, HardDelete operations.
                todo!("Implement other operations like Insert, Update, Upsert, Delete, HardDelete");
            }
        };

        let instance_url = self
            .sfdc_client
            .instance_url
            .clone()
            .ok_or_else(Error::NoSalesforceInstanceURL)?;

        // Configure HTTP client with endpoint and auth.
        let mut client = self.client.post(instance_url + DEFAULT_URI_PATH);

        client = client.bearer_auth(token_result.access_token().secret());
        client = client.json(&payload);

        // Execute API request and retrieve response.
        let resp = client
            .send()
            .await
            .map_err(|e| Error::Reqwest { source: e })?
            .text()
            .await
            .map_err(|e| Error::Reqwest { source: e })?;

        let data = json!(resp);

        // Generate unique event subject for tracking.
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

        // Create and emit response event.
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
            config: Arc::clone(&self.config),
            current_task_id: self.current_task_id,
            tx: self.tx.clone(),
            client,
            sfdc_client,
        };
        Ok(event_handler)
    }

    /// Main execution loop: listen for events, filter by task ID, spawn handlers.
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

/// Builder for constructing JobCreator instances.
#[derive(Default)]
pub struct ProcessorBuilder {
    config: Option<Arc<super::config::JobCreator>>,
    tx: Option<Sender<Event>>,
    rx: Option<Receiver<Event>>,
    current_task_id: usize,
}

impl ProcessorBuilder {
    /// Creates a new ProcessorBuilder with defaults.
    pub fn new() -> ProcessorBuilder {
        ProcessorBuilder {
            ..Default::default()
        }
    }

    /// Sets the job configuration.
    pub fn config(mut self, config: Arc<super::config::JobCreator>) -> Self {
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

    /// Builds JobCreator after validating required fields.
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
        assert_eq!(DEFAULT_MESSAGE_SUBJECT, "salesforce_query_job_create");
        assert_eq!(DEFAULT_URI_PATH, "/services/data/v61.0/jobs/query");
    }

    #[test]
    fn test_query_job_payload_serialization() {
        let payload = QueryJobPayload {
            operation: super::super::config::Operation::Query,
            query: Some("SELECT Id FROM Account".to_string()),
            content_type: Some(super::super::config::ContentType::Csv),
            column_delimiter: Some(super::super::config::ColumnDelimiter::Comma),
            line_ending: Some(super::super::config::LineEnding::Lf),
        };

        let json = serde_json::to_value(&payload).unwrap();
        assert!(json.get("operation").is_some());
        assert!(json.get("query").is_some());
        assert!(json.get("contentType").is_some());
        assert!(json.get("columnDelimiter").is_some());
        assert!(json.get("lineEnding").is_some());
    }

    #[test]
    fn test_query_job_payload_deserialization() {
        let json_str = r#"{
            "operation": "query",
            "query": "SELECT Id FROM Account",
            "contentType": "CSV",
            "columnDelimiter": "COMMA",
            "lineEnding": "LF"
        }"#;

        let payload: QueryJobPayload = serde_json::from_str(json_str).unwrap();
        assert_eq!(payload.operation, super::super::config::Operation::Query);
        assert_eq!(payload.query, Some("SELECT Id FROM Account".to_string()));
    }

    #[test]
    fn test_query_job_payload_clone() {
        let payload1 = QueryJobPayload {
            operation: super::super::config::Operation::QueryAll,
            query: Some("SELECT Id FROM Contact".to_string()),
            content_type: Some(super::super::config::ContentType::Csv),
            column_delimiter: Some(super::super::config::ColumnDelimiter::Tab),
            line_ending: Some(super::super::config::LineEnding::Crlf),
        };

        let payload2 = payload1.clone();
        assert_eq!(payload1.operation, payload2.operation);
        assert_eq!(payload1.query, payload2.query);
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

        assert_eq!(
            subject,
            "salesforce_query_job_create.test_label.1234567890123456"
        );
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

        assert_eq!(subject, "salesforce_query_job_create.1234567890123456");
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

    #[test]
    fn test_error_from_conversions() {
        let service_err = Error::MissingRequiredAttribute("test".to_string());
        let _: Error = service_err;

        fn _test_conversions() {
            let _: Error = Error::MissingRequiredAttribute("x".to_string());
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
        let ops = [
            super::super::config::Operation::Query,
            super::super::config::Operation::QueryAll,
            super::super::config::Operation::Insert,
            super::super::config::Operation::Update,
            super::super::config::Operation::Delete,
            super::super::config::Operation::HardDelete,
            super::super::config::Operation::Upsert,
        ];

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
        assert!(DEFAULT_URI_PATH.contains("v61.0"));
        assert!(DEFAULT_URI_PATH.starts_with("/services/data/"));
        assert!(DEFAULT_URI_PATH.ends_with("/jobs/query"));
    }

    #[test]
    fn test_message_subject_format() {
        assert_eq!(DEFAULT_MESSAGE_SUBJECT, "salesforce_query_job_create");
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
}

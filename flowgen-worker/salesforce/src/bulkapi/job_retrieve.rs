use flowgen_core::buffer::{ContentType, FromReader};
use flowgen_core::config::ConfigExt;
use flowgen_core::event::{Event, EventBuilder, EventData, EventExt};
use serde::{Deserialize, Serialize};
use std::io::Cursor;
use std::sync::Arc;
use tokio::sync::mpsc::{Receiver, Sender};
use tracing::{error, Instrument};

/// Errors for Salesforce bulk job retrieval operations.
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum Error {
    #[error("Failed to send event message: {source}")]
    SendMessage {
        #[source]
        source: flowgen_core::event::Error,
    },
    #[error("Missing required builder attribute: {}", _0)]
    MissingBuilderAttribute(String),
    #[error(transparent)]
    Service(#[from] flowgen_core::service::Error),
    #[error("HTTP request failed: {source}")]
    Reqwest {
        #[source]
        source: reqwest::Error,
    },
    #[error(transparent)]
    SalesforceAuth(#[from] salesforce_core::client::Error),
    #[error(transparent)]
    EventError(#[from] flowgen_core::event::Error),
    #[error(transparent)]
    ConfigRender(#[from] flowgen_core::config::Error),
    #[error("No salesforce access token provided")]
    NoSalesforceAuthToken(),
    #[error("No salesforce instance URL provided")]
    NoSalesforceInstanceURL(),
    #[error("Salesforce API returned {count} error(s): {details}", count = .errors.len(), details = format_salesforce_errors(.errors))]
    SalesforceApi {
        errors: Vec<SalesforceErrorResponse>,
    },
    #[error("Task failed after all retry attempts: {source}")]
    RetryExhausted {
        #[source]
        source: Box<Error>,
    },
    #[error("Missing job ID in event data")]
    MissingJobId(),
}

/// Salesforce API error response structure.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SalesforceErrorResponse {
    pub error_code: String,
    pub message: String,
}

/// Formats Salesforce errors for display in error messages.
fn format_salesforce_errors(errors: &[SalesforceErrorResponse]) -> String {
    errors
        .iter()
        .map(|e| format!("[{}] {}", e.error_code, e.message))
        .collect::<Vec<_>>()
        .join(", ")
}

/// Processor for retrieving Salesforce bulk API job status and results.
pub struct JobRetrieve {
    config: Arc<super::config::JobRetrieve>,
    tx: Option<Sender<Event>>,
    rx: Receiver<Event>,
    current_task_id: usize,
    task_type: &'static str,
    task_context: Arc<flowgen_core::task::context::TaskContext>,
}

/// Event handler for processing individual job retrieval requests.
pub struct EventHandler {
    /// HTTP client for Salesforce API requests.
    client: Arc<reqwest::Client>,
    /// Processor configuration.
    config: Arc<super::config::JobRetrieve>,
    /// Channel sender for emitting job retrieval responses.
    tx: Option<Sender<Event>>,
    /// Task identifier for event correlation.
    current_task_id: usize,
    /// Salesforce client for authentication and API access (wrapped in Mutex for token refresh).
    sfdc_client: Arc<tokio::sync::Mutex<salesforce_core::client::Client>>,
    /// Task type for event categorization and logging.
    task_type: &'static str,
}

impl EventHandler {
    /// Processes a job retrieval request: authenticate, fetch job status, emit response.
    async fn handle(&self, event: Event) -> Result<(), Error> {
        // Render config to support templates in job_id field.
        let event_value = serde_json::value::Value::try_from(&event)?;
        let config = self.config.render(&event_value)?;

        // Try request, and if it fails with INVALID_SESSION_ID, force token refresh and retry once.
        match self.handle_request(&event, &config).await {
            Err(Error::SalesforceApi { ref errors })
                if errors.iter().any(|e| e.error_code == "INVALID_SESSION_ID") =>
            {
                // Token is invalid - force refresh by calling access_token() which auto-refreshes.
                {
                    let sfdc_client = self.sfdc_client.lock().await;
                    (*sfdc_client).access_token().await?;
                }

                // Retry the request with refreshed token.
                self.handle_request(&event, &config).await
            }
            result => result,
        }
    }

    /// Internal method to handle the actual API request.
    async fn handle_request(
        &self,
        _event: &Event,
        config: &super::config::JobRetrieve,
    ) -> Result<(), Error> {
        // Lock the Salesforce client to get token and instance URL.
        let sfdc_client = self.sfdc_client.lock().await;
        let access_token = sfdc_client.access_token().await?;
        let instance_url = sfdc_client
            .instance_url
            .clone()
            .ok_or_else(Error::NoSalesforceInstanceURL)?;
        drop(sfdc_client);

        let job_id = &config.job_id;

        // Build API endpoint URL for downloading results.
        let url = format!(
            "{}{}{}/{}/results",
            instance_url,
            super::config::DEFAULT_URI_PATH,
            self.config.job_type.as_str(),
            job_id
        );

        // Configure HTTP client with endpoint and auth.
        let mut client = self.client.get(&url);
        client = client.bearer_auth(access_token);

        // Download CSV results from Salesforce.
        let response = client
            .send()
            .await
            .map_err(|e| Error::Reqwest { source: e })?;

        let csv_data = response
            .text()
            .await
            .map_err(|e| Error::Reqwest { source: e })?;

        // Check if response is a JSON error (Salesforce returns errors as JSON even for CSV endpoints).
        if csv_data.trim_start().starts_with('[') || csv_data.trim_start().starts_with('{') {
            // Try to parse as JSON error response.
            if let Ok(json_value) = serde_json::from_str::<serde_json::Value>(&csv_data) {
                // Check if it's an error array.
                if let Some(errors_array) = json_value.as_array() {
                    if !errors_array.is_empty() {
                        // Try to deserialize all errors from the array.
                        let mut errors = Vec::new();
                        for error_value in errors_array {
                            if let Ok(error_response) =
                                serde_json::from_value::<SalesforceErrorResponse>(
                                    error_value.clone(),
                                )
                            {
                                errors.push(error_response);
                            }
                        }

                        if !errors.is_empty() {
                            return Err(Error::SalesforceApi { errors });
                        }
                    }
                }
            }
        }

        // Parse CSV using FromReader trait (following flowgen_core::event pattern).
        let cursor = Cursor::new(csv_data.as_bytes());
        let content_type = ContentType::Csv {
            batch_size: self.config.batch_size,
            has_header: self.config.has_header,
            delimiter: None, // Use default comma delimiter
        };

        // Parse CSV to Arrow RecordBatch using EventData::from_reader.
        let mut events = EventData::from_reader(cursor, content_type)?;

        // If no data rows, from_reader returns empty vec but we still need to send an event
        // to maintain the event chain. Create an empty Arrow batch so downstream knows job completed.
        if events.is_empty() {
            let empty_batch = arrow::record_batch::RecordBatch::new_empty(std::sync::Arc::new(
                arrow::datatypes::Schema::empty(),
            ));
            events.push(EventData::ArrowRecordBatch(empty_batch));
        }

        // Emit all parsed events (CSV files may produce multiple batches, or one empty batch).
        for event_data in events {
            let e = EventBuilder::new()
                .data(event_data)
                .subject(self.config.name.to_owned())
                .task_id(self.current_task_id)
                .task_type(self.task_type)
                .build()?;

            e.send_with_logging(self.tx.as_ref())
                .await
                .map_err(|e| Error::SendMessage { source: e })?;
        }

        Ok(())
    }
}

#[async_trait::async_trait]
impl flowgen_core::task::runner::Runner for JobRetrieve {
    type Error = Error;
    type EventHandler = EventHandler;

    /// Initializes HTTPS client and creates event handler.
    async fn init(&self) -> Result<EventHandler, Error> {
        let config = self.config.as_ref();

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
            current_task_id: self.current_task_id,
            tx: self.tx.clone(),
            client,
            sfdc_client: Arc::new(tokio::sync::Mutex::new(sfdc_client)),
            task_type: self.task_type,
        };
        Ok(event_handler)
    }

    /// Main execution loop: listen for events, filter by task ID, spawn handlers.
    #[tracing::instrument(skip(self), fields(task = %self.config.name, task_id = self.current_task_id, task_type = %self.task_type))]
    async fn run(mut self) -> Result<(), Self::Error> {
        let retry_config =
            flowgen_core::retry::RetryConfig::merge(&self.task_context.retry, &self.config.retry);

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

        loop {
            match self.rx.recv().await {
                Some(event) => {
                    if Some(event.task_id) == event_handler.current_task_id.checked_sub(1) {
                        let event_handler = Arc::clone(&event_handler);
                        let retry_strategy = retry_config.strategy();
                        tokio::spawn(
                            async move {
                                let result = tokio_retry::Retry::spawn(retry_strategy, || async {
                                    match event_handler.handle(event.clone()).await {
                                        Ok(result) => Ok(result),
                                        Err(e) => {
                                            error!("{}", e);
                                            Err(e)
                                        }
                                    }
                                })
                                .await;

                                if let Err(err) = result {
                                    error!(
                                        "{}",
                                        Error::RetryExhausted {
                                            source: Box::new(err)
                                        }
                                    );
                                }
                            }
                            .instrument(tracing::Span::current()),
                        );
                    }
                }
                None => return Ok(()),
            }
        }
    }
}

/// Builder for constructing JobRetrieve instances.
#[derive(Default)]
pub struct JobRetrieveBuilder {
    config: Option<Arc<super::config::JobRetrieve>>,
    tx: Option<Sender<Event>>,
    rx: Option<Receiver<Event>>,
    current_task_id: usize,
    task_context: Option<Arc<flowgen_core::task::context::TaskContext>>,
    task_type: Option<&'static str>,
}

impl JobRetrieveBuilder {
    /// Creates a new JobRetrieveBuilder with defaults.
    pub fn new() -> JobRetrieveBuilder {
        JobRetrieveBuilder {
            ..Default::default()
        }
    }

    /// Sets the job retriever configuration.
    pub fn config(mut self, config: Arc<super::config::JobRetrieve>) -> Self {
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

    /// Sets the task type identifier for event categorization.
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

    /// Builds JobRetrieve after validating required fields.
    pub async fn build(self) -> Result<JobRetrieve, Error> {
        Ok(JobRetrieve {
            config: self
                .config
                .ok_or_else(|| Error::MissingBuilderAttribute("config".to_string()))?,
            rx: self
                .rx
                .ok_or_else(|| Error::MissingBuilderAttribute("receiver".to_string()))?,
            tx: self.tx,
            current_task_id: self.current_task_id,
            task_context: self
                .task_context
                .ok_or_else(|| Error::MissingBuilderAttribute("task_context".to_string()))?,
            task_type: self
                .task_type
                .ok_or_else(|| Error::MissingBuilderAttribute("task_type".to_string()))?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{json, Map, Value};
    use std::path::PathBuf;
    use std::sync::Arc;
    use tokio::sync::mpsc;

    /// Creates a mock TaskContext for testing.
    fn create_mock_task_context() -> Arc<flowgen_core::task::context::TaskContext> {
        let mut labels = Map::new();
        labels.insert(
            "description".to_string(),
            Value::String("Clone Test".to_string()),
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

    #[test]
    fn test_error_display() {
        let err = Error::MissingBuilderAttribute("config".to_string());
        assert_eq!(
            err.to_string(),
            "Missing required builder attribute: config"
        );

        let err = Error::NoSalesforceAuthToken();
        assert_eq!(err.to_string(), "No salesforce access token provided");

        let err = Error::NoSalesforceInstanceURL();
        assert_eq!(err.to_string(), "No salesforce instance URL provided");

        let err = Error::MissingJobId();
        assert_eq!(err.to_string(), "Missing job ID in event data");
    }

    #[test]
    fn test_error_debug() {
        let err = Error::MissingBuilderAttribute("test".to_string());
        let debug_str = format!("{err:?}");
        assert!(debug_str.contains("MissingBuilderAttribute"));
    }

    #[tokio::test]
    async fn test_processor_builder_new() {
        let builder = JobRetrieveBuilder::new();
        assert!(builder.config.is_none());
        assert!(builder.tx.is_none());
        assert!(builder.rx.is_none());
        assert_eq!(builder.current_task_id, 0);
    }

    #[tokio::test]
    async fn test_processor_builder_config() {
        let config = Arc::new(super::super::config::JobRetrieve {
            name: "test_retriever".to_string(),
            credentials_path: PathBuf::from("/test/creds.json"),
            job_type: super::super::config::JobType::Query,
            job_id: "test_job_123".to_string(),
            batch_size: 10000,
            has_header: true,
            retry: None,
        });

        let builder = JobRetrieveBuilder::new().config(Arc::clone(&config));
        assert!(builder.config.is_some());
    }

    #[tokio::test]
    async fn test_processor_builder_channels() {
        let (tx, rx) = mpsc::channel::<Event>(100);

        let builder = JobRetrieveBuilder::new().sender(tx.clone()).receiver(rx);

        assert!(builder.tx.is_some());
        assert!(builder.rx.is_some());
    }

    #[tokio::test]
    async fn test_processor_builder_current_task_id() {
        let builder = JobRetrieveBuilder::new().current_task_id(5);
        assert_eq!(builder.current_task_id, 5);
    }

    #[tokio::test]
    async fn test_processor_builder_build_missing_config() {
        let (tx, rx) = mpsc::channel::<Event>(100);

        let builder = JobRetrieveBuilder::new().sender(tx).receiver(rx);

        let result = builder.build().await;
        assert!(result.is_err());

        match result {
            Err(Error::MissingBuilderAttribute(attr)) => {
                assert_eq!(attr, "config");
            }
            _ => panic!("Expected MissingBuilderAttribute error"),
        }
    }

    #[tokio::test]
    async fn test_processor_builder_build_missing_receiver() {
        let (tx, _) = mpsc::channel::<Event>(100);

        let config = Arc::new(super::super::config::JobRetrieve {
            name: "test".to_string(),
            credentials_path: PathBuf::from("/test.json"),
            job_type: super::super::config::JobType::Query,
            job_id: "test_job_456".to_string(),
            batch_size: 10000,
            has_header: true,
            retry: None,
        });

        let builder = JobRetrieveBuilder::new().config(config).sender(tx);

        let result = builder.build().await;
        assert!(result.is_err());

        match result {
            Err(Error::MissingBuilderAttribute(attr)) => {
                assert_eq!(attr, "receiver");
            }
            _ => panic!("Expected MissingBuilderAttribute error"),
        }
    }

    #[tokio::test]
    async fn test_processor_builder_build_missing_task_context() {
        let (_, rx) = mpsc::channel::<Event>(100);

        let config = Arc::new(super::super::config::JobRetrieve {
            name: "test".to_string(),
            credentials_path: PathBuf::from("/test.json"),
            job_type: super::super::config::JobType::Query,
            job_id: "test_job_789".to_string(),
            batch_size: 10000,
            has_header: true,
            retry: None,
        });

        let builder = JobRetrieveBuilder::new().config(config).receiver(rx);

        let result = builder.build().await;
        assert!(result.is_err());

        match result {
            Err(Error::MissingBuilderAttribute(attr)) => {
                assert_eq!(attr, "task_context");
            }
            _ => panic!("Expected MissingBuilderAttribute error"),
        }
    }

    #[test]
    fn test_error_from_conversions() {
        let service_err = Error::MissingBuilderAttribute("test".to_string());
        let _: Error = service_err;
    }

    #[tokio::test]
    async fn test_builder_default_trait() {
        let builder1 = JobRetrieveBuilder::new();
        let builder2 = JobRetrieveBuilder::default();

        assert_eq!(builder1.current_task_id, builder2.current_task_id);
    }

    #[tokio::test]
    async fn test_job_retriever_structure() {
        let (tx, rx) = mpsc::channel::<Event>(100);

        let config = Arc::new(super::super::config::JobRetrieve {
            name: "struct_test".to_string(),
            credentials_path: PathBuf::from("/test.json"),
            job_type: super::super::config::JobType::Query,
            job_id: "struct_test_job".to_string(),
            batch_size: 10000,
            has_header: true,
            retry: None,
        });

        let processor = JobRetrieve {
            config: Arc::clone(&config),
            tx: Some(tx.clone()),
            rx,
            current_task_id: 5,
            task_type: "",
            task_context: create_mock_task_context(),
        };

        assert_eq!(processor.current_task_id, 5);
        assert_eq!(processor.config.name, "struct_test");
    }

    #[test]
    fn test_uri_path_version() {
        assert!(super::super::config::DEFAULT_URI_PATH.contains("v65.0"));
        assert!(super::super::config::DEFAULT_URI_PATH.starts_with("/services/data/"));
        assert!(super::super::config::DEFAULT_URI_PATH.ends_with("/jobs/"));
    }

    #[test]
    fn test_job_id_extraction_from_json_object() {
        let json = json!({"id": "job_abc123"});
        let event = EventBuilder::new()
            .data(EventData::Json(json))
            .subject("test".to_string())
            .task_id(0)
            .task_type("test")
            .build()
            .unwrap();

        match event.data {
            EventData::Json(ref json_data) => {
                let job_id = json_data.get("id").and_then(|v| v.as_str());
                assert_eq!(job_id, Some("job_abc123"));
            }
            _ => panic!("Expected JSON data"),
        }
    }

    #[test]
    fn test_job_id_extraction_from_string() {
        let json = json!("job_xyz789");
        let event = EventBuilder::new()
            .data(EventData::Json(json))
            .subject("test".to_string())
            .task_id(0)
            .task_type("test")
            .build()
            .unwrap();

        match event.data {
            EventData::Json(ref json_data) => {
                let job_id = json_data.as_str();
                assert_eq!(job_id, Some("job_xyz789"));
            }
            _ => panic!("Expected JSON data"),
        }
    }
}

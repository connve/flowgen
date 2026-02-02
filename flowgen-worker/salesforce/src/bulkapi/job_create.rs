use flowgen_core::config::ConfigExt;
use flowgen_core::event::{Event, EventBuilder, EventData, EventExt};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::mpsc::{Receiver, Sender};
use tracing::{error, Instrument};

/// Errors for Salesforce bulk job creation operations.
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
    #[error("Salesforce API error: [{error_code}] {message}")]
    SalesforceApi { error_code: String, message: String },
    #[error("Task failed after all retry attempts: {source}")]
    RetryExhausted {
        #[source]
        source: Box<Error>,
    },
    #[error("Operation not implemented")]
    NotImplemented(),
    #[error("Failed to load resource: {source}")]
    ResourceLoad {
        #[source]
        source: flowgen_core::resource::Error,
    },
}

/// Salesforce API error response structure.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SalesforceErrorResponse {
    pub error_code: String,
    pub message: String,
}

/// Request payload for Salesforce bulk query job creation.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct QueryJobPayload {
    /// Type of operation (query or queryAll).
    operation: super::config::Operation,
    /// SOQL query string.
    query: Option<String>,
    /// Output file format (serialized as uppercase for API).
    #[serde(serialize_with = "serialize_content_type")]
    content_type: Option<super::config::ContentType>,
    /// CSV column delimiter (serialized as uppercase for API).
    #[serde(serialize_with = "serialize_column_delimiter")]
    column_delimiter: Option<super::config::ColumnDelimiter>,
    /// Line ending style (serialized as uppercase for API).
    #[serde(serialize_with = "serialize_line_ending")]
    line_ending: Option<super::config::LineEnding>,
}

/// Custom serializer for ContentType - converts to uppercase API format.
fn serialize_content_type<S>(
    content_type: &Option<super::config::ContentType>,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    match content_type {
        Some(ct) => serializer.serialize_str(ct.as_api_str()),
        None => serializer.serialize_none(),
    }
}

/// Custom serializer for ColumnDelimiter - converts to uppercase API format.
fn serialize_column_delimiter<S>(
    column_delimiter: &Option<super::config::ColumnDelimiter>,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    match column_delimiter {
        Some(cd) => serializer.serialize_str(cd.as_api_str()),
        None => serializer.serialize_none(),
    }
}

/// Custom serializer for LineEnding - converts to uppercase API format.
fn serialize_line_ending<S>(
    line_ending: &Option<super::config::LineEnding>,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    match line_ending {
        Some(le) => serializer.serialize_str(le.as_api_str()),
        None => serializer.serialize_none(),
    }
}

/// Processor for creating Salesforce bulk API jobs.
pub struct JobCreate {
    config: Arc<super::config::JobCreate>,
    tx: Option<Sender<Event>>,
    rx: Receiver<Event>,
    current_task_id: usize,
    task_type: &'static str,
    task_context: Arc<flowgen_core::task::context::TaskContext>,
}

/// Event handler for processing individual job creation requests.
pub struct EventHandler {
    /// HTTP client for Salesforce API requests.
    client: Arc<reqwest::Client>,
    /// Processor configuration.
    config: Arc<super::config::JobCreate>,
    /// Channel sender for emitting job creation responses.
    tx: Option<Sender<Event>>,
    /// Task identifier for event correlation.
    current_task_id: usize,
    /// Salesforce client for authentication and API access (wrapped in Mutex for token refresh).
    sfdc_client: Arc<tokio::sync::Mutex<salesforce_core::client::Client>>,
    /// Task type for event categorization and logging.
    task_type: &'static str,
    /// Optional resource loader for loading external query files.
    resource_loader: Option<flowgen_core::resource::ResourceLoader>,
}

impl EventHandler {
    /// Processes a job creation request: authenticate, build payload, create job, emit response.
    async fn handle(&self, event: Event) -> Result<(), Error> {
        // Render config to support templates in configuration fields.
        let event_value = serde_json::value::Value::try_from(&event)?;
        let config = self.config.render(&event_value)?;
        // Lock the Salesforce client to get token and instance URL.
        let sfdc_client = self.sfdc_client.lock().await;
        let access_token = sfdc_client.access_token().await?;
        let instance_url = sfdc_client
            .instance_url
            .clone()
            .ok_or_else(Error::NoSalesforceInstanceURL)?;
        drop(sfdc_client);

        // Resolve query from inline or resource source.
        let query_string = match &config.query {
            Some(flowgen_core::resource::Source::Inline(soql)) => Some(soql.clone()),
            Some(flowgen_core::resource::Source::Resource(key)) => {
                let loader = self
                    .resource_loader
                    .as_ref()
                    .ok_or_else(|| Error::ResourceLoad {
                        source: flowgen_core::resource::Error::ResourcePathNotConfigured,
                    })?;
                Some(
                    loader
                        .load(key)
                        .await
                        .map_err(|source| Error::ResourceLoad { source })?,
                )
            }
            None => None,
        };

        // Build API payload based on operation type.
        // Note: Custom serializers convert lowercase config values to uppercase API values.
        let payload = match config.operation {
            super::config::Operation::Query | super::config::Operation::QueryAll => {
                // Query operations require SOQL query and output format specs.
                QueryJobPayload {
                    operation: config.operation.clone(),
                    query: query_string,
                    content_type: config.content_type.clone(),
                    column_delimiter: config.column_delimiter.clone(),
                    line_ending: config.line_ending.clone(),
                }
            }
            _ => {
                // Insert, Update, Upsert, Delete, HardDelete operations not yet implemented.
                return Err(Error::NotImplemented());
            }
        };

        // Configure HTTP client with endpoint and auth.
        let mut client = self
            .client
            .post(instance_url + super::config::DEFAULT_URI_PATH + config.job_type.as_str());

        client = client.bearer_auth(access_token);
        client = client.json(&payload);

        // Execute API request and retrieve response.
        let resp = client
            .send()
            .await
            .map_err(|e| Error::Reqwest { source: e })?
            .json::<serde_json::Value>()
            .await
            .map_err(|e| Error::Reqwest { source: e })?;

        // Check if response is an error array (Salesforce returns errors as array).
        if let Some(errors_array) = resp.as_array() {
            if let Some(first_error) = errors_array.first() {
                if let Ok(error_response) =
                    serde_json::from_value::<SalesforceErrorResponse>(first_error.clone())
                {
                    return Err(Error::SalesforceApi {
                        error_code: error_response.error_code,
                        message: error_response.message,
                    });
                }
            }
        }

        // Response is valid data - create and emit event.
        let e = EventBuilder::new()
            .data(EventData::Json(resp))
            .subject(config.name.to_owned())
            .task_id(self.current_task_id)
            .task_type(self.task_type)
            .build()?;

        e.send_with_logging(self.tx.as_ref())
            .await
            .map_err(|e| Error::SendMessage { source: e })?;

        Ok(())
    }
}

#[async_trait::async_trait]
impl flowgen_core::task::runner::Runner for JobCreate {
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
            resource_loader: self.task_context.resource_loader.clone(),
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
                        let event_clone = event.clone();
                        tokio::spawn(
                            async move {
                                let result = tokio_retry::Retry::spawn(retry_strategy, || async {
                                    match event_handler.handle(event_clone.clone()).await {
                                        Ok(result) => Ok(result),
                                        Err(e) => {
                                            error!("{}", e);
                                            // If this is an INVALID_SESSION_ID error, reconnect before retrying.
                                            if let Error::SalesforceApi { ref error_code, .. } = e {
                                                if error_code == "INVALID_SESSION_ID" {
                                                    let mut sfdc_client =
                                                        event_handler.sfdc_client.lock().await;
                                                    if let Err(reconnect_err) =
                                                        (*sfdc_client).reconnect().await
                                                    {
                                                        error!(
                                                            "Failed to reconnect: {}",
                                                            reconnect_err
                                                        );
                                                        return Err(Error::SalesforceAuth(
                                                            reconnect_err,
                                                        ));
                                                    }
                                                }
                                            }
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

/// Builder for constructing JobCreate instances.
#[derive(Default)]
pub struct JobCreateBuilder {
    config: Option<Arc<super::config::JobCreate>>,
    tx: Option<Sender<Event>>,
    rx: Option<Receiver<Event>>,
    current_task_id: usize,
    task_context: Option<Arc<flowgen_core::task::context::TaskContext>>,
    task_type: Option<&'static str>,
}

impl JobCreateBuilder {
    /// Creates a new JobCreateBuilder with defaults.
    pub fn new() -> JobCreateBuilder {
        JobCreateBuilder {
            ..Default::default()
        }
    }

    /// Sets the job configuration.
    pub fn config(mut self, config: Arc<super::config::JobCreate>) -> Self {
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

    /// Builds JobCreate after validating required fields.
    pub async fn build(self) -> Result<JobCreate, Error> {
        Ok(JobCreate {
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
    use serde_json::json;
    use serde_json::{Map, Value};
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
            "contentType": "csv",
            "columnDelimiter": "comma",
            "lineEnding": "lf"
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
        let err = Error::MissingBuilderAttribute("config".to_string());
        assert_eq!(
            err.to_string(),
            "Missing required builder attribute: config"
        );

        let err = Error::NoSalesforceAuthToken();
        assert_eq!(err.to_string(), "No salesforce access token provided");

        let err = Error::NoSalesforceInstanceURL();
        assert_eq!(err.to_string(), "No salesforce instance URL provided");
    }

    #[test]
    fn test_error_debug() {
        let err = Error::MissingBuilderAttribute("test".to_string());
        let debug_str = format!("{err:?}");
        assert!(debug_str.contains("MissingBuilderAttribute"));
    }

    #[tokio::test]
    async fn test_processor_builder_new() {
        let builder = JobCreateBuilder::new();
        assert!(builder.config.is_none());
        assert!(builder.tx.is_none());
        assert!(builder.rx.is_none());
        assert_eq!(builder.current_task_id, 0);
    }

    #[tokio::test]
    async fn test_processor_builder_config() {
        let config = Arc::new(super::super::config::JobCreate {
            name: "test_job".to_string(),
            credentials_path: PathBuf::from("/test/creds.json"),
            query: Some(flowgen_core::resource::Source::Inline(
                "SELECT Id FROM Account".to_string(),
            )),
            object: None,
            operation: super::super::config::Operation::Query,
            job_type: super::super::config::JobType::Query,
            content_type: Some(super::super::config::ContentType::Csv),
            column_delimiter: Some(super::super::config::ColumnDelimiter::Comma),
            line_ending: Some(super::super::config::LineEnding::Lf),
            assignment_rule_id: None,
            external_id_field_name: None,
            retry: None,
        });

        let builder = JobCreateBuilder::new().config(Arc::clone(&config));
        assert!(builder.config.is_some());
    }

    #[tokio::test]
    async fn test_processor_builder_channels() {
        let (tx, rx) = mpsc::channel::<Event>(100);

        let builder = JobCreateBuilder::new().sender(tx.clone()).receiver(rx);

        assert!(builder.tx.is_some());
        assert!(builder.rx.is_some());
    }

    #[tokio::test]
    async fn test_processor_builder_current_task_id() {
        let builder = JobCreateBuilder::new().current_task_id(5);
        assert_eq!(builder.current_task_id, 5);
    }

    #[tokio::test]
    async fn test_processor_builder_build_missing_config() {
        let (tx, rx) = mpsc::channel::<Event>(100);

        let builder = JobCreateBuilder::new().sender(tx).receiver(rx);

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

        let config = Arc::new(super::super::config::JobCreate {
            name: "test".to_string(),
            credentials_path: PathBuf::from("/test.json"),
            query: Some(flowgen_core::resource::Source::Inline(
                "SELECT Id FROM Account".to_string(),
            )),
            job_type: super::super::config::JobType::Query,
            object: None,
            operation: super::super::config::Operation::Query,
            content_type: None,
            column_delimiter: None,
            line_ending: None,
            assignment_rule_id: None,
            external_id_field_name: None,
            retry: None,
        });

        let builder = JobCreateBuilder::new().config(config).sender(tx);

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

        let config = Arc::new(super::super::config::JobCreate {
            name: "test".to_string(),
            credentials_path: PathBuf::from("/test.json"),
            query: Some(flowgen_core::resource::Source::Inline(
                "SELECT Id FROM Account".to_string(),
            )),
            job_type: super::super::config::JobType::Query,
            object: None,
            operation: super::super::config::Operation::Query,
            content_type: None,
            column_delimiter: None,
            line_ending: None,
            assignment_rule_id: None,
            external_id_field_name: None,
            retry: None,
        });

        let builder = JobCreateBuilder::new().config(config).receiver(rx);

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
        let service_err = Error::MissingBuilderAttribute("test".to_string());
        let _: Error = service_err;

        fn _test_conversions() {
            let _: Error = Error::MissingBuilderAttribute("x".to_string());
        }
    }

    #[tokio::test]
    async fn test_builder_default_trait() {
        let builder1 = JobCreateBuilder::new();
        let builder2 = JobCreateBuilder::default();

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
        let (tx, rx) = mpsc::channel::<Event>(100);

        let config = Arc::new(super::super::config::JobCreate {
            name: "struct_test".to_string(),
            credentials_path: PathBuf::from("/test.json"),
            query: Some(flowgen_core::resource::Source::Inline(
                "SELECT Id FROM Account".to_string(),
            )),
            job_type: super::super::config::JobType::Query,
            object: None,
            operation: super::super::config::Operation::Query,
            content_type: None,
            column_delimiter: None,
            line_ending: None,
            assignment_rule_id: None,
            external_id_field_name: None,
            retry: None,
        });

        let processor = JobCreate {
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
}

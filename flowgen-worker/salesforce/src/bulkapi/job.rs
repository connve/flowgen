//! Unified Salesforce Bulk API job processor.
//!
//! Handles both create and retrieve operations through a single processor
//! with operation-specific logic in the handle method.

use flowgen_core::buffer::{ContentType, FromReader};
use flowgen_core::config::ConfigExt;
use flowgen_core::event::{Event, EventBuilder, EventData, EventExt};
use serde::{Deserialize, Serialize};
use std::io::Cursor;
use std::sync::Arc;
use tokio::sync::mpsc::{Receiver, Sender};
use tracing::{error, Instrument};

/// Errors for Salesforce bulk job operations.
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
    NoSalesforceAuthToken,
    #[error("No salesforce instance URL provided")]
    NoSalesforceInstanceURL,
    #[error("Salesforce API error: [{error_code}] {message}")]
    SalesforceApi { error_code: String, message: String },
    #[error("Task failed after all retry attempts: {source}")]
    RetryExhausted {
        #[source]
        source: Box<Error>,
    },
    #[error("Operation not implemented")]
    NotImplemented,
    #[error("Failed to load resource: {source}")]
    ResourceLoad {
        #[source]
        source: flowgen_core::resource::Error,
    },
    #[error("Retrieve operation requires job_id")]
    MissingJobIdForRetrieve,
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
    operation: super::config::Operation,
    query: Option<String>,
    #[serde(serialize_with = "serialize_content_type")]
    content_type: Option<super::config::ContentType>,
    #[serde(serialize_with = "serialize_column_delimiter")]
    column_delimiter: Option<super::config::ColumnDelimiter>,
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

/// Event handler for processing individual job operation requests.
pub struct EventHandler {
    client: Arc<reqwest::Client>,
    config: Arc<super::config::Job>,
    tx: Option<Sender<Event>>,
    current_task_id: usize,
    sfdc_client: Arc<tokio::sync::Mutex<salesforce_core::client::Client>>,
    task_type: &'static str,
    resource_loader: Option<flowgen_core::resource::ResourceLoader>,
}

impl EventHandler {
    async fn handle(&self, event: Event) -> Result<(), Error> {
        let event_value = serde_json::value::Value::try_from(&event)?;
        let config = self.config.render(&event_value)?;

        // Execute operation based on type.
        match config.operation {
            super::config::JobOperation::Create => self.create_job(&config, &event_value).await?,
            super::config::JobOperation::Retrieve => self.retrieve_job(&config).await?,
        }

        Ok(())
    }

    /// Creates a Salesforce bulk API job.
    async fn create_job(
        &self,
        config: &super::config::Job,
        event_value: &serde_json::Value,
    ) -> Result<(), Error> {
        let sfdc_client = self.sfdc_client.lock().await;
        let access_token = sfdc_client.access_token().await?;
        let instance_url = sfdc_client
            .instance_url
            .clone()
            .ok_or(Error::NoSalesforceInstanceURL)?;
        drop(sfdc_client);

        // Resolve query from inline or resource source.
        let query_string = match &config.query {
            Some(flowgen_core::resource::Source::Inline(soql)) => Some(soql.clone()),
            Some(flowgen_core::resource::Source::Resource { resource }) => {
                let loader = self
                    .resource_loader
                    .as_ref()
                    .ok_or_else(|| Error::ResourceLoad {
                        source: flowgen_core::resource::Error::ResourcePathNotConfigured,
                    })?;
                let template = loader
                    .load(resource)
                    .await
                    .map_err(|source| Error::ResourceLoad { source })?;
                let rendered = flowgen_core::config::render_template(&template, event_value)?;
                Some(rendered)
            }
            None => None,
        };

        // Build API payload based on operation type.
        let operation_type = config.operation_type.clone().unwrap_or_default();
        let payload = match operation_type {
            super::config::Operation::Query | super::config::Operation::QueryAll => {
                QueryJobPayload {
                    operation: operation_type,
                    query: query_string,
                    content_type: config.content_type.clone(),
                    column_delimiter: config.column_delimiter.clone(),
                    line_ending: config.line_ending.clone(),
                }
            }
            _ => return Err(Error::NotImplemented),
        };

        // Configure HTTP client with endpoint and auth.
        let mut client = self
            .client
            .post(instance_url + super::config::DEFAULT_URI_PATH + config.job_type.as_str());

        client = client.bearer_auth(access_token);
        client = client.json(&payload);

        // Execute API request.
        let resp = client
            .send()
            .await
            .map_err(|e| Error::Reqwest { source: e })?
            .json::<serde_json::Value>()
            .await
            .map_err(|e| Error::Reqwest { source: e })?;

        // Check if response is an error array.
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

        // Emit event with job response.
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

    /// Retrieves Salesforce bulk API job results.
    async fn retrieve_job(&self, config: &super::config::Job) -> Result<(), Error> {
        let sfdc_client = self.sfdc_client.lock().await;
        let access_token = sfdc_client.access_token().await?;
        let instance_url = sfdc_client
            .instance_url
            .clone()
            .ok_or(Error::NoSalesforceInstanceURL)?;
        drop(sfdc_client);

        let job_id = config
            .job_id
            .as_ref()
            .ok_or(Error::MissingJobIdForRetrieve)?;

        // Build API endpoint URL for downloading results.
        let url = format!(
            "{}{}{}/{}/results",
            instance_url,
            super::config::DEFAULT_URI_PATH,
            config.job_type.as_str(),
            job_id
        );

        // Configure HTTP client with endpoint and auth.
        let mut client = self.client.get(&url);
        client = client.bearer_auth(access_token);

        // Download CSV results.
        let response = client
            .send()
            .await
            .map_err(|e| Error::Reqwest { source: e })?;

        let csv_data = response
            .text()
            .await
            .map_err(|e| Error::Reqwest { source: e })?;

        // Check if response is a JSON error.
        if csv_data.trim_start().starts_with('[') || csv_data.trim_start().starts_with('{') {
            if let Ok(json_value) = serde_json::from_str::<serde_json::Value>(&csv_data) {
                if let Some(errors_array) = json_value.as_array() {
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
            }
        }

        // Parse CSV using FromReader trait.
        let cursor = Cursor::new(csv_data.as_bytes());
        let content_type = ContentType::Csv {
            batch_size: config.batch_size,
            has_header: config.has_header,
            delimiter: None,
        };

        let mut events = EventData::from_reader(cursor, content_type)?;

        // If no data rows, create empty batch.
        if events.is_empty() {
            let empty_batch = arrow::record_batch::RecordBatch::new_empty(std::sync::Arc::new(
                arrow::datatypes::Schema::empty(),
            ));
            events.push(EventData::ArrowRecordBatch(empty_batch));
        }

        // Emit all parsed events.
        for event_data in events {
            let e = EventBuilder::new()
                .data(event_data)
                .subject(config.name.to_owned())
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

/// Unified Salesforce Bulk API job processor.
#[derive(Debug)]
pub struct Processor {
    config: Arc<super::config::Job>,
    rx: Receiver<Event>,
    tx: Option<Sender<Event>>,
    task_id: usize,
    task_context: Arc<flowgen_core::task::context::TaskContext>,
    task_type: &'static str,
}

#[async_trait::async_trait]
impl flowgen_core::task::runner::Runner for Processor {
    type Error = Error;
    type EventHandler = EventHandler;

    async fn init(&self) -> Result<EventHandler, Error> {
        let client = reqwest::ClientBuilder::new()
            .https_only(true)
            .build()
            .map_err(|e| Error::Reqwest { source: e })?;
        let client = Arc::new(client);

        let sfdc_client = salesforce_core::client::Builder::new()
            .credentials_path(self.config.credentials_path.clone())
            .build()?
            .connect()
            .await?;

        let event_handler = EventHandler {
            config: Arc::clone(&self.config),
            current_task_id: self.task_id,
            tx: self.tx.clone(),
            client,
            sfdc_client: Arc::new(tokio::sync::Mutex::new(sfdc_client)),
            task_type: self.task_type,
            resource_loader: self.task_context.resource_loader.clone(),
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
                                            let needs_reconnect = match &e {
                                                Error::SalesforceApi { error_code, .. } => {
                                                    error_code == "INVALID_SESSION_ID"
                                                }
                                                Error::SalesforceAuth(auth_err) => matches!(
                                                    auth_err,
                                                    salesforce_core::client::Error::NoRefreshToken
                                                ),
                                                _ => false,
                                            };

                                            if needs_reconnect {
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

/// Builder for creating Processor instances.
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

        let err = Error::NoSalesforceAuthToken;
        assert!(matches!(err, Error::NoSalesforceAuthToken));

        let err = Error::MissingJobIdForRetrieve;
        assert!(matches!(err, Error::MissingJobIdForRetrieve));

        let salesforce_err = Error::SalesforceApi {
            error_code: "INVALID_QUERY".to_string(),
            message: "Query syntax error".to_string(),
        };
        assert!(matches!(salesforce_err, Error::SalesforceApi { .. }));

        let no_token_err = Error::NoSalesforceAuthToken;
        assert!(matches!(no_token_err, Error::NoSalesforceAuthToken));

        let no_instance_err = Error::NoSalesforceInstanceURL;
        assert!(matches!(no_instance_err, Error::NoSalesforceInstanceURL));
    }

    #[test]
    fn test_salesforce_error_response_deserialization() {
        let json = r#"{
            "errorCode": "INVALID_JOB",
            "message": "Job not found"
        }"#;
        let error: SalesforceErrorResponse = serde_json::from_str(json).unwrap();
        assert_eq!(error.error_code, "INVALID_JOB");
        assert_eq!(error.message, "Job not found");
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
    async fn test_processor_builder_config_create() {
        let config = Arc::new(super::super::config::Job {
            name: "test_job".to_string(),
            operation: super::super::config::JobOperation::Create,
            credentials_path: PathBuf::from("/test/creds.json"),
            job_type: super::super::config::JobType::Query,
            query: Some(flowgen_core::resource::Source::Inline(
                "SELECT Id FROM Account".to_string(),
            )),
            object: None,
            operation_type: Some(super::super::config::Operation::Query),
            content_type: Some(super::super::config::ContentType::Csv),
            column_delimiter: Some(super::super::config::ColumnDelimiter::Comma),
            line_ending: Some(super::super::config::LineEnding::Lf),
            assignment_rule_id: None,
            external_id_field_name: None,
            job_id: None,
            batch_size: 10000,
            has_header: true,
            retry: None,
        });

        let builder = ProcessorBuilder::new().config(config.clone());
        assert!(builder.config.is_some());
        assert_eq!(builder.config.unwrap().name, "test_job");
    }

    #[tokio::test]
    async fn test_processor_builder_config_retrieve() {
        let config = Arc::new(super::super::config::Job {
            name: "test_retrieve".to_string(),
            operation: super::super::config::JobOperation::Retrieve,
            credentials_path: PathBuf::from("/test/creds.json"),
            job_type: super::super::config::JobType::Query,
            query: None,
            object: None,
            operation_type: None,
            content_type: None,
            column_delimiter: None,
            line_ending: None,
            assignment_rule_id: None,
            external_id_field_name: None,
            job_id: Some("job_abc123".to_string()),
            batch_size: 5000,
            has_header: false,
            retry: None,
        });

        let builder = ProcessorBuilder::new().config(config.clone());
        assert!(builder.config.is_some());
        let built_config = builder.config.unwrap();
        assert_eq!(built_config.name, "test_retrieve");
        assert_eq!(built_config.batch_size, 5000);
        assert!(!built_config.has_header);
    }

    #[tokio::test]
    async fn test_processor_builder_task_id() {
        let builder = ProcessorBuilder::new().task_id(99);
        assert_eq!(builder.task_id, Some(99));
    }

    #[tokio::test]
    async fn test_processor_builder_task_type() {
        let builder = ProcessorBuilder::new().task_type("salesforce_bulkapi_job");
        assert_eq!(builder.task_type, Some("salesforce_bulkapi_job"));
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
            .task_type("salesforce_bulkapi_job")
            .build()
            .await;

        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            Error::MissingBuilderAttribute(_)
        ));
    }

    #[tokio::test]
    async fn test_processor_builder_missing_task_id() {
        let config = Arc::new(super::super::config::Job {
            name: "test_job".to_string(),
            operation: super::super::config::JobOperation::Create,
            credentials_path: PathBuf::from("/test/creds.json"),
            job_type: super::super::config::JobType::Query,
            query: Some(flowgen_core::resource::Source::Inline(
                "SELECT Id FROM Account".to_string(),
            )),
            object: None,
            operation_type: Some(super::super::config::Operation::Query),
            content_type: Some(super::super::config::ContentType::Csv),
            column_delimiter: Some(super::super::config::ColumnDelimiter::Comma),
            line_ending: Some(super::super::config::LineEnding::Lf),
            assignment_rule_id: None,
            external_id_field_name: None,
            job_id: None,
            batch_size: 10000,
            has_header: true,
            retry: None,
        });
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
            .config(config)
            .receiver(rx)
            .sender(tx)
            .task_context(task_context)
            .task_type("salesforce_bulkapi_job")
            .build()
            .await;

        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            Error::MissingBuilderAttribute(_)
        ));
    }

    #[test]
    fn test_job_operation_serialization() {
        let create_op = super::super::config::JobOperation::Create;
        let json = serde_json::to_string(&create_op).unwrap();
        assert_eq!(json, "\"create\"");

        let retrieve_op = super::super::config::JobOperation::Retrieve;
        let json = serde_json::to_string(&retrieve_op).unwrap();
        assert_eq!(json, "\"retrieve\"");
    }

    #[test]
    fn test_job_type_serialization() {
        let query_type = super::super::config::JobType::Query;
        let json = serde_json::to_string(&query_type).unwrap();
        assert_eq!(json, "\"query\"");

        let ingest_type = super::super::config::JobType::Ingest;
        let json = serde_json::to_string(&ingest_type).unwrap();
        assert_eq!(json, "\"ingest\"");
    }

    #[test]
    fn test_operation_type_serialization() {
        let query = super::super::config::Operation::Query;
        assert_eq!(serde_json::to_string(&query).unwrap(), "\"query\"");

        let query_all = super::super::config::Operation::QueryAll;
        assert_eq!(serde_json::to_string(&query_all).unwrap(), "\"queryAll\"");

        let insert = super::super::config::Operation::Insert;
        assert_eq!(serde_json::to_string(&insert).unwrap(), "\"insert\"");

        let delete = super::super::config::Operation::Delete;
        assert_eq!(serde_json::to_string(&delete).unwrap(), "\"delete\"");

        let hard_delete = super::super::config::Operation::HardDelete;
        assert_eq!(
            serde_json::to_string(&hard_delete).unwrap(),
            "\"hardDelete\""
        );

        let update = super::super::config::Operation::Update;
        assert_eq!(serde_json::to_string(&update).unwrap(), "\"update\"");

        let upsert = super::super::config::Operation::Upsert;
        assert_eq!(serde_json::to_string(&upsert).unwrap(), "\"upsert\"");
    }

    #[test]
    fn test_content_type_api_str() {
        let csv = super::super::config::ContentType::Csv;
        assert_eq!(csv.as_api_str(), "CSV");
    }

    #[test]
    fn test_column_delimiter_api_str() {
        assert_eq!(
            super::super::config::ColumnDelimiter::Comma.as_api_str(),
            "COMMA"
        );
        assert_eq!(
            super::super::config::ColumnDelimiter::Tab.as_api_str(),
            "TAB"
        );
        assert_eq!(
            super::super::config::ColumnDelimiter::Semicolon.as_api_str(),
            "SEMICOLON"
        );
        assert_eq!(
            super::super::config::ColumnDelimiter::Pipe.as_api_str(),
            "PIPE"
        );
        assert_eq!(
            super::super::config::ColumnDelimiter::Caret.as_api_str(),
            "CARET"
        );
        assert_eq!(
            super::super::config::ColumnDelimiter::Backquote.as_api_str(),
            "BACKQUOTE"
        );
    }

    #[test]
    fn test_line_ending_api_str() {
        assert_eq!(super::super::config::LineEnding::Lf.as_api_str(), "LF");
        assert_eq!(super::super::config::LineEnding::Crlf.as_api_str(), "CRLF");
    }
}

//! Salesforce Bulk API Query Job processor.
//!
//! Handles query job operations: create, get, delete, abort, and get_results.

use flowgen_core::buffer::{ContentType, FromReader};
use flowgen_core::config::ConfigExt;
use flowgen_core::event::{Event, EventBuilder, EventData, EventExt};
use salesforce_core::bulkapi::{
    ColumnDelimiter as SdkColumnDelimiter, ContentType as SdkContentType, CreateQueryJobRequest,
    LineEnding as SdkLineEnding, QueryOperation as SdkQueryOperation,
};
use std::io::Cursor;
use std::sync::Arc;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio_stream::StreamExt;
use tracing::{error, Instrument};

/// Errors for Salesforce bulk query job operations.
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
    SalesforceAuth(#[from] salesforce_core::client::Error),
    #[error("Bulk API query error: {source}")]
    BulkApiQuery {
        #[source]
        source: Box<salesforce_core::bulkapi::QueryError>,
    },
    #[error("Stream error: {source}")]
    Stream {
        #[source]
        source: Box<reqwest::Error>,
    },
    #[error("Serialization error: {source}")]
    SerdeExt {
        #[source]
        source: flowgen_core::serde::Error,
    },
    #[error(transparent)]
    EventError(#[from] flowgen_core::event::Error),
    #[error(transparent)]
    ConfigRender(#[from] flowgen_core::config::Error),
    #[error("Task failed after all retry attempts: {source}")]
    RetryExhausted {
        #[source]
        source: Box<Error>,
    },
    #[error("Failed to load resource: {source}")]
    ResourceLoad {
        #[source]
        source: flowgen_core::resource::Error,
    },
    #[error("Operation requires job_id")]
    MissingJobId,
    #[error("Create operation requires query")]
    MissingQuery,
}

/// Converts config QueryOperation to SDK QueryOperation.
fn to_sdk_query_operation(op: &super::config::QueryOperation) -> SdkQueryOperation {
    match op {
        super::config::QueryOperation::Query => SdkQueryOperation::Query,
        super::config::QueryOperation::QueryAll => SdkQueryOperation::QueryAll,
    }
}

/// Converts config ContentType to SDK ContentType.
fn to_sdk_content_type(ct: &super::config::ContentType) -> Option<SdkContentType> {
    Some(match ct {
        super::config::ContentType::Csv => SdkContentType::Csv,
    })
}

/// Converts config ColumnDelimiter to SDK ColumnDelimiter.
fn to_sdk_column_delimiter(cd: &super::config::ColumnDelimiter) -> Option<SdkColumnDelimiter> {
    Some(match cd {
        super::config::ColumnDelimiter::Comma => SdkColumnDelimiter::Comma,
        super::config::ColumnDelimiter::Tab => SdkColumnDelimiter::Tab,
        super::config::ColumnDelimiter::Semicolon => SdkColumnDelimiter::Semicolon,
        super::config::ColumnDelimiter::Pipe => SdkColumnDelimiter::Pipe,
        super::config::ColumnDelimiter::Caret => SdkColumnDelimiter::Caret,
        super::config::ColumnDelimiter::Backquote => SdkColumnDelimiter::Backquote,
    })
}

/// Converts config LineEnding to SDK LineEnding.
fn to_sdk_line_ending(le: &super::config::LineEnding) -> Option<SdkLineEnding> {
    Some(match le {
        super::config::LineEnding::Lf => SdkLineEnding::Lf,
        super::config::LineEnding::Crlf => SdkLineEnding::Crlf,
    })
}

/// Event handler for processing individual query job operation requests.
pub struct EventHandler {
    client: Arc<salesforce_core::bulkapi::Client>,
    config: Arc<super::config::QueryJob>,
    tx: Option<Sender<Event>>,
    current_task_id: usize,
    sfdc_client: Arc<tokio::sync::Mutex<salesforce_core::client::Client>>,
    task_type: &'static str,
    resource_loader: Option<flowgen_core::resource::ResourceLoader>,
}

impl EventHandler {
    async fn handle(&self, event: Event) -> Result<(), Error> {
        // Run handler with event context for automatic meta preservation
        let event = Arc::new(event);

        flowgen_core::event::with_event_context(&Arc::clone(&event), async move {
            let event_value = serde_json::value::Value::try_from(event.as_ref())?;
            let config = self.config.render(&event_value)?;

            // Execute operation based on type.
            match config.operation {
                super::config::QueryJobOperation::Create => {
                    self.create_job(&config, &event_value).await?
                }
                super::config::QueryJobOperation::Get => self.get_job(&config).await?,
                super::config::QueryJobOperation::Delete => self.delete_job(&config).await?,
                super::config::QueryJobOperation::Abort => self.abort_job(&config).await?,
                super::config::QueryJobOperation::GetResults => self.get_results(&config).await?,
            }

            Ok(())
        })
        .await
    }

    /// Creates a Salesforce bulk query job using the SDK.
    async fn create_job(
        &self,
        config: &super::config::QueryJob,
        event_value: &serde_json::Value,
    ) -> Result<(), Error> {
        // Resolve query from inline or resource source.
        let query_string = match &config.query {
            Some(flowgen_core::resource::Source::Inline(soql)) => soql.clone(),
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
                flowgen_core::config::render_template(&template, event_value)?
            }
            None => return Err(Error::MissingQuery),
        };

        // Build SDK request.
        let request = CreateQueryJobRequest {
            operation: config
                .query_operation
                .as_ref()
                .map(to_sdk_query_operation)
                .unwrap_or(SdkQueryOperation::Query),
            query: query_string,
            content_type: config.content_type.as_ref().and_then(to_sdk_content_type),
            column_delimiter: config
                .column_delimiter
                .as_ref()
                .and_then(to_sdk_column_delimiter),
            line_ending: config.line_ending.as_ref().and_then(to_sdk_line_ending),
        };

        // Create job using SDK.
        let query_client = self.client.query();
        let job_info =
            query_client
                .create_job(&request)
                .await
                .map_err(|e| Error::BulkApiQuery {
                    source: Box::new(e),
                })?;

        // Serialize the full QueryJobInfo payload to JSON.
        let resp = serde_json::to_value(&job_info).map_err(|e| Error::SerdeExt {
            source: flowgen_core::serde::Error::Serde { source: e },
        })?;

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

    /// Gets a Salesforce bulk query job status and metadata using the SDK.
    async fn get_job(&self, config: &super::config::QueryJob) -> Result<(), Error> {
        let job_id = config.job_id.as_ref().ok_or(Error::MissingJobId)?;

        // Get job info using SDK.
        let query_client = self.client.query();
        let job_info = query_client
            .get_job(job_id)
            .await
            .map_err(|e| Error::BulkApiQuery {
                source: Box::new(e),
            })?;

        // Serialize the full QueryJobInfo payload to JSON.
        let resp = serde_json::to_value(&job_info).map_err(|e| Error::SerdeExt {
            source: flowgen_core::serde::Error::Serde { source: e },
        })?;

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

    /// Deletes a Salesforce bulk query job using the SDK.
    async fn delete_job(&self, config: &super::config::QueryJob) -> Result<(), Error> {
        let job_id = config.job_id.as_ref().ok_or(Error::MissingJobId)?;

        // Delete job using SDK.
        let query_client = self.client.query();
        query_client
            .delete_job(job_id)
            .await
            .map_err(|e| Error::BulkApiQuery {
                source: Box::new(e),
            })?;

        // Emit confirmation event.
        let resp = serde_json::json!({
            "job_id": job_id,
            "deleted": true
        });

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

    /// Aborts a running Salesforce bulk query job using the SDK.
    async fn abort_job(&self, config: &super::config::QueryJob) -> Result<(), Error> {
        let job_id = config.job_id.as_ref().ok_or(Error::MissingJobId)?;

        // Abort job using SDK.
        let query_client = self.client.query();
        let job_info = query_client
            .abort_job(job_id)
            .await
            .map_err(|e| Error::BulkApiQuery {
                source: Box::new(e),
            })?;

        // Serialize the full QueryJobInfo payload to JSON.
        let resp = serde_json::to_value(&job_info).map_err(|e| Error::SerdeExt {
            source: flowgen_core::serde::Error::Serde { source: e },
        })?;

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

    /// Retrieves Salesforce bulk query job results using the SDK.
    async fn get_results(&self, config: &super::config::QueryJob) -> Result<(), Error> {
        let job_id = config.job_id.as_ref().ok_or(Error::MissingJobId)?;

        // Get results using SDK.
        let query_client = self.client.query();
        let mut byte_stream = query_client
            .get_results(job_id, None, None)
            .await
            .map_err(|e| Error::BulkApiQuery {
                source: Box::new(e),
            })?;

        // Collect the stream into bytes.
        let mut csv_bytes = Vec::new();
        while let Some(chunk) = byte_stream.next().await {
            let chunk = chunk.map_err(|e| Error::Stream {
                source: Box::new(e),
            })?;
            csv_bytes.extend_from_slice(&chunk);
        }

        // Parse CSV using FromReader trait.
        let cursor = Cursor::new(csv_bytes);
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

/// Salesforce Bulk API Query Job processor.
#[derive(Debug)]
pub struct Processor {
    config: Arc<super::config::QueryJob>,
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
        let sfdc_client = salesforce_core::client::Builder::new()
            .credentials_path(self.config.credentials_path.clone())
            .build()?
            .connect()
            .await?;

        // Create Bulk API client using Builder with default API version.
        let bulk_client = salesforce_core::bulkapi::ClientBuilder::new(sfdc_client.clone()).build();

        let event_handler = EventHandler {
            config: Arc::clone(&self.config),
            current_task_id: self.task_id,
            tx: self.tx.clone(),
            client: Arc::new(bulk_client),
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
                                            // Check if reconnect is needed for auth errors.
                                            let needs_reconnect = matches!(
                                                &e,
                                                Error::SalesforceAuth(
                                                    salesforce_core::client::Error::NoRefreshToken
                                                ) | Error::BulkApiQuery { .. }
                                            );

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
    config: Option<Arc<super::config::QueryJob>>,
    rx: Option<Receiver<Event>>,
    tx: Option<Sender<Event>>,
    task_id: Option<usize>,
    task_context: Option<Arc<flowgen_core::task::context::TaskContext>>,
    task_type: Option<&'static str>,
}

impl Default for ProcessorBuilder {
    fn default() -> Self {
        Self::new()
    }
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

    pub fn config(mut self, config: Arc<super::config::QueryJob>) -> Self {
        self.config = Some(config);
        self
    }

    pub fn receiver(mut self, receiver: Receiver<Event>) -> Self {
        self.rx = Some(receiver);
        self
    }

    pub fn sender(mut self, sender: Sender<Event>) -> Self {
        self.tx = Some(sender);
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

    pub fn build(self) -> Result<Processor, Error> {
        Ok(Processor {
            config: self
                .config
                .ok_or_else(|| Error::MissingBuilderAttribute("config".to_string()))?,
            rx: self
                .rx
                .ok_or_else(|| Error::MissingBuilderAttribute("receiver".to_string()))?,
            tx: self.tx,
            task_id: self.task_id.unwrap_or(0),
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
    use std::path::PathBuf;

    #[test]
    fn test_error_types() {
        let err = Error::MissingBuilderAttribute("config".to_string());
        assert!(matches!(err, Error::MissingBuilderAttribute(_)));

        let err = Error::MissingJobId;
        assert!(matches!(err, Error::MissingJobId));

        let err = Error::MissingQuery;
        assert!(matches!(err, Error::MissingQuery));
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
        let config = Arc::new(super::super::config::QueryJob {
            name: "test_query".to_string(),
            credentials_path: PathBuf::from("/test/creds.json"),
            operation: super::super::config::QueryJobOperation::Create,
            query: Some(flowgen_core::resource::Source::Inline(
                "SELECT Id FROM Account".to_string(),
            )),
            query_operation: Some(super::super::config::QueryOperation::Query),
            content_type: Some(super::super::config::ContentType::Csv),
            column_delimiter: Some(super::super::config::ColumnDelimiter::Comma),
            line_ending: Some(super::super::config::LineEnding::Lf),
            job_id: None,
            batch_size: 5000,
            has_header: true,
            retry: None,
        });

        let builder = ProcessorBuilder::new().config(config.clone());
        assert!(builder.config.is_some());
    }

    #[tokio::test]
    async fn test_processor_builder_task_id() {
        let builder = ProcessorBuilder::new().task_id(42);
        assert_eq!(builder.task_id, Some(42));
    }

    #[tokio::test]
    async fn test_processor_builder_task_type() {
        let builder = ProcessorBuilder::new().task_type("salesforce_bulkapi_query_job");
        assert_eq!(builder.task_type, Some("salesforce_bulkapi_query_job"));
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
            .task_type("salesforce_bulkapi_query_job")
            .build();

        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            Error::MissingBuilderAttribute(_)
        ));
    }

    #[tokio::test]
    async fn test_processor_builder_missing_receiver() {
        let config = Arc::new(super::super::config::QueryJob {
            name: "test_query".to_string(),
            credentials_path: PathBuf::from("/test/creds.json"),
            operation: super::super::config::QueryJobOperation::Create,
            query: Some(flowgen_core::resource::Source::Inline(
                "SELECT Id FROM Account".to_string(),
            )),
            query_operation: Some(super::super::config::QueryOperation::Query),
            content_type: Some(super::super::config::ContentType::Csv),
            column_delimiter: Some(super::super::config::ColumnDelimiter::Comma),
            line_ending: Some(super::super::config::LineEnding::Lf),
            job_id: None,
            batch_size: 5000,
            has_header: true,
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
            .task_type("salesforce_bulkapi_query_job")
            .build();

        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            Error::MissingBuilderAttribute(_)
        ));
    }
}

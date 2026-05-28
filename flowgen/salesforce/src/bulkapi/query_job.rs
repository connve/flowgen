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
use serde::{Deserialize, Serialize};
use std::io::Cursor;
use std::sync::Arc;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio_stream::StreamExt;
use tracing::{error, Instrument};

/// Response for delete job operation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeleteJobResponse {
    /// The ID of the deleted job.
    pub job_id: String,
    /// Confirmation that the job was deleted.
    pub deleted: bool,
}

/// Errors for Salesforce bulk query job operations.
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum Error {
    #[error("Error sending event message: {source}")]
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
    #[error("Arrow error: {source}")]
    Arrow {
        #[source]
        source: arrow::error::ArrowError,
    },
    #[error(transparent)]
    ConfigRender(#[from] flowgen_core::config::Error),
    #[error("Task failed after all retry attempts: {source}")]
    RetryExhausted {
        #[source]
        source: Box<Error>,
    },
    #[error("Error loading resource: {source}")]
    ResourceLoad {
        #[source]
        source: flowgen_core::resource::Error,
    },
    #[error("Operation requires job_id")]
    MissingJobId,
    #[error("Create operation requires query")]
    MissingQuery,
    #[error("Failed to build Bulk API client: {source}")]
    BulkClientBuild {
        #[source]
        source: salesforce_core::bulkapi::ClientError,
    },
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
    task_context: Arc<flowgen_core::task::context::TaskContext>,
}

impl EventHandler {
    /// Attaches the completion signal to the event and sends it downstream.
    async fn send_final_event(
        &self,
        mut event: Event,
        completion_tx_arc: &Option<flowgen_core::event::SharedCompletionTx>,
    ) -> Result<(), Error> {
        match self.tx {
            Some(_) => {
                event.completion_tx = completion_tx_arc.clone();
            }
            None => {
                if let Some(arc) = completion_tx_arc.as_ref() {
                    arc.signal_completion(event.data_as_json().ok());
                }
            }
        }
        event
            .send_with_logging(self.tx.as_ref())
            .await
            .map_err(|e| Error::SendMessage { source: e })?;
        Ok(())
    }

    #[tracing::instrument(skip(self, event), name = "task.handle")]
    async fn handle(&self, event: Event) -> Result<(), Error> {
        if self.task_context.cancellation_token.is_cancelled() {
            return Ok(());
        }

        // Run handler with event context for automatic meta preservation.
        let event = Arc::new(event);
        let completion_tx_arc = Arc::clone(&event).completion_tx.clone();

        flowgen_core::event::with_event_context(&Arc::clone(&event), async move {
            let event_value = serde_json::value::Value::try_from(event.as_ref())?;
            let config = self.config.render(&event_value)?;

            // Execute operation based on type.
            match config.operation {
                super::config::QueryJobOperation::Create => {
                    self.create_job(&config, &event_value, completion_tx_arc)
                        .await?
                }
                super::config::QueryJobOperation::Get => {
                    self.get_job(&config, completion_tx_arc).await?
                }
                super::config::QueryJobOperation::Delete => {
                    self.delete_job(&config, completion_tx_arc).await?
                }
                super::config::QueryJobOperation::Abort => {
                    self.abort_job(&config, completion_tx_arc).await?
                }
                super::config::QueryJobOperation::GetResults => {
                    self.get_results(&config, completion_tx_arc).await?
                }
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
        completion_tx_arc: Option<flowgen_core::event::SharedCompletionTx>,
    ) -> Result<(), Error> {
        // Render query (inline queries already rendered, resource files need rendering).
        let query_string = match &config.query {
            Some(source) => source
                .render(self.task_context.resource_loader.as_ref(), event_value)
                .await
                .map_err(|source| Error::ResourceLoad { source })?,
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
        let mut job_info =
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

        let e = EventBuilder::new()
            .data(EventData::Json(resp))
            .subject(config.name.to_owned())
            .id(std::mem::take(&mut job_info.id))
            .task_id(self.current_task_id)
            .task_type(self.task_type)
            .build()?;

        self.send_final_event(e, &completion_tx_arc).await
    }

    /// Gets a Salesforce bulk query job status and metadata using the SDK.
    async fn get_job(
        &self,
        config: &super::config::QueryJob,
        completion_tx_arc: Option<flowgen_core::event::SharedCompletionTx>,
    ) -> Result<(), Error> {
        let job_id = config.job_id.as_ref().ok_or(Error::MissingJobId)?;

        // Get job info using SDK.
        let query_client = self.client.query();
        let mut job_info = query_client
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
            .id(std::mem::take(&mut job_info.id))
            .task_id(self.current_task_id)
            .task_type(self.task_type)
            .build()?;

        self.send_final_event(e, &completion_tx_arc).await
    }

    /// Deletes a Salesforce bulk query job using the SDK.
    async fn delete_job(
        &self,
        config: &super::config::QueryJob,
        completion_tx_arc: Option<flowgen_core::event::SharedCompletionTx>,
    ) -> Result<(), Error> {
        let job_id = config.job_id.as_ref().ok_or(Error::MissingJobId)?;

        // Delete job using SDK.
        let query_client = self.client.query();
        query_client
            .delete_job(job_id)
            .await
            .map_err(|e| Error::BulkApiQuery {
                source: Box::new(e),
            })?;

        // Create response struct.
        let response = DeleteJobResponse {
            job_id: job_id.to_string(),
            deleted: true,
        };

        // Serialize response to JSON.
        let resp = serde_json::to_value(&response).map_err(|e| Error::SerdeExt {
            source: flowgen_core::serde::Error::Serde { source: e },
        })?;

        let e = EventBuilder::new()
            .data(EventData::Json(resp))
            .subject(config.name.to_owned())
            .id(response.job_id)
            .task_id(self.current_task_id)
            .task_type(self.task_type)
            .build()?;

        self.send_final_event(e, &completion_tx_arc).await
    }

    /// Aborts a running Salesforce bulk query job using the SDK.
    async fn abort_job(
        &self,
        config: &super::config::QueryJob,
        completion_tx_arc: Option<flowgen_core::event::SharedCompletionTx>,
    ) -> Result<(), Error> {
        let job_id = config.job_id.as_ref().ok_or(Error::MissingJobId)?;

        // Abort job using SDK.
        let query_client = self.client.query();
        let mut job_info =
            query_client
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
            .id(std::mem::take(&mut job_info.id))
            .task_id(self.current_task_id)
            .task_type(self.task_type)
            .build()?;

        self.send_final_event(e, &completion_tx_arc).await
    }

    /// Retrieves Salesforce bulk query job results using the SDK.
    async fn get_results(
        &self,
        config: &super::config::QueryJob,
        completion_tx_arc: Option<flowgen_core::event::SharedCompletionTx>,
    ) -> Result<(), Error> {
        let job_id = config.job_id.as_ref().ok_or(Error::MissingJobId)?;

        // Get results using SDK.
        let query_client = self.client.query();
        let mut byte_stream = query_client
            .get_results(job_id, None, None)
            .await
            .map_err(|e| Error::BulkApiQuery {
                source: Box::new(e),
            })?;

        // Buffer the full CSV response for schema inference (requires seek).
        let mut csv_bytes = Vec::new();
        while let Some(chunk) = byte_stream.next().await {
            let chunk = chunk.map_err(|e| Error::Stream {
                source: Box::new(e),
            })?;
            csv_bytes.extend_from_slice(&chunk);
        }

        let content_type = ContentType::Csv {
            batch_size: config.batch_size,
            has_header: config.has_header,
            delimiter: None,
            infer_schema_max_records: None,
        };

        let iter = EventData::from_reader(Cursor::new(csv_bytes), content_type)?;

        // Emit batches one at a time. Hold the previous event so we can
        // attach completion_tx to the final one (count unknown upfront).
        let mut pending_event: Option<(Event, usize)> = None;

        for (batch_index, item_result) in iter.enumerate() {
            let event_data = item_result?;
            let num_records = match &event_data {
                EventData::ArrowRecordBatch(batch) => batch.num_rows(),
                _ => 1,
            };

            if let Some((prev, prev_records)) = pending_event.take() {
                prev.send_with_logging(self.tx.as_ref())
                    .context("num_records", prev_records)
                    .await
                    .map_err(|e| Error::SendMessage { source: e })?;
            }

            let event_id = format!("{job_id}-{batch_index}");
            let e = EventBuilder::new()
                .data(event_data)
                .subject(config.name.to_owned())
                .id(event_id)
                .task_id(self.current_task_id)
                .task_type(self.task_type)
                .build()?;

            pending_event = Some((e, num_records));
        }

        // Send the final event (or an empty batch if no data) with completion_tx.
        let final_event = match pending_event.take() {
            Some((e, _num_records)) => e,
            None => {
                let empty_batch = arrow::record_batch::RecordBatch::new_empty(std::sync::Arc::new(
                    arrow::datatypes::Schema::empty(),
                ));
                EventBuilder::new()
                    .data(EventData::ArrowRecordBatch(empty_batch))
                    .subject(config.name.to_owned())
                    .id(job_id.clone())
                    .task_id(self.current_task_id)
                    .task_type(self.task_type)
                    .build()?
            }
        };

        self.send_final_event(final_event, &completion_tx_arc).await
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
        let init_config = self.config.render(&serde_json::json!({}))?;

        let sfdc_client = salesforce_core::client::Builder::new()
            .credentials_path(init_config.credentials_path.clone())
            .build()?
            .connect()
            .await?;

        let bulk_client = salesforce_core::bulkapi::ClientBuilder::new(sfdc_client.clone())
            .build()
            .map_err(|e| Error::BulkClientBuild { source: e })?;

        let event_handler = EventHandler {
            config: Arc::clone(&self.config),
            current_task_id: self.task_id,
            tx: self.tx.clone(),
            client: Arc::new(bulk_client),
            sfdc_client: Arc::new(tokio::sync::Mutex::new(sfdc_client)),
            task_type: self.task_type,
            task_context: Arc::clone(&self.task_context),
        };
        Ok(event_handler)
    }

    #[tracing::instrument(skip(self), name = "task.run", fields(task = %self.config.name, task_id = self.task_id, task_type = %self.task_type))]
    async fn run(mut self) -> Result<(), Self::Error> {
        let retry_config =
            flowgen_core::retry::RetryConfig::merge(&self.task_context.retry, &self.config.retry);

        let event_handler = match tokio_retry::Retry::spawn(retry_config.strategy(), || async {
            match self.init().await {
                Ok(handler) => Ok(handler),
                Err(e) => {
                    error!(error = %e, "Failed to initialize bulk query job processor");
                    Err(tokio_retry::RetryError::transient(e))
                }
            }
        })
        .await
        {
            Ok(handler) => Arc::new(handler),
            Err(e) => {
                return Err(e);
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
                                            error!(error = %e, "Failed to process bulk query job");
                                            // Only reconnect for actual auth failures.
                                            // Communication errors (network timeouts, connection
                                            // refused) are transient and don't need a full OAuth
                                            // reconnect — the bulk API client gets a fresh access
                                            // token automatically on the next attempt.
                                            let needs_reconnect = matches!(&e,
                                                Error::SalesforceAuth(_)
                                            ) || matches!(&e,
                                                Error::BulkApiQuery { source }
                                                    if matches!(source.as_ref(), salesforce_core::bulkapi::QueryError::Auth { .. })
                                            );

                                            if needs_reconnect {
                                                let mut sfdc_client =
                                                    event_handler.sfdc_client.lock().await;
                                                if let Err(reconnect_err) =
                                                    (*sfdc_client).reconnect().await
                                                {
                                                    error!(error = %reconnect_err, "Failed to reconnect");
                                                    return Err(tokio_retry::RetryError::transient(Error::SalesforceAuth(
                                                        reconnect_err,
                                                    )));
                                                }
                                            }
                                            Err(tokio_retry::RetryError::transient(e))
                                        }
                                    }
                                })
                                .await;

                                if let Err(err) = result {
                                    error!(error = %err, "Bulk query job failed after all retry attempts");
                                    // Emit error event downstream for error handling.
                                    let mut error_event = event_clone.clone();
                                    error_event.error = Some(err.to_string());
                                    if let Some(ref tx) = event_handler.tx {
                                        tx.send(error_event).await.ok();
                                    }
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

    #[test]
    fn test_delete_job_response_creation() {
        let response = DeleteJobResponse {
            job_id: "750xx000000XXXX".to_string(),
            deleted: true,
        };

        assert_eq!(response.job_id, "750xx000000XXXX");
        assert!(response.deleted);
    }

    #[test]
    fn test_delete_job_response_serialization() {
        let response = DeleteJobResponse {
            job_id: "750xx000000XXXX".to_string(),
            deleted: true,
        };

        let json = serde_json::to_string(&response).unwrap();
        assert!(json.contains("750xx000000XXXX"));
        assert!(json.contains("\"deleted\":true"));
    }

    #[test]
    fn test_delete_job_response_deserialization() {
        let json = r#"{"job_id":"750xx000000XXXX","deleted":true}"#;
        let response: DeleteJobResponse = serde_json::from_str(json).unwrap();

        assert_eq!(response.job_id, "750xx000000XXXX");
        assert!(response.deleted);
    }

    // ── SDK enum mapping functions ─────────────────────────────────

    #[test]
    fn to_sdk_query_operation_query() {
        let result = to_sdk_query_operation(&super::super::config::QueryOperation::Query);
        assert!(matches!(result, SdkQueryOperation::Query));
    }

    #[test]
    fn to_sdk_query_operation_query_all() {
        let result = to_sdk_query_operation(&super::super::config::QueryOperation::QueryAll);
        assert!(matches!(result, SdkQueryOperation::QueryAll));
    }

    #[test]
    fn to_sdk_content_type_csv() {
        let result = to_sdk_content_type(&super::super::config::ContentType::Csv);
        match result {
            Some(ct) => assert!(matches!(ct, SdkContentType::Csv)),
            None => panic!("expected Some(Csv)"),
        }
    }

    #[test]
    fn to_sdk_column_delimiter_comma() {
        let result = to_sdk_column_delimiter(&super::super::config::ColumnDelimiter::Comma);
        match result {
            Some(cd) => assert!(matches!(cd, SdkColumnDelimiter::Comma)),
            None => panic!("expected Some(Comma)"),
        }
    }

    #[test]
    fn to_sdk_column_delimiter_tab() {
        let result = to_sdk_column_delimiter(&super::super::config::ColumnDelimiter::Tab);
        match result {
            Some(cd) => assert!(matches!(cd, SdkColumnDelimiter::Tab)),
            None => panic!("expected Some(Tab)"),
        }
    }

    #[test]
    fn to_sdk_column_delimiter_semicolon() {
        let result = to_sdk_column_delimiter(&super::super::config::ColumnDelimiter::Semicolon);
        match result {
            Some(cd) => assert!(matches!(cd, SdkColumnDelimiter::Semicolon)),
            None => panic!("expected Some(Semicolon)"),
        }
    }

    #[test]
    fn to_sdk_column_delimiter_pipe() {
        let result = to_sdk_column_delimiter(&super::super::config::ColumnDelimiter::Pipe);
        match result {
            Some(cd) => assert!(matches!(cd, SdkColumnDelimiter::Pipe)),
            None => panic!("expected Some(Pipe)"),
        }
    }

    #[test]
    fn to_sdk_column_delimiter_caret() {
        let result = to_sdk_column_delimiter(&super::super::config::ColumnDelimiter::Caret);
        match result {
            Some(cd) => assert!(matches!(cd, SdkColumnDelimiter::Caret)),
            None => panic!("expected Some(Caret)"),
        }
    }

    #[test]
    fn to_sdk_column_delimiter_backquote() {
        let result = to_sdk_column_delimiter(&super::super::config::ColumnDelimiter::Backquote);
        match result {
            Some(cd) => assert!(matches!(cd, SdkColumnDelimiter::Backquote)),
            None => panic!("expected Some(Backquote)"),
        }
    }

    #[test]
    fn to_sdk_line_ending_lf() {
        let result = to_sdk_line_ending(&super::super::config::LineEnding::Lf);
        match result {
            Some(le) => assert!(matches!(le, SdkLineEnding::Lf)),
            None => panic!("expected Some(Lf)"),
        }
    }

    #[test]
    fn to_sdk_line_ending_crlf() {
        let result = to_sdk_line_ending(&super::super::config::LineEnding::Crlf);
        match result {
            Some(le) => assert!(matches!(le, SdkLineEnding::Crlf)),
            None => panic!("expected Some(Crlf)"),
        }
    }
}

//! Salesforce Bulk API Query Job processor.
//!
//! Handles query job operations: create, get, delete, abort, and get_results.

use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use flowgen_core::config::ConfigExt;
use flowgen_core::event::{Event, EventBuilder, EventData, EventExt};
use salesforce_core::bulkapi::{
    ColumnDelimiter as SdkColumnDelimiter, ContentType as SdkContentType, CreateQueryJobRequest,
    LineEnding as SdkLineEnding, QueryOperation as SdkQueryOperation,
};
use serde::{Deserialize, Serialize};
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::num::NonZeroU64;
use std::sync::Arc;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio_stream::StreamExt;
use tracing::error;

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
    #[error("Failed to send event message to next task: {source}")]
    SendMessage {
        #[source]
        source: flowgen_core::event::Error,
    },
    #[error("Missing required builder attribute: {}", _0)]
    MissingBuilderAttribute(String),
    #[error("Salesforce authentication error: {source}")]
    SalesforceAuth {
        #[source]
        source: salesforce_core::client::Error,
    },
    #[error(transparent)]
    Connect(#[from] crate::client::Error),
    #[error("Failed to describe {object} for the results schema: {source}")]
    Describe {
        object: String,
        #[source]
        source: Box<salesforce_core::restapi::SObjectError>,
    },
    #[error("Failed to bind the query parameters: {source}")]
    Bind {
        #[source]
        source: crate::soql::Error,
    },
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
    #[error("Failed to serialize or deserialize data: {source}")]
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
    #[error("Failed to render configuration template: {source}")]
    ConfigRender {
        #[source]
        source: flowgen_core::config::Error,
    },
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
    #[error("Failed to build REST API client: {source}")]
    RestClientBuild {
        #[source]
        source: salesforce_core::restapi::ClientError,
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
    rest_client: Arc<salesforce_core::restapi::Client>,
    retry: flowgen_core::retry::RetryConfig,
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

    async fn process(&self, event: Event) -> Result<(), Error> {
        if self.task_context.cancellation_token.is_cancelled() {
            return Ok(());
        }

        // Run handler with event context for automatic meta preservation.
        let event = Arc::new(event);
        let completion_tx_arc = Arc::clone(&event).completion_tx.clone();

        flowgen_core::event::with_event_context(&Arc::clone(&event), async move {
            let config = self
                .config
                .render_for_event(&event)
                .map_err(|source| Error::ConfigRender { source })?;

            // Execute operation based on type.
            match config.operation {
                super::config::QueryJobOperation::Create => {
                    self.create_job(&config, &event, completion_tx_arc).await?
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

    async fn reconnect_on_auth_error(&self, error: Error) -> Error {
        match failure(&error) {
            crate::page::Failure::Auth => match self.sfdc_client.lock().await.reconnect().await {
                Ok(()) => error,
                Err(source) => {
                    error!(error = %error, "Reconnecting after an authentication error failed");
                    Error::SalesforceAuth { source }
                }
            },
            crate::page::Failure::Transient | crate::page::Failure::Permanent => error,
        }
    }

    /// Creates a Salesforce bulk query job using the SDK.
    async fn create_job(
        &self,
        config: &super::config::QueryJob,
        event: &Event,
        completion_tx_arc: Option<flowgen_core::event::SharedCompletionTx>,
    ) -> Result<(), Error> {
        // An inline query was rendered with the config; a resource file is rendered here.
        let query = match &config.query {
            Some(flowgen_core::resource::Source::Inline(query)) => query.clone(),
            Some(source) => source
                .render(
                    self.task_context.resource_loader.as_ref(),
                    &flowgen_core::event::template_value(event, true)?,
                )
                .await
                .map_err(|source| Error::ResourceLoad { source })?,
            None => return Err(Error::MissingQuery),
        };
        let query_string = match &config.parameters {
            Some(parameters) => {
                crate::soql::bind(&query, parameters).map_err(|source| Error::Bind { source })?
            }
            None => query,
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

    /// Emits every result page of a bulk query job as Arrow batches, typed
    /// from the describe of the queried object. A page holds at most
    /// `batch_size` records and is retried on its own, so a page already sent
    /// downstream is never sent again.
    async fn get_results(
        &self,
        config: &super::config::QueryJob,
        completion_tx_arc: Option<flowgen_core::event::SharedCompletionTx>,
    ) -> Result<(), Error> {
        let job_id = config.job_id.as_ref().ok_or(Error::MissingJobId)?;
        let query_client = self.client.query();
        let job = query_client
            .get_job(job_id)
            .await
            .map_err(|source| Error::BulkApiQuery {
                source: Box::new(source),
            })?;
        let delimiter = column_delimiter(job.column_delimiter.as_ref());
        let max_records = u64::try_from(config.batch_size)
            .ok()
            .and_then(NonZeroU64::new);

        let mut known_schema: Option<SchemaRef> = None;
        let mut locator: Option<String> = None;
        let mut pending_event: Option<(Event, usize)> = None;
        let mut batch_index = 0;
        loop {
            if self.task_context.cancellation_token.is_cancelled() {
                return Ok(());
            }
            let (page, next) = crate::page::fetch(
                &self.retry,
                &self.task_context.cancellation_token,
                &self.sfdc_client,
                failure,
                || read_page(&query_client, job_id, max_records, locator.as_deref()),
            )
            .await
            .map_err(|source| Error::RetryExhausted {
                source: Box::new(source),
            })?;
            let schema = match &known_schema {
                Some(schema) => Arc::clone(schema),
                None => Arc::new(
                    self.results_schema(&job.object, &page, delimiter, config.has_header)
                        .await?,
                ),
            };
            known_schema = Some(Arc::clone(&schema));

            let batches = arrow::csv::ReaderBuilder::new(schema)
                .with_header(config.has_header)
                .with_delimiter(delimiter)
                .with_batch_size(config.batch_size)
                .build(std::io::Cursor::new(page))
                .map_err(|source| Error::Arrow { source })?;
            for batch in batches {
                let batch = batch.map_err(|source| Error::Arrow { source })?;
                if let Some((prev, prev_records)) = pending_event.take() {
                    prev.send_with_logging(self.tx.as_ref())
                        .context("num_records", prev_records)
                        .await
                        .map_err(|e| Error::SendMessage { source: e })?;
                }

                let num_records = batch.num_rows();
                let e = EventBuilder::new()
                    .data(EventData::ArrowRecordBatch(batch))
                    .subject(config.name.to_owned())
                    .id(format!("{job_id}-{batch_index}"))
                    .task_id(self.current_task_id)
                    .task_type(self.task_type)
                    .build()?;
                pending_event = Some((e, num_records));
                batch_index += 1;
            }

            locator = match next {
                Some(next) => Some(next),
                None => break,
            };
        }

        // Send the final event (or an empty batch if no data) with completion_tx.
        let final_event = match pending_event.take() {
            Some((e, _num_records)) => e,
            None => {
                let schema = match known_schema {
                    Some(schema) => schema,
                    None => Arc::new(Schema::empty()),
                };
                let empty_batch = arrow::record_batch::RecordBatch::new_empty(schema);
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

    /// The schema of a job's results: each column typed from the describe of
    /// `object`, following relationships for columns such as `Owner.Name`.
    /// Columns that can't be resolved, and all columns without a header row,
    /// are strings.
    async fn results_schema(
        &self,
        object: &str,
        page: &[u8],
        delimiter: u8,
        has_header: bool,
    ) -> Result<Schema, Error> {
        let (header, _) = arrow::csv::reader::Format::default()
            .with_header(has_header)
            .with_delimiter(delimiter)
            .infer_schema(page, Some(0))
            .map_err(|source| Error::Arrow { source })?;
        let mut describes = HashMap::new();
        let mut fields = Vec::with_capacity(header.fields().len());
        for column in header.fields() {
            let data_type = match has_header {
                true => {
                    self.column_type(&mut describes, object, column.name())
                        .await?
                }
                false => DataType::Utf8,
            };
            fields.push(Field::new(column.name(), data_type, true));
        }
        Ok(Schema::new(fields))
    }

    /// The type of `column` (a field, or a relationship path to one) of
    /// `object`. A polymorphic relationship or an unknown field is a string.
    async fn column_type(
        &self,
        describes: &mut HashMap<String, salesforce_core::restapi::SObjectDescribe>,
        object: &str,
        column: &str,
    ) -> Result<DataType, Error> {
        let mut object = object.to_string();
        let mut path = column.split('.').peekable();
        while let Some(name) = path.next() {
            let describe = match describes.entry(object.clone()) {
                Entry::Occupied(entry) => entry.into_mut(),
                Entry::Vacant(entry) => {
                    let describe = self
                        .rest_client
                        .describe(object.as_str())
                        .send()
                        .await
                        .map_err(|source| Error::Describe {
                            object: object.clone(),
                            source: Box::new(source),
                        })?;
                    entry.insert(describe)
                }
            };
            if path.peek().is_none() {
                let field = describe
                    .fields
                    .iter()
                    .find(|field| field.name.eq_ignore_ascii_case(name));
                return Ok(match field {
                    Some(field) => arrow_type(&field.type_),
                    None => DataType::Utf8,
                });
            }
            let relationship = describe.fields.iter().find(|field| {
                field
                    .relationship_name
                    .as_deref()
                    .is_some_and(|relationship| relationship.eq_ignore_ascii_case(name))
            });
            object = match relationship.map(|field| field.reference_to.as_slice()) {
                Some([related]) => related.clone(),
                _ => return Ok(DataType::Utf8),
            };
        }
        Ok(DataType::Utf8)
    }
}

/// Downloads one page of a job's results and the locator of the next page.
async fn read_page(
    query_client: &salesforce_core::bulkapi::query::QueryClient,
    job_id: &str,
    max_records: Option<NonZeroU64>,
    locator: Option<&str>,
) -> Result<(Vec<u8>, Option<String>), Error> {
    let mut page = query_client
        .get_results(job_id, max_records, locator)
        .await
        .map_err(|source| Error::BulkApiQuery {
            source: Box::new(source),
        })?;
    let mut body = Vec::new();
    while let Some(chunk) = page.body.next().await {
        let chunk = chunk.map_err(|source| Error::Stream {
            source: Box::new(source),
        })?;
        body.extend_from_slice(&chunk);
    }
    Ok((body, page.locator))
}

/// The Arrow type a Salesforce field type is read as.
fn arrow_type(field_type: &str) -> DataType {
    match field_type {
        "boolean" => DataType::Boolean,
        "int" | "long" => DataType::Int64,
        "double" | "currency" | "percent" => DataType::Float64,
        "date" => DataType::Date32,
        "datetime" => DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
        _ => DataType::Utf8,
    }
}

/// The byte separating columns in a job's CSV results.
fn column_delimiter(delimiter: Option<&SdkColumnDelimiter>) -> u8 {
    match delimiter {
        Some(SdkColumnDelimiter::Tab) => b'\t',
        Some(SdkColumnDelimiter::Semicolon) => b';',
        Some(SdkColumnDelimiter::Pipe) => b'|',
        Some(SdkColumnDelimiter::Caret) => b'^',
        Some(SdkColumnDelimiter::Backquote) => b'`',
        Some(SdkColumnDelimiter::Comma) | None => b',',
    }
}

/// How a failed request is retried.
fn failure(error: &Error) -> crate::page::Failure {
    use crate::page::Failure;
    match error {
        Error::SalesforceAuth { .. } => Failure::Auth,
        Error::BulkApiQuery { source } => match source.as_ref() {
            salesforce_core::bulkapi::QueryError::Auth { .. } => Failure::Auth,
            source if source.is_retryable() => Failure::Transient,
            _ => Failure::Permanent,
        },
        Error::Describe { source, .. } => match source.as_ref() {
            salesforce_core::restapi::SObjectError::Auth { .. } => Failure::Auth,
            source if source.is_retryable() => Failure::Transient,
            _ => Failure::Permanent,
        },
        Error::Stream { .. } => Failure::Transient,
        _ => Failure::Permanent,
    }
}

#[async_trait::async_trait]
impl flowgen_core::task::runner::Handler for EventHandler {
    type Error = Error;

    fn sender(&self) -> Option<&Sender<Event>> {
        self.tx.as_ref()
    }

    fn is_retryable(&self, error: &Error) -> bool {
        !matches!(failure(error), crate::page::Failure::Permanent)
    }

    /// Ignores events addressed to another task.
    #[tracing::instrument(skip(self, event), name = "task.handle", fields(duration_ms = tracing::field::Empty))]
    async fn handle(&self, event: Event) -> Result<(), Error> {
        if Some(event.task_id) != self.current_task_id.checked_sub(1) {
            return Ok(());
        }
        match self.process(event).await {
            Ok(()) => Ok(()),
            Err(error) => Err(self.reconnect_on_auth_error(error).await),
        }
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
        let init_config = self
            .config
            .render(&serde_json::json!({}))
            .map_err(|source| Error::ConfigRender { source })?;

        let sfdc_client = crate::client::connect(
            &self.task_context,
            self.task_type,
            &init_config.credentials_path,
        )
        .await?;

        let (bulk_client, rest_client) = {
            let guard = sfdc_client.lock().await;
            let bulk_client = salesforce_core::bulkapi::ClientBuilder::new(guard.clone())
                .build()
                .map_err(|e| Error::BulkClientBuild { source: e })?;
            let rest_client = salesforce_core::restapi::ClientBuilder::new(guard.clone())
                .build()
                .map_err(|source| Error::RestClientBuild { source })?;
            (bulk_client, rest_client)
        };

        let event_handler = EventHandler {
            config: Arc::clone(&self.config),
            current_task_id: self.task_id,
            tx: self.tx.clone(),
            client: Arc::new(bulk_client),
            rest_client: Arc::new(rest_client),
            retry: flowgen_core::retry::RetryConfig::merge(
                &self.task_context.retry,
                &self.config.retry,
            ),
            sfdc_client,
            task_type: self.task_type,
            task_context: Arc::clone(&self.task_context),
        };
        Ok(event_handler)
    }

    #[tracing::instrument(skip(self), name = "task.run", fields(task = %self.config.name, task_id = self.task_id, task_type = %self.task_type))]
    async fn run(self) -> Result<(), Self::Error> {
        let retry_config =
            flowgen_core::retry::RetryConfig::merge(&self.task_context.retry, &self.config.retry);

        let event_handler = match tokio_retry::Retry::spawn(
            retry_config.init_strategy(self.task_context.startup_delay),
            || async {
                match self.init().await {
                    Ok(handler) => Ok(handler),
                    Err(e) => {
                        error!(error = %e, "Failed to initialize bulk query job processor");
                        Err(tokio_retry::RetryError::transient(e))
                    }
                }
            },
        )
        .await
        {
            Ok(handler) => Arc::new(handler),
            Err(e) => {
                return Err(e);
            }
        };

        flowgen_core::task::runner::process_events(
            self.rx,
            event_handler,
            &retry_config,
            self.config.max_in_flight,
            &self.task_context,
        )
        .await;
        Ok(())
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

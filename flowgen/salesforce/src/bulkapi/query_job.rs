//! The `salesforce_bulkapi_query_job` task: Bulk API 2.0 query jobs (create,
//! get, delete, abort) and their results as Arrow batches.

use crate::processor::{
    required, to_json, Call, Emitter, Error, Operation, Output, QueryOperation,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use flowgen_core::config::ConfigExt;
use flowgen_core::task::TaskConfig;
use salesforce_core::bulkapi as sdk;
use serde::{Deserialize, Serialize};
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::num::NonZeroU64;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio_stream::StreamExt;

/// Default batch size for CSV parsing (10,000 rows per RecordBatch).
const fn default_batch_size() -> usize {
    10000
}

/// Default header setting for CSV parsing (true = first row is header).
const fn default_has_header() -> bool {
    true
}

/// Query job operations.
#[derive(PartialEq, Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum QueryJobOperation {
    /// Create a new bulk query job.
    Create,
    /// Get job status and metadata.
    Get,
    /// Delete a job (removes job metadata and results).
    Delete,
    /// Abort a running job.
    Abort,
    /// Get job results (CSV data).
    GetResults,
}

/// Configuration for Salesforce Bulk API Query Job operations.
///
/// # Examples
///
/// Create a query job:
/// ```yaml
/// salesforce_bulkapi_query_job:
///   name: query_accounts
///   operation: create
///   credentials_path: /path/to/salesforce_creds.json
///   query_operation: query
///   query: "SELECT Id, Name FROM Account"
///   content_type: csv
///   column_delimiter: comma
///   line_ending: lf
/// ```
///
/// Get job status:
/// ```yaml
/// salesforce_bulkapi_query_job:
///   name: get_job_status
///   operation: get
///   credentials_path: /path/to/salesforce_creds.json
///   job_id: "{{event.data.id}}"
/// ```
///
/// Get job results:
/// ```yaml
/// salesforce_bulkapi_query_job:
///   name: get_results
///   operation: get_results
///   credentials_path: /path/to/salesforce_creds.json
///   job_id: "{{event.data.id}}"
///   batch_size: 10000
///   has_header: true
/// ```
#[derive(PartialEq, Clone, Debug, Deserialize, Serialize, TaskConfig)]
#[serde(deny_unknown_fields)]
pub struct QueryJob {
    /// Unique task identifier.
    pub name: String,
    /// Query job operation type.
    pub operation: QueryJobOperation,
    /// Path to Salesforce authentication credentials.
    pub credentials_path: PathBuf,
    /// SOQL query string (create only).
    #[serde(default)]
    pub query: Option<flowgen_core::resource::Source>,
    /// Values bound to `:name` in the query as SOQL literals (create only).
    #[serde(default)]
    pub parameters: Option<HashMap<String, crate::soql::Parameter>>,
    /// Type of query operation (query or queryAll) (create only).
    #[serde(default)]
    pub query_operation: Option<QueryOperation>,
    /// Output file format (create only).
    #[serde(default)]
    pub content_type: Option<ContentType>,
    /// Column separator for CSV output (create only).
    #[serde(default)]
    pub column_delimiter: Option<ColumnDelimiter>,
    /// Line termination style (create only).
    #[serde(default)]
    pub line_ending: Option<LineEnding>,
    /// Job ID for get/delete/abort/get_results operations.
    #[serde(default)]
    pub job_id: Option<String>,
    /// Records per result page and per Arrow batch (get_results only).
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
    /// Whether CSV results include header row (get_results only).
    #[serde(default = "default_has_header")]
    pub has_header: bool,
    /// Optional list of upstream task names this task depends on.
    /// When set, this task only receives events from the named tasks.
    /// When not set, the task receives from the previous task in the list (linear chain).
    #[serde(default)]
    pub depends_on: Option<Vec<String>>,
    /// Optional retry configuration (overrides app-level retry config).
    #[serde(default)]
    pub retry: Option<flowgen_core::retry::RetryConfig>,
    /// Events handled at once; defaults to the number of CPUs.
    #[serde(default)]
    pub max_in_flight: Option<std::num::NonZeroUsize>,
}

impl ConfigExt for QueryJob {}

/// Output file content types.
#[derive(PartialEq, Clone, Debug, Default, Deserialize, Serialize)]
pub enum ContentType {
    #[default]
    #[serde(rename = "csv")]
    Csv,
}

impl From<&ContentType> for sdk::ContentType {
    fn from(content_type: &ContentType) -> Self {
        match content_type {
            ContentType::Csv => sdk::ContentType::Csv,
        }
    }
}

/// CSV column delimiters.
#[derive(PartialEq, Clone, Debug, Default, Deserialize, Serialize)]
pub enum ColumnDelimiter {
    #[default]
    #[serde(rename = "comma")]
    Comma,
    #[serde(rename = "tab")]
    Tab,
    #[serde(rename = "semicolon")]
    Semicolon,
    #[serde(rename = "pipe")]
    Pipe,
    #[serde(rename = "caret")]
    Caret,
    #[serde(rename = "backquote")]
    Backquote,
}

impl From<&ColumnDelimiter> for sdk::ColumnDelimiter {
    fn from(delimiter: &ColumnDelimiter) -> Self {
        match delimiter {
            ColumnDelimiter::Comma => sdk::ColumnDelimiter::Comma,
            ColumnDelimiter::Tab => sdk::ColumnDelimiter::Tab,
            ColumnDelimiter::Semicolon => sdk::ColumnDelimiter::Semicolon,
            ColumnDelimiter::Pipe => sdk::ColumnDelimiter::Pipe,
            ColumnDelimiter::Caret => sdk::ColumnDelimiter::Caret,
            ColumnDelimiter::Backquote => sdk::ColumnDelimiter::Backquote,
        }
    }
}

/// Line ending styles for cross-platform compatibility.
#[derive(PartialEq, Clone, Debug, Default, Deserialize, Serialize)]
pub enum LineEnding {
    /// Unix/Linux style (\n).
    #[default]
    #[serde(rename = "lf")]
    Lf,
    /// Windows style (\r\n).
    #[serde(rename = "crlf")]
    Crlf,
}

impl From<&LineEnding> for sdk::LineEnding {
    fn from(line_ending: &LineEnding) -> Self {
        match line_ending {
            LineEnding::Lf => sdk::LineEnding::Lf,
            LineEnding::Crlf => sdk::LineEnding::Crlf,
        }
    }
}

impl From<&QueryOperation> for sdk::QueryOperation {
    fn from(operation: &QueryOperation) -> Self {
        match operation {
            QueryOperation::Query => sdk::QueryOperation::Query,
            QueryOperation::QueryAll => sdk::QueryOperation::QueryAll,
        }
    }
}

/// Response for delete job operation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeleteJobResponse {
    /// The ID of the deleted job.
    pub job_id: String,
    /// Confirmation that the job was deleted.
    pub deleted: bool,
}

/// The clients a query job uses: Bulk API for the job, REST API to describe
/// the queried object.
pub struct Clients {
    bulk: sdk::Client,
    rest: salesforce_core::restapi::Client,
}

#[async_trait::async_trait]
impl Operation for QueryJob {
    type Client = Clients;

    fn client(auth: &salesforce_core::client::Client) -> Result<Self::Client, Error> {
        Ok(Clients {
            bulk: sdk::ClientBuilder::new(auth.clone())
                .build()
                .map_err(|source| Error::BulkClient { source })?,
            rest: crate::processor::rest_client(auth)?,
        })
    }

    fn credentials_path(&self) -> &Path {
        &self.credentials_path
    }

    fn retries_safely(&self) -> bool {
        !matches!(self.operation, QueryJobOperation::Create)
    }

    async fn call(
        &self,
        client: &Self::Client,
        call: &Call<'_>,
        emitter: &mut Emitter<'_>,
    ) -> Result<(), Error> {
        let jobs = client.bulk.query();
        match self.operation {
            QueryJobOperation::Create => {
                let query = required(self.query.as_ref(), "query")?;
                let request = sdk::CreateQueryJobRequest {
                    operation: match &self.query_operation {
                        Some(operation) => operation.into(),
                        None => sdk::QueryOperation::Query,
                    },
                    query: call.soql(query, self.parameters.as_ref()).await?,
                    content_type: self.content_type.as_ref().map(Into::into),
                    column_delimiter: self.column_delimiter.as_ref().map(Into::into),
                    line_ending: self.line_ending.as_ref().map(Into::into),
                };
                let job = jobs.create_job(&request).await.map_err(bulk_error)?;
                emitter.emit(Output::json(to_json(&job)?).id(job.id)).await
            }
            QueryJobOperation::Get => {
                let job_id = required(self.job_id.as_deref(), "job_id")?;
                let job = jobs.get_job(job_id).await.map_err(bulk_error)?;
                emitter.emit(Output::json(to_json(&job)?).id(job.id)).await
            }
            QueryJobOperation::Delete => {
                let job_id = required(self.job_id.as_deref(), "job_id")?;
                jobs.delete_job(job_id).await.map_err(bulk_error)?;
                let response = DeleteJobResponse {
                    job_id: job_id.to_string(),
                    deleted: true,
                };
                emitter
                    .emit(Output::json(to_json(&response)?).id(response.job_id))
                    .await
            }
            QueryJobOperation::Abort => {
                let job_id = required(self.job_id.as_deref(), "job_id")?;
                let job = jobs.abort_job(job_id).await.map_err(bulk_error)?;
                emitter.emit(Output::json(to_json(&job)?).id(job.id)).await
            }
            QueryJobOperation::GetResults => self.get_results(client, call, emitter).await,
        }
    }
}

impl QueryJob {
    /// Emits every result page of a bulk query job as Arrow batches, typed
    /// from the describe of the queried object. A page holds at most
    /// `batch_size` records and is retried on its own, so a page already sent
    /// downstream is never sent again.
    async fn get_results(
        &self,
        client: &Clients,
        call: &Call<'_>,
        emitter: &mut Emitter<'_>,
    ) -> Result<(), Error> {
        let job_id = required(self.job_id.as_deref(), "job_id")?;
        let jobs = client.bulk.query();
        let job = jobs.get_job(job_id).await.map_err(bulk_error)?;
        let delimiter = column_delimiter(job.column_delimiter.as_ref());
        let max_records = u64::try_from(self.batch_size)
            .ok()
            .and_then(NonZeroU64::new);

        let mut schema: Option<SchemaRef> = None;
        let mut locator: Option<String> = None;
        let mut batch_index = 0;
        loop {
            if call.is_cancelled() {
                return Ok(());
            }
            let (page, next) = call
                .page(|| read_page(&jobs, job_id, max_records, locator.as_deref()))
                .await?;
            let page_schema = match &schema {
                Some(schema) => Arc::clone(schema),
                None => Arc::new(
                    results_schema(&client.rest, &job.object, &page, delimiter, self.has_header)
                        .await?,
                ),
            };
            schema = Some(Arc::clone(&page_schema));

            let batches = arrow::csv::ReaderBuilder::new(page_schema)
                .with_header(self.has_header)
                .with_delimiter(delimiter)
                .with_batch_size(self.batch_size)
                .build(std::io::Cursor::new(page))
                .map_err(|source| Error::Arrow { source })?;
            for batch in batches {
                let batch = batch.map_err(|source| Error::Arrow { source })?;
                emitter
                    .emit(Output::batch(batch).id(format!("{job_id}-{batch_index}")))
                    .await?;
                batch_index += 1;
            }

            locator = match next {
                Some(next) => Some(next),
                None => break,
            };
        }

        match batch_index {
            0 => {
                let schema = match schema {
                    Some(schema) => schema,
                    None => Arc::new(Schema::empty()),
                };
                let empty = arrow::record_batch::RecordBatch::new_empty(schema);
                emitter.emit(Output::batch(empty).id(job_id)).await
            }
            _ => Ok(()),
        }
    }
}

/// Downloads one page of a job's results and the locator of the next page.
async fn read_page(
    jobs: &sdk::query::QueryClient,
    job_id: &str,
    max_records: Option<NonZeroU64>,
    locator: Option<&str>,
) -> Result<(Vec<u8>, Option<String>), Error> {
    let mut page = jobs
        .get_results(job_id, max_records, locator)
        .await
        .map_err(bulk_error)?;
    let mut body = Vec::new();
    while let Some(chunk) = page.body.next().await {
        let chunk = chunk.map_err(|source| Error::Stream {
            source: Box::new(source),
        })?;
        body.extend_from_slice(&chunk);
    }
    Ok((body, page.locator))
}

/// The schema of a job's results: each column typed from the describe of
/// `object`, following relationships for columns such as `Owner.Name`.
/// Columns that can't be resolved, and all columns without a header row,
/// are strings.
async fn results_schema(
    rest: &salesforce_core::restapi::Client,
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
            true => column_type(rest, &mut describes, object, column.name()).await?,
            false => DataType::Utf8,
        };
        fields.push(Field::new(column.name(), data_type, true));
    }
    Ok(Schema::new(fields))
}

/// The type of `column` (a field, or a relationship path to one) of
/// `object`. A polymorphic relationship or an unknown field is a string.
async fn column_type(
    rest: &salesforce_core::restapi::Client,
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
                let describe = rest
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
fn column_delimiter(delimiter: Option<&sdk::ColumnDelimiter>) -> u8 {
    match delimiter {
        Some(sdk::ColumnDelimiter::Tab) => b'\t',
        Some(sdk::ColumnDelimiter::Semicolon) => b';',
        Some(sdk::ColumnDelimiter::Pipe) => b'|',
        Some(sdk::ColumnDelimiter::Caret) => b'^',
        Some(sdk::ColumnDelimiter::Backquote) => b'`',
        Some(sdk::ColumnDelimiter::Comma) | None => b',',
    }
}

fn bulk_error(source: sdk::QueryError) -> Error {
    Error::BulkQuery {
        source: Box::new(source),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_delete_job_response_serialization() {
        let response = DeleteJobResponse {
            job_id: "750xx000000XXXX".to_string(),
            deleted: true,
        };

        let json = serde_json::to_string(&response).unwrap();
        assert_eq!(json, r#"{"job_id":"750xx000000XXXX","deleted":true}"#);
    }

    #[test]
    fn creating_a_job_does_not_retry_safely() {
        let config = |operation| QueryJob {
            operation,
            ..serde_json::from_str(r#"{"name": "a", "operation": "get", "credentials_path": "/a"}"#)
                .unwrap()
        };

        assert!(config(QueryJobOperation::GetResults).retries_safely());
        assert!(!config(QueryJobOperation::Create).retries_safely());
    }

    #[test]
    fn every_config_delimiter_maps_to_its_sdk_delimiter_and_byte() {
        for (delimiter, byte) in [
            (ColumnDelimiter::Comma, b','),
            (ColumnDelimiter::Tab, b'\t'),
            (ColumnDelimiter::Semicolon, b';'),
            (ColumnDelimiter::Pipe, b'|'),
            (ColumnDelimiter::Caret, b'^'),
            (ColumnDelimiter::Backquote, b'`'),
        ] {
            let sdk_delimiter: sdk::ColumnDelimiter = (&delimiter).into();
            assert_eq!(column_delimiter(Some(&sdk_delimiter)), byte);
        }
        assert_eq!(column_delimiter(None), b',');
    }

    #[test]
    fn salesforce_field_types_map_to_arrow_types() {
        assert_eq!(arrow_type("boolean"), DataType::Boolean);
        assert_eq!(arrow_type("int"), DataType::Int64);
        assert_eq!(arrow_type("currency"), DataType::Float64);
        assert_eq!(arrow_type("date"), DataType::Date32);
        assert_eq!(arrow_type("reference"), DataType::Utf8);
    }

    #[test]
    fn test_query_job_operation_serialization() {
        for (operation, name) in [
            (QueryJobOperation::Create, "\"create\""),
            (QueryJobOperation::Get, "\"get\""),
            (QueryJobOperation::Delete, "\"delete\""),
            (QueryJobOperation::Abort, "\"abort\""),
            (QueryJobOperation::GetResults, "\"get_results\""),
        ] {
            assert_eq!(serde_json::to_string(&operation).unwrap(), name);
            assert_eq!(
                serde_json::from_str::<QueryJobOperation>(name).unwrap(),
                operation
            );
        }
    }

    #[test]
    fn test_column_delimiter_serialization() {
        for (delimiter, name) in [
            (ColumnDelimiter::Comma, "\"comma\""),
            (ColumnDelimiter::Tab, "\"tab\""),
            (ColumnDelimiter::Semicolon, "\"semicolon\""),
            (ColumnDelimiter::Pipe, "\"pipe\""),
            (ColumnDelimiter::Caret, "\"caret\""),
            (ColumnDelimiter::Backquote, "\"backquote\""),
        ] {
            assert_eq!(serde_json::to_string(&delimiter).unwrap(), name);
            assert_eq!(
                serde_json::from_str::<ColumnDelimiter>(name).unwrap(),
                delimiter
            );
        }
    }

    #[test]
    fn test_line_ending_and_content_type_serialization() {
        assert_eq!(serde_json::to_string(&LineEnding::Lf).unwrap(), "\"lf\"");
        assert_eq!(
            serde_json::to_string(&LineEnding::Crlf).unwrap(),
            "\"crlf\""
        );
        assert_eq!(serde_json::to_string(&ContentType::Csv).unwrap(), "\"csv\"");
    }
}

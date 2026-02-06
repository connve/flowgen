use flowgen_core::config::ConfigExt;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

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
#[derive(PartialEq, Clone, Debug, Deserialize, Serialize)]
pub struct QueryJob {
    /// Unique task identifier.
    pub name: String,
    /// Query job operation type.
    pub operation: QueryJobOperation,
    /// Path to Salesforce authentication credentials.
    pub credentials_path: PathBuf,

    // Fields for create operation
    /// SOQL query string (create only).
    #[serde(default)]
    pub query: Option<flowgen_core::resource::Source>,
    /// Type of query operation (query or queryAll) (create only).
    #[serde(default, rename = "query_operation")]
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

    // Fields for get, delete, abort, get_results operations
    /// Job ID for get/delete/abort/get_results operations.
    #[serde(default)]
    pub job_id: Option<String>,

    // Fields for get_results operation
    /// Number of rows per Arrow RecordBatch when parsing CSV results (get_results only).
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
    /// Whether CSV results include header row (get_results only).
    #[serde(default = "default_has_header")]
    pub has_header: bool,

    /// Optional retry configuration (overrides app-level retry config).
    #[serde(default)]
    pub retry: Option<flowgen_core::retry::RetryConfig>,
}

impl ConfigExt for QueryJob {}

/// Salesforce Bulk API query operation types.
#[derive(PartialEq, Clone, Debug, Default, Deserialize, Serialize)]
pub enum QueryOperation {
    /// Query active records only.
    #[default]
    #[serde(rename = "query")]
    Query,
    /// Query including deleted/archived records.
    #[serde(rename = "queryAll")]
    QueryAll,
}

/// Output file content types.
#[derive(PartialEq, Clone, Debug, Default, Deserialize, Serialize)]
pub enum ContentType {
    #[default]
    #[serde(rename = "csv")]
    Csv,
}

impl ContentType {
    /// Returns the Salesforce API representation (uppercase).
    pub fn as_api_str(&self) -> &str {
        match self {
            ContentType::Csv => "CSV",
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

impl ColumnDelimiter {
    /// Returns the Salesforce API representation (uppercase).
    pub fn as_api_str(&self) -> &str {
        match self {
            ColumnDelimiter::Comma => "COMMA",
            ColumnDelimiter::Tab => "TAB",
            ColumnDelimiter::Semicolon => "SEMICOLON",
            ColumnDelimiter::Pipe => "PIPE",
            ColumnDelimiter::Caret => "CARET",
            ColumnDelimiter::Backquote => "BACKQUOTE",
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

impl LineEnding {
    /// Returns the Salesforce API representation (uppercase).
    pub fn as_api_str(&self) -> &str {
        match self {
            LineEnding::Lf => "LF",
            LineEnding::Crlf => "CRLF",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_query_operation_enum_defaults() {
        let op = QueryOperation::default();
        assert_eq!(op, QueryOperation::Query);
    }

    #[test]
    fn test_query_operation_serialization() {
        assert_eq!(
            serde_json::to_string(&QueryOperation::Query).unwrap(),
            "\"query\""
        );
        assert_eq!(
            serde_json::to_string(&QueryOperation::QueryAll).unwrap(),
            "\"queryAll\""
        );
    }

    #[test]
    fn test_query_operation_deserialization() {
        assert_eq!(
            serde_json::from_str::<QueryOperation>("\"query\"").unwrap(),
            QueryOperation::Query
        );
        assert_eq!(
            serde_json::from_str::<QueryOperation>("\"queryAll\"").unwrap(),
            QueryOperation::QueryAll
        );
    }

    #[test]
    fn test_query_job_operation_serialization() {
        assert_eq!(
            serde_json::to_string(&QueryJobOperation::Create).unwrap(),
            "\"create\""
        );
        assert_eq!(
            serde_json::to_string(&QueryJobOperation::Get).unwrap(),
            "\"get\""
        );
        assert_eq!(
            serde_json::to_string(&QueryJobOperation::Delete).unwrap(),
            "\"delete\""
        );
        assert_eq!(
            serde_json::to_string(&QueryJobOperation::Abort).unwrap(),
            "\"abort\""
        );
        assert_eq!(
            serde_json::to_string(&QueryJobOperation::GetResults).unwrap(),
            "\"get_results\""
        );
    }

    #[test]
    fn test_query_job_operation_deserialization() {
        assert_eq!(
            serde_json::from_str::<QueryJobOperation>("\"create\"").unwrap(),
            QueryJobOperation::Create
        );
        assert_eq!(
            serde_json::from_str::<QueryJobOperation>("\"get\"").unwrap(),
            QueryJobOperation::Get
        );
        assert_eq!(
            serde_json::from_str::<QueryJobOperation>("\"delete\"").unwrap(),
            QueryJobOperation::Delete
        );
        assert_eq!(
            serde_json::from_str::<QueryJobOperation>("\"abort\"").unwrap(),
            QueryJobOperation::Abort
        );
        assert_eq!(
            serde_json::from_str::<QueryJobOperation>("\"get_results\"").unwrap(),
            QueryJobOperation::GetResults
        );
    }

    #[test]
    fn test_content_type_default() {
        let content_type = ContentType::default();
        assert_eq!(content_type, ContentType::Csv);
    }

    #[test]
    fn test_content_type_serialization() {
        assert_eq!(serde_json::to_string(&ContentType::Csv).unwrap(), "\"csv\"");
    }

    #[test]
    fn test_content_type_deserialization() {
        assert_eq!(
            serde_json::from_str::<ContentType>("\"csv\"").unwrap(),
            ContentType::Csv
        );
    }

    #[test]
    fn test_column_delimiter_default() {
        let delimiter = ColumnDelimiter::default();
        assert_eq!(delimiter, ColumnDelimiter::Comma);
    }

    #[test]
    fn test_column_delimiter_serialization() {
        assert_eq!(
            serde_json::to_string(&ColumnDelimiter::Comma).unwrap(),
            "\"comma\""
        );
        assert_eq!(
            serde_json::to_string(&ColumnDelimiter::Tab).unwrap(),
            "\"tab\""
        );
        assert_eq!(
            serde_json::to_string(&ColumnDelimiter::Semicolon).unwrap(),
            "\"semicolon\""
        );
        assert_eq!(
            serde_json::to_string(&ColumnDelimiter::Pipe).unwrap(),
            "\"pipe\""
        );
        assert_eq!(
            serde_json::to_string(&ColumnDelimiter::Caret).unwrap(),
            "\"caret\""
        );
        assert_eq!(
            serde_json::to_string(&ColumnDelimiter::Backquote).unwrap(),
            "\"backquote\""
        );
    }

    #[test]
    fn test_column_delimiter_deserialization() {
        assert_eq!(
            serde_json::from_str::<ColumnDelimiter>("\"comma\"").unwrap(),
            ColumnDelimiter::Comma
        );
        assert_eq!(
            serde_json::from_str::<ColumnDelimiter>("\"tab\"").unwrap(),
            ColumnDelimiter::Tab
        );
        assert_eq!(
            serde_json::from_str::<ColumnDelimiter>("\"semicolon\"").unwrap(),
            ColumnDelimiter::Semicolon
        );
        assert_eq!(
            serde_json::from_str::<ColumnDelimiter>("\"pipe\"").unwrap(),
            ColumnDelimiter::Pipe
        );
        assert_eq!(
            serde_json::from_str::<ColumnDelimiter>("\"caret\"").unwrap(),
            ColumnDelimiter::Caret
        );
        assert_eq!(
            serde_json::from_str::<ColumnDelimiter>("\"backquote\"").unwrap(),
            ColumnDelimiter::Backquote
        );
    }

    #[test]
    fn test_line_ending_default() {
        let line_ending = LineEnding::default();
        assert_eq!(line_ending, LineEnding::Lf);
    }

    #[test]
    fn test_line_ending_serialization() {
        assert_eq!(serde_json::to_string(&LineEnding::Lf).unwrap(), "\"lf\"");
        assert_eq!(
            serde_json::to_string(&LineEnding::Crlf).unwrap(),
            "\"crlf\""
        );
    }

    #[test]
    fn test_line_ending_deserialization() {
        assert_eq!(
            serde_json::from_str::<LineEnding>("\"lf\"").unwrap(),
            LineEnding::Lf
        );
        assert_eq!(
            serde_json::from_str::<LineEnding>("\"crlf\"").unwrap(),
            LineEnding::Crlf
        );
    }

    #[test]
    fn test_all_delimiters_unique() {
        let delimiters = [
            ColumnDelimiter::Comma,
            ColumnDelimiter::Tab,
            ColumnDelimiter::Semicolon,
            ColumnDelimiter::Pipe,
            ColumnDelimiter::Caret,
            ColumnDelimiter::Backquote,
        ];

        // Each delimiter should serialize to a different string
        let serialized: Vec<String> = delimiters
            .iter()
            .map(|d| serde_json::to_string(d).unwrap())
            .collect();

        let unique_count = serialized
            .iter()
            .collect::<std::collections::HashSet<_>>()
            .len();
        assert_eq!(unique_count, delimiters.len());
    }
}

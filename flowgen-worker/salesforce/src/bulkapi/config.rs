use flowgen_core::config::ConfigExt;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// Salesforce Bulk API endpoint base path (API v65.0).
pub const DEFAULT_URI_PATH: &str = "/services/data/v65.0/jobs/";

/// Default batch size for CSV parsing (10,000 rows per RecordBatch).
const fn default_batch_size() -> usize {
    10000
}

/// Default header setting for CSV parsing (true = first row is header).
const fn default_has_header() -> bool {
    true
}

/// Processor for creating salesforce account query job.
/// ```json
/// {
///    "salesforce_bulkapi_job_creator": {
///         "credentials_path": "/path/to/salesforce_test_creds.json",
///         "operation": "query",
///         "job": "Select Id from Account",
///         "content_type": "csv",
///         "column_delimiter": "comma",
///         "line_ending": "crlf"
///     }
///  }
/// ```
///
/// Processor for creating salesforce account query all job.
/// ```json
/// {
///    "salesforce_bulkapi_job_creator": {
///         "credentials_path": "/path/to/salesforce_test_creds.json",
///         "operation": "queryAll",
///         "job": "Select Id from Account",
///         "content_type": "csv",
///         "column_delimiter": "comma",
///         "line_ending": "crlf"
///     }
///  }
/// ```
/// Job operation type for unified job processor.
#[derive(PartialEq, Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum JobOperation {
    /// Create a new bulk API job.
    Create,
    /// Retrieve/poll bulk API job status and results.
    Retrieve,
}

/// Unified configuration for Salesforce Bulk API job operations.
///
/// This single config handles both create and retrieve operations.
/// The operation type determines which fields are required and what action is performed.
///
/// # Examples
///
/// Create a query job:
/// ```yaml
/// salesforce_bulkapi_job:
///   name: query_accounts
///   operation: create
///   credentials_path: /path/to/salesforce_creds.json
///   job_type: query
///   operation_type: query
///   query: "SELECT Id, Name FROM Account"
///   content_type: csv
///   column_delimiter: comma
///   line_ending: lf
/// ```
///
/// Retrieve job results:
/// ```yaml
/// salesforce_bulkapi_job:
///   name: get_results
///   operation: retrieve
///   credentials_path: /path/to/salesforce_creds.json
///   job_type: query
///   job_id: "{{event.data.id}}"
///   batch_size: 10000
///   has_header: true
/// ```
#[derive(PartialEq, Clone, Debug, Deserialize, Serialize)]
pub struct Job {
    /// Unique task identifier.
    pub name: String,
    /// Job operation type (create, retrieve).
    pub operation: JobOperation,
    /// Path to Salesforce authentication credentials.
    pub credentials_path: PathBuf,
    /// Salesforce Job Type like query, ingest.
    #[serde(default)]
    pub job_type: JobType,

    // Fields for create operation
    /// SOQL query for query/queryAll operations (create only).
    #[serde(default)]
    pub query: Option<flowgen_core::resource::Source>,
    /// Salesforce object API name (create only).
    #[serde(default)]
    pub object: Option<String>,
    /// Type of bulk operation to perform (create only).
    #[serde(default, rename = "operation_type")]
    pub operation_type: Option<Operation>,
    /// Output file format (create only).
    #[serde(default)]
    pub content_type: Option<ContentType>,
    /// Column separator for CSV output (create only).
    #[serde(default)]
    pub column_delimiter: Option<ColumnDelimiter>,
    /// Line termination style (create only).
    #[serde(default)]
    pub line_ending: Option<LineEnding>,
    /// Assignment rule ID for Case or Lead objects (create only).
    #[serde(default)]
    pub assignment_rule_id: Option<String>,
    /// External ID field name for upsert operations (create only).
    #[serde(default)]
    pub external_id_field_name: Option<String>,

    // Fields for retrieve operation
    /// Template for extracting job ID from event data (retrieve only).
    #[serde(default)]
    pub job_id: Option<String>,
    /// Number of rows per Arrow RecordBatch when parsing CSV results (retrieve only).
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
    /// Whether CSV results include header row (retrieve only).
    #[serde(default = "default_has_header")]
    pub has_header: bool,

    /// Optional retry configuration (overrides app-level retry config).
    #[serde(default)]
    pub retry: Option<flowgen_core::retry::RetryConfig>,
}

impl ConfigExt for Job {}

/// Salesforce Bulk API Job types.
#[derive(PartialEq, Clone, Debug, Default, Deserialize, Serialize)]
pub enum JobType {
    /// Query Job type.
    #[default]
    #[serde(rename = "query")]
    Query,
    /// Ingest Job type.
    #[serde(rename = "ingest")]
    Ingest,
}

impl JobType {
    pub fn as_str(&self) -> &str {
        match self {
            JobType::Query => "query",
            JobType::Ingest => "ingest",
        }
    }
}

/// Salesforce Bulk API operation types.
#[derive(PartialEq, Clone, Debug, Default, Deserialize, Serialize)]
pub enum Operation {
    /// Query active records only.
    #[default]
    #[serde(rename = "query")]
    Query,
    /// Query including deleted/archived records.
    #[serde(rename = "queryAll")]
    QueryAll,
    /// Create new records.
    #[serde(rename = "insert")]
    Insert,
    /// Soft delete (move to recycle bin).
    #[serde(rename = "delete")]
    Delete,
    /// Permanently delete records.
    #[serde(rename = "hardDelete")]
    HardDelete,
    /// Update existing records.
    #[serde(rename = "update")]
    Update,
    /// Insert or update based on external ID.
    #[serde(rename = "upsert")]
    Upsert,
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
    fn test_operation_enum_defaults() {
        let op = Operation::default();
        assert_eq!(op, Operation::Query);
    }

    #[test]
    fn test_operation_serialization() {
        assert_eq!(
            serde_json::to_string(&Operation::Query).unwrap(),
            "\"query\""
        );
        assert_eq!(
            serde_json::to_string(&Operation::QueryAll).unwrap(),
            "\"queryAll\""
        );
        assert_eq!(
            serde_json::to_string(&Operation::Insert).unwrap(),
            "\"insert\""
        );
        assert_eq!(
            serde_json::to_string(&Operation::Delete).unwrap(),
            "\"delete\""
        );
        assert_eq!(
            serde_json::to_string(&Operation::HardDelete).unwrap(),
            "\"hardDelete\""
        );
        assert_eq!(
            serde_json::to_string(&Operation::Update).unwrap(),
            "\"update\""
        );
        assert_eq!(
            serde_json::to_string(&Operation::Upsert).unwrap(),
            "\"upsert\""
        );
    }

    #[test]
    fn test_operation_deserialization() {
        assert_eq!(
            serde_json::from_str::<Operation>("\"query\"").unwrap(),
            Operation::Query
        );
        assert_eq!(
            serde_json::from_str::<Operation>("\"queryAll\"").unwrap(),
            Operation::QueryAll
        );
        assert_eq!(
            serde_json::from_str::<Operation>("\"insert\"").unwrap(),
            Operation::Insert
        );
        assert_eq!(
            serde_json::from_str::<Operation>("\"delete\"").unwrap(),
            Operation::Delete
        );
        assert_eq!(
            serde_json::from_str::<Operation>("\"hardDelete\"").unwrap(),
            Operation::HardDelete
        );
        assert_eq!(
            serde_json::from_str::<Operation>("\"update\"").unwrap(),
            Operation::Update
        );
        assert_eq!(
            serde_json::from_str::<Operation>("\"upsert\"").unwrap(),
            Operation::Upsert
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

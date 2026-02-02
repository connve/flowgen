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
/// Configuration for retrieving existing Salesforce bulk jobs.
#[derive(PartialEq, Clone, Debug, Deserialize, Serialize)]
pub struct JobRetrieve {
    /// Unique task identifier.
    pub name: String,
    /// Path to Salesforce authentication credentials.
    pub credentials_path: PathBuf,
    /// Salesforce Job Type like query, ingest.
    #[serde(default)]
    pub job_type: JobType,
    /// Template for extracting job ID from event data.
    /// Example: "{{event.data.JobIdentifier}}" or "{{event.data.id}}"
    /// This is required to dynamically extract the job ID from incoming events.
    pub job_id: String,
    /// Number of rows per Arrow RecordBatch when parsing CSV results.
    /// Default: 10000
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
    /// Whether CSV results include header row.
    /// Default: true
    #[serde(default = "default_has_header")]
    pub has_header: bool,
    /// Optional retry configuration (overrides app-level retry config).
    #[serde(default)]
    pub retry: Option<flowgen_core::retry::RetryConfig>,
}

impl Default for JobRetrieve {
    fn default() -> Self {
        Self {
            name: String::new(),
            credentials_path: PathBuf::new(),
            job_type: JobType::default(),
            job_id: String::new(),
            batch_size: default_batch_size(),
            has_header: default_has_header(),
            retry: None,
        }
    }
}

impl ConfigExt for JobRetrieve {}

/// Configuration for creating new Salesforce bulk jobs.
#[derive(PartialEq, Clone, Debug, Default, Deserialize, Serialize)]
pub struct JobCreate {
    /// Unique task identifier.
    pub name: String,
    /// Path to Salesforce authentication credentials.
    pub credentials_path: PathBuf,
    /// Salesforce Job Type like query, ingest.
    pub job_type: JobType,
    /// SOQL query for query/queryAll operations.
    /// Can be specified as inline SOQL or loaded from external resource file.
    pub query: Option<flowgen_core::resource::Source>,
    /// Salesforce object API name (e.g., "Account", "Contact").
    pub object: Option<String>,
    /// Type of bulk operation to perform.
    pub operation: Operation,
    /// Output file format (currently only CSV supported).
    pub content_type: Option<ContentType>,
    /// Column separator for CSV output.
    pub column_delimiter: Option<ColumnDelimiter>,
    /// Line termination style (LF or CRLF).
    pub line_ending: Option<LineEnding>,
    /// Assignment rule ID for Case or Lead objects.
    pub assignment_rule_id: Option<String>,
    /// External ID field name for upsert operations.
    pub external_id_field_name: Option<String>,
    /// Optional retry configuration (overrides app-level retry config).
    #[serde(default)]
    pub retry: Option<flowgen_core::retry::RetryConfig>,
}

impl ConfigExt for JobCreate {}

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
    use serde_json;
    use std::path::PathBuf;

    #[test]
    fn test_job_retriever_default() {
        let retriever = JobRetrieve::default();
        assert_eq!(retriever.name, "");
        assert_eq!(retriever.credentials_path, PathBuf::new());
        assert_eq!(retriever.retry, None);
    }

    #[test]
    fn test_job_retriever_creation() {
        let retriever = JobRetrieve {
            name: "test_retriever".to_string(),
            credentials_path: PathBuf::from("/path/to/creds.json"),
            job_type: JobType::Query,
            job_id: "test_job_id_123".to_string(),
            batch_size: 10000,
            has_header: true,
            retry: None,
        };
        assert_eq!(retriever.name, "test_retriever");
        assert_eq!(
            retriever.credentials_path,
            PathBuf::from("/path/to/creds.json")
        );
    }

    #[test]
    fn test_job_retriever_serialization() {
        let retriever = JobRetrieve {
            name: "serialization_test".to_string(),
            credentials_path: PathBuf::from("/test/path.json"),
            job_type: JobType::Query,
            job_id: "ser_job_456".to_string(),
            batch_size: 10000,
            has_header: true,
            retry: None,
        };

        let json = serde_json::to_string(&retriever).unwrap();
        let deserialized: JobRetrieve = serde_json::from_str(&json).unwrap();

        assert_eq!(retriever, deserialized);
    }

    #[test]
    fn test_job_creator_default() {
        let creator = JobCreate::default();
        assert_eq!(creator.name, "");
        assert_eq!(creator.credentials_path, PathBuf::new());
        assert_eq!(creator.query, None);
        assert_eq!(creator.object, None);
        assert_eq!(creator.operation, Operation::Query);
        assert_eq!(creator.content_type, None);
        assert_eq!(creator.column_delimiter, None);
        assert_eq!(creator.line_ending, None);
        assert_eq!(creator.assignment_rule_id, None);
        assert_eq!(creator.external_id_field_name, None);
    }

    #[test]
    fn test_job_creator_query_operation() {
        let creator = JobCreate {
            name: "query_job".to_string(),
            credentials_path: PathBuf::from("/creds.json"),
            query: Some(flowgen_core::resource::Source::Inline(
                "SELECT Id, Name FROM Account".to_string(),
            )),
            job_type: JobType::Query,
            object: None,
            operation: Operation::Query,
            content_type: Some(ContentType::Csv),
            column_delimiter: Some(ColumnDelimiter::Comma),
            line_ending: Some(LineEnding::Crlf),
            assignment_rule_id: None,
            external_id_field_name: None,
            retry: None,
        };

        assert_eq!(creator.operation, Operation::Query);
        assert!(creator.query.is_some());
        assert!(creator.object.is_none());
    }

    #[test]
    fn test_job_creator_insert_operation() {
        let creator = JobCreate {
            name: "insert_job".to_string(),
            credentials_path: PathBuf::from("/creds.json"),
            query: None,
            job_type: JobType::Query,
            object: Some("Contact".to_string()),
            operation: Operation::Insert,
            content_type: Some(ContentType::Csv),
            column_delimiter: Some(ColumnDelimiter::Comma),
            line_ending: Some(LineEnding::Lf),
            assignment_rule_id: None,
            external_id_field_name: None,
            retry: None,
        };

        assert_eq!(creator.operation, Operation::Insert);
        assert!(creator.object.is_some());
        assert!(creator.query.is_none());
    }

    #[test]
    fn test_job_creator_upsert_operation() {
        let creator = JobCreate {
            name: "upsert_job".to_string(),
            credentials_path: PathBuf::from("/creds.json"),
            query: None,
            job_type: JobType::Query,
            object: Some("Account".to_string()),
            operation: Operation::Upsert,
            content_type: Some(ContentType::Csv),
            column_delimiter: Some(ColumnDelimiter::Pipe),
            line_ending: Some(LineEnding::Lf),
            assignment_rule_id: None,
            external_id_field_name: Some("External_ID__c".to_string()),
            retry: None,
        };

        assert_eq!(creator.operation, Operation::Upsert);
        assert_eq!(
            creator.external_id_field_name,
            Some("External_ID__c".to_string())
        );
    }

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
    fn test_job_creator_full_serialization() {
        let creator = JobCreate {
            name: "full_job".to_string(),
            credentials_path: PathBuf::from("/path/to/creds.json"),
            query: Some(flowgen_core::resource::Source::Inline(
                "SELECT Id FROM Account".to_string(),
            )),
            job_type: JobType::Query,
            object: None,
            operation: Operation::Query,
            content_type: Some(ContentType::Csv),
            column_delimiter: Some(ColumnDelimiter::Comma),
            line_ending: Some(LineEnding::Crlf),
            assignment_rule_id: Some("rule123".to_string()),
            external_id_field_name: None,
            retry: None,
        };

        let json = serde_json::to_string(&creator).unwrap();
        let deserialized: JobCreate = serde_json::from_str(&json).unwrap();

        assert_eq!(creator, deserialized);
    }

    #[test]
    fn test_job_creator_clone() {
        let creator1 = JobCreate {
            name: "clone_test".to_string(),
            credentials_path: PathBuf::from("/test.json"),
            query: Some(flowgen_core::resource::Source::Inline(
                "SELECT Id FROM Contact".to_string(),
            )),
            job_type: JobType::Query,
            object: None,
            operation: Operation::QueryAll,
            content_type: Some(ContentType::Csv),
            column_delimiter: Some(ColumnDelimiter::Tab),
            line_ending: Some(LineEnding::Lf),
            assignment_rule_id: None,
            external_id_field_name: None,
            retry: None,
        };

        let creator2 = creator1.clone();
        assert_eq!(creator1, creator2);
    }

    #[test]
    fn test_job_retriever_clone() {
        let retriever1 = JobRetrieve {
            name: "clone_test".to_string(),
            credentials_path: PathBuf::from("/test.json"),
            job_type: JobType::Query,
            job_id: "{{event.data.id}}".to_string(),
            batch_size: 10000,
            has_header: true,
            retry: None,
        };

        let retriever2 = retriever1.clone();
        assert_eq!(retriever1, retriever2);
    }

    #[test]
    fn test_partial_job_creator_deserialization() {
        let json = r#"{
            "name": "minimal_job",
            "credentials_path": "/creds.json",
            "operation": "insert",
            "job_type": "query",
            "object": "Account"
        }"#;

        let creator: JobCreate = serde_json::from_str(json).unwrap();
        assert_eq!(creator.name, "minimal_job");
        assert_eq!(creator.operation, Operation::Insert);
        assert_eq!(creator.object, Some("Account".to_string()));
        assert_eq!(creator.query, None);
    }

    #[test]
    fn test_job_creator_with_assignment_rule() {
        let creator = JobCreate {
            name: "case_job".to_string(),
            credentials_path: PathBuf::from("/creds.json"),
            query: None,
            job_type: JobType::Query,
            object: Some("Case".to_string()),
            operation: Operation::Insert,
            content_type: Some(ContentType::Csv),
            column_delimiter: Some(ColumnDelimiter::Comma),
            line_ending: Some(LineEnding::Lf),
            assignment_rule_id: Some("01Q5g000000abcdEAA".to_string()),
            external_id_field_name: None,
            retry: None,
        };

        assert_eq!(
            creator.assignment_rule_id,
            Some("01Q5g000000abcdEAA".to_string())
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

    #[test]
    fn test_debug_implementations() {
        let creator = JobCreate::default();
        let debug_str = format!("{creator:?}");
        assert!(debug_str.contains("JobCreate"));

        let retriever = JobRetrieve::default();
        let debug_str = format!("{retriever:?}");
        assert!(debug_str.contains("JobRetrieve"));
    }
}

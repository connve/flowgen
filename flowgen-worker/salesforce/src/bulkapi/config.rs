use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// Processor for creating salesforce account query job.
/// ```json
/// {
///    "salesforce_bulkapi_job_creator": {
///    "label": "salesforce_query_job",
///         "credentials_path": "/path/to/salesforce_test_creds.json",
///         "operation": "query",
///         "job": "Select Id from Account",
///         "content_type": "CSV",
///         "column_delimiter": "COMMA",
///         "line_ending": "CRLF"
///     }
///  }
/// ```
///
/// Processor for creating salesforce account query all job.
/// ```json
/// {
///    "salesforce_bulkapi_job_creator": {
///    "label": "salesforce_query_all_job",
///         "credentials_path": "/path/to/salesforce_test_creds.json",
///         "operation": "queryAll",
///         "job": "Select Id from Account",
///         "content_type": "CSV",
///         "column_delimiter": "COMMA",
///         "line_ending": "CRLF"
///     }
///  }
/// ```
/// Configuration for retrieving existing Salesforce bulk jobs.
#[derive(PartialEq, Clone, Debug, Default, Deserialize, Serialize)]
pub struct JobRetriever {
    /// Human-readable label for this job retriever.
    pub label: Option<String>,

    /// Path to Salesforce authentication credentials.
    pub credentials_path: PathBuf,

    /// Salesforce Job Type like query, ingest.
    pub job_type: JobType,
}

/// Configuration for creating new Salesforce bulk jobs.
#[derive(PartialEq, Clone, Debug, Default, Deserialize, Serialize)]
pub struct JobCreator {
    /// Unique task identifier.
    pub name: String,

    /// Human-readable label for this job.
    pub label: Option<String>,

    /// Path to Salesforce authentication credentials.
    pub credentials_path: PathBuf,

    /// Salesforce Job Type like query, ingest.
    pub job_type: JobType,

    /// SOQL query for query/queryAll operations.
    pub query: Option<String>,

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
}

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
    #[serde(rename = "CSV")]
    Csv,
}

/// CSV column delimiters.
#[derive(PartialEq, Clone, Debug, Default, Deserialize, Serialize)]
pub enum ColumnDelimiter {
    #[default]
    #[serde(rename = "COMMA")]
    Comma,

    #[serde(rename = "TAB")]
    Tab,

    #[serde(rename = "SEMICOLON")]
    Semicolon,

    #[serde(rename = "PIPE")]
    Pipe,

    #[serde(rename = "CARET")]
    Caret,

    #[serde(rename = "BACKQUOTE")]
    Backquote,
}

/// Line ending styles for cross-platform compatibility.
#[derive(PartialEq, Clone, Debug, Default, Deserialize, Serialize)]
pub enum LineEnding {
    /// Unix/Linux style (\n).
    #[default]
    #[serde(rename = "LF")]
    Lf,

    /// Windows style (\r\n).
    #[serde(rename = "CRLF")]
    Crlf,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json;
    use std::path::PathBuf;

    #[test]
    fn test_job_retriever_default() {
        let retriever = JobRetriever::default();
        assert_eq!(retriever.label, None);
        assert_eq!(retriever.credentials_path, PathBuf::new());
    }

    #[test]
    fn test_job_retriever_creation() {
        let retriever = JobRetriever {
            label: Some("test_retriever".to_string()),
            credentials_path: PathBuf::from("/path/to/creds.json"),
            job_type: JobType::Query,
        };

        assert_eq!(retriever.label, Some("test_retriever".to_string()));
        assert_eq!(
            retriever.credentials_path,
            PathBuf::from("/path/to/creds.json")
        );
    }

    #[test]
    fn test_job_retriever_serialization() {
        let retriever = JobRetriever {
            label: Some("test_label".to_string()),
            credentials_path: PathBuf::from("/test/path.json"),
            job_type: JobType::Query,
        };

        let json = serde_json::to_string(&retriever).unwrap();
        let deserialized: JobRetriever = serde_json::from_str(&json).unwrap();

        assert_eq!(retriever, deserialized);
    }

    #[test]
    fn test_job_creator_default() {
        let creator = JobCreator::default();
        assert_eq!(creator.name, "");
        assert_eq!(creator.label, None);
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
        let creator = JobCreator {
            name: "query_job".to_string(),
            label: Some("Account Query".to_string()),
            credentials_path: PathBuf::from("/creds.json"),
            query: Some("SELECT Id, Name FROM Account".to_string()),
            job_type: JobType::Query,
            object: None,
            operation: Operation::Query,
            content_type: Some(ContentType::Csv),
            column_delimiter: Some(ColumnDelimiter::Comma),
            line_ending: Some(LineEnding::Crlf),
            assignment_rule_id: None,
            external_id_field_name: None,
        };

        assert_eq!(creator.operation, Operation::Query);
        assert!(creator.query.is_some());
        assert!(creator.object.is_none());
    }

    #[test]
    fn test_job_creator_insert_operation() {
        let creator = JobCreator {
            name: "insert_job".to_string(),
            label: Some("Insert Contacts".to_string()),
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
        };

        assert_eq!(creator.operation, Operation::Insert);
        assert!(creator.object.is_some());
        assert!(creator.query.is_none());
    }

    #[test]
    fn test_job_creator_upsert_operation() {
        let creator = JobCreator {
            name: "upsert_job".to_string(),
            label: Some("Upsert Accounts".to_string()),
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
        assert_eq!(serde_json::to_string(&ContentType::Csv).unwrap(), "\"CSV\"");
    }

    #[test]
    fn test_content_type_deserialization() {
        assert_eq!(
            serde_json::from_str::<ContentType>("\"CSV\"").unwrap(),
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
            "\"COMMA\""
        );
        assert_eq!(
            serde_json::to_string(&ColumnDelimiter::Tab).unwrap(),
            "\"TAB\""
        );
        assert_eq!(
            serde_json::to_string(&ColumnDelimiter::Semicolon).unwrap(),
            "\"SEMICOLON\""
        );
        assert_eq!(
            serde_json::to_string(&ColumnDelimiter::Pipe).unwrap(),
            "\"PIPE\""
        );
        assert_eq!(
            serde_json::to_string(&ColumnDelimiter::Caret).unwrap(),
            "\"CARET\""
        );
        assert_eq!(
            serde_json::to_string(&ColumnDelimiter::Backquote).unwrap(),
            "\"BACKQUOTE\""
        );
    }

    #[test]
    fn test_column_delimiter_deserialization() {
        assert_eq!(
            serde_json::from_str::<ColumnDelimiter>("\"COMMA\"").unwrap(),
            ColumnDelimiter::Comma
        );
        assert_eq!(
            serde_json::from_str::<ColumnDelimiter>("\"TAB\"").unwrap(),
            ColumnDelimiter::Tab
        );
        assert_eq!(
            serde_json::from_str::<ColumnDelimiter>("\"SEMICOLON\"").unwrap(),
            ColumnDelimiter::Semicolon
        );
        assert_eq!(
            serde_json::from_str::<ColumnDelimiter>("\"PIPE\"").unwrap(),
            ColumnDelimiter::Pipe
        );
        assert_eq!(
            serde_json::from_str::<ColumnDelimiter>("\"CARET\"").unwrap(),
            ColumnDelimiter::Caret
        );
        assert_eq!(
            serde_json::from_str::<ColumnDelimiter>("\"BACKQUOTE\"").unwrap(),
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
        assert_eq!(serde_json::to_string(&LineEnding::Lf).unwrap(), "\"LF\"");
        assert_eq!(
            serde_json::to_string(&LineEnding::Crlf).unwrap(),
            "\"CRLF\""
        );
    }

    #[test]
    fn test_line_ending_deserialization() {
        assert_eq!(
            serde_json::from_str::<LineEnding>("\"LF\"").unwrap(),
            LineEnding::Lf
        );
        assert_eq!(
            serde_json::from_str::<LineEnding>("\"CRLF\"").unwrap(),
            LineEnding::Crlf
        );
    }

    #[test]
    fn test_job_creator_full_serialization() {
        let creator = JobCreator {
            name: "full_job".to_string(),
            label: Some("Full Job Config".to_string()),
            credentials_path: PathBuf::from("/path/to/creds.json"),
            query: Some("SELECT Id FROM Account".to_string()),
            job_type: JobType::Query,
            object: None,
            operation: Operation::Query,
            content_type: Some(ContentType::Csv),
            column_delimiter: Some(ColumnDelimiter::Comma),
            line_ending: Some(LineEnding::Crlf),
            assignment_rule_id: Some("rule123".to_string()),
            external_id_field_name: None,
        };

        let json = serde_json::to_string(&creator).unwrap();
        let deserialized: JobCreator = serde_json::from_str(&json).unwrap();

        assert_eq!(creator, deserialized);
    }

    #[test]
    fn test_job_creator_clone() {
        let creator1 = JobCreator {
            name: "clone_test".to_string(),
            label: Some("Clone Test".to_string()),
            credentials_path: PathBuf::from("/test.json"),
            query: Some("SELECT Id FROM Contact".to_string()),
            job_type: JobType::Query,
            object: None,
            operation: Operation::QueryAll,
            content_type: Some(ContentType::Csv),
            column_delimiter: Some(ColumnDelimiter::Tab),
            line_ending: Some(LineEnding::Lf),
            assignment_rule_id: None,
            external_id_field_name: None,
        };

        let creator2 = creator1.clone();
        assert_eq!(creator1, creator2);
    }

    #[test]
    fn test_job_retriever_clone() {
        let retriever1 = JobRetriever {
            label: Some("clone_test".to_string()),
            credentials_path: PathBuf::from("/test.json"),
            job_type: JobType::Query,
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

        let creator: JobCreator = serde_json::from_str(json).unwrap();
        assert_eq!(creator.name, "minimal_job");
        assert_eq!(creator.operation, Operation::Insert);
        assert_eq!(creator.object, Some("Account".to_string()));
        assert_eq!(creator.label, None);
        assert_eq!(creator.query, None);
    }

    #[test]
    fn test_job_creator_with_assignment_rule() {
        let creator = JobCreator {
            name: "case_job".to_string(),
            label: Some("Case Assignment".to_string()),
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
        let creator = JobCreator::default();
        let debug_str = format!("{:?}", creator);
        assert!(debug_str.contains("JobCreator"));

        let retriever = JobRetriever::default();
        let debug_str = format!("{:?}", retriever);
        assert!(debug_str.contains("JobRetriever"));
    }
}

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
///
/// /// Processor for creating salesforce account query all job.
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
///
/// This struct is used to configure a job retrieval operation that can fetch
/// information about previously created Salesforce bulk jobs, including their
/// status, results, and metadata.
#[derive(PartialEq, Clone, Debug, Default, Deserialize, Serialize)]
pub struct JobRetriever {
    /// Optional human-readable label for identifying this job retriever configuration.
    ///
    /// Useful for distinguishing between multiple job retrieval configurations
    /// in logging, monitoring, and configuration management contexts.
    pub label: Option<String>,

    /// Reference to credential store entry containing Salesforce authentication details.
    ///
    /// This should be a key or path to securely stored Salesforce credentials
    /// (username, password, security token, or OAuth tokens) required for
    /// authenticating with the Salesforce Bulk API.
    pub credentials_path: PathBuf,
}

/// Configuration for creating new Salesforce bulk jobs.
///
/// This struct encapsulates all the parameters needed to create and configure
/// a Salesforce Bulk API job for various operations like querying, inserting,
/// updating, or deleting records in bulk.
#[derive(PartialEq, Clone, Debug, Default, Deserialize, Serialize)]
pub struct JobCreator {
    /// The unique name / identifier of the task.
    pub name: String,
    /// Optional human-readable label for identifying this job creator configuration.
    ///
    /// Helpful for distinguishing between different job configurations in logs,
    /// monitoring dashboards, and configuration management systems.
    pub label: Option<String>,

    /// Reference to credential store entry containing Salesforce authentication details.
    ///
    /// Points to securely stored Salesforce credentials needed for Bulk API authentication.
    /// Should not contain actual credentials for security reasons.
    pub credentials_path: PathBuf,

    /// SOQL query string for query and queryAll operations.
    ///
    /// Required for query-based operations. Should contain a valid SOQL query
    /// that will be executed against the Salesforce database.
    /// Example: "SELECT Id, Name FROM Account WHERE CreatedDate = TODAY"
    pub query: Option<String>,

    /// Salesforce object API name for data manipulation operations.
    ///
    /// Required for insert, update, delete, and upsert operations.
    /// Specifies the Salesforce object type to operate on.
    /// Examples: "Account", "Contact", "Custom_Object__c"
    pub object: Option<String>,

    /// The type of bulk operation to perform.
    ///
    /// Determines whether this job will query data, insert records,
    /// update existing records, etc. Each operation has different
    /// requirements for other configuration fields.
    pub operation: Operation,

    /// Output file format for bulk job results.
    ///
    /// Specifies how the job results should be formatted when retrieved.
    /// Currently only CSV format is supported by most Salesforce bulk operations.
    pub content_type: Option<ContentType>,

    /// Column separator character for CSV output files.
    ///
    /// Defines how columns are separated in the result CSV files.
    /// Different delimiters may be needed for compatibility with
    /// downstream systems or to handle data containing commas.
    pub column_delimiter: Option<ColumnDelimiter>,

    /// Line termination style for output files.
    ///
    /// Specifies whether to use Unix-style (LF) or Windows-style (CRLF)
    /// line endings in the output files. Important for cross-platform
    /// compatibility of generated files.
    pub line_ending: Option<LineEnding>,

    /// The ID of an assignment rule to run for Case or Lead objects.
    ///
    /// When creating or updating Case or Lead records, this field can specify
    /// an assignment rule to automatically assign the records to appropriate
    /// users or queues. The rule can be active or inactive.
    ///
    /// The ID can be retrieved using Salesforce SOAP or REST APIs to query
    /// the AssignmentRule object.
    pub assignment_rule_id: Option<String>,

    /// The external ID field name for upsert operations.
    ///
    /// Required only for upsert operations. Specifies which field should be used
    /// as the external identifier to determine whether to insert a new record
    /// or update an existing one.
    ///
    /// Example: "External_ID__c" or "Email" for Contact objects
    pub external_id_field_name: Option<String>,
}

/// Enumeration of supported Salesforce Bulk API operations.
///
/// Each operation type has different requirements and behaviors:
/// - Query operations retrieve data and require a SOQL query
/// - Data manipulation operations require an object name and appropriate data
#[derive(PartialEq, Clone, Debug, Default, Deserialize, Serialize)]
pub enum Operation {
    /// Standard query operation that returns only non-deleted records.
    /// Requires a valid SOQL query in the `query` field. Results include
    /// only records that are currently active (not in the recycle bin).
    #[default]
    #[serde(rename = "query")]
    Query,

    /// Query all operation that includes deleted and archived records.
    /// Similar to Query but also returns soft-deleted records from the
    /// recycle bin. Useful for data archival and complete data exports.
    #[serde(rename = "queryAll")]
    QueryAll,

    /// Insert operation for creating new records.
    #[serde(rename = "insert")]
    Insert,

    /// Soft delete operation that moves records to the recycle bin.
    #[serde(rename = "delete")]
    Delete,

    /// Hard delete operation that permanently removes records.
    #[serde(rename = "hardDelete")]
    HardDelete,

    /// Update operation for modifying existing records.
    #[serde(rename = "update")]
    Update,

    /// Upsert operation that inserts or updates based on external ID.
    #[serde(rename = "upsert")]
    Upsert,
}

/// Supported content types for bulk job output files.
#[derive(PartialEq, Clone, Debug, Default, Deserialize, Serialize)]
pub enum ContentType {
    /// Comma-Separated Values format.
    #[default]
    #[serde(rename = "CSV")]
    Csv, // Currently only supports CSV.
}

/// Available column delimiter options for CSV output formatting.
#[derive(PartialEq, Clone, Debug, Default, Deserialize, Serialize)]
pub enum ColumnDelimiter {
    /// Standard comma delimiter (most common CSV format).
    #[default]
    #[serde(rename = "COMMA")]
    Comma,

    /// Tab character as delimiter.
    #[serde(rename = "TAB")]
    Tab,

    /// Semicolon delimiter.
    #[serde(rename = "SEMICOLON")]
    Semicolon,

    /// Pipe character (|) as delimiter.
    #[serde(rename = "PIPE")]
    Pipe,

    /// Caret character (^) as delimiter.
    ///
    /// Rarely appears in business data, providing reliable field separation.
    /// Sometimes used in legacy systems or specialized data formats.
    #[serde(rename = "CARET")]
    Caret,

    /// Backquote character (`) as delimiter.
    /// Another rare character that can serve as a reliable delimiter
    /// when standard options might conflict with data content.
    #[serde(rename = "BACKQUOTE")]
    Backquote,
}

/// Line ending styles for output file formatting.
/// Different operating systems use different conventions for line endings.
/// Choosing the correct style ensures proper file handling across platforms.
#[derive(PartialEq, Clone, Debug, Default, Deserialize, Serialize)]
pub enum LineEnding {
    /// Unix/Linux style line ending (Line Feed only).
    /// Single LF character (\n). Standard on Unix-like systems including
    /// Linux and macOS. More compact than CRLF.
    #[default]
    #[serde(rename = "LF")]
    Lf,

    /// Windows style line ending (Carriage Return + Line Feed).
    /// Two-character sequence (\r\n). Required for proper display
    /// in Windows text editors and some legacy systems.
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
        let delimiters = vec![
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

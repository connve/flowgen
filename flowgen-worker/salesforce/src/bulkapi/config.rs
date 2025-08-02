use flowgen_core::config::ConfigExt;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::path::PathBuf;

/// Processor for creating salesforce account query job.
/// ```json
/// {
///     "salesforce_query": {
///    "label": "salesforce_query_job",
///         "credentials": "/etc/sfdc_dev.json",
///         "operation": "Query",
///         "job": "Select Id from Account",
///         "content_type": "Csv",
///         "column_delimiter": "Comma",
///         "line_ending": "Crlf"
///     }
///  }
/// ```
/// 
/// Processor for creating salesforce insert contact job.
/// ```json
/// {
///     "salesforce_query": {
///    "label": "salesforce_insert_job",
///         "credentials": "/etc/sfdc_dev.json",
///         "operation": "Insert",
///         "job": "Contact",
///         "content_type": "Csv",
///         "column_delimiter": "Comma",
///         "line_ending": "Crlf"
///     }
///  }
/// ```
#[derive(PartialEq, Clone, Debug, Default, Deserialize, Serialize)]
pub struct Processor {
    /// Optional human-readable label for identifying this subscriber configuration.
    pub label: Option<String>,
    /// Reference to credential store entry containing Salesforce authentication details.
    pub credentials: String,
    /// Salesforce query input for job create.
    pub query:  Option<String>,
    /// Salesforce object for job create.
    pub object: Option<String>,
    /// Operation name related to Salesforce bulk job.
    pub operation: Option<Operation>,
    /// Output file format for the bulk job.
    pub content_type: Option<ContentType>,
    /// Column delimeter for output file for the bulk job.
    pub column_delimiter: Option<ColumnDelimiter>,
    /// Line ending for output file for the bulk job.
    pub line_ending: Option<LineEnding>,
    /// The ID of an assignment rule to run for a Case or a Lead.
    /// The assignment rule can be active or inactive. 
    /// The ID can be retrieved by using the Lightning Platform SOAP API, 
    /// or the Lightning Platform REST API to query the AssignmentRule object.
    pub assignment_rule_id: Option<String>,
    ///The external ID field in the object being updated. 
    /// Only needed for Upsert operations.
    pub external_id_field_name: Option<String>,  
}


#[derive(PartialEq, Clone, Debug, Default, Deserialize, Serialize)]
pub enum Operation {
    /// Defaults to query job.
    #[default]
    #[serde(rename = "query")]
    Query,
    #[serde(rename = "queryAll")]
    QueryAll,
    #[serde(rename = "insert")]
    Insert,
    #[serde(rename = "delete")]
    Delete,
    #[serde(rename = "hardDelete")]
    HardDelete,
    #[serde(rename = "update")]
    Update,
    #[serde(rename = "upsert")]
    Upsert,
}

#[derive(PartialEq, Clone, Debug, Default, Deserialize, Serialize)]
pub enum ContentType {
    #[default]
    #[serde(rename = "CSV")]
    Csv, // Currently only supports CSV.
}

#[derive(PartialEq, Clone, Debug, Default, Deserialize, Serialize)]
pub enum ColumnDelimiter {
    /// Defaults to comma as column delimiter.
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

#[derive(PartialEq, Clone, Debug, Default, Deserialize, Serialize)]
pub enum LineEnding {
    /// Defaults to CRLF as line ending.
    #[default]
    #[serde(rename = "LF")]
    Lf,
    #[serde(rename = "CRLF")]
    Crlf,
}

impl ConfigExt for Processor {}

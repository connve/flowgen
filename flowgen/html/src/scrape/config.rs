//! Configuration for the `html_scrape` task.
//!
//! Defines how to extract structured data from an HTML document using CSS
//! selectors. The task receives HTML content in `event.data` (as a string or
//! JSON string field) and emits extracted rows as JSON.

use flowgen_core::config::ConfigExt;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Configuration for the `html_scrape` processor.
///
/// # Example YAML
///
/// ```yaml
/// - html_scrape:
///     name: extract_rows
///     row_selector: "table.results tbody tr"
///     fields:
///       case_number: "td:nth-child(1)"
///       company: "td:nth-child(2)"
///       document_url: "a.download-link@href"
///     emit: rows
/// ```
///
/// The `row_selector` picks repeating elements (one per output row). Each
/// `fields` entry is a CSS selector evaluated relative to the matched row
/// element. Append `@attribute` to the selector to extract an attribute value
/// (e.g. `a@href`) instead of the text content. Use `.` as the selector to
/// refer to the row element itself — typically combined with `@attr` to read
/// an attribute off the row (e.g. `.@data-url`).
#[derive(PartialEq, Clone, Debug, Default, Deserialize, Serialize)]
pub struct Processor {
    /// Unique task name.
    pub name: String,
    /// CSS selector that matches each row element to extract.
    /// One output row is produced per matched element.
    pub row_selector: String,
    /// Mapping of output field name to CSS selector.
    /// Selectors are evaluated relative to each matched row element.
    /// Append `@attribute` to extract an attribute value instead of text
    /// (e.g. `a.link@href`).
    pub fields: HashMap<String, String>,
    /// How to emit the extracted rows: as a single JSON array (`rows`) or
    /// as a wrapper object (`wrapped`).
    #[serde(default)]
    pub emit: EmitStrategy,
    /// Optional field name on the incoming `event.data` that contains the HTML
    /// content. If not set, the whole `event.data` is treated as HTML (either a
    /// JSON string or a raw string).
    pub html_field: Option<String>,
    /// Optional list of upstream task names this task depends on.
    /// When set, this task only receives events from the named tasks.
    /// When not set, the task receives from the previous task in the list (linear chain).
    #[serde(default)]
    pub depends_on: Option<Vec<String>>,
    /// Optional retry configuration (overrides app-level retry config).
    #[serde(default)]
    pub retry: Option<flowgen_core::retry::RetryConfig>,
}

impl ConfigExt for Processor {}

/// Controls the shape of the emitted event data.
#[derive(PartialEq, Eq, Clone, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum EmitStrategy {
    /// Emit a JSON array of extracted row objects.
    #[default]
    Rows,
    /// Emit a wrapper object `{ "rows": [...] }` containing the extracted rows.
    Wrapped,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_processor_default() {
        let processor = Processor::default();
        assert_eq!(processor.name, String::new());
        assert_eq!(processor.row_selector, String::new());
        assert!(processor.fields.is_empty());
        assert_eq!(processor.emit, EmitStrategy::Rows);
        assert!(processor.html_field.is_none());
    }

    #[test]
    fn test_processor_serialization() {
        let mut fields = HashMap::new();
        fields.insert("title".to_string(), "h1".to_string());
        fields.insert("link".to_string(), "a@href".to_string());

        let processor = Processor {
            name: "extract".to_string(),
            row_selector: "article".to_string(),
            fields,
            emit: EmitStrategy::Wrapped,
            html_field: Some("body".to_string()),
            depends_on: None,
            retry: None,
        };

        let json = serde_json::to_string(&processor).unwrap();
        let deserialized: Processor = serde_json::from_str(&json).unwrap();
        assert_eq!(processor, deserialized);
    }

    #[test]
    fn test_emit_strategy_default() {
        assert_eq!(EmitStrategy::default(), EmitStrategy::Rows);
    }
}

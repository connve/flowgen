//! HTML scrape processor for extracting structured data from HTML.
//!
//! Parses the HTML content from `event.data` using CSS selectors defined in the
//! configuration and emits a new event containing the extracted rows. The HTML
//! is usually produced by an upstream `http_request` task.

use flowgen_core::config::ConfigExt;
use flowgen_core::event::{Event, EventBuilder, EventData, EventExt};
use futures_util::future;
use scraper::{Html, Selector};
use serde_json::{Map, Value};
use std::sync::Arc;
use tokio::sync::mpsc::{Receiver, Sender};
use tracing::{error, Instrument};

/// Selector literal that refers to the row element itself rather than a
/// descendant. Combined with `@attr` to read an attribute off the row.
const SELF_SELECTOR: &str = ".";

/// Separator between a CSS selector and an attribute name (e.g., `a@href`).
const ATTRIBUTE_SEPARATOR: char = '@';

/// Errors that can occur during HTML scrape processing.
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum Error {
    #[error("Error sending event to channel: {source}")]
    SendMessage {
        #[source]
        source: flowgen_core::event::Error,
    },
    #[error("Error building event: {source}")]
    EventBuilder {
        #[source]
        source: flowgen_core::event::Error,
    },
    #[error("Failed to render configuration template: {source}")]
    ConfigRender {
        #[source]
        source: flowgen_core::config::Error,
    },
    #[error("Invalid CSS selector '{selector}': {message}")]
    InvalidSelector { selector: String, message: String },
    #[error("Incoming event data must contain HTML as a string.")]
    HtmlNotString,
    #[error("Field '{field}' not found on event data.")]
    HtmlFieldMissing { field: String },
    #[error("Failed to decode event data as JSON: {source}")]
    InvalidEventData {
        #[source]
        source: flowgen_core::event::Error,
    },
    #[error("Missing required builder attribute: {}", _0)]
    MissingRequiredAttribute(String),
    #[error("Task failed after all retry attempts: {source}")]
    RetryExhausted {
        #[source]
        source: Box<Error>,
    },
}

/// Parsed CSS selector pair for a single output field.
#[derive(Clone, Debug)]
struct FieldSelector {
    /// Output field name on the extracted row.
    name: String,
    /// Compiled CSS selector evaluated relative to each row element.
    /// `None` means extract directly from the row element itself (selector `.`).
    selector: Option<Selector>,
    /// If set, extract this attribute's value instead of the element text.
    attribute: Option<String>,
}

/// Compiled CSS selectors ready to run against HTML documents.
#[derive(Clone, Debug)]
struct CompiledSelectors {
    /// Selector matching each row element.
    row: Selector,
    /// Selectors for each output field, evaluated relative to a row element.
    fields: Vec<FieldSelector>,
}

/// Handles individual HTML scrape operations.
#[derive(Debug)]
pub struct EventHandler {
    /// Processor configuration settings.
    config: Arc<super::config::Processor>,
    /// Channel sender for emitting extracted rows downstream.
    tx: Option<Sender<Event>>,
    /// Task identifier for event tracking.
    task_id: usize,
    /// Task type identifier for event categorization and logging.
    task_type: &'static str,
    /// Task execution context providing metadata and runtime configuration.
    task_context: Arc<flowgen_core::task::context::TaskContext>,
    /// Pre-compiled CSS selectors to avoid parsing on every event.
    selectors: CompiledSelectors,
}

impl EventHandler {
    /// Extracts HTML content from the incoming event data.
    ///
    /// If `html_field` is set on the config, looks up that field on the event's
    /// JSON data. Otherwise, treats the entire `event.data` JSON as either a raw
    /// string or a JSON string.
    fn extract_html(&self, event: &Event) -> Result<String, Error> {
        let data = event
            .data_as_json()
            .map_err(|source| Error::InvalidEventData { source })?;

        let value = match &self.config.html_field {
            Some(field) => data
                .get(field)
                .ok_or_else(|| Error::HtmlFieldMissing {
                    field: field.clone(),
                })?
                .clone(),
            None => data,
        };

        match value {
            Value::String(s) => Ok(s),
            _ => Err(Error::HtmlNotString),
        }
    }

    /// Extracts the text or attribute value for a single field from a row element.
    ///
    /// When `field.selector` is `None`, the row element itself is used as the
    /// target (the `.` selector syntax). Otherwise the first descendant
    /// matching the selector is used. Missing targets yield `Value::Null` so
    /// downstream consumers can distinguish absent values from empty strings.
    fn extract_field(element: scraper::ElementRef<'_>, field: &FieldSelector) -> Value {
        let target = match &field.selector {
            None => element,
            Some(sel) => match element.select(sel).next() {
                Some(found) => found,
                None => return Value::Null,
            },
        };

        match &field.attribute {
            Some(attr) => match target.value().attr(attr) {
                Some(value) => Value::String(value.to_string()),
                None => Value::Null,
            },
            None => {
                let text = target.text().collect::<String>().trim().to_string();
                if text.is_empty() {
                    Value::Null
                } else {
                    Value::String(text)
                }
            }
        }
    }

    /// Parses the HTML document and extracts all matching rows using the
    /// processor's pre-compiled selectors.
    fn scrape(&self, html: &str) -> Vec<Value> {
        scrape_document(html, &self.selectors)
    }

    /// Processes a single event: extract HTML, scrape, and emit results.
    #[tracing::instrument(skip(self, event), name = "task.handle", fields(task = %self.config.name, task_id = self.task_id, task_type = %self.task_type))]
    async fn handle(&self, event: Event) -> Result<(), Error> {
        if self.task_context.cancellation_token.is_cancelled() {
            return Ok(());
        }

        let event = Arc::new(event);
        let completion_tx_arc = Arc::clone(&event).completion_tx.clone();

        flowgen_core::event::with_event_context(&Arc::clone(&event), async move {
            let html = self.extract_html(&event)?;
            let rows = self.scrape(&html);

            let data = match self.config.emit {
                super::config::EmitStrategy::Rows => Value::Array(rows),
                super::config::EmitStrategy::Wrapped => {
                    let mut wrapped = Map::new();
                    wrapped.insert("rows".to_string(), Value::Array(rows));
                    Value::Object(wrapped)
                }
            };

            let mut e = EventBuilder::new()
                .data(EventData::Json(data))
                .subject(self.config.name.to_owned())
                .task_id(self.task_id)
                .task_type(self.task_type)
                .build()
                .map_err(|source| Error::EventBuilder { source })?;

            // Signal completion or pass through to next task.
            match self.tx {
                None => {
                    // Leaf task: signal completion.
                    if let Some(arc) = completion_tx_arc.as_ref() {
                        arc.signal_completion(None);
                    }
                }
                Some(_) => {
                    e.completion_tx = completion_tx_arc.clone();
                }
            }

            e.send_with_logging(self.tx.as_ref())
                .await
                .map_err(|source| Error::SendMessage { source })?;

            Ok(())
        })
        .await
    }
}

/// HTML scrape processor that extracts structured data using CSS selectors.
#[derive(Debug)]
pub struct Processor {
    /// Processor configuration.
    config: Arc<super::config::Processor>,
    /// Channel sender for extracted rows.
    tx: Option<Sender<Event>>,
    /// Channel receiver for incoming HTML events.
    rx: Receiver<Event>,
    /// Task identifier.
    task_id: usize,
    /// Task execution context providing metadata and runtime configuration.
    task_context: Arc<flowgen_core::task::context::TaskContext>,
    /// Task type identifier for event categorization and logging.
    task_type: &'static str,
}

/// Parses the HTML document and extracts all rows matching `selectors.row`.
///
/// Each matching element is turned into a JSON object keyed by field name.
/// Extracted as a free function so it can be unit-tested without constructing
/// the full `EventHandler` (which requires tokio channels and a task context).
fn scrape_document(html: &str, selectors: &CompiledSelectors) -> Vec<Value> {
    let document = Html::parse_document(html);
    let mut rows = Vec::new();

    for element in document.select(&selectors.row) {
        let mut row = Map::new();
        for field in &selectors.fields {
            row.insert(
                field.name.clone(),
                EventHandler::extract_field(element, field),
            );
        }
        rows.push(Value::Object(row));
    }

    rows
}

/// Splits a selector string of the form `selector@attribute` into the base
/// selector and an optional attribute name.
///
/// A trailing `@` with no attribute after it is treated as part of the
/// selector to avoid silently producing a surprising empty attribute name.
fn split_selector(raw: &str) -> (&str, Option<String>) {
    match raw.rsplit_once(ATTRIBUTE_SEPARATOR) {
        Some((selector, attr)) if !attr.is_empty() => (selector.trim(), Some(attr.to_string())),
        _ => (raw, None),
    }
}

/// Parses a raw CSS selector string into a compiled `Selector`.
fn parse_selector(raw: &str) -> Result<Selector, Error> {
    Selector::parse(raw).map_err(|e| Error::InvalidSelector {
        selector: raw.to_string(),
        message: e.to_string(),
    })
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

        let row_selector = parse_selector(&init_config.row_selector)?;

        let mut fields = Vec::with_capacity(init_config.fields.len());
        for (name, raw) in &init_config.fields {
            let (selector_part, attribute) = split_selector(raw);
            // The literal `.` refers to the row element itself. It is used to
            // read an attribute off the row element via `.@attr` (for example
            // `.@data-url`). Any other string is compiled as a CSS selector.
            let selector = match selector_part {
                SELF_SELECTOR => None,
                other => Some(parse_selector(other)?),
            };
            fields.push(FieldSelector {
                name: name.clone(),
                selector,
                attribute,
            });
        }

        Ok(EventHandler {
            config: Arc::new(init_config),
            tx: self.tx.clone(),
            task_id: self.task_id,
            task_type: self.task_type,
            task_context: Arc::clone(&self.task_context),
            selectors: CompiledSelectors {
                row: row_selector,
                fields,
            },
        })
    }

    #[tracing::instrument(skip(self), name = "task.run", fields(task = %self.config.name, task_id = self.task_id, task_type = %self.task_type))]
    async fn run(mut self) -> Result<(), Error> {
        let retry_config =
            flowgen_core::retry::RetryConfig::merge(&self.task_context.retry, &self.config.retry);

        let event_handler = Arc::new(
            tokio_retry::Retry::spawn(retry_config.strategy(), || async {
                self.init().await.map_err(|e| {
                    error!(error = %e, "Failed to initialize HTML scrape processor.");
                    tokio_retry::RetryError::transient(e)
                })
            })
            .await?,
        );

        let mut handlers = Vec::new();

        loop {
            match self.rx.recv().await {
                Some(event) => {
                    let event_handler = Arc::clone(&event_handler);
                    let retry_strategy = retry_config.strategy();
                    let handle = tokio::spawn(
                        async move {
                            let result = tokio_retry::Retry::spawn(retry_strategy, || async {
                                event_handler.handle(event.clone()).await.map_err(|e| {
                                    error!(error = %e, "Failed to scrape HTML.");
                                    tokio_retry::RetryError::transient(e)
                                })
                            })
                            .await;

                            if let Err(err) = result {
                                error!(error = %err, "HTML scrape failed after all retry attempts.");
                                // Emit error event downstream for error handling.
                                let mut error_event = event.clone();
                                error_event.error = Some(err.to_string());
                                if let Some(ref tx) = event_handler.tx {
                                    tx.send(error_event).await.ok();
                                }
                            }
                        }
                        .instrument(tracing::Span::current()),
                    );
                    handlers.push(handle);
                }
                None => {
                    // Channel closed, wait for all spawned handlers to complete.
                    future::join_all(handlers).await;
                    return Ok(());
                }
            }
        }
    }
}

/// Builder for constructing the HTML scrape processor.
#[derive(Debug, Default)]
pub struct ProcessorBuilder {
    config: Option<Arc<super::config::Processor>>,
    tx: Option<Sender<Event>>,
    rx: Option<Receiver<Event>>,
    task_id: Option<usize>,
    task_context: Option<Arc<flowgen_core::task::context::TaskContext>>,
    task_type: Option<&'static str>,
}

impl ProcessorBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn config(mut self, config: Arc<super::config::Processor>) -> Self {
        self.config = Some(config);
        self
    }

    pub fn receiver(mut self, rx: Receiver<Event>) -> Self {
        self.rx = Some(rx);
        self
    }

    pub fn sender(mut self, tx: Sender<Event>) -> Self {
        self.tx = Some(tx);
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

    pub async fn build(self) -> Result<Processor, Error> {
        Ok(Processor {
            config: self
                .config
                .ok_or_else(|| Error::MissingRequiredAttribute("config".to_string()))?,
            tx: self.tx,
            rx: self
                .rx
                .ok_or_else(|| Error::MissingRequiredAttribute("receiver".to_string()))?,
            task_id: self
                .task_id
                .ok_or_else(|| Error::MissingRequiredAttribute("task_id".to_string()))?,
            task_context: self
                .task_context
                .ok_or_else(|| Error::MissingRequiredAttribute("task_context".to_string()))?,
            task_type: self
                .task_type
                .ok_or_else(|| Error::MissingRequiredAttribute("task_type".to_string()))?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn test_split_selector_no_attribute() {
        let (s, a) = split_selector("td:nth-child(1)");
        assert_eq!(s, "td:nth-child(1)");
        assert!(a.is_none());
    }

    #[test]
    fn test_split_selector_with_attribute() {
        let (s, a) = split_selector("a.link@href");
        assert_eq!(s, "a.link");
        assert_eq!(a, Some("href".to_string()));
    }

    #[test]
    fn test_split_selector_trailing_at_ignored() {
        let (s, a) = split_selector("a.link@");
        assert_eq!(s, "a.link@");
        assert!(a.is_none());
    }

    #[test]
    fn test_parse_selector_valid() {
        assert!(parse_selector("table.results tr").is_ok());
    }

    #[test]
    fn test_parse_selector_invalid() {
        let result = parse_selector(">>>");
        assert!(matches!(result, Err(Error::InvalidSelector { .. })));
    }

    /// Verifies the `.` self-selector returns the row element itself so that
    /// `.@attr` can read attributes off the row (e.g., `data-url` on a `<tr>`).
    #[test]
    fn test_extract_field_self_selector_attribute() {
        let html = r#"<table><tr data-url="wyszukiwanie.php?details=7"><td>row</td></tr></table>"#;
        let document = Html::parse_document(html);
        let row_selector = Selector::parse("tr").unwrap();
        let row = document.select(&row_selector).next().unwrap();

        let field = FieldSelector {
            name: "details_path".to_string(),
            selector: None,
            attribute: Some("data-url".to_string()),
        };

        let value = EventHandler::extract_field(row, &field);
        assert_eq!(
            value,
            Value::String("wyszukiwanie.php?details=7".to_string())
        );
    }

    #[test]
    fn test_extract_field_missing_attribute_returns_null() {
        let html = r#"<table><tr><td>row</td></tr></table>"#;
        let document = Html::parse_document(html);
        let row_selector = Selector::parse("tr").unwrap();
        let row = document.select(&row_selector).next().unwrap();

        let field = FieldSelector {
            name: "details_path".to_string(),
            selector: None,
            attribute: Some("data-url".to_string()),
        };

        assert_eq!(EventHandler::extract_field(row, &field), Value::Null);
    }

    /// End-to-end check of the scrape pipeline against UOKiK-shaped HTML:
    /// multi-row table with a `data-url` attribute on each `<tr>` and nine
    /// `<td>` columns. Exercises the `.` self-selector, `nth-child`, and
    /// missing-value handling all in one pass.
    #[test]
    fn test_scrape_document_extracts_rows_and_self_attribute() {
        let html = r#"
            <html><body>
                <table class="results">
                    <thead><tr><th>LP</th><th>DATE</th></tr></thead>
                    <tbody>
                        <tr data-url="wyszukiwanie.php?details=1" class="result_item">
                            <td>100</td>
                            <td>2023-01-01</td>
                        </tr>
                        <tr data-url="wyszukiwanie.php?details=2" class="result_item">
                            <td>101</td>
                            <td></td>
                        </tr>
                    </tbody>
                </table>
            </body></html>
        "#;

        let mut fields_map = HashMap::new();
        fields_map.insert("lp".to_string(), "td:nth-child(1)".to_string());
        fields_map.insert("judgment_date".to_string(), "td:nth-child(2)".to_string());
        fields_map.insert("details_path".to_string(), ".@data-url".to_string());

        let row = parse_selector("table.results tbody tr.result_item").unwrap();
        let mut fields = Vec::new();
        for (name, raw) in &fields_map {
            let (sel_part, attribute) = split_selector(raw);
            let selector = match sel_part {
                SELF_SELECTOR => None,
                other => Some(parse_selector(other).unwrap()),
            };
            fields.push(FieldSelector {
                name: name.clone(),
                selector,
                attribute,
            });
        }

        let rows = scrape_document(html, &CompiledSelectors { row, fields });

        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0]["lp"], Value::String("100".to_string()));
        assert_eq!(
            rows[0]["judgment_date"],
            Value::String("2023-01-01".to_string())
        );
        assert_eq!(
            rows[0]["details_path"],
            Value::String("wyszukiwanie.php?details=1".to_string())
        );
        assert_eq!(rows[1]["lp"], Value::String("101".to_string()));
        assert_eq!(rows[1]["judgment_date"], Value::Null);
        assert_eq!(
            rows[1]["details_path"],
            Value::String("wyszukiwanie.php?details=2".to_string())
        );
    }

    #[test]
    fn test_extract_field_text_trims_whitespace() {
        let html = "<table><tr><td>   hello world   </td></tr></table>";
        let document = Html::parse_document(html);
        let row_selector = Selector::parse("tr").unwrap();
        let row = document.select(&row_selector).next().unwrap();

        let field = FieldSelector {
            name: "cell".to_string(),
            selector: Some(Selector::parse("td").unwrap()),
            attribute: None,
        };

        assert_eq!(
            EventHandler::extract_field(row, &field),
            Value::String("hello world".to_string())
        );
    }
}

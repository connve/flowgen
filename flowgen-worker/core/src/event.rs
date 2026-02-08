//! Event system for processing and routing data through workflows.
//!
//! Provides event structures, data format handling, subject generation utilities,
//! and logging functionality for the flowgen event processing pipeline.

use crate::buffer::{ContentType, FromReader, ToWriter};
use apache_avro::{from_avro_datum, Reader as AvroReader};
use arrow::{array::RecordBatchWriter, csv::reader::Format};
use chrono::Utc;
use serde::{Serialize, Serializer};
use serde_json::{Map, Value};
use std::cell::RefCell;
use std::io::{Read, Seek, Write};
use std::sync::Arc;
use tracing::info;

tokio::task_local! {
    /// Task-local storage for the current event context.
    /// Used by EventBuilder::new() to automatically preserve meta fields from the incoming event.
    /// Unlike thread_local!, this stays with the tokio task even when it migrates between threads.
    static CURRENT_EVENT_META: RefCell<Option<Map<String, Value>>>;
}

/// Runs an async function with event context set for automatic meta preservation.
/// Use this to wrap handler functions that should preserve event meta in EventBuilder::new() calls.
///
/// Takes an Arc<Event> to avoid cloning the event data, only the Arc pointer is cloned inside.
///
/// # Example
/// ```ignore
/// async fn handle(&self, event: Event) -> Result<(), Error> {
///     let event = Arc::new(event);
///     with_event_context(&Arc::clone(&event), async move {
///         // EventBuilder::new() will automatically preserve event.meta
///         // Access event fields via event.data, event.subject, etc.
///         let new_event = EventBuilder::new()
///             .data(some_data)
///             .subject("example")
///             .build()?;
///         Ok(())
///     }).await
/// }
/// ```
pub async fn with_event_context<F, R>(event: &Arc<Event>, f: F) -> R
where
    F: std::future::Future<Output = R>,
{
    CURRENT_EVENT_META
        .scope(RefCell::new(event.meta.clone()), f)
        .await
}

/// Builder for sending events with structured logging context.
pub struct EventLogger<'a> {
    event: Event,
    tx: Option<&'a tokio::sync::mpsc::Sender<Event>>,
    fields: Vec<(&'static str, String)>,
}

impl<'a> EventLogger<'a> {
    /// Add a context field to the structured log output.
    ///
    /// # Example
    /// ```ignore
    /// event.send_with_logging(Some(&tx))
    ///     .context("row_count", 1000)
    ///     .context("external_id", "job-123")
    ///     .await?;
    /// ```
    pub fn context(mut self, key: &'static str, value: impl std::fmt::Display) -> Self {
        self.fields.push((key, value.to_string()));
        self
    }
}

// Implement IntoFuture to make EventLogger awaitable
impl<'a> std::future::IntoFuture for EventLogger<'a> {
    type Output = Result<(), Error>;
    type IntoFuture =
        std::pin::Pin<Box<dyn std::future::Future<Output = Self::Output> + Send + 'a>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move {
            let event_id = match &self.event.id {
                Some(ref id) => id.to_string(),
                None => self.event.timestamp.to_string(),
            };
            let subject = self.event.subject.clone();

            if let Some(tx) = self.tx {
                tx.send(self.event).await.map_err(|_| Error::SendMessage)?;
            }

            // Build structured log with context fields
            if self.fields.is_empty() {
                info!(
                    event.subject = %subject,
                    event.id = %event_id,
                );
            } else {
                // Create log record with dynamic fields
                let field_str = self
                    .fields
                    .iter()
                    .map(|(k, v)| format!("{k}={v}"))
                    .collect::<Vec<_>>()
                    .join(", ");

                info!(
                    event.subject = %subject,
                    event.id = %event_id,
                    context = %field_str,
                );
            }

            Ok(())
        })
    }
}

/// Extension trait for event processing with logging.
pub trait EventExt {
    /// Logs event processing and optionally sends to the next task.
    ///
    /// This method always logs the event, then sends it to the next task if a sender is provided.
    /// Use this in task handlers to ensure visibility of event processing throughout the pipeline.
    ///
    /// Returns a builder that allows adding context fields via `.context()` calls.
    /// The builder implements `IntoFuture`, so you can await it directly.
    ///
    /// # Example
    /// ```ignore
    /// // Simple usage without context
    /// event.send_with_logging(Some(&tx)).await?;
    ///
    /// // With context fields
    /// event.send_with_logging(Some(&tx))
    ///     .context("row_count", 1000)
    ///     .context("external_id", "job-123")
    ///     .await?;
    /// ```
    fn send_with_logging<'a>(
        self,
        tx: Option<&'a tokio::sync::mpsc::Sender<Event>>,
    ) -> EventLogger<'a>;
}

impl EventExt for Event {
    fn send_with_logging<'a>(
        self,
        tx: Option<&'a tokio::sync::mpsc::Sender<Event>>,
    ) -> EventLogger<'a> {
        EventLogger {
            event: self,
            tx,
            fields: Vec::new(),
        }
    }
}

/// Errors that can occur during event processing operations.
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum Error {
    #[error("IO error: {source}")]
    IO {
        #[source]
        source: std::io::Error,
    },
    #[error("Arrow error: {source}")]
    Arrow {
        #[source]
        source: arrow::error::ArrowError,
    },
    #[error("Avro error: {source}")]
    Avro {
        #[source]
        source: apache_avro::Error,
    },
    #[error("JSON error: {source}")]
    SerdeJson {
        #[source]
        source: serde_json::error::Error,
    },
    #[error("Missing required builder attribute: {}", _0)]
    MissingBuilderAttribute(String),
    #[error("Content type conversion not supported: {from} to {to}")]
    UnsupportedContentTypeConversion { from: String, to: String },
    #[error("Error sending event to channel (receiver dropped)")]
    SendMessage,
}

/// Core event structure containing data and metadata for workflow processing.
#[derive(Debug, Clone)]
pub struct Event {
    /// Event payload in one of the supported data formats.
    pub data: EventData,
    /// Subject identifier for event routing and filtering.
    pub subject: String,
    /// Optional unique identifier for the event.
    pub id: Option<String>,
    /// Event creation timestamp in microseconds since Unix epoch.
    pub timestamp: i64,
    /// Task identifier for tracking event flow through pipeline stages.
    pub task_id: usize,
    /// Task type for categorization and logging.
    pub task_type: &'static str,
    /// Optional metadata for passing contextual information between tasks.
    /// Metadata can be set by script tasks and accessed in templates using event.meta syntax.
    /// Useful for adding context that should travel with the event but is separate from the payload.
    pub meta: Option<Map<String, Value>>,
}

impl TryFrom<&Event> for Value {
    type Error = Error;
    fn try_from(event: &Event) -> Result<Self, Self::Error> {
        let event_data = serde_json::Value::try_from(&event.data)?;
        Ok(serde_json::json!({
            "event": {
                "subject": event.subject,
                "data": event_data,
                "id": event.id,
                "timestamp": event.timestamp,
                "task_id": event.task_id,
                "task_type": event.task_type,
                "meta": event.meta,
            }
        }))
    }
}

impl std::fmt::Display for Event {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let event_data = serde_json::Value::try_from(&self.data)
            .unwrap_or_else(|_| serde_json::json!(format!("{:?}", self.data)));

        let event_json = serde_json::json!({
            "subject": self.subject,
            "data": event_data,
            "id": self.id,
            "timestamp": self.timestamp,
            "task_id": self.task_id,
            "task_type": self.task_type,
            "meta": self.meta,
        });

        let formatted =
            serde_json::to_string_pretty(&event_json).unwrap_or_else(|_| format!("{self:?}"));

        write!(f, "{formatted}")
    }
}

/// Event data payload supporting multiple serialization formats.
#[derive(Debug, Clone)]
pub enum EventData {
    /// Apache Arrow columnar data format for analytics workloads.
    ArrowRecordBatch(arrow::array::RecordBatch),
    /// Apache Avro binary format with embedded schema.
    Avro(AvroData),
    /// JSON format for flexible structured data.
    Json(serde_json::Value),
}

/// Avro data container with schema and serialized payload.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct AvroData {
    /// Avro schema definition in JSON format.
    pub schema: String,
    /// Binary-encoded Avro data according to the schema.
    pub raw_bytes: Vec<u8>,
}

impl TryFrom<&EventData> for Value {
    type Error = Error;

    fn try_from(event_data: &EventData) -> Result<Self, Self::Error> {
        let data = match event_data {
            EventData::ArrowRecordBatch(data) => {
                let buf = Vec::new();
                let mut writer = arrow_json::ArrayWriter::new(buf);
                writer
                    .write_batches(&[data])
                    .map_err(|e| Error::Arrow { source: e })?;
                writer.finish().map_err(|e| Error::Arrow { source: e })?;
                let json_data = writer.into_inner();
                let json_rows: Vec<Map<String, Value>> =
                    serde_json::from_reader(json_data.as_slice())
                        .map_err(|e| Error::SerdeJson { source: e })?;
                json_rows.into()
            }
            EventData::Avro(data) => {
                let schema = apache_avro::Schema::parse_str(&data.schema)
                    .map_err(|e| Error::Avro { source: e })?;
                let avro_value = from_avro_datum(&schema, &mut &data.raw_bytes[..], None)
                    .map_err(|e| Error::Avro { source: e })?;
                serde_json::Value::try_from(avro_value).map_err(|e| Error::Avro { source: e })?
            }
            EventData::Json(data) => data.clone(),
        };
        Ok(data)
    }
}

impl Serialize for EventData {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let json_value = serde_json::Value::try_from(self).map_err(serde::ser::Error::custom)?;
        json_value.serialize(serializer)
    }
}

/// Builder for constructing Event instances with validation.
#[derive(Default, Debug)]
pub struct EventBuilder {
    /// Event data payload (required for build).
    pub data: Option<EventData>,
    /// Arrow record batch extensions for metadata.
    pub extensions: Option<arrow::array::RecordBatch>,
    /// Event subject for routing (required for build).
    pub subject: Option<String>,
    /// Optional unique event identifier.
    pub id: Option<String>,
    /// Event timestamp, defaults to current time.
    pub timestamp: Option<i64>,
    /// Current task identifier for pipeline tracking.
    pub task_id: Option<usize>,
    /// Task type for categorization and logging (required for build).
    pub task_type: Option<&'static str>,
    /// Optional metadata for contextual information.
    pub meta: Option<Map<String, Value>>,
}

impl EventBuilder {
    /// Creates a new EventBuilder.
    /// Automatically preserves meta from the current event context (if set via with_event_context).
    pub fn new() -> Self {
        let meta = CURRENT_EVENT_META
            .try_with(|m| m.borrow().clone())
            .ok()
            .flatten();
        EventBuilder {
            timestamp: Some(Utc::now().timestamp_micros()),
            meta,
            ..Default::default()
        }
    }

    pub fn data(mut self, data: EventData) -> Self {
        self.data = Some(data);
        self
    }
    pub fn subject(mut self, subject: String) -> Self {
        self.subject = Some(subject);
        self
    }
    pub fn task_id(mut self, task_id: usize) -> Self {
        self.task_id = Some(task_id);
        self
    }
    pub fn id(mut self, id: String) -> Self {
        self.id = Some(id);
        self
    }
    pub fn timestamp(mut self, timestamp: i64) -> Self {
        self.timestamp = Some(timestamp);
        self
    }
    pub fn task_type(mut self, task_type: &'static str) -> Self {
        self.task_type = Some(task_type);
        self
    }
    pub fn meta(mut self, meta: Map<String, Value>) -> Self {
        self.meta = Some(meta);
        self
    }

    pub fn build(self) -> Result<Event, Error> {
        Ok(Event {
            data: self
                .data
                .ok_or_else(|| Error::MissingBuilderAttribute("data".to_string()))?,
            subject: self
                .subject
                .ok_or_else(|| Error::MissingBuilderAttribute("subject".to_string()))?,
            id: self.id,
            timestamp: self
                .timestamp
                .ok_or_else(|| Error::MissingBuilderAttribute("timestamp".to_string()))?,
            task_id: self
                .task_id
                .ok_or_else(|| Error::MissingBuilderAttribute("task_id".to_string()))?,
            task_type: self
                .task_type
                .ok_or_else(|| Error::MissingBuilderAttribute("task_type".to_string()))?,
            meta: self.meta,
        })
    }
}

impl<R: Read + Seek> FromReader<R> for EventData {
    type Error = Error;

    fn from_reader(mut reader: R, content_type: ContentType) -> Result<Vec<Self>, Self::Error> {
        match content_type {
            ContentType::Json => {
                let data: Value =
                    serde_json::from_reader(reader).map_err(|e| Error::SerdeJson { source: e })?;
                Ok(vec![EventData::Json(data)])
            }
            ContentType::Csv {
                batch_size,
                has_header,
                delimiter,
            } => {
                let delimiter_byte = delimiter.unwrap_or(b',');

                let (schema, _) = Format::default()
                    .with_header(has_header)
                    .with_delimiter(delimiter_byte)
                    .infer_schema(&mut reader, Some(100))
                    .map_err(|e| Error::Arrow { source: e })?;
                reader.rewind().map_err(|e| Error::IO { source: e })?;

                let csv = arrow::csv::ReaderBuilder::new(Arc::new(schema))
                    .with_header(has_header)
                    .with_delimiter(delimiter_byte)
                    .with_batch_size(batch_size)
                    .build(reader)
                    .map_err(|e| Error::Arrow { source: e })?;

                let mut events = Vec::new();
                for batch in csv {
                    events.push(EventData::ArrowRecordBatch(
                        batch.map_err(|e| Error::Arrow { source: e })?,
                    ));
                }
                Ok(events)
            }

            ContentType::Parquet { batch_size } => {
                // Read all bytes into memory for Parquet (requires random access).
                let mut buffer = Vec::new();
                reader
                    .read_to_end(&mut buffer)
                    .map_err(|e| Error::IO { source: e })?;

                let parquet_reader =
                    parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(
                        bytes::Bytes::from(buffer),
                    )
                    .map_err(|e| Error::Arrow {
                        source: arrow::error::ArrowError::ExternalError(Box::new(e)),
                    })?
                    .with_batch_size(batch_size)
                    .build()
                    .map_err(|e| Error::Arrow {
                        source: arrow::error::ArrowError::ExternalError(Box::new(e)),
                    })?;

                let mut events = Vec::new();
                for batch_result in parquet_reader {
                    let batch = batch_result.map_err(|e| Error::Arrow { source: e })?;
                    events.push(EventData::ArrowRecordBatch(batch));
                }
                Ok(events)
            }

            ContentType::Avro => {
                let avro_reader = AvroReader::new(reader).map_err(|e| Error::Avro { source: e })?;
                let schema = avro_reader.writer_schema().clone();
                let schema_json = schema.canonical_form();

                let mut events = Vec::new();
                for record in avro_reader {
                    let value = record.map_err(|e| Error::Avro { source: e })?;
                    let raw_bytes = apache_avro::to_avro_datum(&schema, value)
                        .map_err(|e| Error::Avro { source: e })?;

                    let avro_data = AvroData {
                        schema: schema_json.clone(),
                        raw_bytes,
                    };
                    events.push(EventData::Avro(avro_data));
                }
                Ok(events)
            }
        }
    }
}

impl<W: Write> ToWriter<W> for EventData {
    type Error = Error;

    fn to_writer(self, writer: W) -> Result<(), Self::Error> {
        match self {
            EventData::Json(data) => {
                serde_json::to_writer(writer, &data).map_err(|e| Error::SerdeJson { source: e })?;
                Ok(())
            }
            EventData::ArrowRecordBatch(batch) => {
                let mut csv_writer = arrow::csv::WriterBuilder::new()
                    .with_header(true) // Assume has header.
                    .build(writer);
                csv_writer
                    .write(&batch)
                    .map_err(|e| Error::Arrow { source: e })?;
                csv_writer.close().map_err(|e| Error::Arrow { source: e })?;
                Ok(())
            }
            EventData::Avro(avro_data) => {
                let schema = apache_avro::Schema::parse_str(&avro_data.schema)
                    .map_err(|e| Error::Avro { source: e })?;
                let value = from_avro_datum(&schema, &mut &avro_data.raw_bytes[..], None)
                    .map_err(|e| Error::Avro { source: e })?;
                let mut avro_writer = apache_avro::Writer::new(&schema, writer);
                avro_writer
                    .append(value)
                    .map_err(|e| Error::Avro { source: e })?;
                avro_writer.flush().map_err(|e| Error::Avro { source: e })?;
                Ok(())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::io::Cursor;

    #[test]
    fn test_event_builder_success() {
        let event = EventBuilder::new()
            .data(EventData::Json(json!({"test": "value"})))
            .subject("test.subject".to_string())
            .id("test-id".to_string())
            .task_id(1)
            .task_type("test")
            .build()
            .unwrap();

        assert_eq!(event.subject, "test.subject");
        assert_eq!(event.id, Some("test-id".to_string()));
        assert_eq!(event.task_id, 1);
        assert!(event.timestamp > 0);

        match event.data {
            EventData::Json(value) => assert_eq!(value, json!({"test": "value"})),
            _ => panic!("Expected JSON data"),
        }
    }

    #[test]
    fn test_event_builder_missing_data() {
        let result = EventBuilder::new()
            .subject("test.subject".to_string())
            .build();

        assert!(matches!(
            result,
            Err(Error::MissingBuilderAttribute(attr)) if attr == "data"
        ));
    }

    #[test]
    fn test_event_builder_missing_subject() {
        let result = EventBuilder::new()
            .data(EventData::Json(json!({"test": "value"})))
            .build();

        assert!(matches!(
            result,
            Err(Error::MissingBuilderAttribute(attr)) if attr == "subject"
        ));
    }

    #[test]
    fn test_avro_data_serialization() {
        let avro_data = AvroData {
            schema: r#"{"type": "string"}"#.to_string(),
            raw_bytes: vec![1, 2, 3, 4],
        };

        let serialized = serde_json::to_string(&avro_data).unwrap();
        let deserialized: AvroData = serde_json::from_str(&serialized).unwrap();

        assert_eq!(avro_data.schema, deserialized.schema);
        assert_eq!(avro_data.raw_bytes, deserialized.raw_bytes);
    }

    #[test]
    fn test_event_data_json_conversion() {
        let json_data = json!({"field": "value", "number": 42});
        let event_data = EventData::Json(json_data.clone());

        let converted = Value::try_from(&event_data).unwrap();
        assert_eq!(converted, json_data);
    }

    #[test]
    fn test_event_data_json_to_writer() {
        let json_data = json!({"test": "data"});
        let event_data = EventData::Json(json_data);

        let mut buffer = Vec::new();
        event_data.to_writer(&mut buffer).unwrap();

        let result: serde_json::Value = serde_json::from_slice(&buffer).unwrap();
        assert_eq!(result, json!({"test": "data"}));
    }

    #[test]
    fn test_event_data_from_json_reader() {
        let json_content = r#"{"name": "test", "value": 123}"#;
        let cursor = Cursor::new(json_content);

        let events = EventData::from_reader(cursor, ContentType::Json).unwrap();
        assert_eq!(events.len(), 1);

        match &events[0] {
            EventData::Json(value) => {
                assert_eq!(value["name"], "test");
                assert_eq!(value["value"], 123);
            }
            _ => panic!("Expected JSON event data"),
        }
    }

    #[test]
    fn test_event_data_parquet_roundtrip() {
        use arrow::array::{Int32Array, StringArray};
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use std::sync::Arc;

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]));

        let original_batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["alice", "bob", "charlie"])),
            ],
        )
        .unwrap();

        let mut buffer = Vec::new();
        let props = parquet::file::properties::WriterProperties::builder().build();
        let mut parquet_writer =
            parquet::arrow::ArrowWriter::try_new(&mut buffer, schema.clone(), Some(props)).unwrap();
        parquet_writer.write(&original_batch).unwrap();
        parquet_writer.close().unwrap();

        let cursor = Cursor::new(buffer);
        let events =
            EventData::from_reader(cursor, ContentType::Parquet { batch_size: 1024 }).unwrap();

        assert_eq!(events.len(), 1);
        match &events[0] {
            EventData::ArrowRecordBatch(batch) => {
                assert_eq!(batch.num_rows(), 3);
                assert_eq!(batch.num_columns(), 2);
                assert_eq!(batch.schema(), schema);
            }
            _ => panic!("Expected Arrow RecordBatch"),
        }
    }
}

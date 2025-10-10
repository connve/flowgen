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
use std::io::{Read, Seek, Write};
use std::sync::Arc;

/// Default log message format for event processing.
pub const DEFAULT_LOG_MESSAGE: &str = "Event processed";

/// Subject suffix options for event subjects.
pub enum SubjectSuffix<'a> {
    /// Use current timestamp as suffix.
    Timestamp,
    /// Use custom ID as suffix.
    Id(&'a str),
}

/// Generates a structured subject string from a base subject, a required task name, and a suffix.
///
/// The resulting subject is formatted as: `<base_subject>.<task_name>.<suffix_value>`.
/// The `task_name` is always converted to lowercase.
///
/// # Arguments
/// * `task_name` - Optional name of the task. If provided, it is used as a component of the subject
///   and automatically converted to lowercase. If None, the task name is omitted from the subject.
/// * `base_subject` - The fixed base prefix for the subject string (e.g., a service or stream name).
/// * `suffix` - The dynamic suffix type (timestamp or a custom ID).
///
/// # Returns
/// A formatted subject string with the dynamic suffix, optionally including the task name.
pub fn generate_subject(
    task_name: Option<&str>,
    base_subject: &str,
    suffix: SubjectSuffix,
) -> String {
    let suffix_str = match suffix {
        SubjectSuffix::Timestamp => Utc::now().timestamp_micros().to_string(),
        SubjectSuffix::Id(id) => id.to_string(),
    };
    match task_name {
        Some(name) => format!("{}.{}.{}", base_subject, name.to_lowercase(), suffix_str),
        None => format!("{base_subject}.{suffix_str}"),
    }
}

/// Errors that can occur during event processing operations.
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum Error {
    /// Input/output operation failed.
    #[error(transparent)]
    IO(#[from] std::io::Error),
    /// Arrow data processing error.
    #[error(transparent)]
    Arrow(#[from] arrow::error::ArrowError),
    /// Avro serialization or deserialization error.
    #[error(transparent)]
    Avro(#[from] apache_avro::Error),
    /// JSON serialization or deserialization error.
    #[error(transparent)]
    SerdeJson(#[from] serde_json::error::Error),
    /// Required builder attribute was not provided.
    #[error("Missing required attribute: {}", _0)]
    MissingRequiredAttribute(String),
    /// Attempted conversion between unsupported content types.
    #[error("Content type conversion not supported: {from} to {to}")]
    UnsupportedContentTypeConversion { from: String, to: String },
}

/// Core event structure containing data and metadata for workflow processing.
#[derive(Debug, Clone)]
pub struct Event {
    /// Event payload in one of the supported data formats.
    pub data: EventData,
    /// Subject identifier for event routing and filtering.
    pub subject: String,
    /// Task identifier for tracking event flow through pipeline stages.
    pub current_task_id: Option<usize>,
    /// Optional unique identifier for the event.
    pub id: Option<String>,
    /// Event creation timestamp in microseconds since Unix epoch.
    pub timestamp: i64,
}

impl Event {}

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
                writer.write_batches(&[data])?;
                writer.finish()?;
                let json_data = writer.into_inner();
                let json_rows: Vec<Map<String, Value>> =
                    serde_json::from_reader(json_data.as_slice())?;
                json_rows.into()
            }
            EventData::Avro(data) => {
                let schema = apache_avro::Schema::parse_str(&data.schema)?;
                let avro_value = from_avro_datum(&schema, &mut &data.raw_bytes[..], None)?;
                serde_json::Value::try_from(avro_value).unwrap()
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
    /// Current task identifier for pipeline tracking.
    pub current_task_id: Option<usize>,
    /// Optional unique event identifier.
    pub id: Option<String>,
    /// Event timestamp, defaults to current time.
    pub timestamp: i64,
}

impl EventBuilder {
    pub fn new() -> Self {
        EventBuilder {
            timestamp: Utc::now().timestamp_micros(),
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
    pub fn current_task_id(mut self, current_task_id: usize) -> Self {
        self.current_task_id = Some(current_task_id);
        self
    }
    pub fn id(mut self, id: String) -> Self {
        self.id = Some(id);
        self
    }
    pub fn time(mut self, timestamp: i64) -> Self {
        self.timestamp = timestamp;
        self
    }

    pub fn build(self) -> Result<Event, Error> {
        Ok(Event {
            data: self
                .data
                .ok_or_else(|| Error::MissingRequiredAttribute("data".to_string()))?,
            subject: self
                .subject
                .ok_or_else(|| Error::MissingRequiredAttribute("subject".to_string()))?,
            id: self.id,
            timestamp: self.timestamp,
            current_task_id: self.current_task_id,
        })
    }
}

impl<R: Read + Seek> FromReader<R> for EventData {
    type Error = Error;

    fn from_reader(mut reader: R, content_type: ContentType) -> Result<Vec<Self>, Self::Error> {
        match content_type {
            ContentType::Json => {
                let data: Value = serde_json::from_reader(reader)?;
                Ok(vec![EventData::Json(data)])
            }
            ContentType::Csv {
                batch_size,
                has_header,
            } => {
                let (schema, _) = Format::default()
                    .with_header(has_header)
                    .infer_schema(&mut reader, Some(100))?;
                reader.rewind()?;

                let csv = arrow::csv::ReaderBuilder::new(Arc::new(schema))
                    .with_header(has_header)
                    .with_batch_size(batch_size)
                    .build(reader)?;

                let mut events = Vec::new();
                for batch in csv {
                    events.push(EventData::ArrowRecordBatch(batch?));
                }
                Ok(events)
            }

            ContentType::Avro => {
                let avro_reader = AvroReader::new(reader)?;
                let schema = avro_reader.writer_schema().clone();
                let schema_json = schema.canonical_form();

                let mut events = Vec::new();
                for record in avro_reader {
                    let value = record?;
                    let raw_bytes = apache_avro::to_avro_datum(&schema, value)?;

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
                serde_json::to_writer(writer, &data)?;
                Ok(())
            }
            EventData::ArrowRecordBatch(batch) => {
                let mut csv_writer = arrow::csv::WriterBuilder::new()
                    .with_header(true) // Assume has header.
                    .build(writer);
                csv_writer.write(&batch)?;
                csv_writer.close()?;
                Ok(())
            }
            EventData::Avro(avro_data) => {
                let schema = apache_avro::Schema::parse_str(&avro_data.schema)?;
                let value = from_avro_datum(&schema, &mut &avro_data.raw_bytes[..], None)?;
                let mut avro_writer = apache_avro::Writer::new(&schema, writer);
                avro_writer.append(value)?;
                avro_writer.flush()?;
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
    fn test_generate_subject_with_id() {
        let subject = generate_subject(Some("task-name"), "base.subject", SubjectSuffix::Id("123"));
        assert_eq!(subject, "base.subject.task-name.123");
    }

    #[test]
    fn test_generate_subject_with_id_no_task() {
        let subject = generate_subject(None, "base.subject", SubjectSuffix::Id("123"));
        assert_eq!(subject, "base.subject.123");
    }

    #[test]
    fn test_generate_subject_with_timestamp() {
        let subject = generate_subject(Some("task-name"), "base.subject", SubjectSuffix::Timestamp);
        assert!(subject.starts_with("base.subject.task-name."));
        assert!(subject.len() > "base.subject.task-name.".len());
    }

    #[test]
    fn test_generate_subject_with_timestamp_no_task() {
        let subject = generate_subject(None, "base.subject", SubjectSuffix::Timestamp);
        assert!(subject.starts_with("base.subject."));
        assert!(subject.len() > "base.subject.".len());
    }

    #[test]
    fn test_event_builder_success() {
        let event = EventBuilder::new()
            .data(EventData::Json(json!({"test": "value"})))
            .subject("test.subject".to_string())
            .id("test-id".to_string())
            .current_task_id(1)
            .build()
            .unwrap();

        assert_eq!(event.subject, "test.subject");
        assert_eq!(event.id, Some("test-id".to_string()));
        assert_eq!(event.current_task_id, Some(1));
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

        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Missing required attribute: data"));
    }

    #[test]
    fn test_event_builder_missing_subject() {
        let result = EventBuilder::new()
            .data(EventData::Json(json!({"test": "value"})))
            .build();

        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Missing required attribute: subject"));
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
}

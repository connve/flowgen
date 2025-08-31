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
use tracing::{event, Level};

/// Subject suffix options for event subjects.
pub enum SubjectSuffix<'a> {
    /// Use current timestamp as suffix.
    Timestamp,
    /// Use custom ID as suffix.
    Id(&'a str),
}

/// Generates a subject string using optional label or base subject with suffix.
///
/// # Arguments
/// * `label` - Optional label to use as prefix
/// * `base_subject` - Base subject to use when label is None
/// * `suffix` - Suffix type (timestamp or custom ID)
///
/// # Returns
/// Formatted subject string with suffix
pub fn generate_subject(label: Option<&str>, base_subject: &str, suffix: SubjectSuffix) -> String {
    let suffix_str = match suffix {
        SubjectSuffix::Timestamp => Utc::now().timestamp_micros().to_string(),
        SubjectSuffix::Id(id) => id.to_string(),
    };
    match label {
        Some(label) => format!("{}.{}", label.to_lowercase(), suffix_str),
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
    #[error("missing required attribute: {}", _0)]
    MissingRequiredAttribute(String),
    /// Attempted conversion between unsupported content types.
    #[error("content type conversion not supported: {from} to {to}")]
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

impl Event {
    /// Logs the event processing with INFO level.
    pub fn log(&self) {
        event!(Level::INFO, "Event processed: {}", self.subject);
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
                    .with_header(true)
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

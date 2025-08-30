use apache_avro::{from_avro_datum, Reader as AvroReader};
use arrow::csv::reader::Format;
use chrono::Utc;
use crate::file::{FileType, FromReader};
use serde::{Serialize, Serializer};
use serde_json::{Map, Value};
use std::io::{Read, Seek};
use std::sync::Arc;

#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum Error {
    #[error(transparent)]
    IO(#[from] std::io::Error),
    #[error(transparent)]
    Arrow(#[from] arrow::error::ArrowError),
    #[error(transparent)]
    Avro(#[from] apache_avro::Error),
    #[error(transparent)]
    SerdeJson(#[from] serde_json::error::Error),
    #[error("missing required attribute: {}", _0)]
    MissingRequiredAttribute(String),
}

#[derive(Debug, Clone)]
pub struct Event {
    pub data: EventData,
    pub subject: String,
    pub current_task_id: Option<usize>,
    pub id: Option<String>,
    pub timestamp: i64,
}

#[derive(Debug, Clone)]
pub enum EventData {
    ArrowRecordBatch(arrow::array::RecordBatch),
    Avro(AvroData),
    Json(serde_json::Value),
}
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct AvroData {
    pub schema: String,
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

#[derive(Default, Debug)]
pub struct EventBuilder {
    pub data: Option<EventData>,
    pub extensions: Option<arrow::array::RecordBatch>,
    pub subject: Option<String>,
    pub current_task_id: Option<usize>,
    pub id: Option<String>,
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

    fn from_reader(mut reader: R, file_type: FileType) -> Result<Vec<Self>, Self::Error> {
        match file_type {
            FileType::Json => {
                let data: Value = serde_json::from_reader(reader)?;
                Ok(vec![EventData::Json(data)])
            }
            FileType::Csv { batch_size, has_header } => {
                let (schema, _) = Format::default()
                    .with_header(true)
                    .infer_schema(&mut reader, Some(100))
                    .map_err(Error::Arrow)?;
                reader.rewind().map_err(Error::IO)?;

                let csv = arrow::csv::ReaderBuilder::new(Arc::new(schema))
                    .with_header(has_header)
                    .with_batch_size(batch_size)
                    .build(reader)
                    .map_err(Error::Arrow)?;

                let mut events = Vec::new();
                for batch in csv {
                    events.push(EventData::ArrowRecordBatch(batch?));
                }
                Ok(events)
            }
            FileType::Avro => {
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

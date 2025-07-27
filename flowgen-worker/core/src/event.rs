use apache_avro::from_avro_datum;
use chrono::Utc;
use serde::{Serialize, Serializer};
use serde_json::{Map, Value};

#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum Error {
    #[error(transparent)]
    Arrow(#[from] arrow::error::ArrowError),
    #[error(transparent)]
    Avro(#[from] apache_avro::Error),
    #[error(transparent)]
    SerdeJson(#[from] serde_json::error::Error),
    #[error("missing required attribute")]
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
    Json(serde_json::Value)
}
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct AvroData {
    pub schema: String,
    pub raw_bytes: Vec<u8>,
}

impl TryFrom<&EventData> for Value {
    type Error = Error;
    
    fn try_from(event_data: &EventData) -> Result<Self, Self::Error> {
            let  value = match event_data {
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
                EventData::Json(value) => {
                            value.clone()
                    },
            };
        Ok(value)
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


#[derive(Default)]
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

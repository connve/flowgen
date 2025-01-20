use arrow::{
    array::{Array, MapBuilder, RecordBatch, StringArray, StringBuilder},
    datatypes::{DataType, Field, Fields},
};
use std::sync::Arc;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("There was an error with an Apache Arrow data.")]
    Arrow(#[source] arrow::error::ArrowError),
    #[error("Provided value is not an object.")]
    NotObject(),
    #[error("Missing required attributes.")]
    MissingRequiredAttribute(String),
}

#[derive(PartialEq, Debug, Clone)]
pub struct Event {
    pub data: arrow::array::RecordBatch,
    pub extensions: Option<arrow::array::RecordBatch>,
    pub subject: String,
    pub current_task_id: Option<usize>,
}

#[derive(PartialEq, Debug, Clone, Default)]
pub struct EventBuilder {
    pub data: Option<arrow::array::RecordBatch>,
    pub extensions: Option<arrow::array::RecordBatch>,
    pub subject: Option<String>,
    pub current_task_id: Option<usize>,
}

impl EventBuilder {
    pub fn new() -> Self {
        EventBuilder {
            ..Default::default()
        }
    }
    pub fn data(mut self, data: arrow::array::RecordBatch) -> Self {
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
    pub fn extensions(mut self, extensions: arrow::array::RecordBatch) -> Self {
        self.extensions = Some(extensions);
        self
    }
    pub fn build(self) -> Result<Event, Error> {
        Ok(Event {
            data: self
                .data
                .ok_or_else(|| Error::MissingRequiredAttribute("data".to_string()))?,
            extensions: self.extensions,
            subject: self
                .subject
                .ok_or_else(|| Error::MissingRequiredAttribute("subject".to_string()))?,
            current_task_id: self.current_task_id,
        })
    }
}

pub trait SerdeValueExt {
    type Error;
    fn to_recordbatch(&self) -> Result<arrow::array::RecordBatch, Self::Error>;
}

impl SerdeValueExt for serde_json::Value {
    type Error = Error;
    fn to_recordbatch(&self) -> Result<arrow::array::RecordBatch, Self::Error> {
        let map = self.as_object().ok_or_else(Error::NotObject)?;
        let mut fields = Vec::new();
        let mut columns: Vec<Arc<dyn Array>> = Vec::new();

        for (key, value) in map {
            if value.is_string() {
                fields.push(Field::new(key, DataType::Utf8, true));
                let v = value.to_string().trim_matches('"').to_string();
                let array = StringArray::from(vec![Some(v)]);
                columns.push(Arc::new(array));
            }
            if value.is_object() {
                let map = self.as_object().ok_or_else(Error::NotObject)?;
                fields.push(Field::new(
                    key,
                    DataType::Map(
                        Arc::new(Field::new(
                            "entries",
                            DataType::Struct(Fields::from(vec![
                                Field::new("keys", DataType::Utf8, false),
                                Field::new("values", DataType::Utf8, true),
                            ])),
                            false,
                        )),
                        false,
                    ),
                    false,
                ));
                let key_builder = StringBuilder::new();
                let value_builder = StringBuilder::new();
                let mut builder = MapBuilder::new(None, key_builder, value_builder);

                for (key, value) in map {
                    builder.keys().append_value(key);
                    builder
                        .values()
                        .append_value(value.to_string().trim_matches('"'));
                    builder.append(true).map_err(Error::Arrow)?;
                }
                let array = builder.finish();
                columns.push(Arc::new(array));
            }
        }

        let schema = arrow::datatypes::Schema::new(fields);
        let batch = RecordBatch::try_new(Arc::new(schema), columns).map_err(Error::Arrow)?;
        Ok(batch)
    }
}

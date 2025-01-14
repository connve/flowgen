use arrow::{
    array::{Array, RecordBatch, StringArray},
    datatypes::{DataType, Field},
};
use std::{collections::HashMap, sync::Arc};

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("There was an error with an Apache Arrow data.")]
    Arrow(#[source] arrow::error::ArrowError),
    #[error("Provided value is not an object.")]
    NotObject(),
    #[error("Missing required event attrubute.")]
    MissingRequiredAttribute(String),
}

#[derive(PartialEq, Debug, Clone)]
pub struct Event {
    pub data: arrow::array::RecordBatch,
    pub subject: String,
    pub current_task_id: Option<usize>,
    pub extensions: HashMap<String, String>,
}

#[derive(PartialEq, Debug, Clone, Default)]
pub struct EventBuilder {
    pub data: Option<arrow::array::RecordBatch>,
    pub subject: Option<String>,
    pub current_task_id: Option<usize>,
    pub extensions: HashMap<String, String>,
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
    pub fn extensions(mut self, extensions: HashMap<String, String>) -> Self {
        self.extensions = extensions;
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
            current_task_id: self.current_task_id,
            extensions: Default::default(),
        })
    }
}

pub trait RecordBatchExt {
    type Error;
    fn to_recordbatch(&self) -> Result<arrow::array::RecordBatch, Self::Error>;
}

impl RecordBatchExt for serde_json::Value {
    type Error = Error;
    fn to_recordbatch(&self) -> Result<arrow::array::RecordBatch, Self::Error> {
        let map = self.as_object().ok_or_else(Error::NotObject)?;
        let mut fields = Vec::new();
        let mut values = Vec::new();

        for (key, value) in map {
            fields.push(Field::new(key, DataType::Utf8, true));
            let v = value.to_string().trim_matches('"').to_string();
            let array = StringArray::from(vec![Some(v)]);
            values.push(Arc::new(array));
        }

        let columns = values
            .into_iter()
            .map(|x| x as Arc<dyn Array>)
            .collect::<Vec<Arc<dyn Array>>>();

        let schema = arrow::datatypes::Schema::new(fields);
        let batch = RecordBatch::try_new(Arc::new(schema), columns).map_err(Error::Arrow)?;
        Ok(batch)
    }
}

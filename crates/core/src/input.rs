use arrow::{array::StringArray, datatypes::DataType};
use serde::{Deserialize, Serialize};

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("There was an error with an Apache Arrow data.")]
    Arrow(#[source] arrow::error::ArrowError),
    #[error("Provided value is not an object.")]
    NotObject(),
    #[error("Missing required attributes.")]
    MissingRequiredAttribute(String),
}

#[derive(PartialEq, Clone, Debug, Default, Deserialize, Serialize)]
pub struct Input {
    pub value: String,
    pub is_static: bool,
    pub is_extension: bool,
    pub nested: Option<Box<Self>>,
}

fn extract(record_batch: &arrow::array::RecordBatch, column_name: &str) -> Result<String, Error> {
    let mut value = String::new();
    let array = record_batch.column_by_name(column_name);
    if let Some(array) = array {
        let data_type = array.data_type();
        match data_type {
            DataType::Utf8 => {
                let array_data: StringArray = array.to_data().into();
                for item in (&array_data).into_iter().flatten() {
                    value = item.to_string();
                }
            }
            _ => {}
        }
    }
    Ok(value)
}
impl Input {
    pub fn extract_from(
        &self,
        data: &arrow::array::RecordBatch,
        extensions: &Option<arrow::array::RecordBatch>,
    ) -> Result<String, Error> {
        let mut value = String::new();
        if !self.is_extension {
            value = extract(data, &self.value).unwrap();
        }
        if self.is_extension {
            if let Some(extensions) = extensions {
                value = extract(extensions, &self.value).unwrap();
            }
        }
        if self.is_static {
            value = self.value.to_string()
        }
        Ok(value)
    }
}

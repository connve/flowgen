use arrow::{
    array::{Array, ListArray, RecordBatch, StringArray},
    datatypes::{DataType, Field, Schema},
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::Arc;

use crate::conversion::serde::SerdeValueExt;

#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum Error {
    #[error("error with an Apache Arrow data")]
    Arrow(#[source] arrow::error::ArrowError),
    #[error("error with converting data to Serde Value")]
    SerdeValue(#[source] crate::conversion::serde::Error),
    #[error("no data available in the array")]
    EmptyArray(),
}

#[derive(PartialEq, Clone, Debug, Default, Deserialize, Serialize)]
pub struct Input {
    pub column: String,
    pub is_static: bool,
    pub is_extension: bool,
    pub index: Option<usize>,
    pub nested: Option<Box<Self>>,
}

fn extract_from_array(array: &Arc<dyn Array>, input: &Input) -> Result<Value, Error> {
    let mut value = Value::Null;
    let data_type = array.data_type();
    match data_type {
        DataType::Utf8 => {
            let array_data = match array.as_any().downcast_ref::<StringArray>() {
                Some(arr) => arr,
                None => return Err(Error::EmptyArray()),
            };

            match &input.index {
                Some(index) => value = Value::String(array_data.value(*index).to_string()),
                None => {
                    let schema = Schema::new(vec![Field::new(
                        input.column.to_owned(),
                        array_data.data_type().to_owned(),
                        false,
                    )]);
                    let array_data = array_data.to_owned();

                    value = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(array_data)])
                        .map_err(Error::Arrow)?
                        .try_from()
                        .map_err(Error::SerdeValue)?;
                }
            }
        }
        DataType::List(_) => {
            let array_data = match array.as_any().downcast_ref::<ListArray>() {
                Some(arr) => arr,
                None => return Err(Error::EmptyArray()),
            };

            if let Some(nested_input) = &input.nested {
                match &input.index {
                    Some(index) => {
                        value = extract_from_array(&array_data.value(*index), nested_input)?
                    }
                    None => value = extract_from_array(array_data.values(), nested_input)?,
                }
            } else {
                match &input.index {
                    Some(index) => value = extract_from_array(&array_data.value(*index), input)?,
                    None => {
                        let schema = Schema::new(vec![Field::new(
                            input.column.to_owned(),
                            array_data.data_type().to_owned(),
                            false,
                        )]);
                        let array_data = array_data.to_owned();

                        value = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(array_data)])
                            .map_err(Error::Arrow)?
                            .try_from()
                            .map_err(Error::SerdeValue)?;
                    }
                }
            }
        }
        _ => unimplemented!(),
    }
    Ok(value)
}

impl Input {
    pub fn extract(
        &self,
        data: &arrow::array::RecordBatch,
        extensions: &Option<arrow::array::RecordBatch>,
    ) -> Result<Value, Error> {
        let mut value = Value::Null;
        if !self.is_extension {
            let array: Option<&Arc<dyn Array>> = data.column_by_name(&self.column);
            if let Some(array) = array {
                value = extract_from_array(array, self)?;
            }
        }
        if self.is_extension {
            if let Some(extensions) = extensions {
                let array: Option<&Arc<dyn Array>> = extensions.column_by_name(&self.column);
                if let Some(array) = array {
                    value = extract_from_array(array, self)?;
                }
            }
        }
        if self.is_static {
            value = Value::String(self.column.to_string());
        }
        Ok(value)
    }
}

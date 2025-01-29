use arrow::{
    array::{Array, ListArray, StringArray, StructArray},
    datatypes::DataType,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::Arc;

#[derive(thiserror::Error, Debug)]
pub enum InputError {
    #[error("There was an error with an Apache Arrow data.")]
    ArrowError(#[source] arrow::error::ArrowError),
    #[error("There is not data available in the array.")]
    EmptyArrayError(),
}

#[derive(PartialEq, Clone, Debug, Default, Deserialize, Serialize)]
pub struct Input {
    pub value: String,
    pub is_static: bool,
    pub is_extension: bool,
    pub index: usize,
    pub nested: Option<Box<Self>>,
}

fn extract_from_array(array: &Arc<dyn Array>, input: &Input) -> Result<Value, InputError> {
    let mut value = Value::Null;
    let data_type = array.data_type();
    match data_type {
        DataType::Utf8 => {
            let array_data = array.as_any().downcast_ref::<StringArray>();
            value = Value::String(
                array_data
                    .ok_or_else(InputError::EmptyArrayError)?
                    .value(input.index)
                    .to_string()
                    .to_string(),
            );
        }
        DataType::List(_) => {
            let array_data = array.as_any().downcast_ref::<ListArray>();
            let nested_array = array_data
                .ok_or_else(InputError::EmptyArrayError)?
                .value(input.index);
            if let Some(nested_input) = &input.nested {
                let input = &Input {
                    value: nested_input.value.clone(),
                    is_static: nested_input.is_static,
                    is_extension: nested_input.is_extension,
                    index: nested_input.index,
                    nested: nested_input.nested.clone(),
                };
                value = extract_from_array(&nested_array, input)?;
            } else {
                value = extract_from_array(&nested_array, input)?;
            }
        }
        DataType::Struct(_) => {
            let array_data = array.as_any().downcast_ref::<StructArray>();
            let nested_array = array_data
                .ok_or_else(InputError::EmptyArrayError)?
                .column_by_name(&input.value)
                .ok_or_else(InputError::EmptyArrayError)?;

            if let Some(nested_input) = &input.nested {
                let input = &Input {
                    value: nested_input.value.clone(),
                    is_static: nested_input.is_static,
                    is_extension: nested_input.is_extension,
                    index: nested_input.index,
                    nested: nested_input.nested.clone(),
                };
                value = extract_from_array(nested_array, input)?;
            } else {
                value = extract_from_array(nested_array, input)?;
            }
        }
        _ => {}
    }
    Ok(value)
}

impl Input {
    pub fn extract(
        &self,
        data: &arrow::array::RecordBatch,
        extensions: &Option<arrow::array::RecordBatch>,
    ) -> Result<Value, InputError> {
        let mut value = Value::Null;
        if !self.is_extension {
            let array: Option<&Arc<dyn Array>> = data.column_by_name(&self.value);
            if let Some(array) = array {
                value = extract_from_array(array, self)?;
            }
        }
        if self.is_extension {
            if let Some(extensions) = extensions {
                let array: Option<&Arc<dyn Array>> = extensions.column_by_name(&self.value);
                if let Some(array) = array {
                    value = extract_from_array(array, self)?;
                }
            }
        }
        if self.is_static {
            value = Value::String(self.value.to_string());
        }
        Ok(value)
    }
}

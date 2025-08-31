//! Custom serialization and deserialization utilities.
//!
//! Provides extension traits for common data type conversions between JSON,
//! Arrow, and string formats used throughout the flowgen event processing pipeline.

use std::str::FromStr;

/// Errors that can occur during serialization operations.
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum Error {
    /// JSON serialization or deserialization error.
    #[error(transparent)]
    Serde(#[from] serde_json::Error),
    /// Arrow data processing error.
    #[error(transparent)]
    Arrow(#[from] arrow::error::ArrowError),
}

/// Extension trait for JSON Map types to enable string serialization.
pub trait MapExt {
    /// Error type for serialization operations.
    type Error;
    /// Converts the map to a JSON string representation.
    fn to_string(&self) -> Result<String, Self::Error>;
}

impl<K, V> MapExt for serde_json::Map<K, V>
where
    serde_json::Map<K, V>: serde::Serialize,
{
    type Error = Error;
    fn to_string(&self) -> Result<String, Self::Error> {
        let string = serde_json::to_string(self).map_err(Error::Serde)?;
        Ok(string)
    }
}

/// Extension trait for String types to enable JSON value parsing.
pub trait StringExt {
    /// Error type for parsing operations.
    type Error;
    /// Parses the string as a JSON value.
    fn to_value(&self) -> Result<serde_json::Value, Self::Error>;
}

impl StringExt for String {
    type Error = Error;
    fn to_value(&self) -> Result<serde_json::Value, Self::Error> {
        let value = serde_json::Value::from_str(self).map_err(Error::Serde)?;
        Ok(value)
    }
}

/// Extension trait for converting data types to JSON values.
pub trait SerdeValueExt {
    /// Error type for conversion operations.
    type Error;
    /// Converts the data to a JSON value representation.
    fn try_from(&self) -> Result<serde_json::Value, Self::Error>;
}

impl SerdeValueExt for arrow::record_batch::RecordBatch {
    type Error = Error;
    fn try_from(&self) -> Result<serde_json::Value, Self::Error> {
        let buf = Vec::new();
        let mut writer = arrow_json::ArrayWriter::new(buf);
        writer.write_batches(&[self]).map_err(Error::Arrow)?;
        writer.finish().map_err(Error::Arrow)?;
        let json_data = writer.into_inner();

        use serde_json::{Map, Value};
        let json_rows: Vec<Map<String, Value>> =
            serde_json::from_reader(json_data.as_slice()).map_err(Error::Serde)?;
        Ok(json_rows.into())
    }
}

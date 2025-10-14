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
    #[error("JSON serialization/deserialization failed: {source}")]
    Serde {
        #[source]
        source: serde_json::Error,
    },
    /// Arrow data processing error.
    #[error("Arrow data processing failed: {source}")]
    Arrow {
        #[source]
        source: arrow::error::ArrowError,
    },
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
        let string = serde_json::to_string(self).map_err(|e| Error::Serde { source: e })?;
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
        let value = serde_json::Value::from_str(self).map_err(|e| Error::Serde { source: e })?;
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
        writer
            .write_batches(&[self])
            .map_err(|e| Error::Arrow { source: e })?;
        writer.finish().map_err(|e| Error::Arrow { source: e })?;
        let json_data = writer.into_inner();

        use serde_json::{Map, Value};
        let json_rows: Vec<Map<String, Value>> = serde_json::from_reader(json_data.as_slice())
            .map_err(|e| Error::Serde { source: e })?;
        Ok(json_rows.into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use serde_json::{json, Map, Value};
    use std::sync::Arc;

    #[test]
    fn test_map_ext_to_string() {
        let mut map = Map::new();
        map.insert("key1".to_string(), json!("value1"));
        map.insert("key2".to_string(), json!(42));

        let result = map.to_string().unwrap();
        let parsed: Value = serde_json::from_str(&result).unwrap();

        assert_eq!(parsed["key1"], "value1");
        assert_eq!(parsed["key2"], 42);
    }

    #[test]
    fn test_string_ext_to_value() {
        let json_string = r#"{"name": "test", "count": 5}"#.to_string();
        let value = json_string.to_value().unwrap();

        assert_eq!(value["name"], "test");
        assert_eq!(value["count"], 5);
    }

    #[test]
    fn test_string_ext_invalid_json() {
        let invalid_json = "{ invalid json }".to_string();
        let result = invalid_json.to_value();

        assert!(result.is_err());
    }

    #[test]
    fn test_record_batch_to_json_value() {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]);

        let id_array = Int32Array::from(vec![1, 2, 3]);
        let name_array = StringArray::from(vec!["Alice", "Bob", "Charlie"]);

        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(id_array), Arc::new(name_array)],
        )
        .unwrap();

        let result = batch.try_from().unwrap();

        if let Value::Array(rows) = result {
            assert_eq!(rows.len(), 3);
            assert_eq!(rows[0]["id"], 1);
            assert_eq!(rows[0]["name"], "Alice");
            assert_eq!(rows[1]["id"], 2);
            assert_eq!(rows[1]["name"], "Bob");
        } else {
            panic!("Expected array of records");
        }
    }

    #[test]
    fn test_empty_record_batch_conversion() {
        let schema = Schema::new(vec![Field::new("id", DataType::Int32, false)]);

        let id_array = Int32Array::from(Vec::<i32>::new());
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(id_array)]).unwrap();

        let result = batch.try_from().unwrap();

        if let Value::Array(rows) = result {
            assert_eq!(rows.len(), 0);
        } else {
            panic!("Expected empty array");
        }
    }
}

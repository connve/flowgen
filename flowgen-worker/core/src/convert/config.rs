//! Configuration structures for convert task types.
//!
//! Defines configuration options for data transformation tasks that convert
//! events between different formats within workflows.

use serde::{Deserialize, Serialize};

/// Configuration for convert processor tasks that transform event data formats.
#[derive(PartialEq, Clone, Debug, Default, Deserialize, Serialize)]
pub struct Processor {
    /// The unique name / identifier of the task.
    pub name: String,
    /// Target format for event data conversion.
    pub target_format: TargetFormat,
    /// Optional schema definition for target format validation.
    pub schema: Option<String>,
}

/// Supported target formats for event data conversion.
#[derive(PartialEq, Eq, Clone, Debug, Default, Deserialize, Serialize)]
pub enum TargetFormat {
    /// Convert to Apache Avro binary format.
    #[default]
    Avro,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_processor_config_default() {
        // NOTE: The `Default` trait for a struct with a `String` field
        // will set the string to an empty string (`""`).
        let config = Processor::default();
        assert_eq!(config.name, String::new()); // Check for default empty string
        assert_eq!(config.target_format, TargetFormat::Avro);
        assert!(config.schema.is_none());
    }

    #[test]
    fn test_target_format_default() {
        let format = TargetFormat::default();
        assert_eq!(format, TargetFormat::Avro);
    }

    #[test]
    fn test_processor_config_creation() {
        let processor_name = "convert_test".to_string();
        let schema_json = r#"{"type": "record", "name": "Test"}"#.to_string();

        let config = Processor {
            name: processor_name.clone(),
            target_format: TargetFormat::Avro, // Fixed: Changed to Avro
            schema: Some(schema_json.clone()),
        };

        assert_eq!(config.name, processor_name);
        assert_eq!(config.target_format, TargetFormat::Avro);
        assert_eq!(config.schema, Some(schema_json));
    }

    #[test]
    fn test_processor_config_serialization() {
        let config = Processor {
            name: "serialize_test".to_string(),
            target_format: TargetFormat::Avro, // Fixed: Changed to Avro
            schema: None,
        };

        let serialized = serde_json::to_string(&config).unwrap();

        // Fixed: Assert the serialized output uses "Avro"
        let expected_json = json!({
            "name": "serialize_test",
            "target_format": "Avro",
            "schema": null
        });
        assert_eq!(serialized, serde_json::to_string(&expected_json).unwrap());

        // Test deserialization
        let deserialized: Processor = serde_json::from_str(&serialized).unwrap();
        assert_eq!(config, deserialized);
    }

    #[test]
    fn test_target_format_serialization() {
        // Fixed: Removed Json and Protobuf, only Avro remains
        let formats = vec![TargetFormat::Avro];

        for format in formats {
            let serialized = serde_json::to_string(&format).unwrap();
            let deserialized: TargetFormat = serde_json::from_str(&serialized).unwrap();
            assert_eq!(format, deserialized);
        }
    }

    #[test]
    fn test_config_clone() {
        let config = Processor {
            name: "clone_test".to_string(),
            target_format: TargetFormat::Avro,
            schema: Some("test_schema".to_string()),
        };

        let cloned = config.clone();
        assert_eq!(config, cloned);
    }
}

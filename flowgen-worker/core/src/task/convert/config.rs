//! Configuration structures for convert task types.
//!
//! Defines configuration options for data transformation tasks that convert
//! events between different formats within workflows.

use serde::{Deserialize, Serialize};

/// Configuration for convert processor tasks that transform event data formats.
#[derive(PartialEq, Clone, Debug, Default, Deserialize, Serialize)]
pub struct Processor {
    /// Optional label for logging.
    pub label: Option<String>,
    /// Target format for event data conversion.
    pub target_format: TargetFormat,
    /// Optional schema definition for target format validation.
    pub schema: Option<String>,
}

/// Supported target formats for event data conversion.
#[derive(PartialEq, Clone, Debug, Default, Deserialize, Serialize)]
pub enum TargetFormat {
    /// Convert to Apache Avro binary format.
    #[default]
    Avro,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_processor_config_default() {
        let config = Processor::default();
        assert!(config.label.is_none());
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
        let config = Processor {
            label: Some("convert_test".to_string()),
            target_format: TargetFormat::Avro,
            schema: Some(r#"{"type": "record", "name": "Test"}"#.to_string()),
        };

        assert_eq!(config.label, Some("convert_test".to_string()));
        assert_eq!(config.target_format, TargetFormat::Avro);
        assert!(config.schema.is_some());
    }

    #[test]
    fn test_processor_config_serialization() {
        let config = Processor {
            label: Some("serialize_test".to_string()),
            target_format: TargetFormat::Avro,
            schema: None,
        };

        let serialized = serde_json::to_string(&config).unwrap();
        let deserialized: Processor = serde_json::from_str(&serialized).unwrap();

        assert_eq!(config, deserialized);
    }

    #[test]
    fn test_target_format_serialization() {
        let format = TargetFormat::Avro;
        let serialized = serde_json::to_string(&format).unwrap();
        let deserialized: TargetFormat = serde_json::from_str(&serialized).unwrap();

        assert_eq!(format, deserialized);
    }

    #[test]
    fn test_config_clone() {
        let config = Processor {
            label: None,
            target_format: TargetFormat::Avro,
            schema: Some("test_schema".to_string()),
        };

        let cloned = config.clone();
        assert_eq!(config, cloned);
    }
}

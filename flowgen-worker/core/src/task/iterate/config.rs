//! Configuration structures for loop task types.
//!
//! Defines configuration options for loop tasks that iterate over arrays
//! and emit individual events for each element.

use serde::{Deserialize, Serialize};

/// Configuration for loop processor tasks that iterate over JSON arrays.
#[derive(PartialEq, Clone, Debug, Default, Deserialize, Serialize)]
pub struct Processor {
    /// The unique name / identifier of the task.
    pub name: String,
    /// Optional key to extract array from JSON object.
    /// If None, assumes the root element is an array.
    /// If Some("key"), extracts the array from data["key"].
    pub iterate_key: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_processor_config_default() {
        let config = Processor::default();
        assert_eq!(config.name, String::new());
        assert!(config.iterate_key.is_none());
    }

    #[test]
    fn test_processor_config_creation() {
        let config = Processor {
            name: "loop_test".to_string(),
            iterate_key: Some("items".to_string()),
        };

        assert_eq!(config.name, "loop_test");
        assert_eq!(config.iterate_key, Some("items".to_string()));
    }

    #[test]
    fn test_processor_config_serialization() {
        let config = Processor {
            name: "serialize_test".to_string(),
            iterate_key: Some("data".to_string()),
        };

        let serialized = serde_json::to_string(&config).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&serialized).unwrap();
        let expected_json = json!({
            "name": "serialize_test",
            "iterate_key": "data"
        });
        assert_eq!(parsed, expected_json);

        let deserialized: Processor = serde_json::from_str(&serialized).unwrap();
        assert_eq!(config, deserialized);
    }

    #[test]
    fn test_processor_config_serialization_no_key() {
        let config = Processor {
            name: "no_key_test".to_string(),
            iterate_key: None,
        };

        let serialized = serde_json::to_string(&config).unwrap();
        let deserialized: Processor = serde_json::from_str(&serialized).unwrap();
        assert_eq!(config, deserialized);
    }

    #[test]
    fn test_config_clone() {
        let config = Processor {
            name: "clone_test".to_string(),
            iterate_key: Some("elements".to_string()),
        };

        let cloned = config.clone();
        assert_eq!(config, cloned);
    }
}

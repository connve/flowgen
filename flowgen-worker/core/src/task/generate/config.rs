//! Configuration structures for generate task types.
//!
//! Defines configuration options for event generation tasks that produce
//! synthetic or scheduled data streams in workflows.

use serde::{Deserialize, Serialize};

/// Configuration for generate subscriber tasks that produce scheduled events.
#[derive(PartialEq, Clone, Debug, Default, Deserialize, Serialize, Hash)]
pub struct Subscriber {
    /// Optional label for event subject generation.
    pub label: Option<String>,
    /// Optional message content for generated events.
    pub message: Option<String>,
    /// Interval between generated events in milliseconds.
    pub interval: u64,
    /// Optional maximum number of events to generate before stopping.
    pub count: Option<u64>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_subscriber_config_default() {
        let config = Subscriber::default();
        assert!(config.label.is_none());
        assert!(config.message.is_none());
        assert_eq!(config.interval, 0);
        assert!(config.count.is_none());
    }

    #[test]
    fn test_subscriber_config_creation() {
        let config = Subscriber {
            label: Some("test_label".to_string()),
            message: Some("test message".to_string()),
            interval: 5000,
            count: Some(10),
        };

        assert_eq!(config.label, Some("test_label".to_string()));
        assert_eq!(config.message, Some("test message".to_string()));
        assert_eq!(config.interval, 5000);
        assert_eq!(config.count, Some(10));
    }

    #[test]
    fn test_subscriber_config_serialization() {
        let config = Subscriber {
            label: Some("serialize_test".to_string()),
            message: None,
            interval: 1000,
            count: Some(5),
        };

        let serialized = serde_json::to_string(&config).unwrap();
        let deserialized: Subscriber = serde_json::from_str(&serialized).unwrap();

        assert_eq!(config, deserialized);
    }

    #[test]
    fn test_subscriber_config_clone() {
        let config = Subscriber {
            label: Some("clone_test".to_string()),
            message: Some("clone message".to_string()),
            interval: 2000,
            count: None,
        };

        let cloned = config.clone();
        assert_eq!(config, cloned);
    }
}

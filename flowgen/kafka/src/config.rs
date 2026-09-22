//! # Kafka Configuration
//!
//! Configuration for the Kafka produce task: broker addresses, credentials,
//! the target topic and its message key, and the settings applied when the
//! task creates a topic that does not yet exist.

use flowgen_core::config::ConfigExt;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::time::Duration;

fn default_brokers() -> String {
    "localhost:9092".to_string()
}

fn default_ack_timeout() -> Duration {
    Duration::from_secs(30)
}

/// Kafka topic property for how long a topic retains a message.
const TOPIC_RETENTION_MS: &str = "retention.ms";

fn default_partitions() -> i32 {
    1
}

fn default_replication_factor() -> i32 {
    1
}

/// Settings for a topic created by `create_or_update`.
#[derive(PartialEq, Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TopicOptions {
    /// Number of partitions for the new topic.
    #[serde(default = "default_partitions")]
    pub partitions: i32,
    /// Replication factor for the new topic. Cannot exceed the number of
    /// brokers in the cluster.
    #[serde(default = "default_replication_factor")]
    pub replication_factor: i32,
    /// How long the topic retains a message (e.g. "7d", "24h").
    #[serde(default, with = "humantime_serde")]
    pub retention: Option<Duration>,
    /// Any other topic-level setting, passed to the broker verbatim
    /// (e.g. `cleanup.policy`, `max.message.bytes`). Values are rendered as
    /// strings, so a template resolving to a number or boolean still works.
    #[serde(default)]
    pub config: BTreeMap<String, serde_json::Value>,
}

impl TopicOptions {
    /// Topic settings as the broker expects them: the typed fields rendered
    /// into their Kafka property names, then `config` layered on top.
    pub fn broker_config(&self) -> BTreeMap<String, String> {
        let mut config = BTreeMap::new();
        if let Some(retention) = self.retention {
            config.insert(
                TOPIC_RETENTION_MS.to_string(),
                retention.as_millis().to_string(),
            );
        }
        for (key, value) in &self.config {
            // The broker takes every setting as a string, and a JSON string
            // would otherwise arrive wrapped in its own quotes.
            let value = match value {
                serde_json::Value::String(s) => s.clone(),
                other => other.to_string(),
            };
            config.insert(key.clone(), value);
        }
        config
    }
}

impl Default for TopicOptions {
    fn default() -> Self {
        Self {
            partitions: default_partitions(),
            replication_factor: default_replication_factor(),
            retention: None,
            config: BTreeMap::new(),
        }
    }
}

/// Kafka produce task configuration.
#[derive(PartialEq, Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Produce {
    /// The unique name / identifier of the task.
    pub name: String,
    /// Path to credentials file containing SASL/SSL authentication details.
    /// Omit to connect without authentication.
    #[serde(default)]
    pub credentials_path: Option<PathBuf>,
    /// Comma-separated bootstrap broker addresses.
    #[serde(default = "default_brokers")]
    pub brokers: String,
    /// Topic to publish to.
    pub topic: String,
    /// Message key template, e.g. `key-{{event.id}}`.
    #[serde(default)]
    pub message_key: Option<String>,
    /// Whether to create the topic if it does not exist.
    /// When false, an error is returned if the topic is absent from the cluster.
    #[serde(default)]
    pub create_or_update: bool,
    /// Settings applied when `create_or_update` creates the topic. Ignored
    /// for a topic that already exists.
    #[serde(default)]
    pub topic_options: TopicOptions,
    /// How long to wait for the broker to acknowledge a message.
    #[serde(default = "default_ack_timeout", with = "humantime_serde")]
    pub ack_timeout: Duration,
    /// Optional list of upstream task names this task depends on.
    /// When set, this task only receives events from the named tasks.
    /// When not set, the task receives from the previous task in the list (linear chain).
    #[serde(default)]
    pub depends_on: Option<Vec<String>>,
    /// Optional retry configuration (overrides app-level retry config).
    #[serde(default)]
    pub retry: Option<flowgen_core::retry::RetryConfig>,
}

impl Default for Produce {
    fn default() -> Self {
        Self {
            name: String::new(),
            credentials_path: None,
            brokers: default_brokers(),
            topic: String::new(),
            message_key: None,
            create_or_update: false,
            topic_options: TopicOptions::default(),
            ack_timeout: default_ack_timeout(),
            depends_on: None,
            retry: None,
        }
    }
}

impl ConfigExt for Produce {}

impl Produce {
    /// Validates that required string fields are non-empty after any
    /// templating has been applied.
    pub fn validate(&self) -> Result<(), crate::produce::Error> {
        if self.topic.trim().is_empty() {
            return Err(crate::produce::Error::InvalidConfig {
                message: "topic must be non-empty".to_string(),
            });
        }
        if self.brokers.trim().is_empty() {
            return Err(crate::produce::Error::InvalidConfig {
                message: "brokers must be non-empty".to_string(),
            });
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    #[test]
    fn test_produce_default() {
        let config = Produce::default();
        assert_eq!(config.name, String::new());
        assert_eq!(config.brokers, default_brokers());
        assert_eq!(config.topic, String::new());
        assert_eq!(config.credentials_path, None);
        assert_eq!(config.ack_timeout, default_ack_timeout());
    }

    #[test]
    fn test_defaults_match_deserialized_defaults() {
        let json = r#"{ "name": "n", "topic": "t" }"#;
        let deserialized: Produce = serde_json::from_str(json).unwrap();

        assert_eq!(deserialized.brokers, Produce::default().brokers);
        assert_eq!(deserialized.ack_timeout, Produce::default().ack_timeout);
    }

    #[test]
    fn test_ack_timeout_parses_humantime() {
        let json = r#"{ "name": "n", "topic": "t", "ack_timeout": "5s" }"#;
        let config: Produce = serde_json::from_str(json).unwrap();
        assert_eq!(config.ack_timeout, Duration::from_secs(5));
    }

    #[test]
    fn test_rejects_unknown_fields() {
        let json = r#"{ "name": "n", "topic": "t", "bogus": 1 }"#;
        assert!(serde_json::from_str::<Produce>(json).is_err());
    }

    #[test]
    fn test_topic_options_default_to_single_partition() {
        let json = r#"{ "name": "n", "topic": "t" }"#;
        let config: Produce = serde_json::from_str(json).unwrap();

        assert_eq!(config.topic_options.partitions, 1);
        assert_eq!(config.topic_options.replication_factor, 1);
        assert_eq!(config.topic_options.retention, None);
        assert!(config.topic_options.config.is_empty());
    }

    #[test]
    fn test_topic_options_parse() {
        let json = r#"{
            "name": "n", "topic": "t",
            "create_or_update": true,
            "topic_options": {
                "partitions": 6,
                "replication_factor": 3,
                "retention": "7d",
                "config": { "cleanup.policy": "compact" }
            }
        }"#;
        let config: Produce = serde_json::from_str(json).unwrap();

        assert_eq!(config.topic_options.partitions, 6);
        assert_eq!(config.topic_options.replication_factor, 3);
        assert_eq!(
            config.topic_options.retention,
            Some(Duration::from_secs(7 * 24 * 60 * 60))
        );
    }

    #[test]
    fn test_broker_config_renders_retention_as_millis() {
        let options = TopicOptions {
            retention: Some(Duration::from_secs(7 * 24 * 60 * 60)),
            ..Default::default()
        };

        assert_eq!(
            options
                .broker_config()
                .get("retention.ms")
                .map(String::as_str),
            Some("604800000")
        );
    }

    #[test]
    fn test_broker_config_is_empty_without_settings() {
        assert!(TopicOptions::default().broker_config().is_empty());
    }

    #[test]
    fn test_broker_config_lets_raw_config_win() {
        let options = TopicOptions {
            retention: Some(Duration::from_secs(60)),
            config: [(TOPIC_RETENTION_MS.to_string(), serde_json::json!("1"))]
                .into_iter()
                .collect(),
            ..Default::default()
        };

        assert_eq!(
            options
                .broker_config()
                .get("retention.ms")
                .map(String::as_str),
            Some("1")
        );
    }

    #[test]
    fn test_topic_options_config_accepts_a_numeric_template() {
        let json = r#"{
            "name": "n", "topic": "t",
            "topic_options": { "config": { "max.message.bytes": "{{event.data.size}}" } }
        }"#;
        let config: Produce = serde_json::from_str(json).unwrap();
        let context = serde_json::json!({ "event": { "data": { "size": 1048576 } } });

        let rendered = config.render(&context).expect("numeric template renders");

        assert_eq!(
            rendered
                .topic_options
                .broker_config()
                .get("max.message.bytes")
                .map(String::as_str),
            Some("1048576")
        );
    }

    #[test]
    fn test_topic_options_reject_unknown_fields() {
        let json = r#"{
            "name": "n", "topic": "t",
            "topic_options": { "partitions": 2, "bogus": 1 }
        }"#;
        assert!(serde_json::from_str::<Produce>(json).is_err());
    }

    #[test]
    fn test_produce_creation() {
        let config = Produce {
            name: "test_producer".to_string(),
            brokers: "kafka:9092".to_string(),
            topic: "test-topic".to_string(),
            message_key: Some("key-{{id}}".to_string()),
            credentials_path: Some(PathBuf::from("/path/to/kafka.creds")),
            ..Default::default()
        };
        assert_eq!(config.name, "test_producer");
        assert_eq!(config.brokers, "kafka:9092");
        assert_eq!(config.topic, "test-topic");
        assert_eq!(config.message_key, Some("key-{{id}}".to_string()));
    }

    #[test]
    fn test_produce_serialization() {
        let config = Produce {
            name: "serial_producer".to_string(),
            brokers: "broker1:9092,broker2:9092".to_string(),
            topic: "serial-topic".to_string(),
            ..Default::default()
        };
        let json = serde_json::to_string(&config).unwrap();
        let deserialized: Produce = serde_json::from_str(&json).unwrap();
        assert_eq!(config, deserialized);
    }

    #[test]
    fn test_produce_clone() {
        let config = Produce {
            name: "clone_producer".to_string(),
            brokers: "clone:9092".to_string(),
            topic: "clone-topic".to_string(),
            ..Default::default()
        };
        let cloned = config.clone();
        assert_eq!(config, cloned);
    }

    #[test]
    fn test_validate_rejects_empty_topic() {
        let config = Produce {
            name: "test".into(),
            brokers: "b:9092".into(),
            topic: "".into(),
            ..Default::default()
        };
        let err = config.validate().unwrap_err().to_string();
        assert!(err.contains("topic must be non-empty"));
    }

    #[test]
    fn test_validate_rejects_whitespace_topic() {
        let config = Produce {
            name: "test".into(),
            brokers: "b:9092".into(),
            topic: "   ".into(),
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_validate_rejects_empty_brokers() {
        let config = Produce {
            name: "test".into(),
            brokers: "".into(),
            topic: "t".into(),
            ..Default::default()
        };
        let err = config.validate().unwrap_err().to_string();
        assert!(err.contains("brokers must be non-empty"));
    }

    #[test]
    fn test_validate_accepts_non_empty_topic_and_brokers() {
        let config = Produce {
            name: "test".into(),
            brokers: "b:9092".into(),
            topic: "t".into(),
            ..Default::default()
        };
        assert!(config.validate().is_ok());
    }
}

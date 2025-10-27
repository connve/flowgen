use flowgen_core::config::ConfigExt;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// Unified configuration for both NATS JetStream publisher and subscriber tasks.
#[derive(PartialEq, Clone, Debug, Default, Deserialize, Serialize)]
pub struct Config {
    /// The unique name / identifier of the task.
    pub name: String,
    /// Path to credentials file containing NATS authentication details.
    pub credentials_path: PathBuf,
    /// Subject to publish/subscribe to/from.
    /// For publisher: supports handlebars templates like "pubsub.{{event.subject}}" which renders with event data.
    /// For subscriber: the subject pattern to subscribe to.
    pub subject: String,
    /// Optional stream configuration. If provided for publisher, ensures stream exists.
    /// Required for subscriber as the stream name to consume from.
    pub stream: Option<StreamOptions>,
    /// Durable consumer name (subscriber only).
    pub durable_name: Option<String>,
    /// Number of messages to fetch per batch (subscriber only).
    pub batch_size: Option<usize>,
    /// Delay in seconds between message batch fetches (subscriber only).
    pub delay_secs: Option<u64>,
}

/// Type alias for backward compatibility with publisher code.
pub type Publisher = Config;

/// Type alias for backward compatibility with subscriber code.
pub type Subscriber = Config;

impl ConfigExt for Config {}

#[derive(PartialEq, Clone, Debug, Deserialize, Serialize)]
pub struct StreamOptions {
    /// Stream name.
    pub name: String,
    /// Stream description.
    pub description: Option<String>,
    /// Subject patterns for the stream (can include wildcards). Required for publisher when creating stream.
    #[serde(default)]
    pub subjects: Vec<String>,
    /// Maximum age of messages in seconds.
    pub max_age_secs: Option<u64>,
    /// Maximum number of messages per subject.
    pub max_messages_per_subject: Option<i64>,
    /// Whether to create or update the stream if it doesn't exist or differs.
    #[serde(default)]
    pub create_or_update: bool,
    /// Retention policy for the stream. If None during update, keeps existing value.
    pub retention: Option<RetentionPolicy>,
    /// Discard policy for when stream limits are reached. If None during update, keeps existing value.
    pub discard: Option<DiscardPolicy>,
}

/// NATS JetStream retention policies.
#[derive(PartialEq, Clone, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum RetentionPolicy {
    /// Limits retention based on stream limits (messages, bytes, age).
    #[default]
    Limits,
    /// Retains messages based on consumer interest.
    Interest,
    /// Retains messages until explicitly deleted.
    WorkQueue,
}

/// NATS JetStream discard policies.
#[derive(PartialEq, Clone, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum DiscardPolicy {
    /// Discard old messages when stream is full.
    #[default]
    Old,
    /// Discard new messages when stream is full.
    New,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    #[test]
    fn test_config_default() {
        let config = Config::default();
        assert_eq!(config.name, String::new());
        assert_eq!(config.credentials_path, PathBuf::new());
        assert_eq!(config.subject, String::new());
        assert_eq!(config.stream, None);
        assert_eq!(config.durable_name, None);
        assert_eq!(config.batch_size, None);
        assert_eq!(config.delay_secs, None);
    }

    #[test]
    fn test_subscriber_creation() {
        let subscriber = Subscriber {
            name: "test_subscriber".to_string(),
            credentials_path: PathBuf::from("/path/to/nats.creds"),
            subject: "test.subject".to_string(),
            stream: Some(StreamOptions {
                name: "test_stream".to_string(),
                description: None,
                subjects: vec!["test.>".to_string()],
                max_age_secs: None,
                max_messages_per_subject: None,
                create_or_update: false,
                retention: Some(RetentionPolicy::Limits),
                discard: Some(DiscardPolicy::Old),
            }),
            durable_name: Some("test_consumer".to_string()),
            batch_size: Some(100),
            delay_secs: Some(5),
        };

        assert_eq!(subscriber.name, "test_subscriber");
        assert_eq!(
            subscriber.credentials_path,
            PathBuf::from("/path/to/nats.creds")
        );
        assert_eq!(subscriber.subject, "test.subject");
        assert!(subscriber.stream.is_some());
        assert_eq!(subscriber.durable_name, Some("test_consumer".to_string()));
        assert_eq!(subscriber.batch_size, Some(100));
        assert_eq!(subscriber.delay_secs, Some(5));
    }

    #[test]
    fn test_subscriber_serialization() {
        let subscriber = Subscriber {
            name: "serial_sub".to_string(),
            credentials_path: PathBuf::from("/credentials/nats.jwt"),
            subject: "my.subject.*".to_string(),
            stream: Some(StreamOptions {
                name: "my_stream".to_string(),
                description: None,
                subjects: vec!["my.>".to_string()],
                max_age_secs: None,
                max_messages_per_subject: None,
                create_or_update: false,
                retention: Some(RetentionPolicy::Limits),
                discard: Some(DiscardPolicy::Old),
            }),
            durable_name: Some("my_durable".to_string()),
            batch_size: Some(50),
            delay_secs: Some(10),
        };

        let json = serde_json::to_string(&subscriber).unwrap();
        let deserialized: Subscriber = serde_json::from_str(&json).unwrap();
        assert_eq!(subscriber, deserialized);
    }

    #[test]
    fn test_subscriber_clone() {
        let subscriber = Subscriber {
            name: "clone_sub".to_string(),
            credentials_path: PathBuf::from("/test/creds.jwt"),
            subject: "clone.subject".to_string(),
            stream: Some(StreamOptions {
                name: "clone_stream".to_string(),
                description: None,
                subjects: vec!["clone.>".to_string()],
                max_age_secs: None,
                max_messages_per_subject: None,
                create_or_update: false,
                retention: Some(RetentionPolicy::Limits),
                discard: Some(DiscardPolicy::Old),
            }),
            durable_name: Some("clone_consumer".to_string()),
            batch_size: Some(25),
            delay_secs: None,
        };

        let cloned = subscriber.clone();
        assert_eq!(subscriber, cloned);
    }

    #[test]
    fn test_publisher_default() {
        let publisher = Publisher::default();
        assert_eq!(publisher.name, String::new());
        assert_eq!(publisher.credentials_path, PathBuf::new());
        assert_eq!(publisher.subject, String::new());
        assert_eq!(publisher.stream, None);
        assert_eq!(publisher.durable_name, None);
        assert_eq!(publisher.batch_size, None);
        assert_eq!(publisher.delay_secs, None);
    }

    #[test]
    fn test_publisher_creation() {
        let stream_opts = StreamOptions {
            name: "pub_stream".to_string(),
            description: Some("Test publisher stream".to_string()),
            subjects: vec!["pub.subject.1".to_string(), "pub.subject.2".to_string()],
            max_age_secs: Some(3600),
            max_messages_per_subject: Some(100),
            create_or_update: true,
            retention: Some(RetentionPolicy::Limits),
            discard: Some(DiscardPolicy::Old),
        };

        let publisher = Publisher {
            name: "test_publisher".to_string(),
            credentials_path: PathBuf::from("/path/to/pub.creds"),
            subject: "pub.subject.1".to_string(),
            stream: Some(stream_opts.clone()),
            durable_name: None,
            batch_size: None,
            delay_secs: None,
        };

        assert_eq!(publisher.name, "test_publisher");
        assert_eq!(
            publisher.credentials_path,
            PathBuf::from("/path/to/pub.creds")
        );
        assert_eq!(publisher.subject, "pub.subject.1");
        assert!(publisher.stream.is_some());
        if let Some(stream) = publisher.stream {
            assert_eq!(stream.name, "pub_stream");
            assert_eq!(stream.subjects, vec!["pub.subject.1", "pub.subject.2"]);
            assert_eq!(stream.max_age_secs, Some(3600));
            assert_eq!(stream.max_messages_per_subject, Some(100));
        }
    }

    #[test]
    fn test_publisher_serialization() {
        let publisher = Publisher {
            name: "serial_pub".to_string(),
            credentials_path: PathBuf::from("/creds/publisher.jwt"),
            subject: "test.serialize".to_string(),
            stream: Some(StreamOptions {
                name: "serialization_stream".to_string(),
                description: Some("Serialization test".to_string()),
                subjects: vec!["test.serialize".to_string()],
                max_age_secs: Some(7200),
                max_messages_per_subject: None,
                create_or_update: true,
                retention: Some(RetentionPolicy::Limits),
                discard: Some(DiscardPolicy::Old),
            }),
            durable_name: None,
            batch_size: None,
            delay_secs: None,
        };

        let json = serde_json::to_string(&publisher).unwrap();
        let deserialized: Publisher = serde_json::from_str(&json).unwrap();
        assert_eq!(publisher, deserialized);
    }

    #[test]
    fn test_publisher_clone() {
        let publisher = Publisher {
            name: "clone_pub".to_string(),
            credentials_path: PathBuf::from("/test/pub.creds"),
            subject: "clone.subject".to_string(),
            stream: Some(StreamOptions {
                name: "clone_stream".to_string(),
                description: None,
                subjects: vec!["clone.subject".to_string()],
                max_age_secs: Some(1800),
                max_messages_per_subject: Some(1),
                create_or_update: false,
                retention: Some(RetentionPolicy::WorkQueue),
                discard: Some(DiscardPolicy::New),
            }),
            durable_name: None,
            batch_size: None,
            delay_secs: None,
        };

        let cloned = publisher.clone();
        assert_eq!(publisher, cloned);
    }

    #[test]
    fn test_publisher_without_stream() {
        let publisher = Publisher {
            name: "no_stream_pub".to_string(),
            credentials_path: PathBuf::from("/no/stream.creds"),
            subject: "simple.subject".to_string(),
            stream: None,
            durable_name: None,
            batch_size: None,
            delay_secs: None,
        };

        assert_eq!(publisher.subject, "simple.subject");
        assert!(publisher.stream.is_none());
    }

    #[test]
    fn test_stream_with_multiple_subjects() {
        let stream_opts = StreamOptions {
            name: "multi_stream".to_string(),
            description: None,
            subjects: vec![
                "subject.1".to_string(),
                "subject.2".to_string(),
                "subject.3".to_string(),
            ],
            max_age_secs: Some(86400),
            max_messages_per_subject: None,
            create_or_update: true,
            retention: Some(RetentionPolicy::Interest),
            discard: Some(DiscardPolicy::Old),
        };

        let publisher = Publisher {
            name: "multi_pub".to_string(),
            credentials_path: PathBuf::from("/multi/subjects.creds"),
            subject: "subject.1".to_string(),
            stream: Some(stream_opts),
            durable_name: None,
            batch_size: None,
            delay_secs: None,
        };

        assert!(publisher.stream.is_some());
        if let Some(stream) = publisher.stream {
            assert_eq!(stream.subjects.len(), 3);
            assert!(stream.subjects.contains(&"subject.2".to_string()));
        }
    }

    #[test]
    fn test_config_equality() {
        let sub1 = Subscriber {
            name: "eq_sub".to_string(),
            credentials_path: PathBuf::from("/path/creds.jwt"),
            subject: "subject1".to_string(),
            stream: Some(StreamOptions {
                name: "stream1".to_string(),
                description: None,
                subjects: vec!["subject1".to_string()],
                max_age_secs: None,
                max_messages_per_subject: None,
                create_or_update: false,
                retention: Some(RetentionPolicy::Limits),
                discard: Some(DiscardPolicy::Old),
            }),
            durable_name: Some("consumer1".to_string()),
            batch_size: Some(10),
            delay_secs: Some(1),
        };

        let sub2 = Subscriber {
            name: "eq_sub".to_string(),
            credentials_path: PathBuf::from("/path/creds.jwt"),
            subject: "subject1".to_string(),
            stream: Some(StreamOptions {
                name: "stream1".to_string(),
                description: None,
                subjects: vec!["subject1".to_string()],
                max_age_secs: None,
                max_messages_per_subject: None,
                create_or_update: false,
                retention: Some(RetentionPolicy::Limits),
                discard: Some(DiscardPolicy::Old),
            }),
            durable_name: Some("consumer1".to_string()),
            batch_size: Some(10),
            delay_secs: Some(1),
        };

        assert_eq!(sub1, sub2);
    }
}

use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(PartialEq, Clone, Debug, Default, Deserialize, Serialize)]
pub struct Subscriber {
    /// The unique name / identifier of the task.
    pub name: String,
    pub credentials: PathBuf,
    pub stream: String,
    pub subject: String,
    pub durable_name: String,
    pub batch_size: usize,
    pub delay_secs: Option<u64>,
}

#[derive(PartialEq, Clone, Debug, Default, Deserialize, Serialize)]
pub struct Publisher {
    /// The unique name / identifier of the task.
    pub name: String,
    pub credentials: PathBuf,
    pub stream: String,
    pub stream_description: Option<String>,
    pub subjects: Vec<String>,
    pub max_age: Option<u64>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    #[test]
    fn test_subscriber_default() {
        let subscriber = Subscriber::default();
        assert_eq!(subscriber.name, String::new()); // FIX: Added name check
        assert_eq!(subscriber.credentials, PathBuf::new());
        assert_eq!(subscriber.stream, "");
        assert_eq!(subscriber.subject, "");
        assert_eq!(subscriber.durable_name, "");
        assert_eq!(subscriber.batch_size, 0);
        assert_eq!(subscriber.delay_secs, None);
    }

    #[test]
    fn test_subscriber_creation() {
        let subscriber = Subscriber {
            name: "test_subscriber".to_string(), // FIX: Added name initialization
            credentials: PathBuf::from("/path/to/nats.creds"),
            stream: "test_stream".to_string(),
            subject: "test.subject".to_string(),
            durable_name: "test_consumer".to_string(),
            batch_size: 100,
            delay_secs: Some(5),
        };

        assert_eq!(subscriber.name, "test_subscriber"); // FIX: Added name assertion
        assert_eq!(subscriber.credentials, PathBuf::from("/path/to/nats.creds"));
        assert_eq!(subscriber.stream, "test_stream");
        assert_eq!(subscriber.subject, "test.subject");
        assert_eq!(subscriber.durable_name, "test_consumer");
        assert_eq!(subscriber.batch_size, 100);
        assert_eq!(subscriber.delay_secs, Some(5));
    }

    #[test]
    fn test_subscriber_serialization() {
        let subscriber = Subscriber {
            name: "serial_sub".to_string(), // FIX: Added name initialization
            credentials: PathBuf::from("/credentials/nats.jwt"),
            stream: "my_stream".to_string(),
            subject: "my.subject.*".to_string(),
            durable_name: "my_durable".to_string(),
            batch_size: 50,
            delay_secs: Some(10),
        };

        let json = serde_json::to_string(&subscriber).unwrap();
        let deserialized: Subscriber = serde_json::from_str(&json).unwrap();
        assert_eq!(subscriber, deserialized);
    }

    #[test]
    fn test_subscriber_clone() {
        let subscriber = Subscriber {
            name: "clone_sub".to_string(), // FIX: Added name initialization
            credentials: PathBuf::from("/test/creds.jwt"),
            stream: "clone_stream".to_string(),
            subject: "clone.subject".to_string(),
            durable_name: "clone_consumer".to_string(),
            batch_size: 25,
            delay_secs: None,
        };

        let cloned = subscriber.clone();
        assert_eq!(subscriber, cloned);
    }

    #[test]
    fn test_publisher_default() {
        let publisher = Publisher::default();
        assert_eq!(publisher.name, String::new()); // FIX: Added name check
        assert_eq!(publisher.credentials, PathBuf::new());
        assert_eq!(publisher.stream, "");
        assert_eq!(publisher.stream_description, None);
        assert!(publisher.subjects.is_empty());
        assert_eq!(publisher.max_age, None);
    }

    #[test]
    fn test_publisher_creation() {
        let publisher = Publisher {
            name: "test_publisher".to_string(), // FIX: Added name initialization
            credentials: PathBuf::from("/path/to/pub.creds"),
            stream: "pub_stream".to_string(),
            stream_description: Some("Test publisher stream".to_string()),
            subjects: vec!["pub.subject.1".to_string(), "pub.subject.2".to_string()],
            max_age: Some(3600),
        };

        assert_eq!(publisher.name, "test_publisher"); // FIX: Added name assertion
        assert_eq!(publisher.credentials, PathBuf::from("/path/to/pub.creds"));
        assert_eq!(publisher.stream, "pub_stream");
        assert_eq!(
            publisher.stream_description,
            Some("Test publisher stream".to_string())
        );
        assert_eq!(publisher.subjects, vec!["pub.subject.1", "pub.subject.2"]);
        assert_eq!(publisher.max_age, Some(3600));
    }

    #[test]
    fn test_publisher_serialization() {
        let publisher = Publisher {
            name: "serial_pub".to_string(), // FIX: Added name initialization
            credentials: PathBuf::from("/creds/publisher.jwt"),
            stream: "serialization_stream".to_string(),
            stream_description: Some("Serialization test".to_string()),
            subjects: vec!["test.serialize".to_string()],
            max_age: Some(7200),
        };

        let json = serde_json::to_string(&publisher).unwrap();
        let deserialized: Publisher = serde_json::from_str(&json).unwrap();
        assert_eq!(publisher, deserialized);
    }

    #[test]
    fn test_publisher_clone() {
        let publisher = Publisher {
            name: "clone_pub".to_string(), // FIX: Added name initialization
            credentials: PathBuf::from("/test/pub.creds"),
            stream: "clone_stream".to_string(),
            stream_description: None,
            subjects: vec!["clone.subject".to_string()],
            max_age: Some(1800),
        };

        let cloned = publisher.clone();
        assert_eq!(publisher, cloned);
    }

    #[test]
    fn test_publisher_empty_subjects() {
        let publisher = Publisher {
            name: "empty_pub".to_string(), // FIX: Added name initialization
            credentials: PathBuf::from("/empty/subjects.creds"),
            stream: "empty_subjects_stream".to_string(),
            stream_description: Some("No subjects".to_string()),
            subjects: vec![],
            max_age: None,
        };

        assert!(publisher.subjects.is_empty());
        assert_eq!(publisher.max_age, None);
    }

    #[test]
    fn test_publisher_multiple_subjects() {
        let publisher = Publisher {
            name: "multi_pub".to_string(), // FIX: Added name initialization
            credentials: PathBuf::from("/multi/subjects.creds"),
            stream: "multi_stream".to_string(),
            stream_description: None,
            subjects: vec![
                "subject.1".to_string(),
                "subject.2".to_string(),
                "subject.3".to_string(),
            ],
            max_age: Some(86400),
        };

        assert_eq!(publisher.subjects.len(), 3);
        assert!(publisher.subjects.contains(&"subject.2".to_string()));
    }

    #[test]
    fn test_config_equality() {
        let sub1 = Subscriber {
            name: "eq_sub".to_string(), // FIX: Added name initialization
            credentials: PathBuf::from("/path/creds.jwt"),
            stream: "stream1".to_string(),
            subject: "subject1".to_string(),
            durable_name: "consumer1".to_string(),
            batch_size: 10,
            delay_secs: Some(1),
        };

        let sub2 = Subscriber {
            name: "eq_sub".to_string(), // FIX: Added name initialization
            credentials: PathBuf::from("/path/creds.jwt"),
            stream: "stream1".to_string(),
            subject: "subject1".to_string(),
            durable_name: "consumer1".to_string(),
            batch_size: 10,
            delay_secs: Some(1),
        };

        assert_eq!(sub1, sub2);
    }
}

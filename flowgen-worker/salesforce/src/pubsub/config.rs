//! # Salesforce Pub/Sub Configuration Module
//!
//! This module provides configuration structures for Salesforce Pub/Sub operations
//! within the flowgen system. It supports both subscribing to and publishing events
//! through Salesforce's Pub/Sub API, enabling real-time integration with Salesforce
//! platform events and change data capture.
//!
//! ## Core Components:
//! - `Subscriber`: Configuration for subscribing to Salesforce Pub/Sub topics.
//! - `Publisher`: Configuration for publishing events to Salesforce Pub/Sub topics.
//! - `Topic`: Topic-specific configuration including durable consumer settings.
//! - `DurableConsumerOptions`: Advanced subscription options for reliable message processing.
//!
//! ## Authentication
//! All configurations require Salesforce credentials which should reference
//! a credential store entry containing the necessary OAuth tokens and connection details.

use flowgen_core::config::ConfigExt;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::path::PathBuf;

/// Default Salesforce Pub/Sub API URL.
pub const DEFAULT_PUBSUB_URL: &str = "https://api.pubsub.salesforce.com";
/// Default Salesforce Pub/Sub API port.
pub const DEFAULT_PUBSUB_PORT: &str = "443";

/// Configuration structure for Salesforce Pub/Sub subscriber operations.
///
/// This structure defines all parameters needed to subscribe to Salesforce Pub/Sub topics,
/// including authentication credentials, topic configuration, and optional custom endpoints.
/// Subscribers can receive real-time events from Salesforce platform events, change data
/// capture, and custom events.
///
/// # Fields
/// - `label`: Optional human-readable identifier for this subscriber configuration.
/// - `credentials`: Reference to credential store entry containing Salesforce OAuth tokens.
/// - `topic`: Topic configuration including name and consumer options.
/// - `endpoint`: Optional Salesforce Pub/Sub endpoint (e.g., "api.pubsub.salesforce.com:7443").
///
/// # Examples
///
/// Basic subscriber configuration for platform events:
/// ```json
/// {
///     "salesforce_subscriber": {
///         "label": "account_changes_subscriber",
///         "credentials": "salesforce_prod_creds",
///         "topic": {
///             "name": "/event/Account_Change__e",
///             "num_requested": 10
///         }
///     }
/// }
/// ```
///
/// Advanced subscriber with durable consumer:
/// ```json
/// {
///     "salesforce_subscriber": {
///         "label": "opportunity_cdc_subscriber",
///         "credentials": "salesforce_prod_creds",
///         "topic": {
///             "name": "/data/OpportunityChangeEvent",
///             "num_requested": 25,
///             "durable_consumer_options": {
///                 "enabled": true,
///                 "managed_subscription": true,
///                 "name": "OpportunityProcessor"
///             }
///         },
///         "endpoint": "api.pubsub.salesforce.com:7443"
///     }
/// }
/// ```
///
/// Subscriber for custom platform event:
/// ```json
/// {
///     "salesforce_subscriber": {
///         "label": "inventory_alerts",
///         "credentials": "salesforce_inventory_creds",
///         "topic": {
///             "name": "/event/Inventory_Alert__e",
///             "num_requested": 5
///         }
///     }
/// }
/// ```
#[derive(PartialEq, Clone, Debug, Default, Deserialize, Serialize)]
pub struct Subscriber {
    /// The unique name / identifier of the task.
    pub name: String,
    /// Path to credentials file containing Salesforce authentication details.
    pub credentials_path: PathBuf,
    /// Topic configuration including name and subscription options.
    pub topic: Topic,
    /// Optional Salesforce Pub/Sub endpoint (e.g., "api.pubsub.salesforce.com:7443" or "api.deu.pubsub.salesforce.com:7443").
    pub endpoint: Option<String>,
}

/// Configuration structure for Salesforce Pub/Sub topic settings.
///
/// This structure defines topic-specific configuration including the topic name,
/// message batch size, and optional durable consumer settings for reliable
/// message processing and replay capabilities.
///
/// # Fields
/// - `name`: The full topic name including namespace (e.g., "/event/Account_Change__e").
/// - `durable_consumer_options`: Optional configuration for durable consumer functionality.
/// - `num_requested`: Number of messages to request in each batch (defaults to server setting).
///
/// # Topic Name Patterns
/// - Platform Events: `/event/CustomEvent__e`
/// - Change Data Capture: `/data/AccountChangeEvent`
/// - Custom Events: `/event/MyCustomEvent__e`
///
/// # Examples
///
/// Simple topic configuration:
/// ```json
/// {
///     "name": "/event/Order_Processed__e",
///     "num_requested": 15
/// }
/// ```
///
/// Topic with durable consumer for guaranteed processing:
/// ```json
/// {
///     "name": "/data/ContactChangeEvent",
///     "num_requested": 50,
///     "durable_consumer_options": {
///         "enabled": true,
///         "managed_subscription": true,
///         "name": "ContactSyncProcessor"
///     }
/// }
/// ```
#[derive(PartialEq, Clone, Debug, Default, Deserialize, Serialize)]
pub struct Topic {
    /// Full topic name including namespace (e.g., "/event/Account_Change__e").
    pub name: String,
    /// Optional durable consumer configuration for reliable message processing.
    pub durable_consumer_options: Option<DurableConsumerOptions>,
    /// Number of messages to request per batch (server default if not specified).
    pub num_requested: Option<i32>,
}

/// Configuration structure for Salesforce Pub/Sub publisher operations.
///
/// This structure defines all parameters needed to publish events to Salesforce Pub/Sub topics,
/// including authentication credentials, target topic, event payload template, and optional
/// dynamic input mappings for runtime data injection.
///
/// # Fields
/// - `label`: Optional human-readable identifier for this publisher configuration.
/// - `credentials`: Reference to credential store entry containing Salesforce OAuth tokens.
/// - `topic`: Target topic name for publishing events.
/// - `payload`: Event payload template with static values and placeholders.
/// - `inputs`: Optional input mappings for dynamic payload population.
/// - `endpoint`: Optional Salesforce Pub/Sub endpoint (e.g., "api.pubsub.salesforce.com:7443" or "api.deu.pubsub.salesforce.com:7443").
///
/// # Payload Template
/// The payload field supports both static values and dynamic placeholders that can be
/// populated at runtime using the inputs configuration. Placeholders use standard
/// template syntax for variable substitution.
///
/// # Examples
///
/// Basic publisher for platform events:
/// ```json
/// {
///     "salesforce_publisher": {
///         "label": "order_status_publisher",
///         "credentials": "salesforce_prod_creds",
///         "topic": "/event/Order_Status__e",
///         "payload": {
///             "Order_ID__c": "ORD-12345",
///             "Status__c": "Shipped",
///             "Timestamp__c": "2024-01-15T10:30:00Z"
///         }
///     }
/// }
/// ```
///
/// Advanced publisher with dynamic inputs:
/// ```json
/// {
///     "salesforce_publisher": {
///         "label": "customer_event_publisher",
///         "credentials": "salesforce_integration_creds",
///         "topic": "/event/Customer_Update__e",
///         "payload": {
///             "Customer_ID__c": "{{customer.id}}",
///             "Event_Type__c": "{{event.type}}",
///             "Data__c": "{{event.data}}",
///             "Source_System__c": "External_API"
///         },
///         "inputs": {
///             "customer": {
///                 "type": "json_path",
///                 "path": "$.customer"
///             },
///             "event": {
///                 "type": "json_path",
///                 "path": "$.event_data"
///             }
///         },
///         "endpoint": "api.pubsub.salesforce.com:7443"
///     }
/// }
/// ```
///
/// Publisher for change data capture simulation:
/// ```json
/// {
///     "salesforce_publisher": {
///         "label": "account_change_simulator",
///         "credentials": "salesforce_test_creds",
///         "topic": "/data/AccountChangeEvent",
///         "payload": {
///             "Id": "{{account.id}}",
///             "Name": "{{account.name}}",
///             "ChangeEventHeader": {
///                 "changeType": "UPDATE",
///                 "changedFields": ["Name", "Phone"],
///                 "recordIds": ["{{account.id}}"]
///             }
///         },
///         "inputs": {
///             "account": {
///                 "type": "previous_event",
///                 "field": "data"
///             }
///         }
///     }
/// }
/// ```
#[derive(PartialEq, Clone, Debug, Default, Deserialize, Serialize)]
pub struct Publisher {
    /// The unique name / identifier of the task.
    pub name: String,
    /// Path to credentials file containing Salesforce authentication details.
    pub credentials_path: PathBuf,
    /// Target topic name for publishing events (e.g., "/event/CustomEvent__e").
    pub topic: String,
    /// Event payload template with static values and dynamic placeholders.
    pub payload: Map<String, Value>,
    /// Optional Salesforce Pub/Sub endpoint (e.g., "api.pubsub.salesforce.com:7443" or "api.deu.pubsub.salesforce.com:7443").
    pub endpoint: Option<String>,
}

/// Configuration structure for Salesforce Pub/Sub durable consumer options.
///
/// Durable consumers provide reliable message processing with replay capabilities,
/// ensuring that messages are not lost even if the consumer temporarily disconnects.
/// This configuration enables advanced subscription management features including
/// message replay from specific points in time and managed subscription lifecycles.
///
/// # Fields
/// - `enabled`: Whether to enable durable consumer functionality.
/// - `managed_subscription`: Whether Salesforce should manage the subscription lifecycle.
/// - `name`: Unique name for this durable consumer subscription.
///
/// # Durable Consumer Benefits
/// - Message replay: Can replay messages from a specific replay ID or timestamp.
/// - Reliability: Messages are preserved even if the consumer disconnects.
/// - State management: Salesforce tracks consumer position and progress.
/// - Multi-instance: Multiple instances can share the same durable consumer.
///
/// # Examples
///
/// Basic durable consumer configuration:
/// ```json
/// {
///     "enabled": true,
///     "managed_subscription": true,
///     "name": "OrderProcessingConsumer"
/// }
/// ```
///
/// Manual subscription management:
/// ```json
/// {
///     "enabled": true,
///     "managed_subscription": false,
///     "name": "CustomEventProcessor_v2"
/// }
/// ```
///
/// High-throughput processing consumer:
/// ```json
/// {
///     "enabled": true,
///     "managed_subscription": true,
///     "name": "BulkDataSyncConsumer"
/// }
/// ```
#[derive(PartialEq, Clone, Debug, Default, Deserialize, Serialize)]
pub struct DurableConsumerOptions {
    /// Whether to enable durable consumer functionality for reliable message processing.
    pub enabled: bool,
    /// Whether Salesforce should automatically manage the subscription lifecycle.
    pub managed_subscription: bool,
    /// Unique name for this durable consumer subscription (must be unique within the org).
    pub name: String,
}

impl ConfigExt for Publisher {}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_subscriber_config_default() {
        let subscriber = Subscriber::default();
        assert_eq!(subscriber.name, String::new());
        assert_eq!(subscriber.credentials_path, PathBuf::new());
        assert_eq!(subscriber.topic, Topic::default());
        assert_eq!(subscriber.endpoint, None);
    }

    #[test]
    fn test_subscriber_config_serialization() {
        let subscriber = Subscriber {
            name: "test_subscriber".to_string(),
            credentials_path: PathBuf::from("test_creds"),
            topic: Topic {
                name: "/event/Test__e".to_string(),
                durable_consumer_options: Some(DurableConsumerOptions {
                    enabled: true,
                    managed_subscription: false,
                    name: "TestConsumer".to_string(),
                }),
                num_requested: Some(50),
            },
            endpoint: Some("api.pubsub.salesforce.com:7443".to_string()),
        };

        let json = serde_json::to_string(&subscriber).unwrap();
        let deserialized: Subscriber = serde_json::from_str(&json).unwrap();
        assert_eq!(subscriber, deserialized);
    }

    #[test]
    fn test_topic_config_default() {
        let topic = Topic::default();
        assert_eq!(topic.name, "");
        assert_eq!(topic.durable_consumer_options, None);
        assert_eq!(topic.num_requested, None);
    }

    #[test]
    fn test_topic_config_creation() {
        let topic = Topic {
            name: "/data/AccountChangeEvent".to_string(),
            durable_consumer_options: Some(DurableConsumerOptions {
                enabled: true,
                managed_subscription: true,
                name: "AccountProcessor".to_string(),
            }),
            num_requested: Some(100),
        };

        assert_eq!(topic.name, "/data/AccountChangeEvent");
        assert!(topic.durable_consumer_options.is_some());
        assert_eq!(topic.num_requested, Some(100));
    }

    #[test]
    fn test_publisher_config_default() {
        let publisher = Publisher::default();
        assert_eq!(publisher.name, String::new());
        assert_eq!(publisher.credentials_path, PathBuf::new());
        assert_eq!(publisher.topic, "");
        assert!(publisher.payload.is_empty());
        assert_eq!(publisher.endpoint, None);
    }

    #[test]
    fn test_publisher_config_serialization() {
        let mut payload = Map::new();
        payload.insert("Order_ID__c".to_string(), json!("ORD-12345"));
        payload.insert("Status__c".to_string(), json!("Shipped"));

        let publisher = Publisher {
            name: "order_publisher".to_string(),
            credentials_path: PathBuf::from("sf_creds"),
            topic: "/event/Order_Status__e".to_string(),
            payload,
            endpoint: Some("api.pubsub.salesforce.com:7443".to_string()),
        };

        let json = serde_json::to_string(&publisher).unwrap();
        let deserialized: Publisher = serde_json::from_str(&json).unwrap();
        assert_eq!(publisher, deserialized);
    }

    #[test]
    fn test_durable_consumer_options_default() {
        let options = DurableConsumerOptions::default();
        assert!(!options.enabled);
        assert!(!options.managed_subscription);
        assert_eq!(options.name, "");
    }

    #[test]
    fn test_durable_consumer_options_creation() {
        let options = DurableConsumerOptions {
            enabled: true,
            managed_subscription: true,
            name: "TestDurableConsumer".to_string(),
        };

        assert!(options.enabled);
        assert!(options.managed_subscription);
        assert_eq!(options.name, "TestDurableConsumer");
    }

    #[test]
    fn test_config_clone() {
        let subscriber = Subscriber {
            name: "test_subscriber".to_string(),
            credentials_path: PathBuf::from("creds"),
            topic: Topic {
                name: "/event/Clone__e".to_string(),
                durable_consumer_options: None,
                num_requested: Some(10),
            },
            endpoint: None,
        };

        let cloned = subscriber.clone();
        assert_eq!(subscriber, cloned);
    }
}

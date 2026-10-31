//! # Kafka Configuration
//!
//! Configuration shared by the Kafka tasks: the default broker address and
//! the validation errors.

pub(crate) fn default_brokers() -> String {
    "localhost:9092".to_string()
}

/// Kafka task configuration errors.
#[derive(thiserror::Error, Debug, PartialEq)]
#[non_exhaustive]
pub enum ConfigError {
    #[error("Topic must be non-empty")]
    EmptyTopic,
    #[error("Brokers must be non-empty")]
    EmptyBrokers,
    #[error("Ack timeout must be greater than zero")]
    ZeroAckTimeout,
    #[error("Start offset must be `earliest`, `latest`, or an RFC 3339 timestamp from 1970 on, got `{value}`")]
    InvalidStartOffset { value: String },
}

//! # Flowgen Kafka Integration
//!
//! Publishes flowgen events to Apache Kafka topics and consumes topics into
//! flows. Covers client and credentials handling, the task configuration,
//! and the produce and subscribe tasks.

/// Kafka client and SASL/SSL credentials handling.
pub mod client;
/// Configuration structures for Kafka tasks.
pub mod config;
/// Kafka produce task.
pub mod produce;
/// Kafka subscribe task.
pub mod subscribe;

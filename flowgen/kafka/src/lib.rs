//! # Flowgen Kafka Integration
//!
//! Publishes flowgen events to Apache Kafka topics. Covers client and
//! credentials handling, the task configuration, and the produce task itself.

/// Kafka client and SASL/SSL credentials handling.
pub mod client;
/// Configuration structures for Kafka tasks.
pub mod config;
/// Kafka produce task.
pub mod produce;

//! # Flowgen Kafka Integration
//!
//! Publishes flowgen events to Apache Kafka topics and consumes topics into
//! flows. Covers client and credentials handling, the task configuration,
//! and the produce and subscribe tasks.

use flowgen_core::task::meta::{Connection, Connector, Icon, TaskMeta};

/// Kafka client and SASL/SSL credentials handling.
pub mod client;
/// Configuration structures for Kafka tasks.
pub mod config;
/// Kafka produce task.
pub mod produce;
/// Kafka subscribe task.
pub mod subscribe;

/// The connector of this crate's task types.
pub const CONNECTOR: Connector = Connector {
    name: "kafka",
    label: "Kafka",
    icon: Icon::Svg(include_str!("../icons/kafka.svg")),
    describe_credentials: None,
};

impl TaskMeta for config::Produce {
    const CONNECTOR: Connector = CONNECTOR;

    fn connections(&self) -> Vec<Connection> {
        vec![Connection::new(CONNECTOR, self.credentials_path.as_deref()).target(&[&self.brokers])]
    }
}

impl TaskMeta for config::Subscribe {
    const CONNECTOR: Connector = CONNECTOR;

    fn connections(&self) -> Vec<Connection> {
        vec![Connection::new(CONNECTOR, self.credentials_path.as_deref()).target(&[&self.brokers])]
    }
}

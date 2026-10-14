//! NATS client integration for the flowgen worker system.
//!
//! This crate provides NATS messaging capabilities including JetStream support
//! for reliable message publishing and consuming. It handles authentication,
//! connection management, and provides both publisher and subscriber implementations
//! that integrate with the flowgen event system.

use flowgen_core::task::meta::{Connection, Connector, Icon, TaskMeta};

/// Credential caching functionality for NATS authentication.
pub mod cache;

/// NATS client connection and authentication management.
pub mod client;

/// JetStream specific functionality for reliable messaging.
pub mod jetstream {
    /// Configuration structures for JetStream publishers and subscribers.
    pub mod config;
    /// JetStream Key-Value store operations (get, put, list, delete).
    pub mod kv_store;
    /// Message conversion utilities for JetStream integration.
    pub mod message;
    /// JetStream publisher implementation for reliable message publishing.
    pub mod publisher;
    /// Stream management utilities for creating and updating JetStream streams.
    pub mod stream;
    /// JetStream subscriber implementation for reliable message consumption.
    pub mod subscriber;
}

/// The connector of this crate's task types.
pub const CONNECTOR: Connector = Connector {
    name: "nats",
    label: "NATS",
    icon: Icon::Svg(include_str!("../icons/nats.svg")),
    describe_credentials: None,
};

impl TaskMeta for jetstream::config::Config {
    const CONNECTOR: Connector = CONNECTOR;

    fn connections(&self) -> Vec<Connection> {
        vec![Connection::new(CONNECTOR, self.credentials_path.as_deref()).target(&[&self.url])]
    }
}

impl TaskMeta for jetstream::kv_store::Config {
    const CONNECTOR: Connector = CONNECTOR;

    fn connections(&self) -> Vec<Connection> {
        vec![Connection::new(CONNECTOR, self.credentials_path.as_deref()).target(&[&self.url])]
    }
}

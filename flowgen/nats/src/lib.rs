//! NATS client integration for the flowgen worker system.
//!
//! This crate provides NATS messaging capabilities including JetStream support
//! for reliable message publishing and consuming. It handles authentication,
//! connection management, and provides both publisher and subscriber implementations
//! that integrate with the flowgen event system.

use flowgen_core::task::context::TaskContext;
use flowgen_core::task::meta::{Connection, Connector, Icon};
use flowgen_core::task::runner::Runner;
use flowgen_core::task::{Task, TaskError, TaskIo, TaskMeta};
use std::sync::Arc;

/// Credential caching functionality for NATS authentication.
pub mod cache;

/// NATS client connection and authentication management.
pub mod client;

/// JetStream specific functionality for reliable messaging.
pub mod jetstream {
    /// Configuration structures for JetStream publishers and subscribers.
    pub mod config;
    /// Message conversion utilities for JetStream integration.
    pub mod message;
    /// Publishes events to JetStream.
    pub mod publish;
    /// Stream management utilities for creating and updating JetStream streams.
    pub mod stream;
    /// Consumes messages from JetStream.
    pub mod subscribe;
}

/// JetStream Key-Value store operations (get, put, list, delete).
pub mod kv_store;

/// The connector of this crate's task types.
pub const CONNECTOR: Connector = Connector {
    name: "nats",
    label: "NATS",
    icon: Icon::Svg(include_str!("../icons/nats.svg")),
    describe_credentials: None,
};

impl jetstream::config::Config {
    fn connections(&self) -> Vec<Connection> {
        vec![Connection::new(CONNECTOR, self.credentials_path.as_deref()).target(&[&self.url])]
    }
}

impl TaskMeta for jetstream::publish::Publish {
    const CONNECTOR: Connector = CONNECTOR;

    fn connections(&self) -> Vec<Connection> {
        self.0.connections()
    }
}

impl TaskMeta for jetstream::subscribe::Subscribe {
    const CONNECTOR: Connector = CONNECTOR;

    fn connections(&self) -> Vec<Connection> {
        self.0.connections()
    }
}

impl TaskMeta for kv_store::KvStore {
    const CONNECTOR: Connector = CONNECTOR;

    fn connections(&self) -> Vec<Connection> {
        vec![Connection::new(CONNECTOR, self.credentials_path.as_deref()).target(&[&self.url])]
    }
}

impl Task for jetstream::publish::Publish {
    async fn run(self, io: TaskIo, context: Arc<TaskContext>) -> Result<(), TaskError> {
        let mut builder = jetstream::publish::ProcessorBuilder::new()
            .config(Arc::new(self.0))
            .task_id(io.task_id)
            .task_type(io.task_type)
            .task_context(context);
        if let Some(rx) = io.rx {
            builder = builder.receiver(rx);
        }
        if let Some(tx) = io.tx {
            builder = builder.sender(tx);
        }
        builder.build().await?.run().await?;
        Ok(())
    }
}

impl Task for jetstream::subscribe::Subscribe {
    async fn run(self, io: TaskIo, context: Arc<TaskContext>) -> Result<(), TaskError> {
        let mut builder = jetstream::subscribe::ProcessorBuilder::new()
            .config(Arc::new(self.0))
            .task_id(io.task_id)
            .task_type(io.task_type)
            .task_context(context);
        if let Some(tx) = io.tx {
            builder = builder.sender(tx);
        }
        builder.build().await?.run().await?;
        Ok(())
    }
}

impl Task for kv_store::KvStore {
    async fn run(self, io: TaskIo, context: Arc<TaskContext>) -> Result<(), TaskError> {
        let mut builder = kv_store::ProcessorBuilder::new()
            .config(Arc::new(self))
            .task_id(io.task_id)
            .task_type(io.task_type)
            .task_context(context);
        if let Some(rx) = io.rx {
            builder = builder.receiver(rx);
        }
        if let Some(tx) = io.tx {
            builder = builder.sender(tx);
        }
        builder.build().await?.run().await?;
        Ok(())
    }
}

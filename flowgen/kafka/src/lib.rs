//! # Flowgen Kafka Integration
//!
//! Publishes flowgen events to Apache Kafka topics and consumes topics into
//! flows. Covers client and credentials handling, the task configuration,
//! and the produce and subscribe tasks.

use flowgen_core::task::context::TaskContext;
use flowgen_core::task::meta::{Connection, Connector, Icon};
use flowgen_core::task::runner::Runner;
use flowgen_core::task::{Task, TaskError, TaskIo, TaskMeta};
use std::sync::Arc;

/// Kafka client and SASL/SSL credentials handling.
pub mod client;
/// Configuration shared by the Kafka tasks.
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

impl TaskMeta for produce::Produce {
    const CONNECTOR: Connector = CONNECTOR;

    fn connections(&self) -> Vec<Connection> {
        vec![Connection::new(CONNECTOR, self.credentials_path.as_deref()).target(&[&self.brokers])]
    }

    fn validate(&self) -> Result<(), TaskError> {
        Ok(produce::Produce::validate(self)?)
    }
}

impl TaskMeta for subscribe::Subscribe {
    const CONNECTOR: Connector = CONNECTOR;
    const RUNS_UNTIL_STOPPED: bool = true;

    fn connections(&self) -> Vec<Connection> {
        vec![Connection::new(CONNECTOR, self.credentials_path.as_deref()).target(&[&self.brokers])]
    }

    fn validate(&self) -> Result<(), TaskError> {
        Ok(subscribe::Subscribe::validate(self)?)
    }
}

impl Task for produce::Produce {
    async fn run(self, io: TaskIo, context: Arc<TaskContext>) -> Result<(), TaskError> {
        let mut builder = produce::ProducerBuilder::new()
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

impl Task for subscribe::Subscribe {
    async fn run(self, io: TaskIo, context: Arc<TaskContext>) -> Result<(), TaskError> {
        let mut builder = subscribe::SubscriberBuilder::new()
            .config(Arc::new(self))
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

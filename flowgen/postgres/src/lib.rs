//! PostgreSQL integration for flowgen.
//!
//! Runs SQL statements with Arrow RecordBatch output over a pooled TLS
//! connection, writes rows with COPY, and captures row changes through
//! logical replication.

use flowgen_core::task::context::TaskContext;
use flowgen_core::task::meta::{read_credentials, Connection, Connector, Icon};
use flowgen_core::task::runner::Runner;
use flowgen_core::task::{Task, TaskError, TaskIo, TaskMeta};
use std::path::Path;
use std::sync::Arc;

pub mod cdc;
pub mod client;
pub mod config;
pub mod query;
pub mod rows;
pub mod write;

pub use client::Client;
pub use query::{Processor, Query};

/// The connector of this crate's task types.
pub const CONNECTOR: Connector = Connector {
    name: "postgres",
    label: "PostgreSQL",
    icon: Icon::Svg(include_str!("../icons/postgres.svg")),
    describe_credentials: Some(describe_credentials),
};

/// The server and database the credentials connect to.
fn describe_credentials(path: &Path) -> Option<String> {
    let credentials: config::Credentials = read_credentials(path)?;
    Some(format!(
        "{}:{}/{}",
        credentials.host, credentials.port, credentials.database
    ))
}

impl TaskMeta for query::Query {
    const CONNECTOR: Connector = CONNECTOR;

    fn connections(&self) -> Vec<Connection> {
        vec![Connection::new(CONNECTOR, Some(&self.credentials_path))]
    }
}

impl TaskMeta for cdc::Cdc {
    const CONNECTOR: Connector = CONNECTOR;
    const RUNS_UNTIL_STOPPED: bool = true;

    fn connections(&self) -> Vec<Connection> {
        vec![Connection::new(CONNECTOR, Some(&self.credentials_path))]
    }
}

impl TaskMeta for write::Write {
    const CONNECTOR: Connector = CONNECTOR;

    fn connections(&self) -> Vec<Connection> {
        vec![Connection::new(CONNECTOR, Some(&self.credentials_path))]
    }
}

impl Task for write::Write {
    async fn run(self, io: TaskIo, context: Arc<TaskContext>) -> Result<(), TaskError> {
        let mut builder = write::ProcessorBuilder::new()
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
        builder.build()?.run().await?;
        Ok(())
    }
}

impl Task for cdc::Cdc {
    async fn run(self, io: TaskIo, context: Arc<TaskContext>) -> Result<(), TaskError> {
        let mut builder = cdc::ProcessorBuilder::new()
            .config(Arc::new(self))
            .task_id(io.task_id)
            .task_type(io.task_type)
            .task_context(context);
        if let Some(tx) = io.tx {
            builder = builder.sender(tx);
        }
        builder.build()?.run().await?;
        Ok(())
    }
}

impl Task for query::Query {
    async fn run(self, io: TaskIo, context: Arc<TaskContext>) -> Result<(), TaskError> {
        let mut builder = query::ProcessorBuilder::new()
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
        builder.build()?.run().await?;
        Ok(())
    }
}

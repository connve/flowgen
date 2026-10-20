//! # Flowgen MongoDB Integration
//!
//! This module provides MongoDB integration components for the Flowgen data processing framework.
//! It includes support for change data capture (CDC), collection read/write/upsert, and message conversion
//! utilities for working with MongoDB collections and change streams.
use flowgen_core::task::context::TaskContext;
use flowgen_core::task::meta::{Connection, Connector, Icon};
use flowgen_core::task::runner::Runner;
use flowgen_core::task::{Task, TaskError, TaskIo, TaskMeta};
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// MongoDB change data capture task.
pub mod change_stream;
/// Configuration for MongoDB client creator.
pub mod client;
/// MongoDB collection read/write/upsert task.
pub mod collection;
/// Message conversion utilities for MongoDB integration.
pub mod message;

/// The connector of this crate's task types.
pub const CONNECTOR: Connector = Connector {
    name: "mongodb",
    label: "MongoDB",
    icon: Icon::Svg(include_str!("../icons/mongodb.svg")),
    describe_credentials: Some(describe_credentials),
};

/// The hosts the credentials connect to.
fn describe_credentials(path: &Path) -> Option<String> {
    match client::MongoCredentials::from_file(path) {
        Ok(credentials) => Some(credentials.authority()),
        Err(_) => None,
    }
}

fn connections(credentials_path: &Option<PathBuf>) -> Vec<Connection> {
    let connection = Connection::new(CONNECTOR, credentials_path.as_deref());
    match credentials_path {
        Some(_) => vec![connection],
        None => vec![connection.target(&[&client::MongoCredentials::default().authority()])],
    }
}

impl TaskMeta for collection::Collection {
    const CONNECTOR: Connector = CONNECTOR;

    fn connections(&self) -> Vec<Connection> {
        connections(&self.credentials_path)
    }
}

impl TaskMeta for change_stream::ChangeStream {
    const CONNECTOR: Connector = CONNECTOR;

    fn connections(&self) -> Vec<Connection> {
        connections(&self.credentials_path)
    }
}

impl Task for collection::Collection {
    async fn run(self, io: TaskIo, context: Arc<TaskContext>) -> Result<(), TaskError> {
        let mut builder = collection::ProcessorBuilder::new()
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

impl Task for change_stream::ChangeStream {
    async fn run(self, io: TaskIo, context: Arc<TaskContext>) -> Result<(), TaskError> {
        let mut builder = change_stream::ChangeStreamReaderBuilder::new()
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn credentials_are_described_by_their_hosts_only() {
        let file = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(
            file.path(),
            r#"{"host": ["mongo-0", "mongo-1:27018"], "username": "u", "password": "secret"}"#,
        )
        .unwrap();

        assert_eq!(
            describe_credentials(file.path()).as_deref(),
            Some("mongo-0:27017,mongo-1:27018")
        );
    }
}

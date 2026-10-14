//! # Flowgen MongoDB Integration
//!
//! This module provides MongoDB integration components for the Flowgen data processing framework.
//! It includes support for change data capture (CDC), collection read/write/upsert, and message conversion
//! utilities for working with MongoDB collections and change streams.
use flowgen_core::task::meta::{Connection, Connector, Icon, TaskMeta};
use std::path::{Path, PathBuf};

/// Configuration for MongoDB change data capture.
pub mod change_stream;
/// Configuration for MongoDB client creator.
pub mod client;
/// MongoDB collection read/write/upsert processor.
pub mod collection;
/// Configuration structures for MongoDB processors.
pub mod config;
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

impl TaskMeta for config::Collection {
    const CONNECTOR: Connector = CONNECTOR;

    fn connections(&self) -> Vec<Connection> {
        connections(&self.credentials_path)
    }
}

impl TaskMeta for config::ChangeStream {
    const CONNECTOR: Connector = CONNECTOR;

    fn connections(&self) -> Vec<Connection> {
        connections(&self.credentials_path)
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

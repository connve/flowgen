//! Salesforce integration for the flowgen worker system.
//!
//! This crate provides Salesforce connectivity and Pub/Sub API support for
//! real-time data streaming. It handles authentication, connection management,
//! and provides both publisher and subscriber implementations that integrate
//! with the flowgen event system.

use flowgen_core::task::meta::{host, read_credentials, Connection, Connector, Icon, TaskMeta};
use std::path::Path;

/// Salesforce Pub/Sub API functionality for real-time messaging.
pub mod pubsubapi {
    /// Configuration structures for Salesforce Pub/Sub publishers and subscribers.
    pub mod config;
    /// Salesforce Pub/Sub publisher implementation for event publishing.
    pub mod publisher;
    /// Salesforce Pub/Sub subscriber implementation for event consumption.
    pub mod subscriber;
}

pub mod bulkapi {
    /// Configuration structures for Salesforce Bulk API operations.
    pub mod config;
    /// Salesforce Bulk API query job operations (create, get, delete, abort, get_results).
    pub mod query_job;
}

/// Salesforce REST API operations (create, get, update, upsert, delete, get_deleted, search).
pub mod restapi {
    pub mod composite;
    pub mod config;
    pub mod search;
    pub mod sobject;
}

/// Salesforce SOAP API operations (merge).
pub mod soapapi {
    /// Configuration structures for SOAP API operations.
    pub mod config;
    /// Record merge processor for combining duplicate SObject records.
    pub mod merge;
}

/// Salesforce Tooling API operations for metadata management.
pub mod toolingapi {
    /// Configuration structures for Salesforce Tooling API operations.
    pub mod config;
    /// Salesforce Tooling API processor implementation.
    pub mod processor;
}

/// The connector of this crate's task types.
pub const CONNECTOR: Connector = Connector {
    name: "salesforce",
    label: "Salesforce",
    icon: Icon::Svg(include_str!("../icons/salesforce.svg")),
    describe_credentials: Some(describe_credentials),
};

/// The host of the org the credentials log in to.
fn describe_credentials(path: &Path) -> Option<String> {
    let credentials: salesforce_core::client::Credentials = read_credentials(path)?;
    Some(host(&credentials.instance_url))
}

fn connections(credentials_path: &Path) -> Vec<Connection> {
    vec![Connection::new(CONNECTOR, Some(credentials_path))]
}

impl TaskMeta for pubsubapi::config::Subscriber {
    const CONNECTOR: Connector = CONNECTOR;

    fn connections(&self) -> Vec<Connection> {
        connections(&self.credentials_path)
    }
}

impl TaskMeta for pubsubapi::config::Publisher {
    const CONNECTOR: Connector = CONNECTOR;

    fn connections(&self) -> Vec<Connection> {
        connections(&self.credentials_path)
    }
}

impl TaskMeta for bulkapi::config::QueryJob {
    const CONNECTOR: Connector = CONNECTOR;
    const CPU_INTENSIVE: bool = true;

    fn connections(&self) -> Vec<Connection> {
        connections(&self.credentials_path)
    }
}

impl TaskMeta for restapi::config::SObject {
    const CONNECTOR: Connector = CONNECTOR;

    fn connections(&self) -> Vec<Connection> {
        connections(&self.credentials_path)
    }
}

impl TaskMeta for restapi::config::Composite {
    const CONNECTOR: Connector = CONNECTOR;

    fn connections(&self) -> Vec<Connection> {
        connections(&self.credentials_path)
    }
}

impl TaskMeta for restapi::config::Search {
    const CONNECTOR: Connector = CONNECTOR;

    fn connections(&self) -> Vec<Connection> {
        connections(&self.credentials_path)
    }
}

impl TaskMeta for soapapi::config::Merge {
    const CONNECTOR: Connector = CONNECTOR;

    fn connections(&self) -> Vec<Connection> {
        connections(&self.credentials_path)
    }
}

impl TaskMeta for toolingapi::config::Tooling {
    const CONNECTOR: Connector = CONNECTOR;

    fn connections(&self) -> Vec<Connection> {
        connections(&self.credentials_path)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn credentials_are_described_by_their_org_host_only() {
        let file = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(
            file.path(),
            r#"{"client_id": "c", "client_secret": "secret", "instance_url": "https://acme.my.salesforce.com", "tenant_id": "t"}"#,
        )
        .unwrap();

        assert_eq!(
            describe_credentials(file.path()).as_deref(),
            Some("acme.my.salesforce.com")
        );
    }
}

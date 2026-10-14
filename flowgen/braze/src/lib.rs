//! Flowgen integration for the Braze REST API.
//!
//! This crate exposes Braze operations as flowgen tasks. The first supported
//! task is [`export::users::processor::Processor`], which calls Braze's
//! `POST /users/export/ids` endpoint.

#![deny(missing_docs)]

use flowgen_core::task::meta::{host, read_credentials, Connection, Connector, Icon, TaskMeta};
use std::path::Path;

pub mod export;

/// The connector of this crate's task types.
pub const CONNECTOR: Connector = Connector {
    name: "braze",
    label: "Braze",
    icon: Icon::Png(include_bytes!("../icons/braze.png")),
    describe_credentials: Some(describe_credentials),
};

/// The REST endpoint host of the Braze instance the credentials belong to.
fn describe_credentials(path: &Path) -> Option<String> {
    let credentials: braze::Credentials = read_credentials(path)?;
    Some(host(&credentials.rest_endpoint))
}

impl TaskMeta for export::users::config::Processor {
    const CONNECTOR: Connector = CONNECTOR;

    fn connections(&self) -> Vec<Connection> {
        vec![Connection::new(CONNECTOR, Some(&self.credentials_path))]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn credentials_are_described_by_their_endpoint_host_only() {
        let file = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(
            file.path(),
            r#"{"api_key": "secret", "rest_endpoint": "https://rest.iad-01.braze.com"}"#,
        )
        .unwrap();

        assert_eq!(
            describe_credentials(file.path()).as_deref(),
            Some("rest.iad-01.braze.com")
        );
    }
}

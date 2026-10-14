//! Microsoft SQL Server integration for flowgen.
//!
//! Provides SQL Server query execution with Arrow RecordBatch output, connection pooling,
//! and template rendering for parameterized queries.

use flowgen_core::task::meta::{read_credentials, Connection, Connector, Icon, TaskMeta};
use std::path::Path;

pub mod client;
pub mod config;
pub mod query;

pub use client::Client;
pub use config::Query;
pub use query::Processor;

/// The connector of this crate's task types.
pub const CONNECTOR: Connector = Connector {
    name: "mssql",
    label: "MSSQL",
    icon: Icon::Svg(include_str!("../icons/mssql.svg")),
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

impl TaskMeta for config::Query {
    const CONNECTOR: Connector = CONNECTOR;

    fn connections(&self) -> Vec<Connection> {
        vec![Connection::new(CONNECTOR, Some(&self.credentials_path))]
    }
}

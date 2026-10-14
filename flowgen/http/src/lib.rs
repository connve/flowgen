//! HTTP processing capabilities for flowgen workers.
//!
//! Provides HTTP request/response processing, endpoint handling, and server
//! management for flowgen event processing pipelines.

use flowgen_core::task::meta::{Connection, Connector, Icon, TaskMeta};

/// Configuration structures for HTTP processors.
pub mod config;
/// HTTP endpoint processor for inbound requests.
pub mod endpoint;
/// HTTP request processor for outbound calls.
pub mod request;
/// Shared HTTP server management.
pub mod server;

/// The connector of this crate's task types.
pub const CONNECTOR: Connector = Connector {
    name: "http",
    label: "HTTP",
    icon: Icon::Svg(include_str!("../icons/http.svg")),
    describe_credentials: None,
};

impl TaskMeta for config::Processor {
    const CONNECTOR: Connector = CONNECTOR;

    fn connections(&self) -> Vec<Connection> {
        vec![Connection::new(CONNECTOR, self.credentials_path.as_deref()).target(&[&self.endpoint])]
    }
}

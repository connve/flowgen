//! HTTP processing capabilities for flowgen workers.
//!
//! Provides HTTP request/response processing, endpoint handling, and server
//! management for flowgen event processing pipelines.

use flowgen_core::task::context::{Services, TaskContext};
use flowgen_core::task::meta::{Connection, Connector, Icon};
use flowgen_core::task::runner::Runner;
use flowgen_core::task::{Task, TaskError, TaskIo, TaskMeta};
use std::sync::Arc;

/// Configuration shared by the HTTP task types.
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

impl config::Processor {
    fn connections(&self) -> Vec<Connection> {
        vec![Connection::new(CONNECTOR, self.credentials_path.as_deref()).target(&[&self.endpoint])]
    }
}

impl TaskMeta for request::Request {
    const CONNECTOR: Connector = CONNECTOR;

    fn connections(&self) -> Vec<Connection> {
        self.0.connections()
    }
}

impl TaskMeta for endpoint::Endpoint {
    const CONNECTOR: Connector = CONNECTOR;
    const REGISTERS: bool = true;

    fn connections(&self) -> Vec<Connection> {
        self.0.connections()
    }
}

impl Task for request::Request {
    async fn run(self, io: TaskIo, context: Arc<TaskContext>) -> Result<(), TaskError> {
        let mut builder = request::ProcessorBuilder::new()
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

fn endpoint_server(services: &Services) -> Result<Arc<server::EndpointServer>, endpoint::Error> {
    services
        .get::<server::EndpointServer>()
        .ok_or(endpoint::Error::HttpServerNotEnabled)
}

impl Task for endpoint::Endpoint {
    fn check(&self, services: &Services) -> Result<(), TaskError> {
        endpoint_server(services)?;
        Ok(())
    }

    async fn run(self, io: TaskIo, context: Arc<TaskContext>) -> Result<(), TaskError> {
        let http_server = endpoint_server(&context.services)?;
        let mut builder = endpoint::ProcessorBuilder::new()
            .config(Arc::new(self.0))
            .task_id(io.task_id)
            .task_type(io.task_type)
            .task_context(context)
            .http_server(http_server);
        if let Some(tx) = io.tx {
            builder = builder.sender(tx);
        }
        builder.build().await?.run().await?;
        Ok(())
    }
}

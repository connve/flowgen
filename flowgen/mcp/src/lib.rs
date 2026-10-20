//! MCP (Model Context Protocol) server implementation for flowgen.
//!
//! Exposes flowgen flows as MCP tools callable by LLMs via Streamable HTTP transport.
//! Implements the MCP spec (2025-03-26) with JSON-RPC 2.0 over a single POST endpoint
//! that returns SSE streams for real-time progress and results.

use flowgen_core::task::context::{Services, TaskContext};
use flowgen_core::task::meta::{Connector, Icon};
use flowgen_core::task::runner::Runner;
use flowgen_core::task::{Task, TaskError, TaskIo, TaskMeta};
use std::sync::Arc;

/// Shared completion-source primitives for prompts and resource templates.
pub mod completion;
/// Configuration shared by the MCP server and tool tasks.
pub mod config;
/// MCP prompt task (exposes slash-command templates to MCP clients).
pub mod prompt;
/// MCP resource task (exposes read-only content as MCP resources).
pub mod resource;
/// MCP server with tool registry and Axum handlers.
pub mod server;
/// MCP tool task (registers tools with the MCP server).
pub mod tool;

/// The connector of this crate's task types and of the MCP servers agents call.
pub const CONNECTOR: Connector = Connector {
    name: "mcp",
    label: "MCP",
    icon: Icon::Svg(include_str!("../icons/mcp.svg")),
    describe_credentials: None,
};

/// A task that needs the MCP server, which the app config does not enable.
#[derive(thiserror::Error, Debug)]
#[error("{task_type} needs the MCP server: enable `mcp_server` in the app config")]
pub struct ServerNotEnabled {
    task_type: &'static str,
}

fn mcp_server(
    services: &Services,
    task_type: &'static str,
) -> Result<Arc<server::McpServer>, ServerNotEnabled> {
    services
        .get::<server::McpServer>()
        .ok_or(ServerNotEnabled { task_type })
}

impl TaskMeta for tool::Tool {
    const CONNECTOR: Connector = CONNECTOR;
    const REGISTERS: bool = true;
}

impl TaskMeta for prompt::Prompt {
    const CONNECTOR: Connector = CONNECTOR;
    const PIPELINE_IO: bool = false;
    const REGISTERS: bool = true;
}

impl TaskMeta for resource::Resource {
    const CONNECTOR: Connector = CONNECTOR;
    const PIPELINE_IO: bool = false;
    const REGISTERS: bool = true;
}

impl Task for tool::Tool {
    fn check(&self, services: &Services) -> Result<(), TaskError> {
        mcp_server(services, "mcp_tool")?;
        Ok(())
    }

    async fn run(self, io: TaskIo, context: Arc<TaskContext>) -> Result<(), TaskError> {
        let mcp_server = mcp_server(&context.services, io.task_type)?;
        let mut builder = tool::ProcessorBuilder::new()
            .config(Arc::new(self))
            .task_id(io.task_id)
            .task_type(io.task_type)
            .task_context(context)
            .mcp_server(mcp_server);
        if let Some(tx) = io.tx {
            builder = builder.sender(tx);
        }
        builder.build().await?.run().await?;
        Ok(())
    }
}

impl Task for prompt::Prompt {
    fn check(&self, services: &Services) -> Result<(), TaskError> {
        mcp_server(services, "mcp_prompt")?;
        Ok(())
    }

    async fn run(self, io: TaskIo, context: Arc<TaskContext>) -> Result<(), TaskError> {
        let mcp_server = mcp_server(&context.services, io.task_type)?;
        prompt::ProcessorBuilder::new()
            .config(Arc::new(self))
            .task_id(io.task_id)
            .task_type(io.task_type)
            .task_context(context)
            .mcp_server(mcp_server)
            .build()
            .await?
            .run()
            .await?;
        Ok(())
    }
}

impl Task for resource::Resource {
    fn check(&self, services: &Services) -> Result<(), TaskError> {
        mcp_server(services, "mcp_resource")?;
        Ok(())
    }

    async fn run(self, io: TaskIo, context: Arc<TaskContext>) -> Result<(), TaskError> {
        let mcp_server = mcp_server(&context.services, io.task_type)?;
        resource::ProcessorBuilder::new()
            .config(Arc::new(self))
            .task_id(io.task_id)
            .task_type(io.task_type)
            .task_context(context)
            .mcp_server(mcp_server)
            .build()
            .await?
            .run()
            .await?;
        Ok(())
    }
}

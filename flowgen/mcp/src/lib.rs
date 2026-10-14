//! MCP (Model Context Protocol) server implementation for flowgen.
//!
//! Exposes flowgen flows as MCP tools callable by LLMs via Streamable HTTP transport.
//! Implements the MCP spec (2025-03-26) with JSON-RPC 2.0 over a single POST endpoint
//! that returns SSE streams for real-time progress and results.

use flowgen_core::task::meta::{Connector, Icon, TaskMeta};

/// Shared completion-source primitives for prompts and resource templates.
pub mod completion;
/// MCP tool task configuration.
pub mod config;
/// MCP tool task processor (registers tools with the MCP server).
pub mod processor;
/// MCP prompt task (exposes slash-command templates to MCP clients).
pub mod prompt;
/// MCP resource task (exposes read-only content as MCP resources).
pub mod resource;
/// MCP server with tool registry and Axum handlers.
pub mod server;

pub use config::Processor;

/// The connector of this crate's task types and of the MCP servers agents call.
pub const CONNECTOR: Connector = Connector {
    name: "mcp",
    label: "MCP",
    icon: Icon::Svg(include_str!("../icons/mcp.svg")),
    describe_credentials: None,
};

impl TaskMeta for config::Processor {
    const CONNECTOR: Connector = CONNECTOR;
}

impl TaskMeta for prompt::config::Processor {
    const CONNECTOR: Connector = CONNECTOR;
}

impl TaskMeta for resource::config::Processor {
    const CONNECTOR: Connector = CONNECTOR;
}

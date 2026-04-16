//! MCP (Model Context Protocol) core types and response registry.
//!
//! Provides shared types for MCP tool result delivery and progress streaming
//! between the MCP server endpoint and flow pipeline tasks.

pub mod registry;

pub use registry::ResponseRegistry;

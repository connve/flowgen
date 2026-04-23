//! MCP (Model Context Protocol) server implementation for flowgen.
//!
//! Exposes flowgen flows as MCP tools callable by LLMs via Streamable HTTP transport.
//! Implements the MCP spec (2025-03-26) with JSON-RPC 2.0 over a single POST endpoint
//! that returns SSE streams for real-time progress and results.

/// MCP tool task configuration.
pub mod config;
/// MCP tool task processor (registers tools with the MCP server).
pub mod processor;
/// MCP server with tool registry and Axum handlers.
pub mod server;

pub use config::Processor;

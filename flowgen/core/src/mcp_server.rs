//! MCP server trait for task context integration.
//!
//! Provides an abstraction for MCP server functionality that can be shared
//! across tasks through the task context. Since implementations use framework-specific
//! types, this trait is intentionally minimal to avoid coupling.

use std::any::Any;
use std::fmt::Debug;

/// Error type for MCP server operations.
pub type Error = Box<dyn std::error::Error + Send + Sync>;

/// MCP server trait marker for task context integration.
///
/// This is a marker trait that allows MCP server instances to be stored
/// in the task context. The actual MCP server implementation lives in the
/// mcp crate and provides concrete methods for tool registration, protocol
/// handling, and SSE streaming.
///
/// This trait is intentionally minimal to avoid coupling the core crate
/// to web framework specifics. Implementations should provide
/// downcasting support via `as_any`.
pub trait McpServer: Debug + Send + Sync + 'static {
    /// Provides downcasting support for trait objects.
    fn as_any(&self) -> &dyn Any;
}

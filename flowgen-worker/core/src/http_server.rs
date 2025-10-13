//! HTTP server trait for task context integration.
//!
//! Provides an abstraction for HTTP server functionality that can be shared
//! across tasks through the task context. Since implementations use axum-specific
//! types, this trait is intentionally minimal to avoid framework coupling.

use std::any::Any;
use std::fmt::Debug;

/// Error type for HTTP server operations.
pub type Error = Box<dyn std::error::Error + Send + Sync>;

/// HTTP server trait marker for task context integration.
///
/// This is a marker trait that allows HTTP server instances to be stored
/// in the task context. The actual HTTP server implementation lives in the
/// http crate and provides concrete methods for route registration and
/// server startup.
///
/// This trait is intentionally minimal to avoid coupling the core crate
/// to web framework specifics like axum. Implementations should provide
/// downcasting support via `as_any`.
pub trait HttpServer: Debug + Send + Sync + 'static {
    /// Provides downcasting support for trait objects.
    fn as_any(&self) -> &dyn Any;
}

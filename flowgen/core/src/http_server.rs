//! HTTP server trait for task context integration.
//!
//! Provides an abstraction for HTTP server functionality that can be shared
//! across tasks through the task context. Route registration uses type-erased
//! `Box<dyn Any>` to avoid coupling core to web framework specifics.

use crate::auth::AuthProvider;
use std::any::Any;
use std::fmt::Debug;
use std::sync::Arc;

/// Error type for HTTP server operations.
pub type Error = Box<dyn std::error::Error + Send + Sync>;

/// HTTP server trait for task context integration.
///
/// Allows tasks to register HTTP routes without depending on the concrete
/// server implementation or web framework types. The `route` parameter in
/// `register_route` is a type-erased `Box<dyn Any + Send>` — implementations
/// downcast it to the expected framework type (e.g., axum `MethodRouter`).
#[async_trait::async_trait]
pub trait HttpServer: Debug + Send + Sync + 'static {
    /// Provides downcasting support for trait objects.
    fn as_any(&self) -> &dyn Any;

    /// Register a route at the given path.
    ///
    /// The `route` parameter is a type-erased route handler. Implementations
    /// should downcast it to the expected type (e.g., `axum::routing::MethodRouter`).
    async fn register_route(&self, path: String, route: Box<dyn Any + Send>);

    /// Returns the configured auth provider, if any.
    ///
    /// Tasks that require user authentication call this to validate bearer
    /// tokens and extract `UserContext` from incoming requests.
    fn auth_provider(&self) -> Option<Arc<dyn AuthProvider>>;
}

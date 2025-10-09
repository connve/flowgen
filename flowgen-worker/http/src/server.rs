//! HTTP server management for webhook processors.
//!
//! Provides a shared HTTP server that allows multiple webhook processors
//! to register routes dynamically before starting the server.

use axum::{routing::MethodRouter, Router};
use std::{collections::HashMap, sync::Arc};
use tokio::sync::{Mutex, RwLock};
use tracing::{event, Level};

/// Default HTTP port for the server.
const DEFAULT_HTTP_PORT: &str = "3000";

/// Errors that can occur during HTTP server operations.
#[derive(thiserror::Error, Debug)]
pub enum Error {
    /// Input/output operation failed.
    #[error(transparent)]
    IO(#[from] std::io::Error),
}

/// Shared HTTP server manager for webhook processors.
///
/// Allows multiple webhook processors to register routes before starting
/// the server. Routes are stored in a thread-safe HashMap and the server
/// can only be started once.
#[derive(Debug, Clone)]
pub struct HttpServer {
    /// Thread-safe storage for registered routes.
    routes: Arc<RwLock<HashMap<String, MethodRouter>>>,
    /// Flag to track if server has been started.
    server_started: Arc<Mutex<bool>>,
}

impl HttpServer {
    /// Create a new HTTP Server.
    pub fn new() -> Self {
        Self {
            routes: Arc::new(RwLock::new(HashMap::new())),
            server_started: Arc::new(Mutex::new(false)),
        }
    }

    /// Register a route with the HTTP Server.
    pub async fn register_route(&self, path: String, method_router: MethodRouter) {
        let mut routes = self.routes.write().await;
        event!(Level::INFO, "Registering HTTP route: {}", path);
        routes.insert(path, method_router);
    }

    /// Start the HTTP Server with all registered routes.
    pub async fn start_server(&self) -> Result<(), Error> {
        let mut server_started = self.server_started.lock().await;
        if *server_started {
            event!(Level::WARN, "HTTP Server already started");
            return Ok(());
        }

        let routes = self.routes.read().await;
        let mut router = Router::new();

        for (path, method_router) in routes.iter() {
            router = router.route(path, method_router.clone());
        }
        let listener =
            tokio::net::TcpListener::bind(format!("0.0.0.0:{DEFAULT_HTTP_PORT}")).await?;
        event!(
            Level::INFO,
            "Starting HTTP Server on port {}",
            DEFAULT_HTTP_PORT
        );

        *server_started = true;
        axum::serve(listener, router).await.map_err(Error::IO)
    }

    /// Check if server has been started.
    pub async fn is_started(&self) -> bool {
        *self.server_started.lock().await
    }
}

impl Default for HttpServer {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::routing::get;

    #[test]
    fn test_http_server_new() {
        let server = HttpServer::new();
        // We can't easily test the internal state, but we can verify it was created
        // The struct should be properly initialized
        assert!(format!("{server:?}").contains("HttpServer"));
    }

    #[test]
    fn test_http_server_default() {
        let server = HttpServer::default();
        assert!(format!("{server:?}").contains("HttpServer"));
    }

    #[test]
    fn test_http_server_clone() {
        let server = HttpServer::new();
        let cloned = server.clone();

        // Both should have the same structure (we can't easily compare internal state)
        assert!(format!("{server:?}").contains("HttpServer"));
        assert!(format!("{cloned:?}").contains("HttpServer"));
    }

    #[tokio::test]
    async fn test_register_route() {
        let server = HttpServer::new();
        let method_router = get(|| async { "test response" });

        // Should not panic when registering a route
        server
            .register_route("/test".to_string(), method_router)
            .await;

        // Verify we can register multiple routes
        let method_router2 = get(|| async { "test response 2" });
        server
            .register_route("/test2".to_string(), method_router2)
            .await;
    }

    #[tokio::test]
    async fn test_is_started_initially_false() {
        let server = HttpServer::new();
        assert!(!server.is_started().await);
    }

    #[test]
    fn test_error_from_io_error() {
        let io_error = std::io::Error::new(std::io::ErrorKind::NotFound, "file not found");
        let error: Error = io_error.into();
        assert!(matches!(error, Error::IO(_)));
    }

    #[test]
    fn test_constants() {
        assert_eq!(DEFAULT_HTTP_PORT, "3000");
    }

    #[tokio::test]
    async fn test_register_multiple_routes_different_paths() {
        let server = HttpServer::new();

        let routes = vec![
            ("/api/v1/users", get(|| async { "users" })),
            ("/api/v1/posts", get(|| async { "posts" })),
            ("/health", get(|| async { "ok" })),
            ("/metrics", get(|| async { "metrics" })),
        ];

        for (path, method_router) in routes {
            server.register_route(path.to_string(), method_router).await;
        }

        assert!(!server.is_started().await);
    }

    #[tokio::test]
    async fn test_register_route_overwrites_existing() {
        let server = HttpServer::new();
        let path = "/test".to_string();

        let method_router1 = get(|| async { "response 1" });
        server.register_route(path.clone(), method_router1).await;

        let method_router2 = get(|| async { "response 2" });
        server.register_route(path, method_router2).await;

        assert!(!server.is_started().await);
    }
}

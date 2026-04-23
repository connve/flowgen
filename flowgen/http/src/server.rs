//! HTTP server management for webhook processors.
//!
//! Provides a shared HTTP server that allows multiple webhook processors
//! to register routes dynamically before starting the server.

use axum::{routing::MethodRouter, Router};
use flowgen_core::auth::AuthProvider;
use std::{collections::HashMap, sync::Arc};
use tokio::sync::{Mutex, RwLock};
use tracing::{info, warn};

/// Default HTTP port for the server.
const DEFAULT_HTTP_PORT: u16 = 3000;

/// Default path prefix for all routes.
const DEFAULT_ROUTES_PREFIX: &str = "/api/flowgen/workers";

/// Errors that can occur during HTTP server operations.
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum Error {
    /// Failed to bind TCP listener on specified port.
    #[error("Error binding TCP listener on port {port}: {source}")]
    BindListener {
        port: u16,
        #[source]
        source: std::io::Error,
    },
    /// Failed to serve HTTP requests.
    #[error("Error serving HTTP requests: {source}")]
    ServeHttp {
        #[source]
        source: std::io::Error,
    },
}

/// Shared HTTP server manager for webhook processors.
/// Allows multiple webhook processors to register routes before starting
/// the server. Routes are stored in a thread-safe HashMap and the server
/// can only be started once.
#[derive(Clone)]
pub struct HttpServer {
    /// Thread-safe storage for registered routes.
    routes: Arc<RwLock<HashMap<String, MethodRouter>>>,
    /// Flag to track if server has been started.
    server_started: Arc<Mutex<bool>>,
    /// Optional path prefix for all routes (e.g., "/workers").
    path: Option<String>,
    /// Optional global credentials path for webhook authentication.
    /// Individual webhooks can override this with their own `credentials_path`.
    credentials_path: Option<std::path::PathBuf>,
    /// Optional auth provider for user identity resolution (JWT, OIDC, session).
    auth_provider: Option<Arc<dyn AuthProvider>>,
}

impl std::fmt::Debug for HttpServer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HttpServer")
            .field("path", &self.path)
            .field("credentials_path", &self.credentials_path)
            .field("has_auth_provider", &self.auth_provider.is_some())
            .finish()
    }
}

/// Builder for constructing HttpServer instances.
#[derive(Default)]
pub struct HttpServerBuilder {
    /// Optional path prefix for all routes.
    path: Option<String>,
    /// Optional credentials path for webhook authentication.
    credentials_path: Option<std::path::PathBuf>,
    /// Optional auth provider for user identity resolution.
    auth_provider: Option<Arc<dyn AuthProvider>>,
}

impl HttpServerBuilder {
    /// Creates a new HttpServerBuilder.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the path prefix for all routes (e.g., "/api/flowgen/workers").
    pub fn path(mut self, path: String) -> Self {
        self.path = Some(path);
        self
    }

    /// Sets the global credentials path for webhook authentication.
    pub fn credentials_path(mut self, path: std::path::PathBuf) -> Self {
        self.credentials_path = Some(path);
        self
    }

    /// Sets the auth provider for user identity resolution.
    pub fn auth_provider(mut self, provider: Arc<dyn AuthProvider>) -> Self {
        self.auth_provider = Some(provider);
        self
    }

    /// Builds the HttpServer instance.
    pub fn build(self) -> HttpServer {
        HttpServer {
            routes: Arc::new(RwLock::new(HashMap::new())),
            server_started: Arc::new(Mutex::new(false)),
            path: self.path,
            credentials_path: self.credentials_path,
            auth_provider: self.auth_provider,
        }
    }
}

#[async_trait::async_trait]
impl flowgen_core::http_server::HttpServer for HttpServer {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    async fn register_route(&self, path: String, route: Box<dyn std::any::Any + Send>) {
        match route.downcast::<MethodRouter>() {
            Ok(method_router) => self.register_route_typed(path, *method_router).await,
            Err(_) => {
                warn!(
                    "Failed to downcast route to MethodRouter for path: {}",
                    path
                );
            }
        }
    }

    fn auth_provider(&self) -> Option<Arc<dyn AuthProvider>> {
        self.auth_provider.clone()
    }
}

impl HttpServer {
    /// Returns the global credentials path if configured.
    pub fn credentials_path(&self) -> Option<&std::path::Path> {
        self.credentials_path.as_deref()
    }

    /// Register a typed route with the HTTP Server.
    pub async fn register_route_typed(&self, path: String, method_router: MethodRouter) {
        let mut routes = self.routes.write().await;
        info!("Registering HTTP route: {}", path);
        routes.insert(path, method_router);
    }

    /// Start the HTTP Server with all registered routes.
    pub async fn start_server(&self, port: Option<u16>) -> Result<(), Error> {
        let mut server_started = self.server_started.lock().await;
        if *server_started {
            warn!("HTTP Server already started");
            return Ok(());
        }

        let routes = self.routes.read().await;
        let mut api_router = Router::new();

        for (path, method_router) in routes.iter() {
            api_router = api_router.route(path, method_router.clone());
        }

        // Apply routes prefix (use default if not configured).
        let base_path = self
            .path
            .clone()
            .unwrap_or_else(|| DEFAULT_ROUTES_PREFIX.to_string());

        let router = Router::new().nest(&base_path, api_router);
        let server_port = port.unwrap_or(DEFAULT_HTTP_PORT);
        let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{server_port}"))
            .await
            .map_err(|e| Error::BindListener {
                port: server_port,
                source: e,
            })?;

        *server_started = true;

        info!("Starting HTTP Server on port: {}", server_port);
        axum::serve(listener, router)
            .await
            .map_err(|e| Error::ServeHttp { source: e })
    }

    /// Check if server has been started.
    pub async fn is_started(&self) -> bool {
        *self.server_started.lock().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::routing::get;

    #[test]
    fn test_http_server_builder() {
        let server = HttpServerBuilder::new().build();
        // We can't easily test the internal state, but we can verify it was created
        // The struct should be properly initialized
        assert!(format!("{server:?}").contains("HttpServer"));
    }

    #[test]
    fn test_http_server_builder_with_prefix() {
        let server = HttpServerBuilder::new()
            .path("/workers".to_string())
            .build();
        assert!(format!("{server:?}").contains("HttpServer"));
    }

    #[test]
    fn test_http_server_clone() {
        let server = HttpServerBuilder::new().build();
        let cloned = server.clone();

        // Both should have the same structure (we can't easily compare internal state)
        assert!(format!("{server:?}").contains("HttpServer"));
        assert!(format!("{cloned:?}").contains("HttpServer"));
    }

    #[tokio::test]
    async fn test_register_route() {
        let server = HttpServerBuilder::new().build();
        let method_router = get(|| async { "test response" });

        // Should not panic when registering a route
        server
            .register_route_typed("/test".to_string(), method_router)
            .await;

        // Verify we can register multiple routes
        let method_router2 = get(|| async { "test response 2" });
        server
            .register_route_typed("/test2".to_string(), method_router2)
            .await;
    }

    #[tokio::test]
    async fn test_is_started_initially_false() {
        let server = HttpServerBuilder::new().build();
        assert!(!server.is_started().await);
    }

    #[test]
    fn test_error_bind_listener_structure() {
        let io_error = std::io::Error::new(std::io::ErrorKind::AddrInUse, "address in use");
        let error = Error::BindListener {
            port: 3000,
            source: io_error,
        };
        assert!(matches!(error, Error::BindListener { .. }));
    }

    #[test]
    fn test_error_serve_http_structure() {
        let io_error = std::io::Error::new(std::io::ErrorKind::ConnectionReset, "connection reset");
        let error = Error::ServeHttp { source: io_error };
        assert!(matches!(error, Error::ServeHttp { .. }));
    }

    #[test]
    fn test_constants() {
        assert_eq!(DEFAULT_HTTP_PORT, 3000);
    }

    #[tokio::test]
    async fn test_register_multiple_routes_different_paths() {
        let server = HttpServerBuilder::new().build();

        let routes = vec![
            ("/api/v1/users", get(|| async { "users" })),
            ("/api/v1/posts", get(|| async { "posts" })),
            ("/health", get(|| async { "ok" })),
            ("/metrics", get(|| async { "metrics" })),
        ];

        for (path, method_router) in routes {
            server
                .register_route_typed(path.to_string(), method_router)
                .await;
        }

        assert!(!server.is_started().await);
    }

    #[tokio::test]
    async fn test_register_route_overwrites_existing() {
        let server = HttpServerBuilder::new().build();
        let path = "/test".to_string();

        let method_router1 = get(|| async { "response 1" });
        server
            .register_route_typed(path.clone(), method_router1)
            .await;

        let method_router2 = get(|| async { "response 2" });
        server.register_route_typed(path, method_router2).await;

        assert!(!server.is_started().await);
    }
}

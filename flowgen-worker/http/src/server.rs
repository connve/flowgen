use axum::{routing::MethodRouter, Router};
use std::{collections::HashMap, sync::Arc};
use tokio::sync::{Mutex, RwLock};
use tracing::{event, Level};

const DEFAULT_HTTP_PORT: &str = "3000";

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error(transparent)]
    IO(#[from] std::io::Error),
}

/// Shared HTTP server manager that allows multiple webhook processors to register routes
#[derive(Debug, Clone)]
pub struct HttpServerManager {
    routes: Arc<RwLock<HashMap<String, MethodRouter>>>,
    server_started: Arc<Mutex<bool>>,
}

impl HttpServerManager {
    /// Create a new HTTP server manager
    pub fn new() -> Self {
        Self {
            routes: Arc::new(RwLock::new(HashMap::new())),
            server_started: Arc::new(Mutex::new(false)),
        }
    }

    /// Register a route with the server manager
    pub async fn register_route(&self, path: String, method_router: MethodRouter) {
        let mut routes = self.routes.write().await;
        event!(Level::INFO, "Registering HTTP route: {}", path);
        routes.insert(path, method_router);
    }

    /// Start the HTTP server with all registered routes
    pub async fn start_server(&self) -> Result<(), Error> {
        let mut server_started = self.server_started.lock().await;
        if *server_started {
            event!(Level::WARN, "HTTP server already started");
            return Ok(());
        }

        let routes = self.routes.read().await;
        let mut router = Router::new();

        // Add all registered routes to the router
        for (path, method_router) in routes.iter() {
            event!(Level::INFO, "Adding route to server: {}", path);
            router = router.route(path, method_router.clone());
        }

        // Start the server
        let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{}", DEFAULT_HTTP_PORT)).await?;
        event!(Level::INFO, "Starting HTTP server on port {}", DEFAULT_HTTP_PORT);

        *server_started = true;
        // This will block until the server shuts down
        axum::serve(listener, router).await.map_err(Error::IO)
    }

    /// Check if server has been started
    pub async fn is_started(&self) -> bool {
        *self.server_started.lock().await
    }
}

impl Default for HttpServerManager {
    fn default() -> Self {
        Self::new()
    }
}
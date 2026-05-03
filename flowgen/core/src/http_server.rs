//! Generic HTTP server with role-specific dispatchers.
//!
//! `HttpServer<D>` is a single, reusable axum listener whose routing behaviour
//! is parameterised by a [`Dispatcher`] implementation. The same server is
//! instantiated multiple times in the worker — once per HTTP-facing role
//! (webhooks, MCP, AI gateway) — each on its own port and path prefix. One
//! infrastructure type, many roles.
//!
//! Each [`Dispatcher`] implementation owns:
//!
//! - a [`Dispatcher::Registration`] data type (carried in the dispatch table,
//!   inserted by the role's processor at flow start time);
//! - a [`Dispatcher::Extras`] associated state for role-specific shared
//!   resources (e.g. MCP's response registry); use `()` when not needed;
//! - a [`Dispatcher::build_router`] method that wires the role's URL layout
//!   (catch-all, JSON-RPC, OpenAI, etc.) to handler functions which look the
//!   request up in the dispatch table.
//!
//! Hot-reload is `DashMap::insert` / `retain` on the table; the axum `Router`
//! is built once at startup and never rebuilt. Bulk deregister-by-flow is
//! enabled by the [`HasFlowName`] supertrait on `Registration`.

use axum::Router;
use dashmap::DashMap;
use std::marker::PhantomData;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{info, warn};

use crate::auth::AuthProvider;

/// Errors produced by the HTTP server lifecycle (binding and serving).
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum Error {
    /// Failed to bind the TCP listener.
    #[error("Error binding TCP listener on port {port}: {source}")]
    BindListener {
        port: u16,
        #[source]
        source: std::io::Error,
    },
    /// axum::serve failed.
    #[error("Error serving HTTP requests: {source}")]
    ServeHttp {
        #[source]
        source: std::io::Error,
    },
}

/// Marker trait for registrations that know which flow they belong to.
///
/// Implemented by every dispatcher's `Registration` type so the server can
/// bulk-remove every entry owned by a flow when that flow is stopped or
/// hot-reloaded.
pub trait HasFlowName {
    /// Name of the flow that owns this registration.
    fn flow_name(&self) -> &str;
}

/// Common axum state shared with every dispatcher.
///
/// Holds a clone of the dispatch table, the server's auth provider /
/// credentials path, and any role-specific extras the dispatcher needs (e.g.
/// MCP's response registry). `extras` defaults to `()` for roles that do not
/// need anything beyond the table.
pub struct DispatchState<R, E = ()> {
    /// Dispatch table keyed by whatever the dispatcher uses (endpoint path,
    /// gateway name, JSON-RPC tool name, etc.).
    pub table: Arc<DashMap<String, R>>,
    /// Optional global credentials path resolved from server config.
    pub credentials_path: Option<std::path::PathBuf>,
    /// Optional auth provider for user identity resolution.
    pub auth_provider: Option<Arc<dyn AuthProvider>>,
    /// Path prefix the server is mounted under.
    pub path: String,
    /// Role-specific extras the dispatcher needs in addition to the common fields.
    pub extras: E,
}

impl<R, E: Clone> Clone for DispatchState<R, E> {
    fn clone(&self) -> Self {
        Self {
            table: Arc::clone(&self.table),
            credentials_path: self.credentials_path.clone(),
            auth_provider: self.auth_provider.clone(),
            path: self.path.clone(),
            extras: self.extras.clone(),
        }
    }
}

/// Per-role behaviour: how to wire HTTP routes to dispatch-table lookups.
///
/// Implementors are typically zero-sized marker types (e.g.
/// `WebhookDispatcher`) used as the type parameter of [`HttpServer<D>`].
pub trait Dispatcher: Send + Sync + 'static {
    /// Per-role registration data inserted into the dispatch table.
    type Registration: Clone + HasFlowName + Send + Sync + 'static;
    /// Role-specific shared state passed via `DispatchState::extras`.
    /// Use `()` when no extras are needed.
    type Extras: Clone + Send + Sync + 'static;

    /// Builds the axum `Router` for this role.
    ///
    /// Called once at `start_server` time. The returned router holds `state`
    /// so handlers can look entries up in the dispatch table.
    fn build_router(state: DispatchState<Self::Registration, Self::Extras>) -> Router;
}

/// Generic HTTP server.
///
/// Owns a dispatch table keyed by the dispatcher's `Registration::Key` and
/// the lifecycle of an axum listener. The actual URL layout and per-request
/// behaviour are delegated to `D::build_router`.
pub struct HttpServer<D: Dispatcher> {
    /// Path prefix under which the server's routes are exposed.
    path: String,
    /// Dispatch table shared with the dispatcher.
    table: Arc<DashMap<String, D::Registration>>,
    /// Role-specific extras passed to the dispatcher.
    extras: D::Extras,
    /// Guards `start_server` so it cannot be called twice.
    server_started: Arc<Mutex<bool>>,
    /// Optional global credentials path for endpoint authentication.
    credentials_path: Option<std::path::PathBuf>,
    /// Optional auth provider for user identity resolution.
    auth_provider: Option<Arc<dyn AuthProvider>>,
    /// Phantom data so `D` participates in the type without being stored.
    _dispatcher: PhantomData<D>,
}

impl<D: Dispatcher> Clone for HttpServer<D> {
    fn clone(&self) -> Self {
        Self {
            path: self.path.clone(),
            table: Arc::clone(&self.table),
            extras: self.extras.clone(),
            server_started: Arc::clone(&self.server_started),
            credentials_path: self.credentials_path.clone(),
            auth_provider: self.auth_provider.clone(),
            _dispatcher: PhantomData,
        }
    }
}

impl<D: Dispatcher> std::fmt::Debug for HttpServer<D> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HttpServer")
            .field("path", &self.path)
            .field("entry_count", &self.table.len())
            .field("credentials_path", &self.credentials_path)
            .field("has_auth_provider", &self.auth_provider.is_some())
            .finish()
    }
}

impl<D: Dispatcher<Extras = ()>> HttpServer<D> {
    /// Creates a new server with `()` extras. Convenience constructor for
    /// dispatchers that do not need role-specific shared state.
    pub fn new(path: String) -> Self {
        Self::new_with_extras(path, ())
    }
}

impl<D: Dispatcher> HttpServer<D> {
    /// Creates a new server with the given path prefix and role-specific extras.
    pub fn new_with_extras(path: String, extras: D::Extras) -> Self {
        Self {
            path,
            table: Arc::new(DashMap::new()),
            extras,
            server_started: Arc::new(Mutex::new(false)),
            credentials_path: None,
            auth_provider: None,
            _dispatcher: PhantomData,
        }
    }

    /// Sets the global credentials path used by dispatchers as a fallback for
    /// per-registration credential paths.
    pub fn with_credentials_path(mut self, path: Option<std::path::PathBuf>) -> Self {
        self.credentials_path = path;
        self
    }

    /// Sets the auth provider used by dispatchers for user identity resolution.
    pub fn with_auth_provider(mut self, provider: Option<Arc<dyn AuthProvider>>) -> Self {
        self.auth_provider = provider;
        self
    }

    /// Returns the configured global credentials path, if any.
    pub fn credentials_path(&self) -> Option<&std::path::Path> {
        self.credentials_path.as_deref()
    }

    /// Returns the configured auth provider, if any.
    pub fn auth_provider(&self) -> Option<Arc<dyn AuthProvider>> {
        self.auth_provider.clone()
    }

    /// Returns the configured path prefix.
    pub fn path(&self) -> &str {
        &self.path
    }

    /// Returns a reference to the role-specific extras.
    pub fn extras(&self) -> &D::Extras {
        &self.extras
    }

    /// Inserts a registration under `key`. Replaces any prior entry; callers
    /// should use [`Self::deregister_flow`] first when reloading a flow.
    pub fn register(&self, key: String, registration: D::Registration) {
        info!(
            key = %key,
            flow = %registration.flow_name(),
            "Registering HTTP server entry"
        );
        self.table.insert(key, registration);
    }

    /// Inserts a registration only if `key` is not already present.
    ///
    /// Returns the rejected registration unchanged when the key is taken so
    /// the caller can surface a collision error. Use this when distinct
    /// flows must not be allowed to claim the same key (e.g. MCP tool names
    /// or webhook endpoints), preferring a hard error to silent overwrite.
    pub fn try_register(
        &self,
        key: String,
        registration: D::Registration,
    ) -> Result<(), D::Registration> {
        if self.table.contains_key(&key) {
            return Err(registration);
        }
        info!(
            key = %key,
            flow = %registration.flow_name(),
            "Registering HTTP server entry"
        );
        self.table.insert(key, registration);
        Ok(())
    }

    /// Removes every registration owned by the named flow.
    pub fn deregister_flow(&self, flow_name: &str) {
        let before = self.table.len();
        self.table.retain(|_, reg| reg.flow_name() != flow_name);
        let removed = before.saturating_sub(self.table.len());
        if removed > 0 {
            info!(
                flow = %flow_name,
                removed,
                "Deregistered HTTP server entries for flow"
            );
        }
    }

    /// Starts the server. The router is built once via the dispatcher and the
    /// listener serves requests until the future is dropped or returns.
    pub async fn start_server(&self, port: u16) -> Result<(), Error> {
        let mut started = self.server_started.lock().await;
        if *started {
            warn!(port, "HTTP server already started");
            return Ok(());
        }
        *started = true;
        drop(started);

        let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{port}"))
            .await
            .map_err(|source| Error::BindListener { port, source })?;

        let state = DispatchState {
            table: Arc::clone(&self.table),
            credentials_path: self.credentials_path.clone(),
            auth_provider: self.auth_provider.clone(),
            path: self.path.clone(),
            extras: self.extras.clone(),
        };
        let router = D::build_router(state);

        info!(port, path = %self.path, "Starting HTTP server");

        axum::serve(listener, router)
            .await
            .map_err(|source| Error::ServeHttp { source })
    }

    /// Returns true if `start_server` has been called.
    pub async fn is_started(&self) -> bool {
        *self.server_started.lock().await
    }
}

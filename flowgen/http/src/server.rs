//! Webhook role for the generic HTTP server.
//!
//! Defines the [`WebhookDispatcher`] and [`WebhookRegistration`] used to wire
//! the worker's webhook traffic onto a `flowgen_core::http_server::HttpServer`.
//! The server lifecycle, dispatch table, and hot-reload semantics live in
//! `flowgen_core::http_server`; this module only owns the webhook-specific
//! URL layout (a single catch-all under `<path>/{*endpoint}`) and the data
//! shape carried per registered webhook.
//!
//! The actual per-request work — auth, body parsing, event creation,
//! completion wait, response formatting — lives in `crate::webhook::dispatch`
//! so the dispatcher logic stays colocated with the webhook config.

use axum::{
    body::Body,
    extract::{Path, State},
    http::{HeaderMap, Method, StatusCode},
    response::{IntoResponse, Response},
    routing::any,
    Router,
};
use flowgen_core::auth::AuthProvider;
use flowgen_core::http_server::{DispatchState, Dispatcher, HasFlowName, HttpServer};
use std::sync::Arc;

/// Default port for the webhook HTTP server.
pub const DEFAULT_WEBHOOK_PORT: u16 = 3000;

/// Default path prefix for webhook routes.
pub const DEFAULT_WEBHOOK_PATH: &str = "/api/flowgen/workers";

/// Convenience type alias for the webhook server.
pub type WebhookServer = HttpServer<WebhookDispatcher>;

/// Dispatcher for webhook traffic.
///
/// Wires a single catch-all route `<path>/{*endpoint}` and dispatches each
/// request to a [`WebhookRegistration`] keyed by the resolved endpoint path.
pub struct WebhookDispatcher;

impl Dispatcher for WebhookDispatcher {
    type Registration = WebhookRegistration;
    type Extras = ();

    fn build_router(state: DispatchState<Self::Registration, Self::Extras>) -> Router {
        let prefix = state.path.trim_end_matches('/').to_string();
        let route = format!("{prefix}/{{*endpoint}}");
        Router::new()
            .route(&route, any(dispatch_webhook))
            .with_state(state)
    }
}

/// Handler for the catch-all webhook route. Looks the requested endpoint up
/// in the dispatch table, validates the HTTP method against the registered
/// webhook's configured method, and forwards to the per-webhook dispatcher
/// in `crate::webhook`.
async fn dispatch_webhook(
    State(state): State<DispatchState<WebhookRegistration>>,
    Path(endpoint): Path<String>,
    method: Method,
    headers: HeaderMap,
    body: Body,
) -> Response<Body> {
    // Endpoint paths are stored with a leading slash; the catch-all extractor
    // strips the prefix so we re-add it for the lookup.
    let lookup_key = format!("/{endpoint}");
    let registration = match state.table.get(&lookup_key) {
        Some(entry) => entry.clone(),
        None => return (StatusCode::NOT_FOUND, "Unknown webhook endpoint").into_response(),
    };

    if !methods_match(&method, &registration.config.method) {
        return (
            StatusCode::METHOD_NOT_ALLOWED,
            "Method not allowed for this webhook",
        )
            .into_response();
    }

    crate::webhook::dispatch(&registration, headers, body).await
}

/// Compares an axum HTTP method against the config-declared method.
fn methods_match(req_method: &Method, configured: &crate::config::Method) -> bool {
    match configured {
        crate::config::Method::Get => req_method == Method::GET,
        crate::config::Method::Post => req_method == Method::POST,
        crate::config::Method::Put => req_method == Method::PUT,
        crate::config::Method::Delete => req_method == Method::DELETE,
        crate::config::Method::Patch => req_method == Method::PATCH,
        crate::config::Method::Head => req_method == Method::HEAD,
    }
}

/// Dispatch-table entry describing one registered webhook endpoint.
///
/// Stored in the webhook server's dispatch table keyed by endpoint path.
/// `flow_name` lets the server bulk-deregister every webhook owned by a flow
/// when the flow is stopped or hot-reloaded.
#[derive(Clone)]
pub struct WebhookRegistration {
    /// Name of the flow that registered this webhook.
    pub flow_name: String,
    /// Full processor configuration. The dispatcher reads `method`,
    /// `max_body_bytes`, `headers`, `auth`, `ack_timeout`, `stream`, and
    /// `name` from here.
    pub config: Arc<crate::config::Processor>,
    /// Optional bearer-token credentials loaded from `config.credentials_path`.
    pub credentials: Option<flowgen_core::credentials::HttpCredentials>,
    /// Optional auth provider for user identity resolution (JWT, OIDC, session).
    pub auth_provider: Option<Arc<dyn AuthProvider>>,
    /// Channel to send the inbound webhook event into the flow pipeline.
    pub tx: tokio::sync::mpsc::Sender<flowgen_core::event::Event>,
    /// Task identifier used when constructing pipeline events.
    pub task_id: usize,
    /// Task type label used when constructing pipeline events.
    pub task_type: &'static str,
    /// Shared response registry for awaiting flow completion or streaming chunks back.
    pub response_registry: Arc<flowgen_core::registry::ResponseRegistry>,
    /// Number of leaf tasks reachable from this webhook source.
    pub leaf_count: usize,
    /// Cancellation token from the owning flow's task tenure.
    pub cancellation_token: tokio_util::sync::CancellationToken,
}

impl HasFlowName for WebhookRegistration {
    fn flow_name(&self) -> &str {
        &self.flow_name
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_constants() {
        assert_eq!(DEFAULT_WEBHOOK_PORT, 3000);
        assert_eq!(DEFAULT_WEBHOOK_PATH, "/api/flowgen/workers");
    }

    #[test]
    fn test_methods_match() {
        assert!(methods_match(&Method::POST, &crate::config::Method::Post));
        assert!(methods_match(&Method::GET, &crate::config::Method::Get));
        assert!(!methods_match(&Method::GET, &crate::config::Method::Post));
        assert!(!methods_match(
            &Method::DELETE,
            &crate::config::Method::Patch
        ));
    }
}

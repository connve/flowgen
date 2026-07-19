//! Dedicated health-check listener for Kubernetes probes.
//!
//! Runs on its own port independent of the API listeners so probes keep
//! working regardless of which API surfaces (http_server / mcp_server /
//! ai_gateway / web) are enabled. Exposes:
//!
//! - `GET /livez` — liveness. 200 as long as the axum task is scheduling.
//! - `GET /healthz` — alias of `/livez` for pre-1.16 k8s convention.
//! - `GET /readyz` — readiness. 200 iff the caller-supplied predicate returns
//!   true. flowgen wires this to "at least one flow is registered".
//!
//! Liveness fails only if the whole process is wedged (kubelet restarts the
//! pod). Readiness fails during startup or when the app cannot do useful
//! work yet (kubelet stops sending traffic without restarting).

use axum::{extract::State, http::StatusCode, routing::get, Router};
use std::sync::Arc;
use tracing::info;

/// Errors produced by the health server lifecycle.
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
    #[error("Error serving health requests: {source}")]
    ServeHttp {
        #[source]
        source: std::io::Error,
    },
}

/// Predicate answering "is the app ready to serve traffic?".
///
/// Called on every `/readyz` request, so implementations must be cheap
/// and lock-friendly. Boxed so `App` can hand in a closure capturing
/// `Arc<RwLock<FlowRegistry>>` without pulling app types into core.
pub type ReadinessCheck = Arc<dyn Fn() -> bool + Send + Sync>;

/// Builds the health router. Exposed separately from `start_health_server`
/// so integration tests can mount it on an ephemeral port without racing
/// on the fixed config port.
pub fn router(readiness: ReadinessCheck) -> Router {
    Router::new()
        .route("/livez", get(livez))
        .route("/healthz", get(livez))
        .route("/readyz", get(readyz))
        .with_state(readiness)
}

/// Starts the health server on `port`. Blocks until the server exits.
pub async fn start_health_server(port: u16, readiness: ReadinessCheck) -> Result<(), Error> {
    let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{port}"))
        .await
        .map_err(|source| Error::BindListener { port, source })?;

    info!(port, "Starting health server");

    axum::serve(listener, router(readiness))
        .await
        .map_err(|source| Error::ServeHttp { source })
}

async fn livez() -> StatusCode {
    StatusCode::OK
}

async fn readyz(State(readiness): State<ReadinessCheck>) -> StatusCode {
    match readiness() {
        true => StatusCode::OK,
        false => StatusCode::SERVICE_UNAVAILABLE,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};

    #[tokio::test]
    async fn livez_always_returns_ok() {
        assert_eq!(livez().await, StatusCode::OK);
    }

    #[tokio::test]
    async fn readyz_returns_ok_when_predicate_true() {
        let readiness: ReadinessCheck = Arc::new(|| true);
        assert_eq!(readyz(State(readiness)).await, StatusCode::OK);
    }

    #[tokio::test]
    async fn readyz_returns_unavailable_when_predicate_false() {
        let readiness: ReadinessCheck = Arc::new(|| false);
        assert_eq!(
            readyz(State(readiness)).await,
            StatusCode::SERVICE_UNAVAILABLE
        );
    }

    #[tokio::test]
    async fn readyz_reflects_predicate_state_changes() {
        let ready = Arc::new(AtomicBool::new(false));
        let ready_clone = Arc::clone(&ready);
        let readiness: ReadinessCheck = Arc::new(move || ready_clone.load(Ordering::Relaxed));

        assert_eq!(
            readyz(State(Arc::clone(&readiness))).await,
            StatusCode::SERVICE_UNAVAILABLE
        );
        ready.store(true, Ordering::Relaxed);
        assert_eq!(readyz(State(readiness)).await, StatusCode::OK);
    }
}

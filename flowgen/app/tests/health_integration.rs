//! Integration tests for the dedicated k8s health listener.
//!
//! Spawns the real health server on an ephemeral port and hits it with
//! reqwest, so router wiring, port binding, and readiness-predicate
//! evaluation all get exercised end-to-end. No Docker or external
//! dependencies required — runs on the default `cargo test`.

use flowgen_core::health::{start_health_server, ReadinessCheck};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

/// Reserves an ephemeral port by binding, then dropping the listener.
///
/// There is a small race between the drop and the health server binding
/// the same port, but the kernel keeps ports out of the reuse pool briefly
/// so back-to-back binds inside one test are safe in practice.
async fn reserve_port() -> u16 {
    let probe = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    probe.local_addr().unwrap().port()
}

async fn wait_for_response(url: &str) -> reqwest::Response {
    for _ in 0..50 {
        if let Ok(resp) = reqwest::get(url).await {
            return resp;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("Health server never came up at {url}");
}

#[tokio::test]
async fn livez_returns_ok_when_server_started() {
    let port = reserve_port().await;
    let readiness: ReadinessCheck = Arc::new(|| true);
    let handle = tokio::spawn(async move {
        start_health_server(port, readiness).await.unwrap();
    });

    let response = wait_for_response(&format!("http://127.0.0.1:{port}/livez")).await;
    assert_eq!(response.status(), reqwest::StatusCode::OK);

    handle.abort();
}

#[tokio::test]
async fn healthz_alias_returns_ok() {
    let port = reserve_port().await;
    let readiness: ReadinessCheck = Arc::new(|| true);
    let handle = tokio::spawn(async move {
        start_health_server(port, readiness).await.unwrap();
    });

    let response = wait_for_response(&format!("http://127.0.0.1:{port}/healthz")).await;
    assert_eq!(response.status(), reqwest::StatusCode::OK);

    handle.abort();
}

#[tokio::test]
async fn readyz_returns_unavailable_before_any_flow_registered() {
    let port = reserve_port().await;
    let readiness: ReadinessCheck = Arc::new(|| false);
    let handle = tokio::spawn(async move {
        start_health_server(port, readiness).await.unwrap();
    });

    let response = wait_for_response(&format!("http://127.0.0.1:{port}/readyz")).await;
    assert_eq!(response.status(), reqwest::StatusCode::SERVICE_UNAVAILABLE);

    handle.abort();
}

#[tokio::test]
async fn readyz_flips_to_ok_once_predicate_becomes_true() {
    let port = reserve_port().await;
    let ready = Arc::new(AtomicBool::new(false));
    let ready_for_check = Arc::clone(&ready);
    let readiness: ReadinessCheck = Arc::new(move || ready_for_check.load(Ordering::Relaxed));
    let handle = tokio::spawn(async move {
        start_health_server(port, readiness).await.unwrap();
    });

    let url = format!("http://127.0.0.1:{port}/readyz");
    let response = wait_for_response(&url).await;
    assert_eq!(response.status(), reqwest::StatusCode::SERVICE_UNAVAILABLE);

    ready.store(true, Ordering::Relaxed);
    let response = reqwest::get(&url).await.unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::OK);

    handle.abort();
}

#[tokio::test]
async fn unknown_path_returns_404() {
    let port = reserve_port().await;
    let readiness: ReadinessCheck = Arc::new(|| true);
    let handle = tokio::spawn(async move {
        start_health_server(port, readiness).await.unwrap();
    });

    let response = wait_for_response(&format!("http://127.0.0.1:{port}/something-else")).await;
    assert_eq!(response.status(), reqwest::StatusCode::NOT_FOUND);

    handle.abort();
}

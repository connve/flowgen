//! HTTP processing capabilities for flowgen workers.
//!
//! Provides HTTP request/response processing, webhook handling, and server
//! management for flowgen event processing pipelines.

/// Configuration structures for HTTP processors.
pub mod config;
/// HTTP request processor for outbound calls.
pub mod request;
/// Shared HTTP server management.
pub mod server;
/// HTTP webhook processor for inbound requests.
pub mod webhook;

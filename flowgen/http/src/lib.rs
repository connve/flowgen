//! HTTP processing capabilities for flowgen workers.
//!
//! Provides HTTP request/response processing, endpoint handling, and server
//! management for flowgen event processing pipelines.

/// Configuration structures for HTTP processors.
pub mod config;
/// HTTP endpoint processor for inbound requests.
pub mod endpoint;
/// HTTP request processor for outbound calls.
pub mod request;
/// Shared HTTP server management.
pub mod server;

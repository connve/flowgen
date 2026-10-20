//! Flowgen core library providing fundamental building blocks for event processing workflows.
//!
//! This crate contains shared types, traits, and utilities used across all flowgen workers
//! including event handling, content buffering, caching, and task execution frameworks.

extern crate self as flowgen_core;

/// Authentication providers for user identity resolution (JWT, OIDC, session).
pub mod auth;
/// Content format handling and reader/writer abstractions.
pub mod buffer;
/// Caching interface for persistent storage across workflow executions.
pub mod cache;
/// HTTP client utilities for external service communication.
pub mod client;
/// Shared connection pooling for external service clients.
pub mod client_registry;
/// Configuration structures and serialization support.
pub mod config;
/// Shared credential types for authenticating with external services.
pub mod credentials;
/// Event system with data formats, subject generation, and logging.
pub mod event;
/// Executor for distributed coordination via cache-based leases.
pub mod executor;
/// Dedicated liveness/readiness listener for Kubernetes probes.
pub mod health;
/// Generic HTTP server with role-specific dispatchers (webhook / MCP / AI gateway).
pub mod http_server;
/// Flow identity and its human/key-safe projections.
pub mod identity;
/// Peer discovery and flow distribution via consistent hashing.
pub mod peer;
/// NsJail sandbox for secure script execution.
pub mod nsjail {
    /// Sandbox executor using nsjail for process isolation.
    pub mod sandbox;

    pub use sandbox::{Error, SandboxConfig, SandboxExecutor, SandboxResult};
}
/// Response registry for correlating requests with pipeline results.
pub mod registry;
/// Resource loading system for external assets.
pub mod resource;
/// Retry configuration and utilities for task execution.
pub mod retry;
/// Custom serialization and deserialization utilities.
pub mod serde;
/// Service discovery and connection management.
pub mod service;
/// OpenTelemetry integration for metrics and distributed tracing.
pub mod telemetry;
/// Flow-scoped runtime bookkeeping: activity metrics fed by a custom
/// tracing layer, plus the SSE broadcast channel the web UI reads.
pub mod flow {
    /// Atomic counters + broadcast channel used by the web API.
    pub mod activity;
    /// Tracing layer that populates activity from runtime log events.
    pub mod activity_layer;
}
/// Validation helpers for config-supplied identifiers and paths.
pub mod validate;
/// Task execution framework with runner trait, context, and manager.
pub mod task {
    pub use flowgen_macros::TaskConfig;
    pub use meta::{Task, TaskConfig, TaskError, TaskIo, TaskMeta};

    pub mod buffer;
    /// Task execution context providing metadata and runtime configuration.
    pub mod context;
    pub mod convert;
    pub mod generate;
    /// Task manager for leader election and coordination.
    pub mod manager;
    /// What a task type says about itself.
    pub mod meta;
    /// Base runner trait for all task implementations.
    pub mod runner;
    /// In-process flow calls: `inproc_endpoint` and `inproc_request`.
    pub mod inproc {
        pub mod endpoint;
        /// Registry of callable flows.
        pub mod registry;
        pub mod request;
    }
    pub mod iterate;
    pub mod log;
    pub mod script;
}

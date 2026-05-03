//! Flowgen core library providing fundamental building blocks for event processing workflows.
//!
//! This crate contains shared types, traits, and utilities used across all flowgen workers
//! including event handling, content buffering, caching, and task execution frameworks.

/// Authentication providers for user identity resolution (JWT, OIDC, session).
pub mod auth;
/// Content format handling and reader/writer abstractions.
pub mod buffer;
/// Caching interface for persistent storage across workflow executions.
pub mod cache;
/// HTTP client utilities for external service communication.
pub mod client;
/// Configuration structures and serialization support.
pub mod config;
/// Shared credential types for authenticating with external services.
pub mod credentials;
/// Event system with data formats, subject generation, and logging.
pub mod event;
/// Executor for distributed coordination via cache-based leases.
pub mod executor;
/// Generic HTTP server with role-specific dispatchers (webhook / MCP / AI gateway).
pub mod http_server;
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
/// Validation helpers for config-supplied identifiers and paths.
pub mod validate;
/// Task execution framework with runner trait, context, and manager.
pub mod task {
    /// Task execution context providing metadata and runtime configuration.
    pub mod context;
    /// Task manager for leader election and coordination.
    pub mod manager;
    /// Base runner trait for all task implementations.
    pub mod runner;
    /// Data conversion and transformation processor.
    pub mod convert {
        /// Configuration for convert processor.
        pub mod config;
        /// Processor implementation for data conversion.
        pub mod processor;
    }
    /// Event generation processor that produces data streams.
    pub mod generate {
        /// Configuration for generate processor.
        pub mod config;
        /// Subscriber implementation for event generation.
        pub mod subscriber;
    }
    /// Iterate processor for iterating over JSON arrays.
    pub mod iterate {
        /// Configuration for iterate processor.
        pub mod config;
        /// Processor implementation for array iteration.
        pub mod processor;
    }
    /// Script processor for executing Rhai scripts on events.
    pub mod script {
        /// Configuration for script processor.
        pub mod config;
        /// Processor implementation for script execution.
        pub mod processor;
    }
    /// Log processor for outputting event data to logs.
    pub mod log {
        /// Configuration for log processor.
        pub mod config;
        /// Processor implementation for logging.
        pub mod processor;
    }
    /// Buffer processor for accumulating events into batches.
    pub mod buffer {
        /// Configuration for buffer processor.
        pub mod config;
        /// Processor implementation for event buffering.
        pub mod processor;
    }
}

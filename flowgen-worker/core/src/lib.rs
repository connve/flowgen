//! Flowgen core library providing fundamental building blocks for event processing workflows.
//!
//! This crate contains shared types, traits, and utilities used across all flowgen workers
//! including event handling, content buffering, caching, and task execution frameworks.

/// Content format handling and reader/writer abstractions.
pub mod buffer;
/// Caching interface for persistent storage across workflow executions.
pub mod cache;
/// HTTP client utilities for external service communication.
pub mod client;
/// Configuration structures and serialization support.
pub mod config;
/// Data conversion and transformation processor.
pub mod convert {
    /// Configuration for convert processor.
    pub mod config;
    /// Processor implementation for data conversion.
    pub mod processor;
}
/// Event system with data formats, subject generation, and logging.
pub mod event;
/// Event generation processor that produces data streams.
pub mod generate {
    /// Configuration for generate processor.
    pub mod config;
    /// Subscriber implementation for event generation.
    pub mod subscriber;
}
/// Host coordination and lease management.
pub mod host;
/// Custom serialization and deserialization utilities.
pub mod serde;
/// Service discovery and connection management.
pub mod service;
/// Task execution framework with runner trait, context, and manager.
pub mod task {
    /// Task execution context providing metadata and runtime configuration.
    pub mod context;
    /// Task manager for leader election and coordination.
    pub mod manager;
    /// Base runner trait for all task implementations.
    pub mod runner;
}

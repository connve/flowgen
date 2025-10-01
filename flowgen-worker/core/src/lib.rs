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
/// Event system with data formats, subject generation, and logging.
pub mod event;
/// Custom serialization and deserialization utilities.
pub mod serde;
/// Service discovery and connection management.
pub mod service;
/// Task execution framework with different processor types.
pub mod task {
    /// Task execution context providing metadata and runtime configuration.
    pub mod context;
    /// Base runner trait for all task implementations.
    pub mod runner;
    /// Event generation tasks that produce data streams.
    pub mod generate {
        /// Configuration for generate task types.
        pub mod config;
        /// Subscriber implementation for generate tasks.
        pub mod subscriber;
    }
    /// Data conversion and transformation tasks.
    pub mod convert {
        /// Configuration for convert task types.
        pub mod config;
        /// Processor implementation for convert tasks.
        pub mod processor;
    }
}

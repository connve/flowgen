//! Flowgen application orchestration and configuration.
//!
//! This crate provides the main application logic for flowgen, including
//! flow configuration parsing, task orchestration, and application lifecycle
//! management. It coordinates multiple processing flows and manages shared
//! resources like HTTP servers and caches.

/// Application lifecycle and flow orchestration.
pub mod app;
/// Configuration structures and deserialization.
pub mod config;
/// Flow execution and task management.
pub mod flow;

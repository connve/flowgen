//! Flowgen application orchestration and configuration.
//!
//! This crate provides the main application logic for flowgen, including
//! flow configuration parsing, task orchestration, and application lifecycle
//! management. It coordinates multiple processing flows and manages shared
//! resources like HTTP servers and caches.

/// Application lifecycle and flow orchestration.
pub mod app;
/// Workspace change proposals and their publishing.
pub mod authoring;
/// Configuration structures and deserialization.
pub mod config;
/// Flow execution and task management.
pub mod flow;
/// Browser-facing OIDC login for the web UI.
pub mod login;
/// Hot-reload reconciler for cache-sourced flows.
pub mod reconciler;
/// Validation of flow and resource files without running them.
pub mod validation;
/// Hot-reload watcher for cache-sourced flows.
pub mod watcher;
/// Embedded web interface.
pub mod web;

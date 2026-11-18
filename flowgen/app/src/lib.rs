//! Flowgen application orchestration and configuration.
//!
//! This crate provides the main application logic for flowgen, including
//! flow configuration parsing, task orchestration, and application lifecycle
//! management. It coordinates multiple processing flows and manages shared
//! resources like HTTP servers and caches.

/// Agent replies the server writes into Agents sessions.
pub mod agent_reply;
/// Application lifecycle and flow orchestration.
pub mod app;
/// Workspace change proposals and their publishing.
pub mod authoring;
/// Configuration structures and deserialization.
pub mod config;
/// External systems the loaded flows connect to, with their status.
pub mod connections;
/// Credentials added in the web UI, encrypted with SOPS and committed.
pub mod credentials;
/// The docs pages of this version, embedded for agents.
pub mod docs;
/// Flow execution and task management.
pub mod flow;
/// Hubs: templates to start new flows and resources from, read through hub flows.
pub mod hubs;
/// Running without a config file, on a workspace in the home folder.
pub mod local;
/// `flowgen.lock`: what a workspace installed from hubs, by digest.
pub mod lock;
/// Browser-facing OIDC login for the web UI.
pub mod login;
/// Hot-reload reconciler for cache-sourced flows.
pub mod reconciler;
/// The newest flowgen release, for the web UI's update notice.
pub mod releases;
/// Validation of flow and resource files without running them.
pub mod validation;
/// Hot-reload watcher for cache-sourced flows.
pub mod watcher;
/// Embedded web interface.
pub mod web;

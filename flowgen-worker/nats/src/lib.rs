//! NATS client integration for the flowgen worker system.
//!
//! This crate provides NATS messaging capabilities including JetStream support
//! for reliable message publishing and consuming. It handles authentication,
//! connection management, and provides both publisher and subscriber implementations
//! that integrate with the flowgen event system.

/// Credential caching functionality for NATS authentication.
pub mod cache;

/// NATS client connection and authentication management.
pub mod client;

/// JetStream specific functionality for reliable messaging.
pub mod jetstream {
    /// Configuration structures for JetStream publishers and subscribers.
    pub mod config;
    /// Message conversion utilities for JetStream integration.
    pub mod message;
    /// JetStream publisher implementation for reliable message publishing.
    pub mod publisher;
    /// JetStream subscriber implementation for reliable message consumption.
    pub mod subscriber;
}

//! Salesforce integration for the flowgen worker system.
//!
//! This crate provides Salesforce connectivity and Pub/Sub API support for
//! real-time data streaming. It handles authentication, connection management,
//! and provides both publisher and subscriber implementations that integrate
//! with the flowgen event system.

/// Salesforce Pub/Sub API functionality for real-time messaging.
pub mod pubsub {
    /// Configuration structures for Salesforce Pub/Sub publishers and subscribers.
    pub mod config;
    /// Salesforce Pub/Sub publisher implementation for event publishing.
    pub mod publisher;
    /// Salesforce Pub/Sub subscriber implementation for event consumption.
    pub mod subscriber;
}

pub mod bulkapi {
    /// Configuration structures for Salesforce Bulk API operations.
    pub mod config;
    /// Unified Salesforce Bulk API job operations (create, retrieve).
    pub mod job;
}

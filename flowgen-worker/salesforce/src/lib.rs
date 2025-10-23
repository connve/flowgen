//! Salesforce integration for the flowgen worker system.
//!
//! This crate provides Salesforce connectivity and Pub/Sub API support for
//! real-time data streaming. It handles authentication, connection management,
//! and provides both publisher and subscriber implementations that integrate
//! with the flowgen event system.

/// Salesforce client authentication and connection management.
pub mod client;

/// Salesforce Pub/Sub API functionality for real-time messaging.
pub mod pubsub {
    /// Configuration structures for Salesforce Pub/Sub publishers and subscribers.
    pub mod config;
    /// Pub/Sub context management and gRPC channel handling.
    pub mod context;
    /// Salesforce Pub/Sub publisher implementation for event publishing.
    pub mod publisher;
    /// Salesforce Pub/Sub subscriber implementation for event consumption.
    pub mod subscriber;
}

pub mod query {
    /// Configuration structures for Salesforce Bulk API Job creator and retriever.
    pub mod config;
    /// Salesforce implementation for creating Bulk API Job.
    pub mod job_creator;
    /// Salesforce implementation for retrieving Bulk API Job.
    pub mod job_retriever;
}

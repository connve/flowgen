//! Google Cloud Platform integration for the flowgen worker system.
//!
//! This crate provides GCP service connectivity for data activation workflows.
//! It handles authentication, connection management, and provides task
//! implementations that integrate with the flowgen event system.

/// BigQuery functionality for data warehousing and analytics.
pub mod bigquery {
    /// Configuration structures for BigQuery operations.
    pub mod config;
    /// Unified BigQuery job processor for all job operations (create, get, cancel, delete).
    pub mod job;
    /// BigQuery query processor implementation for executing SQL queries.
    pub mod query;
    /// BigQuery Storage Read API processor for high-throughput parallel table reads.
    pub mod storage_read;
}

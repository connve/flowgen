//! Microsoft SQL Server integration for flowgen.
//!
//! Provides SQL Server query execution with Arrow RecordBatch output, connection pooling,
//! and template rendering for parameterized queries.

pub mod client;
pub mod config;
pub mod query;

pub use client::Client;
pub use config::{MssqlQuery, QueryProcessor};
pub use query::Processor;

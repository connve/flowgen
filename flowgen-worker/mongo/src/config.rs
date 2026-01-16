//! # Mongo Configuration
//!
//! This module provides the configuration structures necessary for interacting
//! with MongoDB in two distinct modes:
//!
//! 1. **Batch Reader (`Reader`):** Configuration for standard collection reading tasks,
//!    including database targets and retry logic.
//! 2. **Change Stream (`ChangeStream`):** Configuration for Change Data Capture (CDC)
//!    to listen for real-time changes.

use serde::{Deserialize, Serialize};

/// Mongo batch reader configuration.
#[derive(PartialEq, Default, Clone, Debug, Deserialize, Serialize)]
pub struct Reader {
    /// The unique name / identifier of the task.
    pub name: String,
    /// The Database Name from Mongo.
    pub db_name: String,
    /// The Collection Name from Mongo.
    pub collection_name: String,
    #[serde(default)]
    pub retry: Option<flowgen_core::retry::RetryConfig>,
}

/// Mongo Change Data Capture reader configuration.
#[derive(PartialEq, Default, Clone, Debug, Deserialize, Serialize)]
pub struct ChangeStream {
    /// The unique name / identifier of the task.
    pub name: String,
    /// The Database Name from Mongo.
    pub db_name: String,
    #[serde(default)]
    pub retry: Option<flowgen_core::retry::RetryConfig>,
}

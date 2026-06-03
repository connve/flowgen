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
use std::collections::HashMap;
use std::path::PathBuf;

/// Mongo batch reader configuration.
#[derive(PartialEq, Default, Clone, Debug, Deserialize, Serialize)]
pub struct Reader {
    #[serde(default)]
    pub depends_on: Option<Vec<String>>,
    /// Path to credentials file containing Mongo authentication details.
    pub credentials_path: PathBuf,
    /// The unique name / identifier of the task.
    pub name: String,
    /// The Database Name from Mongo.
    pub db_name: String,
    /// The Collection Name from Mongo.
    pub collection_name: String,
    #[serde(default)]
    pub retry: Option<flowgen_core::retry::RetryConfig>,
    #[serde(default)]
    pub filter: HashMap<String, String>,
}

/// Mongo Change Data Capture reader configuration.
#[derive(PartialEq, Default, Clone, Debug, Deserialize, Serialize)]
pub struct ChangeStream {
    #[serde(default)]
    pub depends_on: Option<Vec<String>>,
    /// Path to credentials file containing Mongo authentication details.
    pub credentials_path: PathBuf,
    /// The unique name / identifier of the task.
    pub name: String,
    /// The Database Name from Mongo.
    pub db_name: String,
    #[serde(default)]
    pub retry: Option<flowgen_core::retry::RetryConfig>,
}

#[derive(PartialEq, Clone, Debug, Default, Deserialize, Serialize)]
pub struct Writer {
    #[serde(default)]
    pub depends_on: Option<Vec<String>>,
    /// The unique name / identifier of the task.
    pub name: String,
    /// Path to credentials file containing Salesforce authentication details.
    pub credentials_path: PathBuf,
    /// The Database Name from Mongo.
    pub db_name: String,
    /// The Collection Name from Mongo.
    pub collection_name: String,
    /// Optional retry configuration (overrides app-level retry config).
    #[serde(default)]
    pub retry: Option<flowgen_core::retry::RetryConfig>,
}

#[cfg(test)]
mod tests {
    use super::*;
    // no external json helper required here

    #[test]
    fn test_reader_serde_roundtrip() {
        let r = Reader {
            depends_on: None,
            credentials_path: std::path::PathBuf::from("/tmp/creds.json"),
            name: "task1".to_string(),
            db_name: "db".to_string(),
            collection_name: "col".to_string(),
            retry: None,
            filter: Default::default(),
        };

        let s = serde_json::to_string(&r).unwrap();
        let de: Reader = serde_json::from_str(&s).unwrap();
        assert_eq!(r, de);
    }

    #[test]
    fn test_writer_default_filter() {
        let w: Writer = Default::default();
        // defaults should exist and be debug-printable
        let _ = format!("{:?}", w);
    }
}

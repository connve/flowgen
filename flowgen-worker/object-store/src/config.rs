//! Configuration structures for file operations.
//!
//! Defines settings for CSV file reading and writing, including batch sizes,
//! headers, caching, and file paths.

use flowgen_core::cache::CacheOptions;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, path::PathBuf};

/// Object Store reader configuration.
#[derive(PartialEq, Default, Clone, Debug, Deserialize, Serialize)]
pub struct Reader {
    /// Optional label for identifying the reader.
    pub label: Option<String>,
    /// Path to the object store or file location.
    pub path: PathBuf,
    /// Number of records to process in each batch.
    pub batch_size: Option<usize>,
    /// Whether the input data has a header row.
    pub has_header: Option<bool>,
    /// Caching configuration for performance optimization.
    pub cache_options: Option<CacheOptions>,
}

/// Object Store writer configuration.
#[derive(PartialEq, Default, Clone, Debug, Deserialize, Serialize)]
pub struct Writer {
    /// Path to the object store or output location.
    pub path: PathBuf,
    /// Optional path to service account credentials file.
    pub credentials: Option<PathBuf>,
    /// Additional client connection options.
    pub client_options: Option<HashMap<String, String>>,
    /// Hive-style partitioning configuration.
    pub hive_partition_options: Option<HivePartitionOptions>,
}

/// Configuration for Hive-style directory partitioning.
#[derive(PartialEq, Default, Clone, Debug, Deserialize, Serialize)]
pub struct HivePartitionOptions {
    /// Whether to enable Hive partitioning.
    pub enabled: bool,
    /// List of partition keys to apply.
    pub partition_keys: Vec<HiveParitionKeys>,
}

/// Available partition keys for Hive-style partitioning.
#[derive(PartialEq, Default, Clone, Debug, Deserialize, Serialize)]
pub enum HiveParitionKeys {
    /// Partitions data by event date (year/month/day format).
    #[default]
    EventDate,
}

//! Configuration structures for file operations.
//!
//! Defines settings for CSV file reading and writing, including batch sizes,
//! headers, caching, and file paths.

use flowgen_core::cache::CacheOptions;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// Object Store reader configuration.

#[derive(PartialEq, Default, Clone, Debug, Deserialize, Serialize)]
pub struct Reader {
    pub label: Option<String>,
    pub path: PathBuf,
    pub batch_size: Option<usize>,
    pub has_header: Option<bool>,
    pub cache_options: Option<CacheOptions>,
}

/// Object Store writer configuration.
///
#[derive(PartialEq, Default, Clone, Debug, Deserialize, Serialize)]
pub struct Writer {
    pub path: PathBuf,
}

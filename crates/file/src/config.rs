//! # File Configuration Module
//!
//! This module provides configuration structures for file reading and writing operations
//! within the flowgen file processing system. It defines the configuration options
//! that control how files are processed, including batch sizes, header settings,
//! caching options, and file paths.
//!
//! ## Configuration Structures:
//! - `Reader`: Configuration for file reading operations including CSV parsing settings.
//! - `Writer`: Configuration for file writing operations including output path settings.
//!
//! Both configurations support JSON serialization/deserialization for easy
//! integration with configuration files and external systems.

use flowgen_core::cache::CacheOptions;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// Configuration structure for file reader operations.
///
/// This structure defines all the parameters needed to configure a file reader,
/// including the source file path, processing batch size, CSV header settings,
/// and optional caching configuration for schema reuse.
///
/// # Fields
/// - `label`: Optional human-readable identifier for this reader configuration.
/// - `path`: The file system path to the CSV file to be read.
/// - `batch_size`: Number of records to process in each batch (defaults to 1000 if not specified).
/// - `has_header`: Whether the CSV file contains a header row (defaults to true if not specified).
/// - `cache_options`: Optional caching configuration for storing and retrieving schemas.
///
/// # Examples
///
/// Basic file reader configuration:
/// ```json
/// {
///     "file_reader": {
///         "path": "/data/input/sales_data.csv",
///         "batch_size": 500,
///         "has_header": true
///     }
/// }
/// ```
///
/// Advanced configuration with caching:
/// ```json
/// {
///     "file_reader": {
///         "label": "sales_data_reader",
///         "path": "/data/input/sales_data.csv",
///         "batch_size": 1000,
///         "has_header": true,
///         "cache_options": {
///             "insert_key": "sales_schema_v1",
///         }
///     }
/// }
/// ```

#[derive(PartialEq, Default, Clone, Debug, Deserialize, Serialize)]
pub struct Reader {
    /// Optional human-readable label for identifying this reader configuration.
    pub label: Option<String>,
    /// File system path to the CSV file to be processed.
    pub path: PathBuf,
    /// Number of records to process in each batch (defaults to 1000 if None).
    pub batch_size: Option<usize>,
    /// Whether the CSV file contains a header row (defaults to true if None).
    pub has_header: Option<bool>,
    /// Optional caching configuration for schema storage and retrieval.
    pub cache_options: Option<CacheOptions>,
}

/// Configuration structure for file writer operations.
///
/// This structure defines the parameters needed to configure a file writer,
/// specifically the output path template that will be used to generate
/// timestamped output files.
///
/// # Fields
/// - `path`: The base file path template used for generating output filenames.
///   The actual output files will have timestamps inserted before the file extension.
///
/// # File Naming Convention
/// Output files are automatically timestamped using the pattern:
/// `{filename}.{timestamp_microseconds}.{extension}`
///
/// For example, if the configured path is `/output/results.csv`, the actual
/// output files will be named like `/output/results.1640995200000000.csv`.
///
/// # Examples
///
/// Basic file writer configuration:
/// ```json
/// {
///     "file_writer": {
///         "path": "/data/output/processed_data.csv"
///     }
/// }
/// ```
///
/// Configuration for different output formats:
/// ```json
/// {
///     "file_writer": {
///         "path": "/exports/customer_report.tsv"
///     }
/// }
/// ```
///
#[derive(PartialEq, Default, Clone, Debug, Deserialize, Serialize)]
pub struct Writer {
    /// Base file path template for output files (timestamps will be automatically inserted).
    pub path: PathBuf,
}

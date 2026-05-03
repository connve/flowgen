//! Configuration for object store operations.
//!
//! Defines a unified configuration struct for read, write, list, and move
//! operations on object storage (local files, GCS, S3, Azure).

use flowgen_core::config::ConfigExt;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, path::PathBuf};

/// File extension for Avro format files.
pub const DEFAULT_AVRO_EXTENSION: &str = "avro";
/// File extension for CSV format files.
pub const DEFAULT_CSV_EXTENSION: &str = "csv";
/// File extension for JSON format files.
pub const DEFAULT_JSON_EXTENSION: &str = "json";
/// File extension for Parquet format files.
pub const DEFAULT_PARQUET_EXTENSION: &str = "parquet";

/// Object store operation type.
#[derive(PartialEq, Clone, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Operation {
    /// Read files from object storage.
    #[default]
    Read,
    /// Write data to object storage.
    Write,
    /// List files in object storage.
    List,
    /// Move (copy + delete) files in object storage.
    Move,
}

/// Unified object store processor configuration.
///
/// A single task type that supports read, write, list, and move operations
/// via the `operation` field. Operation-specific fields are optional and
/// only relevant for their respective operations.
///
/// # Examples
///
/// Read a file:
/// ```yaml
/// - object_store:
///     name: read_orders
///     operation: read
///     path: gs://my-bucket/data/orders.csv
///     credentials_path: /path/to/creds.json
///     batch_size: 1000
/// ```
///
/// Write data:
/// ```yaml
/// - object_store:
///     name: write_results
///     operation: write
///     path: gs://my-bucket/output/
///     credentials_path: /path/to/creds.json
///     format: parquet
/// ```
///
/// List files:
/// ```yaml
/// - object_store:
///     name: list_files
///     operation: list
///     path: gs://my-bucket/data/*.parquet
/// ```
///
/// Move files:
/// ```yaml
/// - object_store:
///     name: archive_files
///     operation: move
///     source: gs://my-bucket/incoming/*.csv
///     destination: gs://my-bucket/processed/
/// ```
#[derive(PartialEq, Default, Clone, Debug, Deserialize, Serialize)]
pub struct Processor {
    /// Unique task identifier.
    pub name: String,
    /// Object store operation to perform.
    pub operation: Operation,

    // --- Shared fields (read, write, list) ---
    /// Path to the object store location.
    /// Required for read, write, and list operations.
    #[serde(default)]
    pub path: Option<PathBuf>,
    /// Optional path to credentials file.
    pub credentials_path: Option<PathBuf>,
    /// Additional client connection options.
    pub client_options: Option<HashMap<String, String>>,

    // --- Read-specific fields ---
    /// Number of records to process in each batch (read only).
    pub batch_size: Option<usize>,
    /// Whether the input data has a header row (read only).
    pub has_header: Option<bool>,
    /// CSV delimiter character, defaults to comma (read only).
    pub delimiter: Option<String>,
    /// Delete the file after successfully reading it (read only).
    pub delete_after_read: Option<bool>,

    // --- Write-specific fields ---
    /// Output format for the data, defaults to Auto (write only).
    #[serde(default)]
    pub format: WriteFormat,
    /// Hive-style partitioning configuration (write only).
    pub hive_partition_options: Option<HivePartitionOptions>,

    // --- Move-specific fields ---
    /// Source path pattern with wildcard support (move only).
    /// Ignored if source_files is provided.
    #[serde(default)]
    pub source: Option<PathBuf>,
    /// Explicit list of source file URIs to move (move only).
    /// Takes precedence over source when provided.
    /// Supports both literal arrays and template expressions.
    #[serde(default)]
    pub source_files: Option<serde_json::Value>,
    /// Destination path for moved files (move only).
    pub destination: Option<PathBuf>,

    // --- Common optional fields ---
    /// Optional list of upstream task names this task depends on.
    /// When set, this task only receives events from the named tasks.
    /// When not set, the task receives from the previous task in the list (linear chain).
    #[serde(default)]
    pub depends_on: Option<Vec<String>>,
    /// Optional retry configuration (overrides app-level retry config).
    #[serde(default)]
    pub retry: Option<flowgen_core::retry::RetryConfig>,
}

impl ConfigExt for Processor {}

/// Output format for writing data to object storage.
#[derive(PartialEq, Clone, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum WriteFormat {
    /// Automatically determine format based on event data type.
    #[default]
    Auto,
    /// Apache Parquet columnar format (efficient for Arrow data).
    Parquet,
    /// CSV text format.
    Csv,
    /// Apache Avro binary format.
    Avro,
    /// Newline-delimited JSON.
    Json,
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
    /// Partitions data by event hour (hour format).
    EventHour,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json;
    use std::collections::HashMap;

    #[test]
    fn test_read_config() {
        let mut client_options = HashMap::new();
        client_options.insert("region".to_string(), "us-west-2".to_string());

        let config = Processor {
            name: "test_reader".to_string(),
            operation: Operation::Read,
            path: Some(PathBuf::from("s3://my-bucket/data/")),
            credentials_path: Some(PathBuf::from("/path/to/creds.json")),
            client_options: Some(client_options.clone()),
            batch_size: Some(500),
            has_header: Some(true),
            ..Default::default()
        };

        assert_eq!(config.name, "test_reader");
        assert_eq!(config.path, Some(PathBuf::from("s3://my-bucket/data/")));
        assert_eq!(config.batch_size, Some(500));
        assert_eq!(config.has_header, Some(true));
    }

    #[test]
    fn test_read_config_serialization() {
        let config = Processor {
            name: "test_reader".to_string(),
            operation: Operation::Read,
            path: Some(PathBuf::from("gs://my-bucket/files/")),
            credentials_path: Some(PathBuf::from("/creds.json")),
            batch_size: Some(1000),
            has_header: Some(false),
            ..Default::default()
        };

        let json = serde_json::to_string(&config).unwrap();
        let deserialized: Processor = serde_json::from_str(&json).unwrap();
        assert_eq!(config, deserialized);
    }

    #[test]
    fn test_write_config() {
        let config = Processor {
            name: "test_writer".to_string(),
            operation: Operation::Write,
            path: Some(PathBuf::from("s3://output-bucket/results/")),
            credentials_path: Some(PathBuf::from("/service-account.json")),
            format: WriteFormat::Parquet,
            hive_partition_options: Some(HivePartitionOptions {
                enabled: true,
                partition_keys: vec![HiveParitionKeys::EventDate],
            }),
            ..Default::default()
        };

        assert_eq!(config.name, "test_writer");
        assert_eq!(config.format, WriteFormat::Parquet);
        assert!(config.hive_partition_options.is_some());
    }

    #[test]
    fn test_write_config_serialization() {
        let config = Processor {
            name: "test_writer".to_string(),
            operation: Operation::Write,
            path: Some(PathBuf::from("/local/path/output/")),
            format: WriteFormat::Csv,
            ..Default::default()
        };

        let json = serde_json::to_string(&config).unwrap();
        let deserialized: Processor = serde_json::from_str(&json).unwrap();
        assert_eq!(config, deserialized);
    }

    #[test]
    fn test_list_config() {
        let config = Processor {
            name: "test_lister".to_string(),
            operation: Operation::List,
            path: Some(PathBuf::from("gs://my-bucket/data/*.parquet")),
            ..Default::default()
        };

        assert_eq!(config.name, "test_lister");
        assert_eq!(config.operation, Operation::List);
    }

    #[test]
    fn test_move_config() {
        let config = Processor {
            name: "test_mover".to_string(),
            operation: Operation::Move,
            source: Some(PathBuf::from("gs://bucket/incoming/*.csv")),
            destination: Some(PathBuf::from("gs://bucket/processed/")),
            credentials_path: Some(PathBuf::from("/creds.json")),
            ..Default::default()
        };

        assert_eq!(config.name, "test_mover");
        assert_eq!(config.operation, Operation::Move);
        assert_eq!(
            config.source,
            Some(PathBuf::from("gs://bucket/incoming/*.csv"))
        );
        assert_eq!(
            config.destination,
            Some(PathBuf::from("gs://bucket/processed/"))
        );
    }

    #[test]
    fn test_move_config_with_source_files() {
        let config = Processor {
            name: "test_mover".to_string(),
            operation: Operation::Move,
            source_files: Some(serde_json::json!([
                "gs://bucket/file1.csv",
                "gs://bucket/file2.csv"
            ])),
            destination: Some(PathBuf::from("gs://bucket/processed/")),
            ..Default::default()
        };

        assert!(config.source_files.is_some());
        assert!(config.source.is_none());
    }

    #[test]
    fn test_default_config() {
        let config = Processor::default();
        assert_eq!(config.name, String::new());
        assert_eq!(config.operation, Operation::Read);
        assert_eq!(config.format, WriteFormat::Auto);
        assert!(config.path.is_none());
        assert!(config.credentials_path.is_none());
    }

    #[test]
    fn test_hive_partition_options_default() {
        let options = HivePartitionOptions::default();
        assert!(!options.enabled);
        assert!(options.partition_keys.is_empty());
    }

    #[test]
    fn test_hive_partition_options_creation() {
        let options = HivePartitionOptions {
            enabled: true,
            partition_keys: vec![HiveParitionKeys::EventDate],
        };

        assert!(options.enabled);
        assert_eq!(options.partition_keys.len(), 1);
        assert_eq!(options.partition_keys[0], HiveParitionKeys::EventDate);
    }

    #[test]
    fn test_config_clone() {
        let config = Processor {
            name: "test_reader".to_string(),
            operation: Operation::Read,
            path: Some(PathBuf::from("file:///tmp/data")),
            batch_size: Some(100),
            has_header: Some(true),
            ..Default::default()
        };

        let cloned = config.clone();
        assert_eq!(config, cloned);
    }

    #[test]
    fn test_constants() {
        assert_eq!(DEFAULT_AVRO_EXTENSION, "avro");
        assert_eq!(DEFAULT_CSV_EXTENSION, "csv");
        assert_eq!(DEFAULT_JSON_EXTENSION, "json");
    }

    #[test]
    fn test_delete_after_read() {
        let config = Processor {
            name: "test_reader".to_string(),
            operation: Operation::Read,
            path: Some(PathBuf::from("s3://bucket/file.csv")),
            delete_after_read: Some(true),
            ..Default::default()
        };

        assert_eq!(config.delete_after_read, Some(true));
    }
}

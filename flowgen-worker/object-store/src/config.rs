//! Configuration structures for file operations.
//!
//! Defines settings for CSV file reading and writing, including batch sizes,
//! headers, caching, and file paths.

use flowgen_core::{cache::CacheOptions, config::ConfigExt};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, path::PathBuf};

/// File extension for Avro format files.
pub const DEFAULT_AVRO_EXTENSION: &str = "avro";
/// File extension for CSV format files.
pub const DEFAULT_CSV_EXTENSION: &str = "csv";
/// File extension for JSON format files.
pub const DEFAULT_JSON_EXTENSION: &str = "json";

/// Object Store reader configuration.
#[derive(PartialEq, Default, Clone, Debug, Deserialize, Serialize)]
pub struct Reader {
    /// The unique name / identifier of the task.
    pub name: String,
    /// Path to the object store or file location.
    pub path: PathBuf,
    /// Optional path to credentials file.
    pub credentials: Option<PathBuf>,
    /// Additional client connection options.
    pub client_options: Option<HashMap<String, String>>,
    /// Number of records to process in each batch.
    pub batch_size: Option<usize>,
    /// Whether the input data has a header row.
    pub has_header: Option<bool>,
    /// Caching configuration for performance optimization.
    pub cache_options: Option<CacheOptions>,
    /// Delete the file from object store after successfully reading it.
    pub delete_after_read: Option<bool>,
}

/// Object Store writer configuration.
#[derive(PartialEq, Default, Clone, Debug, Deserialize, Serialize)]
pub struct Writer {
    /// The unique name / identifier of the task.
    pub name: String,
    /// Path to the object store or output location.
    pub path: PathBuf,
    /// Optional path to credentials file.
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

/// Implement default ConfigExt traits.
impl ConfigExt for Reader {}
impl ConfigExt for Writer {}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json;
    use std::collections::HashMap;

    #[test]
    fn test_reader_config_creation() {
        let mut client_options = HashMap::new();
        client_options.insert("region".to_string(), "us-west-2".to_string());

        let reader = Reader {
            name: "test_reader".to_string(),
            path: PathBuf::from("s3://my-bucket/data/"),
            credentials: Some(PathBuf::from("/path/to/creds.json")),
            client_options: Some(client_options.clone()),
            batch_size: Some(500),
            has_header: Some(true),
            cache_options: None,
            delete_after_read: None,
        };

        assert_eq!(reader.name, "test_reader".to_string());
        assert_eq!(reader.path, PathBuf::from("s3://my-bucket/data/"));
        assert_eq!(
            reader.credentials,
            Some(PathBuf::from("/path/to/creds.json"))
        );
        assert_eq!(reader.client_options, Some(client_options));
        assert_eq!(reader.batch_size, Some(500));
        assert_eq!(reader.has_header, Some(true));
    }

    #[test]
    fn test_reader_config_serialization() {
        let reader = Reader {
            name: "test_reader".to_string(),
            path: PathBuf::from("gs://my-bucket/files/"),
            credentials: Some(PathBuf::from("/creds.json")),
            client_options: None,
            batch_size: Some(1000),
            has_header: Some(false),
            cache_options: None,
            delete_after_read: None,
        };

        let json = serde_json::to_string(&reader).unwrap();
        let deserialized: Reader = serde_json::from_str(&json).unwrap();
        assert_eq!(reader, deserialized);
    }

    #[test]
    fn test_writer_config_default() {
        let writer = Writer::default();
        assert_eq!(writer.name, String::new());
        assert_eq!(writer.path, PathBuf::new());
        assert_eq!(writer.credentials, None);
        assert_eq!(writer.client_options, None);
        assert_eq!(writer.hive_partition_options, None);
    }

    #[test]
    fn test_writer_config_creation() {
        let mut client_options = HashMap::new();
        client_options.insert(
            "endpoint".to_string(),
            "https://s3.amazonaws.com".to_string(),
        );

        let hive_options = HivePartitionOptions {
            enabled: true,
            partition_keys: vec![HiveParitionKeys::EventDate],
        };

        let writer = Writer {
            name: "test_writer".to_string(),
            path: PathBuf::from("s3://output-bucket/results/"),
            credentials: Some(PathBuf::from("/service-account.json")),
            client_options: Some(client_options.clone()),
            hive_partition_options: Some(hive_options.clone()),
        };

        assert_eq!(writer.name, "test_writer".to_string());
        assert_eq!(writer.path, PathBuf::from("s3://output-bucket/results/"));
        assert_eq!(
            writer.credentials,
            Some(PathBuf::from("/service-account.json"))
        );
        assert_eq!(writer.client_options, Some(client_options));
        assert_eq!(writer.hive_partition_options, Some(hive_options));
    }

    #[test]
    fn test_writer_config_serialization() {
        let writer = Writer {
            name: "test_writer".to_string(),
            path: PathBuf::from("/local/path/output/"),
            credentials: None,
            client_options: None,
            hive_partition_options: Some(HivePartitionOptions {
                enabled: false,
                partition_keys: vec![],
            }),
        };

        let json = serde_json::to_string(&writer).unwrap();
        let deserialized: Writer = serde_json::from_str(&json).unwrap();
        assert_eq!(writer, deserialized);
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
    fn test_hive_partition_keys_default() {
        let key = HiveParitionKeys::default();
        assert_eq!(key, HiveParitionKeys::EventDate);
    }

    #[test]
    fn test_config_clone() {
        let reader = Reader {
            name: "test_reader".to_string(),
            path: PathBuf::from("file:///tmp/data"),
            credentials: None,
            client_options: None,
            batch_size: Some(100),
            has_header: Some(true),
            cache_options: None,
            delete_after_read: None,
        };

        let cloned = reader.clone();
        assert_eq!(reader, cloned);
    }

    #[test]
    fn test_constants() {
        assert_eq!(DEFAULT_AVRO_EXTENSION, "avro");
        assert_eq!(DEFAULT_CSV_EXTENSION, "csv");
        assert_eq!(DEFAULT_JSON_EXTENSION, "json");
    }

    #[test]
    fn test_reader_delete_after_read_enabled() {
        let reader = Reader {
            name: "test_reader".to_string(),
            path: PathBuf::from("s3://bucket/file.csv"),
            credentials: None,
            client_options: None,
            batch_size: None,
            has_header: None,
            cache_options: None,
            delete_after_read: Some(true),
        };

        assert_eq!(reader.delete_after_read, Some(true));
    }

    #[test]
    fn test_reader_delete_after_read_disabled() {
        let reader = Reader {
            name: "test_reader".to_string(),
            path: PathBuf::from("s3://bucket/file.csv"),
            credentials: None,
            client_options: None,
            batch_size: None,
            has_header: None,
            cache_options: None,
            delete_after_read: Some(false),
        };

        assert_eq!(reader.delete_after_read, Some(false));
    }

    #[test]
    fn test_reader_delete_after_read_default() {
        let reader = Reader {
            name: "test_reader".to_string(),
            path: PathBuf::from("s3://bucket/file.csv"),
            credentials: None,
            client_options: None,
            batch_size: None,
            has_header: None,
            cache_options: None,
            delete_after_read: None,
        };

        assert_eq!(reader.delete_after_read, None);
    }
}

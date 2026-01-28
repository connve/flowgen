//! Configuration structures for BigQuery operations.
//!
//! This module provides configuration for BigQuery query execution with support for
//! parameterized queries to prevent SQL injection attacks. All query parameters are
//! strongly typed and validated before execution.

use flowgen_core::config::ConfigExt;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Duration;

/// BigQuery parameter type constants.
pub const PARAM_TYPE_STRING: &str = "STRING";
pub const PARAM_TYPE_INT64: &str = "INT64";
pub const PARAM_TYPE_FLOAT64: &str = "FLOAT64";
pub const PARAM_TYPE_NUMERIC: &str = "NUMERIC";
pub const PARAM_TYPE_BIGNUMERIC: &str = "BIGNUMERIC";
pub const PARAM_TYPE_BOOL: &str = "BOOL";
pub const PARAM_TYPE_TIMESTAMP: &str = "TIMESTAMP";
pub const PARAM_TYPE_DATETIME: &str = "DATETIME";
pub const PARAM_TYPE_DATE: &str = "DATE";
pub const PARAM_TYPE_TIME: &str = "TIME";
pub const PARAM_TYPE_BYTES: &str = "BYTES";
pub const PARAM_TYPE_GEOGRAPHY: &str = "GEOGRAPHY";
pub const PARAM_TYPE_JSON: &str = "JSON";

/// Default timeout for BigQuery queries (10 seconds per BigQuery API defaults).
fn default_timeout() -> Option<Duration> {
    Some(Duration::from_secs(10))
}

/// Default value for use_query_cache.
fn default_use_query_cache() -> bool {
    true
}

/// Default value for use_legacy_sql.
fn default_use_legacy_sql() -> bool {
    false
}

/// Configuration structure for BigQuery query operations.
///
/// This structure defines all parameters needed to execute queries against BigQuery,
/// including authentication credentials, project configuration, and SQL query with
/// optional parameterization for safe query execution.
///
/// # SQL Injection Protection
///
/// To prevent SQL injection attacks, use parameterized queries with the `parameters`
/// field instead of string interpolation. Parameters are type-safe and properly
/// escaped by the BigQuery client library.
///
/// # Fields
/// - `name`: Unique name / identifier of the task.
/// - `credentials_path`: Path to GCP service account credentials JSON file.
/// - `project_id`: GCP project ID where BigQuery resources are located.
/// - `query`: SQL query source (inline string or resource file reference).
/// - `parameters`: Optional query parameters for safe SQL injection prevention.
/// - `location`: Optional BigQuery dataset location (e.g., "US", "EU", "us-central1").
/// - `max_results`: Optional maximum number of rows to return per page (default: all rows).
/// - `timeout`: Optional query timeout (e.g., "30s", "5m", "1h"). Default: 10 seconds.
/// - `use_query_cache`: Whether to use query cache (default: true).
/// - `use_legacy_sql`: Whether to use legacy SQL syntax (default: false for Standard SQL).
/// - `create_session`: Whether to create a new session for this query.
/// - `labels`: Optional labels for the query job (key-value pairs).
/// - `default_dataset`: Optional default dataset for unqualified table names.
///
/// # Examples
///
/// Basic query with inline SQL:
/// ```yaml
/// bigquery_query:
///   name: fetch_recent_orders
///   credentials_path: /etc/gcp/credentials.json
///   project_id: my-project-id
///   query:
///     inline: "SELECT order_id, customer_id, amount FROM `project.dataset.orders` WHERE order_date >= '2024-01-01' LIMIT 100"
/// ```
///
/// Query using external resource file:
/// ```yaml
/// bigquery_query:
///   name: fetch_recent_orders
///   credentials_path: /etc/gcp/credentials.json
///   project_id: my-project-id
///   query:
///     resource: "queries/get_recent_orders.sql"
/// ```
///
/// Parameterized query for SQL injection protection:
/// ```json
/// {
///     "bigquery_query": {
///         "name": "fetch_orders_by_status",
///         "credentials_path": "/etc/gcp/credentials.json",
///         "project_id": "my-project-id",
///         "query": "SELECT order_id, customer_id, amount FROM `project.dataset.orders` WHERE status = @status AND order_date >= @start_date",
///         "parameters": {
///             "status": "completed",
///             "start_date": "2024-01-01"
///         }
///     }
/// }
/// ```
///
/// Query with location and result limits:
/// ```json
/// {
///     "bigquery_query": {
///         "name": "analyze_eu_customers",
///         "credentials_path": "/etc/gcp/credentials.json",
///         "project_id": "my-project-id",
///         "location": "EU",
///         "query": "SELECT customer_id, COUNT(*) as order_count FROM `project.dataset.orders` GROUP BY customer_id ORDER BY order_count DESC",
///         "max_results": 1000,
///         "timeout": "5m"
///     }
/// }
/// ```
///
/// Query with dynamic parameters from upstream events:
/// ```json
/// {
///     "bigquery_query": {
///         "name": "fetch_customer_orders",
///         "credentials_path": "/etc/gcp/credentials.json",
///         "project_id": "my-project-id",
///         "query": "SELECT * FROM `project.dataset.orders` WHERE customer_id = @customer_id",
///         "parameters": {
///             "customer_id": "{{event.customer_id}}"
///         }
///     }
/// }
/// ```
#[derive(PartialEq, Clone, Debug, Deserialize, Serialize)]
pub struct Query {
    /// The unique name / identifier of the task.
    pub name: String,
    /// Path to GCP service account credentials JSON file.
    pub credentials_path: PathBuf,
    /// GCP project ID where BigQuery resources are located.
    pub project_id: String,
    /// SQL query source (inline or resource).
    pub query: flowgen_core::resource::Source,
    /// Optional query parameters for SQL injection protection.
    /// Keys are parameter names (without @ prefix), values are parameter values.
    #[serde(default)]
    pub parameters: Option<HashMap<String, Value>>,
    /// Optional BigQuery dataset location (e.g., "US", "EU", "us-central1").
    pub location: Option<String>,
    /// Optional maximum number of rows to return per page.
    pub max_results: Option<u64>,
    /// Optional query timeout (e.g., "30s", "5m", "1h"). Default: 10 seconds.
    #[serde(default = "default_timeout", with = "humantime_serde")]
    pub timeout: Option<Duration>,
    /// Whether to use query cache. Default: true.
    #[serde(default = "default_use_query_cache")]
    pub use_query_cache: bool,
    /// Whether to use legacy SQL syntax. Default: false (uses Standard SQL).
    #[serde(default = "default_use_legacy_sql")]
    pub use_legacy_sql: bool,
    /// Whether to create a new session for this query.
    #[serde(default)]
    pub create_session: Option<bool>,
    /// Optional labels for the query job (key-value pairs for organization).
    #[serde(default)]
    pub labels: Option<HashMap<String, String>>,
    /// Optional default dataset for unqualified table names in the query.
    /// Format: "project_id.dataset_id" or "dataset_id" (uses query's project_id).
    #[serde(default)]
    pub default_dataset: Option<String>,
    /// Optional retry configuration (overrides app-level retry config).
    #[serde(default)]
    pub retry: Option<flowgen_core::retry::RetryConfig>,
}

impl ConfigExt for Query {}

impl Query {
    /// Resolves the query string from inline or resource source.
    ///
    /// # Arguments
    /// * `resource_loader` - ResourceLoader instance for loading external files.
    ///
    /// # Returns
    /// The resolved SQL query string.
    pub async fn resolve_query(
        &self,
        resource_loader: &flowgen_core::resource::ResourceLoader,
    ) -> Result<String, ResolveError> {
        match &self.query {
            flowgen_core::resource::Source::Inline(sql) => Ok(sql.clone()),
            flowgen_core::resource::Source::Resource(key) => resource_loader
                .load(key)
                .await
                .map_err(|source| ResolveError::Resource { source }),
        }
    }
}

/// Errors that can occur when resolving query configuration.
#[derive(thiserror::Error, Debug)]
pub enum ResolveError {
    #[error("Failed to load resource: {source}")]
    Resource {
        #[source]
        source: flowgen_core::resource::Error,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_query_config_deserialization_with_inline_query() {
        let json = r#"{
            "name": "test",
            "credentials_path": "/test/creds.json",
            "project_id": "test-project",
            "query": {
                "inline": "SELECT 1"
            }
        }"#;
        let query: Query = serde_json::from_str(json).unwrap();
        assert_eq!(
            query.query,
            flowgen_core::resource::Source::Inline("SELECT 1".to_string())
        );
        assert_eq!(query.timeout, Some(Duration::from_secs(10)));
        assert!(query.use_query_cache);
        assert!(!query.use_legacy_sql);
    }

    #[test]
    fn test_query_config_deserialization_with_resource_query() {
        let json = r#"{
            "name": "test",
            "credentials_path": "/test/creds.json",
            "project_id": "test-project",
            "query": {
                "resource": "queries/get_data.sql"
            }
        }"#;
        let query: Query = serde_json::from_str(json).unwrap();
        assert_eq!(
            query.query,
            flowgen_core::resource::Source::Resource("queries/get_data.sql".to_string())
        );
    }

    #[test]
    fn test_query_config_creation() {
        let mut parameters = HashMap::new();
        parameters.insert("status".to_string(), json!("completed"));
        parameters.insert("start_date".to_string(), json!("2024-01-01"));

        let query = Query {
            name: "test_query".to_string(),
            credentials_path: PathBuf::from("/etc/gcp/credentials.json"),
            project_id: "my-project-id".to_string(),
            query: flowgen_core::resource::Source::Inline(
                "SELECT * FROM `dataset.table` WHERE status = @status AND date >= @start_date"
                    .to_string(),
            ),
            parameters: Some(parameters.clone()),
            location: Some("US".to_string()),
            max_results: Some(1000),
            timeout: Some(Duration::from_secs(300)),
            use_query_cache: true,
            use_legacy_sql: false,
            create_session: None,
            labels: None,
            default_dataset: None,
            retry: None,
        };

        assert_eq!(query.name, "test_query");
        assert_eq!(query.project_id, "my-project-id");
        assert_eq!(query.parameters, Some(parameters));
        assert_eq!(query.location, Some("US".to_string()));
        assert_eq!(query.max_results, Some(1000));
        assert_eq!(query.timeout, Some(Duration::from_secs(300)));
    }

    #[test]
    fn test_query_config_serialization() {
        let query = Query {
            name: "serialize_test".to_string(),
            credentials_path: PathBuf::from("/test/creds.json"),
            project_id: "test-project".to_string(),
            query: flowgen_core::resource::Source::Inline("SELECT 1".to_string()),
            parameters: None,
            location: None,
            max_results: None,
            timeout: Some(Duration::from_secs(10)),
            use_query_cache: true,
            use_legacy_sql: false,
            create_session: None,
            labels: None,
            default_dataset: None,
            retry: None,
        };

        let json = serde_json::to_string(&query).unwrap();
        let deserialized: Query = serde_json::from_str(&json).unwrap();
        assert_eq!(query, deserialized);
    }

    #[test]
    fn test_query_config_with_parameters() {
        let mut parameters = HashMap::new();
        parameters.insert("customer_id".to_string(), json!("CUST-12345"));
        parameters.insert("min_amount".to_string(), json!(100.50));
        parameters.insert("active".to_string(), json!(true));

        let query = Query {
            name: "parameterized_query".to_string(),
            credentials_path: PathBuf::from("/etc/gcp/creds.json"),
            project_id: "analytics-prod".to_string(),
            query: flowgen_core::resource::Source::Inline("SELECT * FROM orders WHERE customer_id = @customer_id AND amount >= @min_amount AND active = @active".to_string()),
            parameters: Some(parameters),
            location: Some("EU".to_string()),
            max_results: Some(500),
            timeout: Some(Duration::from_secs(60)),
            use_query_cache: true,
            use_legacy_sql: false,
            create_session: None,
            labels: None,
            default_dataset: None,
            retry: None,
        };

        assert!(query.parameters.is_some());
        let params = query.parameters.unwrap();
        assert_eq!(params.get("customer_id"), Some(&json!("CUST-12345")));
        assert_eq!(params.get("min_amount"), Some(&json!(100.50)));
        assert_eq!(params.get("active"), Some(&json!(true)));
    }

    #[test]
    fn test_query_config_clone() {
        let query = Query {
            name: "clone_test".to_string(),
            credentials_path: PathBuf::from("/creds.json"),
            project_id: "test-project".to_string(),
            query: flowgen_core::resource::Source::Inline(
                "SELECT COUNT(*) FROM dataset.table".to_string(),
            ),
            location: Some("us-central1".to_string()),
            max_results: Some(100),
            timeout: Some(Duration::from_secs(30)),
            parameters: None,
            use_query_cache: true,
            use_legacy_sql: false,
            create_session: None,
            labels: None,
            default_dataset: None,
            retry: None,
        };

        let cloned = query.clone();
        assert_eq!(query, cloned);
    }
}

/// Data format for BigQuery Storage Read API.
#[derive(PartialEq, Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
#[derive(Default)]
pub enum DataFormat {
    /// Arrow format (default, preferred for columnar processing).
    #[default]
    Arrow,
    /// Avro format (row-based).
    Avro,
}

/// Response compression codec for BigQuery Storage Read API.
#[derive(PartialEq, Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
#[derive(Default)]
pub enum CompressionCodec {
    /// No compression (default).
    #[default]
    Unspecified,
    /// LZ4 compression.
    Lz4,
}

/// Configuration structure for BigQuery Storage Read API operations.
///
/// The Storage Read API provides high-throughput, parallel access to BigQuery tables
/// without running SQL queries. It is optimized for reading large tables with optional
/// column selection and row filtering.
///
/// # Key Benefits
/// - High-throughput parallel reads using multiple streams
/// - Column selection to reduce data transfer
/// - Server-side filtering with row_restriction
/// - Arrow format for efficient columnar processing
/// - Time-travel queries with snapshot_time
///
/// # Fields
/// - `name`: Unique name / identifier of the task.
/// - `credentials_path`: Path to GCP service account credentials JSON file.
/// - `project_id`: GCP project ID where BigQuery resources are located.
/// - `dataset_id`: BigQuery dataset ID containing the table.
/// - `table_id`: BigQuery table ID to read from.
/// - `selected_fields`: Optional list of column names to read (reads all if not specified).
/// - `row_restriction`: Optional SQL WHERE clause (without WHERE keyword) for server-side filtering.
/// - `sample_percentage`: Optional random sampling percentage (0.0 to 100.0).
/// - `compression_codec`: Optional response compression. Default: unspecified.
/// - `snapshot_time`: Optional timestamp for time-travel queries (RFC3339 format).
/// - `max_stream_count`: Optional maximum number of parallel read streams.
/// - `preferred_min_stream_count`: Optional minimum streams hint for optimization.
/// - `data_format`: Data format for results. Default: arrow.
/// - `retry`: Optional retry configuration (overrides app-level retry config).
///
/// # Examples
///
/// Basic table read with all columns:
/// ```yaml
/// gcp_bigquery_storage_read:
///   name: read_orders
///   credentials_path: /etc/gcp/credentials.json
///   project_id: my-project
///   dataset_id: analytics
///   table_id: orders
/// ```
///
/// Read with column selection and filtering:
/// ```yaml
/// gcp_bigquery_storage_read:
///   name: read_recent_orders
///   credentials_path: /etc/gcp/credentials.json
///   project_id: my-project
///   dataset_id: analytics
///   table_id: orders
///   selected_fields:
///     - order_id
///     - customer_id
///     - amount
///   row_restriction: "order_date >= '2024-01-01' AND status = 'completed'"
/// ```
///
/// Parallel read with compression:
/// ```yaml
/// gcp_bigquery_storage_read:
///   name: fast_table_scan
///   credentials_path: /etc/gcp/credentials.json
///   project_id: my-project
///   dataset_id: warehouse
///   table_id: large_table
///   max_stream_count: 10
///   compression_codec: lz4
/// ```
///
/// Time-travel query with sampling:
/// ```yaml
/// gcp_bigquery_storage_read:
///   name: historical_sample
///   credentials_path: /etc/gcp/credentials.json
///   project_id: my-project
///   dataset_id: analytics
///   table_id: events
///   snapshot_time: "2024-01-15T00:00:00Z"
///   sample_percentage: 10.0
/// ```
#[derive(PartialEq, Clone, Debug, Default, Deserialize, Serialize)]
pub struct StorageRead {
    /// The unique name / identifier of the task.
    pub name: String,
    /// Path to GCP service account credentials JSON file.
    pub credentials_path: PathBuf,
    /// GCP project ID where BigQuery resources are located.
    pub project_id: String,
    /// BigQuery dataset ID containing the table.
    pub dataset_id: String,
    /// BigQuery table ID to read from.
    pub table_id: String,
    /// Optional list of column names to read. If not specified, reads all columns.
    /// Supports nested field selection using dot notation (e.g., "struct_field.nested_field").
    #[serde(default)]
    pub selected_fields: Option<Vec<String>>,
    /// Optional SQL WHERE clause for server-side row filtering (without WHERE keyword).
    /// Example: "order_date >= '2024-01-01' AND status = 'completed'".
    #[serde(default)]
    pub row_restriction: Option<String>,
    /// Optional random sampling percentage (0.0 to 100.0).
    /// Samples rows randomly from the table before applying filters.
    #[serde(default)]
    pub sample_percentage: Option<f64>,
    /// Optional response compression codec. Default: unspecified (no compression).
    #[serde(default)]
    pub compression_codec: CompressionCodec,
    /// Optional timestamp for time-travel queries (RFC3339 format).
    /// Example: "2024-01-15T00:00:00Z".
    #[serde(default)]
    pub snapshot_time: Option<String>,
    /// Optional maximum number of parallel read streams.
    /// Higher values can improve throughput for large tables.
    #[serde(default)]
    pub max_stream_count: Option<i32>,
    /// Optional preferred minimum number of streams (optimization hint).
    #[serde(default)]
    pub preferred_min_stream_count: Option<i32>,
    /// Data format for results. Default: arrow (preferred for columnar processing).
    #[serde(default)]
    pub data_format: DataFormat,
    /// Optional retry configuration (overrides app-level retry config).
    #[serde(default)]
    pub retry: Option<flowgen_core::retry::RetryConfig>,
}

impl ConfigExt for StorageRead {}

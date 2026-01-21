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

/// Default timeout for BigQuery queries (5 minutes).
fn default_timeout() -> Option<Duration> {
    Some(Duration::from_secs(300))
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
/// - `query`: SQL query to execute (use `@parameter_name` for parameterized queries).
/// - `parameters`: Optional query parameters for safe SQL injection prevention.
/// - `location`: Optional BigQuery dataset location (e.g., "US", "EU", "us-central1").
/// - `max_results`: Optional maximum number of rows to return (default: all rows).
/// - `timeout`: Optional query timeout (e.g., "30s", "5m", "1h").
///
/// # Examples
///
/// Basic query without parameters:
/// ```json
/// {
///     "bigquery_query": {
///         "name": "fetch_recent_orders",
///         "credentials_path": "/etc/gcp/credentials.json",
///         "project_id": "my-project-id",
///         "query": "SELECT order_id, customer_id, amount FROM `project.dataset.orders` WHERE order_date >= '2024-01-01' LIMIT 100"
///     }
/// }
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
#[derive(PartialEq, Clone, Debug, Default, Deserialize, Serialize)]
pub struct Query {
    /// The unique name / identifier of the task.
    pub name: String,
    /// Path to GCP service account credentials JSON file.
    pub credentials_path: PathBuf,
    /// GCP project ID where BigQuery resources are located.
    pub project_id: String,
    /// SQL query to execute (use @parameter_name for parameterized queries).
    pub query: String,
    /// Optional query parameters for SQL injection protection.
    /// Keys are parameter names (without @ prefix), values are parameter values.
    #[serde(default)]
    pub parameters: Option<HashMap<String, Value>>,
    /// Optional BigQuery dataset location (e.g., "US", "EU", "us-central1").
    pub location: Option<String>,
    /// Optional maximum number of rows to return.
    pub max_results: Option<u64>,
    /// Optional query timeout (e.g., "30s", "5m", "1h"). Default: 5 minutes.
    #[serde(default = "default_timeout", with = "humantime_serde")]
    pub timeout: Option<Duration>,
    /// Optional retry configuration (overrides app-level retry config).
    #[serde(default)]
    pub retry: Option<flowgen_core::retry::RetryConfig>,
}

impl ConfigExt for Query {}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_query_config_default() {
        let query = Query::default();
        assert_eq!(query.name, String::new());
        assert_eq!(query.credentials_path, PathBuf::new());
        assert_eq!(query.project_id, String::new());
        assert_eq!(query.query, String::new());
        assert_eq!(query.parameters, None);
        assert_eq!(query.location, None);
        assert_eq!(query.max_results, None);
        assert_eq!(query.timeout, None);
        assert_eq!(query.retry, None);
    }

    #[test]
    fn test_query_config_deserialization_with_default_timeout() {
        let json = r#"{
            "name": "test",
            "credentials_path": "/test/creds.json",
            "project_id": "test-project",
            "query": "SELECT 1"
        }"#;
        let query: Query = serde_json::from_str(json).unwrap();
        assert_eq!(query.timeout, Some(Duration::from_secs(300)));
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
            query: "SELECT * FROM `dataset.table` WHERE status = @status AND date >= @start_date"
                .to_string(),
            parameters: Some(parameters.clone()),
            location: Some("US".to_string()),
            max_results: Some(1000),
            timeout: Some(Duration::from_secs(300)),
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
            query: "SELECT 1".to_string(),
            parameters: None,
            location: None,
            max_results: None,
            timeout: None,
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
            query: "SELECT * FROM orders WHERE customer_id = @customer_id AND amount >= @min_amount AND active = @active".to_string(),
            parameters: Some(parameters),
            location: Some("EU".to_string()),
            max_results: Some(500),
            timeout: Some(Duration::from_secs(60)),
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
            query: "SELECT COUNT(*) FROM dataset.table".to_string(),
            parameters: None,
            location: Some("us-central1".to_string()),
            max_results: Some(100),
            timeout: Some(Duration::from_secs(30)),
            retry: None,
        };

        let cloned = query.clone();
        assert_eq!(query, cloned);
    }
}

//! Configuration types for Microsoft SQL Server integration.

use flowgen_core::config::ConfigExt;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// Configuration for MSSQL query processor.
///
/// Executes SQL queries against Microsoft SQL Server and returns results as Arrow RecordBatch.
///
/// ## Example
///
/// Basic query with inline SQL:
/// ```yaml
/// - mssql_query:
///     name: fetch_customers
///     credentials_path: /etc/mssql/credentials.json
///     query: "SELECT * FROM customers WHERE created_at > @p1"
///     parameters:
///       - "{{event.data.since}}"
/// ```
///
/// Query from resource file with custom timeout:
/// ```yaml
/// - mssql_query:
///     name: fetch_large_dataset
///     credentials_path: /etc/mssql/credentials.json
///     query:
///       resource: "queries/large_dataset.sql"
///     query_timeout: "5m"
///     batch_size: 5000
/// ```
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
pub struct Query {
    /// Unique task identifier.
    pub name: String,

    /// Path to SQL Server credentials file (JSON format).
    ///
    /// Expected format:
    /// ```json
    /// {
    ///   "host": "localhost",
    ///   "port": 1433,
    ///   "database": "mydb",
    ///   "username": "sa",
    ///   "password": "SecurePassword123",
    ///   "trust_server_certificate": true
    /// }
    /// ```
    pub credentials_path: PathBuf,

    /// SQL query to execute (inline string or resource file).
    ///
    /// Supports Handlebars template syntax for dynamic queries.
    /// Inline: `query: "SELECT * FROM table WHERE id = {{event.data.id}}"`
    /// Resource: `query: { resource: "queries/fetch.sql" }`
    pub query: flowgen_core::resource::Source,

    /// Optional query parameters for parameterized queries.
    ///
    /// Parameters are bound as `@p1`, `@p2`, etc. in the SQL query.
    /// Values support Handlebars template substitution from event data.
    ///
    /// Example: `parameters: ["{{event.data.start_date}}", "{{event.data.end_date}}"]`
    #[serde(default)]
    pub parameters: Option<Vec<String>>,

    /// Number of rows per Arrow RecordBatch.
    ///
    /// Larger batches use more memory but reduce overhead.
    /// Default: 10000 rows per batch.
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,

    /// Maximum number of connections in the connection pool.
    ///
    /// Each task maintains its own connection pool.
    /// Connections are created on-demand up to this limit.
    /// Default: 10 connections.
    #[serde(default = "default_max_connections")]
    pub max_connections: u32,

    /// Connection establishment timeout.
    ///
    /// Maximum time to wait when establishing new database connections.
    /// Useful for unreliable SQL servers with network issues.
    ///
    /// Default: "30s" (30 seconds).
    /// Examples: "30s", "1m", "2m"
    #[serde(default = "default_connection_timeout", with = "humantime_serde")]
    pub connection_timeout: std::time::Duration,

    /// Query execution timeout.
    ///
    /// Maximum time to wait for both acquiring a connection from the pool
    /// and executing the query. Use human-readable duration strings.
    ///
    /// Default: "2m" (2 minutes).
    /// Examples: "30s", "5m", "1h"
    #[serde(default = "default_query_timeout", with = "humantime_serde")]
    pub query_timeout: std::time::Duration,

    /// Optional list of upstream task names this task depends on.
    /// When set, this task only receives events from the named tasks.
    /// When not set, the task receives from the previous task in the list (linear chain).
    #[serde(default)]
    pub depends_on: Option<Vec<String>>,
    /// Optional retry configuration for this task.
    #[serde(default)]
    pub retry: Option<flowgen_core::retry::RetryConfig>,
}

impl ConfigExt for Query {}

impl Query {
    /// Builds ADO connection string from credentials file.
    ///
    /// Loads credentials from JSON file and constructs a SQL Server connection string.
    pub async fn build_connection_string(&self) -> Result<String, std::io::Error> {
        let creds_json = tokio::fs::read_to_string(&self.credentials_path).await?;
        let creds: Credentials = serde_json::from_str(&creds_json)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;

        Ok(format!(
            "server=tcp:{},{};database={};user={};password={};TrustServerCertificate={};Encrypt={}",
            creds.host,
            creds.port,
            creds.database,
            creds.username,
            creds.password,
            creds.trust_server_certificate,
            creds.encrypt
        ))
    }
}

fn default_batch_size() -> usize {
    10000
}

fn default_max_connections() -> u32 {
    10
}

fn default_connection_timeout() -> std::time::Duration {
    std::time::Duration::from_secs(30) // 30 seconds
}

fn default_query_timeout() -> std::time::Duration {
    std::time::Duration::from_secs(120) // 2 minutes
}

/// Microsoft SQL Server connection credentials.
///
/// Loaded from JSON file specified in `credentials_path`.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
pub struct Credentials {
    /// SQL Server host (e.g., "localhost" or "sqlserver.example.com").
    pub host: String,

    /// SQL Server port (default: 1433).
    #[serde(default = "default_port")]
    pub port: u16,

    /// Database name.
    pub database: String,

    /// Username for authentication.
    pub username: String,

    /// Password for authentication.
    pub password: String,

    /// Whether to trust server certificate (required for self-signed certs).
    #[serde(default = "default_trust_cert")]
    pub trust_server_certificate: bool,

    /// Whether to encrypt the connection. Defaults to true for security.
    /// Set to false only for legacy servers that don't support encryption.
    #[serde(default = "default_encrypt")]
    pub encrypt: bool,
}

fn default_port() -> u16 {
    1433
}

fn default_trust_cert() -> bool {
    false
}

fn default_encrypt() -> bool {
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_deserialize_mssql_query() {
        let yaml = r#"
            name: test-query
            credentials_path: /var/secrets/mssql/creds.json
            query: "SELECT * FROM users"
        "#;

        let config: Query = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.name, "test-query");
        assert_eq!(config.batch_size, 10000);
        assert_eq!(config.max_connections, 10);
    }

    #[test]
    fn test_deserialize_with_parameters() {
        let yaml = r#"
            name: parameterized-query
            credentials_path: /var/secrets/mssql/creds.json
            query: "SELECT * FROM users WHERE id = @p1"
            parameters:
              - "{{event.data.user_id}}"
        "#;

        let config: Query = serde_yaml::from_str(yaml).unwrap();
        assert!(config.parameters.is_some());
        assert_eq!(config.parameters.as_ref().unwrap().len(), 1);
    }

    #[test]
    fn test_deserialize_credentials() {
        let json = r#"
        {
            "host": "localhost",
            "port": 1433,
            "database": "testdb",
            "username": "sa",
            "password": "TestPass123",
            "trust_server_certificate": true,
            "encrypt": true
        }
        "#;

        let creds: Credentials = serde_json::from_str(json).unwrap();
        assert_eq!(creds.host, "localhost");
        assert_eq!(creds.port, 1433);
        assert_eq!(creds.database, "testdb");
        assert!(creds.trust_server_certificate);
        assert!(creds.encrypt);
    }

    #[test]
    fn test_credentials_defaults() {
        let json = r#"
        {
            "host": "sqlserver.example.com",
            "database": "proddb",
            "username": "app_user",
            "password": "SecurePass456"
        }
        "#;

        let creds: Credentials = serde_json::from_str(json).unwrap();
        assert_eq!(creds.port, 1433);
        assert!(!creds.trust_server_certificate);
        assert!(creds.encrypt); // Defaults to true for security
    }
}

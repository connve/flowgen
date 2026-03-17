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
/// ```yaml
/// tasks:
///   - type: mssql_query
///     name: fetch-customers
///     credentials_path: /var/secrets/mssql/credentials.json
///     query:
///       inline: "SELECT * FROM customers WHERE created_at > @p1"
///     parameters:
///       - "{{event.data.since}}"
///     batch_size: 1000
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

    /// SQL query to execute (inline or from resource file).
    pub query: flowgen_core::resource::Source,

    /// Optional query parameters for parameterized queries.
    ///
    /// Parameters are passed as `@p1`, `@p2`, etc. in the SQL query.
    /// Values support template substitution from event data.
    #[serde(default)]
    pub parameters: Option<Vec<String>>,

    /// Number of rows per Arrow RecordBatch (default: 10000).
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,

    /// Maximum number of connections in the pool (default: 10).
    #[serde(default = "default_max_connections")]
    pub max_connections: u32,

    /// Optional connection timeout in seconds (default: 30s).
    #[serde(default = "default_connection_timeout", with = "humantime_serde")]
    pub connection_timeout: std::time::Duration,

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
            "server=tcp:{},{};database={};user={};password={};TrustServerCertificate={}",
            creds.host,
            creds.port,
            creds.database,
            creds.username,
            creds.password,
            creds.trust_server_certificate
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
    std::time::Duration::from_secs(30)
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
}

fn default_port() -> u16 {
    1433
}

fn default_trust_cert() -> bool {
    false
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
            "trust_server_certificate": true
        }
        "#;

        let creds: Credentials = serde_json::from_str(json).unwrap();
        assert_eq!(creds.host, "localhost");
        assert_eq!(creds.port, 1433);
        assert_eq!(creds.database, "testdb");
        assert!(creds.trust_server_certificate);
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
    }
}

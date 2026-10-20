//! Configuration types for Microsoft SQL Server integration.

use serde::{Deserialize, Serialize};

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

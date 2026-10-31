//! Configuration types for Microsoft SQL Server integration.

use secrecy::SecretString;
use serde::Deserialize;

/// SQL Server connection credentials, loaded from the JSON file at `credentials_path`.
#[derive(Debug, Clone, Deserialize)]
pub struct Credentials {
    /// Server hostname.
    pub host: String,
    /// Server port.
    #[serde(default = "default_port")]
    pub port: u16,
    /// Database name.
    pub database: String,
    /// Username for SQL Server authentication.
    pub username: String,
    /// Password for SQL Server authentication.
    pub password: SecretString,
    /// Trusts the server certificate without validating it, for self-signed certificates.
    #[serde(default)]
    pub trust_server_certificate: bool,
    /// Encrypts the whole connection; without it only the login is encrypted.
    #[serde(default = "default_encrypt")]
    pub encrypt: bool,
}

fn default_port() -> u16 {
    1433
}

fn default_encrypt() -> bool {
    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use secrecy::ExposeSecret;

    #[test]
    fn credentials_read_every_field() {
        let json = r#"{
            "host": "localhost",
            "port": 1434,
            "database": "testdb",
            "username": "sa",
            "password": "TestPass123",
            "trust_server_certificate": true,
            "encrypt": false
        }"#;
        let credentials: Credentials = serde_json::from_str(json).unwrap();
        assert_eq!(credentials.host, "localhost");
        assert_eq!(credentials.port, 1434);
        assert_eq!(credentials.database, "testdb");
        assert_eq!(credentials.password.expose_secret(), "TestPass123");
        assert!(credentials.trust_server_certificate);
        assert!(!credentials.encrypt);
    }

    #[test]
    fn credentials_use_defaults() {
        let json = r#"{
            "host": "sqlserver.example.com",
            "database": "proddb",
            "username": "app_user",
            "password": "SecurePass456"
        }"#;
        let credentials: Credentials = serde_json::from_str(json).unwrap();
        assert_eq!(credentials.port, 1433);
        assert!(!credentials.trust_server_certificate);
        assert!(credentials.encrypt);
    }

    #[test]
    fn debug_output_hides_the_password() {
        let json = r#"{"host": "h", "database": "d", "username": "u", "password": "secret"}"#;
        let credentials: Credentials = serde_json::from_str(json).unwrap();
        assert!(!format!("{credentials:?}").contains("secret"));
    }
}

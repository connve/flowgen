//! Configuration types for PostgreSQL integration.

use secrecy::SecretString;
use serde::Deserialize;
use std::path::PathBuf;

/// PostgreSQL connection credentials, loaded from the JSON file at `credentials_path`.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Credentials {
    /// Server hostname.
    pub host: String,
    /// Server port.
    #[serde(default = "default_port")]
    pub port: u16,
    /// Database name.
    pub database: String,
    /// Username for password authentication.
    pub username: String,
    /// Password for password authentication.
    pub password: SecretString,
    /// Whether the connection uses TLS.
    #[serde(default)]
    pub ssl_mode: SslMode,
    /// PEM file with the CA certificates that sign the server certificate; system roots when unset.
    #[serde(default)]
    pub ca_certificate_path: Option<PathBuf>,
}

/// Whether the connection uses TLS.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SslMode {
    /// Plain connection.
    Disable,
    /// TLS with a verified certificate when the server offers TLS, plain when
    /// it doesn't.
    #[default]
    Prefer,
    /// TLS with a verified certificate only.
    Require,
}

impl From<SslMode> for tokio_postgres::config::SslMode {
    fn from(mode: SslMode) -> Self {
        match mode {
            SslMode::Disable => Self::Disable,
            SslMode::Prefer => Self::Prefer,
            SslMode::Require => Self::Require,
        }
    }
}

fn default_port() -> u16 {
    5432
}

#[cfg(test)]
mod tests {
    use super::*;
    use secrecy::ExposeSecret;

    #[test]
    fn credentials_use_defaults() {
        let json = r#"{
            "host": "db.internal",
            "database": "app",
            "username": "reader",
            "password": "secret"
        }"#;
        let credentials: Credentials = serde_json::from_str(json).unwrap();
        assert_eq!(credentials.port, 5432);
        assert_eq!(credentials.ssl_mode, SslMode::Prefer);
        assert!(credentials.ca_certificate_path.is_none());
        assert_eq!(credentials.password.expose_secret(), "secret");
    }

    #[test]
    fn credentials_read_ssl_settings() {
        let json = r#"{
            "host": "db.internal",
            "port": 6432,
            "database": "app",
            "username": "reader",
            "password": "secret",
            "ssl_mode": "require",
            "ca_certificate_path": "/etc/postgres/ca.pem"
        }"#;
        let credentials: Credentials = serde_json::from_str(json).unwrap();
        assert_eq!(credentials.port, 6432);
        assert_eq!(credentials.ssl_mode, SslMode::Require);
        assert_eq!(
            credentials.ca_certificate_path,
            Some(PathBuf::from("/etc/postgres/ca.pem"))
        );
    }

    #[test]
    fn credentials_reject_unknown_fields() {
        let json = r#"{
            "host": "db.internal",
            "database": "app",
            "username": "reader",
            "password": "secret",
            "sslmode": "require"
        }"#;
        assert!(serde_json::from_str::<Credentials>(json).is_err());
    }

    #[test]
    fn debug_output_hides_the_password() {
        let json = r#"{
            "host": "db.internal",
            "database": "app",
            "username": "reader",
            "password": "secret"
        }"#;
        let credentials: Credentials = serde_json::from_str(json).unwrap();
        assert!(!format!("{credentials:?}").contains("secret"));
    }
}

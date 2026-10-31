//! PostgreSQL client with connection pooling.

use crate::config::{Credentials, SslMode};
use bb8::{Pool, PooledConnection};
use bb8_postgres::PostgresConnectionManager;
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::CertificateDer;
use secrecy::ExposeSecret;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tokio_postgres_rustls::MakeRustlsConnect;

type ConnectionManager = PostgresConnectionManager<MakeRustlsConnect>;

/// Errors that can occur during PostgreSQL client operations.
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum Error {
    #[error("Error reading credentials file '{path}': {source}")]
    ReadCredentials {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("Error parsing credentials file '{path}': {source}")]
    ParseCredentials {
        path: PathBuf,
        #[source]
        source: serde_json::Error,
    },
    #[error("Error reading CA certificates '{path}': {source}")]
    ReadCertificate {
        path: PathBuf,
        #[source]
        source: rustls::pki_types::pem::Error,
    },
    #[error("CA file '{path}' contains no certificates")]
    EmptyCaFile { path: PathBuf },
    #[error(
        "No trusted root certificates found; set `ca_certificate_path` or `ssl_mode: disable`"
    )]
    NoRootCertificates,
    #[error("Error configuring TLS: {source}")]
    Tls {
        #[source]
        source: rustls::Error,
    },
    #[error("Error connecting to PostgreSQL: {source}")]
    Connect {
        #[source]
        source: bb8::RunError<tokio_postgres::Error>,
    },
}

/// PostgreSQL client with a connection pool.
pub struct Client {
    pool: Pool<ConnectionManager>,
    query_timeout: Duration,
}

impl Client {
    /// Builds the pool and opens one connection to check the credentials.
    pub async fn new(
        credentials: &Credentials,
        max_connections: u32,
        connection_timeout: Duration,
        query_timeout: Duration,
    ) -> Result<Self, Error> {
        let (mut config, tls) = connection_config(credentials, connection_timeout)?;
        config.options(format!(
            "-c statement_timeout={}",
            query_timeout.as_millis()
        ));
        let pool = Pool::builder()
            .max_size(max_connections)
            .connection_timeout(connection_timeout)
            .retry_connection(false)
            .build_unchecked(PostgresConnectionManager::new(config, tls));
        let client = Self {
            pool,
            query_timeout,
        };
        client.connection().await?;
        Ok(client)
    }

    /// A pooled connection, waiting at most the query timeout for one.
    pub async fn connection(&self) -> Result<PooledConnection<'_, ConnectionManager>, Error> {
        match tokio::time::timeout(self.query_timeout, self.pool.get()).await {
            Ok(connection) => connection.map_err(|source| Error::Connect { source }),
            Err(_) => Err(Error::Connect {
                source: bb8::RunError::TimedOut,
            }),
        }
    }

    /// Maximum time a statement runs, and a query waits for a connection and its first rows.
    pub fn query_timeout(&self) -> Duration {
        self.query_timeout
    }
}

/// Whether retrying a statement might help: a lost connection, and server
/// errors of the connection, transaction rollback, resource and operator
/// intervention classes, except a statement timeout.
pub fn is_transient(error: &tokio_postgres::Error) -> bool {
    match error.code() {
        Some(code) if *code == tokio_postgres::error::SqlState::QUERY_CANCELED => false,
        Some(code) => matches!(&code.code()[..2], "08" | "40" | "53" | "57" | "58"),
        None => {
            error.is_closed()
                || std::error::Error::source(error)
                    .is_some_and(|cause| cause.is::<std::io::Error>())
        }
    }
}

/// Connection settings and TLS connector for `credentials`, with TCP
/// keepalives that notice a dead network path within about a minute.
pub fn connection_config(
    credentials: &Credentials,
    connection_timeout: Duration,
) -> Result<(tokio_postgres::Config, MakeRustlsConnect), Error> {
    let mut config = tokio_postgres::Config::new();
    config
        .host(&credentials.host)
        .port(credentials.port)
        .dbname(&credentials.database)
        .user(&credentials.username)
        .password(credentials.password.expose_secret())
        .ssl_mode(credentials.ssl_mode.into())
        .connect_timeout(connection_timeout)
        .keepalives(true)
        .keepalives_idle(Duration::from_secs(30))
        .keepalives_interval(Duration::from_secs(10))
        .keepalives_retries(3)
        .application_name("flowgen");
    Ok((config, MakeRustlsConnect::new(tls_config(credentials)?)))
}

/// Reads the credentials file.
pub fn read_credentials(path: &Path) -> Result<Credentials, Error> {
    let raw = std::fs::read_to_string(path).map_err(|source| Error::ReadCredentials {
        path: path.to_path_buf(),
        source,
    })?;
    serde_json::from_str(&raw).map_err(|source| Error::ParseCredentials {
        path: path.to_path_buf(),
        source,
    })
}

fn tls_config(credentials: &Credentials) -> Result<rustls::ClientConfig, Error> {
    let mut roots = rustls::RootCertStore::empty();
    match (&credentials.ca_certificate_path, credentials.ssl_mode) {
        (_, SslMode::Disable) => {}
        (Some(path), _) => {
            let read_error = |source| Error::ReadCertificate {
                path: path.clone(),
                source,
            };
            let certificates = CertificateDer::pem_file_iter(path)
                .map_err(read_error)?
                .collect::<Result<Vec<_>, _>>()
                .map_err(read_error)?;
            if certificates.is_empty() {
                return Err(Error::EmptyCaFile { path: path.clone() });
            }
            for certificate in certificates {
                roots
                    .add(certificate)
                    .map_err(|source| Error::Tls { source })?;
            }
        }
        (None, _) => {
            let (added, _) =
                roots.add_parsable_certificates(rustls_native_certs::load_native_certs().certs);
            if added == 0 {
                return Err(Error::NoRootCertificates);
            }
        }
    }
    Ok(rustls::ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .map_err(|source| Error::Tls { source })?
    .with_root_certificates(roots)
    .with_no_client_auth())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn credentials(extra: &str) -> Credentials {
        serde_json::from_str(&format!(
            r#"{{"host": "localhost", "database": "app", "username": "reader", "password": "secret"{extra}}}"#
        ))
        .unwrap()
    }

    #[test]
    fn an_empty_ca_file_is_rejected() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let path = file.path().display().to_string();
        let result = tls_config(&credentials(&format!(
            r#", "ca_certificate_path": "{path}""#
        )));
        assert!(matches!(result, Err(Error::EmptyCaFile { .. })));
    }

    #[test]
    fn a_disabled_ssl_mode_ignores_the_ca_file() {
        let result = tls_config(&credentials(
            r#", "ssl_mode": "disable", "ca_certificate_path": "/missing.pem""#,
        ));
        assert!(result.is_ok());
    }

    #[test]
    fn connections_send_tcp_keepalives() {
        let (config, _) = connection_config(
            &credentials(r#", "ssl_mode": "disable""#),
            Duration::from_secs(5),
        )
        .unwrap();
        assert!(config.get_keepalives());
        assert_eq!(config.get_keepalives_idle(), Duration::from_secs(30));
        assert_eq!(
            config.get_keepalives_interval(),
            Some(Duration::from_secs(10))
        );
        assert_eq!(config.get_keepalives_retries(), Some(3));
    }

    #[test]
    fn a_missing_credentials_file_names_the_path() {
        let error = read_credentials(Path::new("/missing/postgres.json")).unwrap_err();
        assert!(error.to_string().contains("/missing/postgres.json"));
    }
}

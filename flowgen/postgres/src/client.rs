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

/// Pools PostgreSQL connections, dropping one that does not answer its checkout check in time.
pub struct ConnectionManager {
    postgres: PostgresConnectionManager<MakeRustlsConnect>,
    check_timeout: Duration,
}

/// Why the pool could not open or check a connection.
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum ConnectionError {
    #[error(transparent)]
    Postgres(#[from] tokio_postgres::Error),
    #[error("The server did not answer a connection check within {timeout:?}")]
    Silent { timeout: Duration },
}

impl bb8::ManageConnection for ConnectionManager {
    type Connection = tokio_postgres::Client;
    type Error = ConnectionError;

    async fn connect(&self) -> Result<Self::Connection, Self::Error> {
        Ok(self.postgres.connect().await?)
    }

    async fn is_valid(&self, connection: &mut Self::Connection) -> Result<(), Self::Error> {
        match tokio::time::timeout(self.check_timeout, self.postgres.is_valid(connection)).await {
            Ok(checked) => Ok(checked?),
            Err(_) => Err(ConnectionError::Silent {
                timeout: self.check_timeout,
            }),
        }
    }

    fn has_broken(&self, connection: &mut Self::Connection) -> bool {
        self.postgres.has_broken(connection)
    }
}

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
        source: bb8::RunError<ConnectionError>,
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
            .build_unchecked(ConnectionManager {
                postgres: PostgresConnectionManager::new(config, tls),
                check_timeout: connection_timeout / 2,
            });
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

    /// Maximum time a statement runs, and a task waits for a connection.
    pub fn query_timeout(&self) -> Duration {
        self.query_timeout
    }

    /// Awaits a round trip, giving up when the server stays silent for twice the query timeout.
    pub async fn answered<T>(
        &self,
        step: impl std::future::Future<Output = T>,
    ) -> Result<T, Silent> {
        let timeout = self.query_timeout.saturating_mul(2);
        match tokio::time::timeout(timeout, step).await {
            Ok(answer) => Ok(answer),
            Err(_) => Err(Silent { timeout }),
        }
    }
}

/// A server that did not answer within `timeout`.
#[derive(Debug, Clone, Copy)]
pub struct Silent {
    pub timeout: Duration,
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

    #[tokio::test]
    #[ignore = "requires Docker"]
    async fn a_connection_whose_server_went_silent_fails_its_check() {
        use bb8::ManageConnection;
        use testcontainers::core::IntoContainerPort;
        use testcontainers::runners::AsyncRunner;
        use testcontainers::{GenericImage, ImageExt};

        let container = GenericImage::new("postgres", "17-alpine")
            .with_exposed_port(5432.tcp())
            .with_env_var("POSTGRES_PASSWORD", "secret")
            .start()
            .await
            .expect("start postgres container");
        let port = container.get_host_port_ipv4(5432).await.expect("map port");
        let credentials: Credentials = serde_json::from_str(&format!(
            r#"{{"host": "127.0.0.1", "port": {port}, "database": "postgres", "username": "postgres", "password": "secret", "ssl_mode": "disable"}}"#
        ))
        .unwrap();
        let (config, tls) = connection_config(&credentials, Duration::from_secs(5)).unwrap();
        let manager = ConnectionManager {
            postgres: PostgresConnectionManager::new(config, tls),
            check_timeout: Duration::from_secs(1),
        };
        let mut connection = None;
        for _ in 0..60 {
            if let Ok(connected) = manager.connect().await {
                connection = Some(connected);
                break;
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
        let mut connection = connection.expect("connect to postgres");
        let healthy = manager.is_valid(&mut connection).await;

        container.pause().await.expect("pause server");
        let silent =
            tokio::time::timeout(Duration::from_secs(10), manager.is_valid(&mut connection)).await;
        container.unpause().await.expect("unpause server");

        assert!(healthy.is_ok());
        assert!(matches!(silent, Ok(Err(ConnectionError::Silent { .. }))));
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

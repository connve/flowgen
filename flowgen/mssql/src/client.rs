//! Microsoft SQL Server client with connection pooling.

use crate::config::Credentials;
use bb8::{Pool, PooledConnection};
use secrecy::ExposeSecret;
use std::path::{Path, PathBuf};
use std::time::Duration;
use tiberius::{AuthMethod, Config, EncryptionLevel};
use tokio::net::TcpStream;
use tokio_util::compat::{Compat, TokioAsyncWriteCompatExt};

/// Errors that can occur during SQL Server client operations.
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
    #[error("Error connecting to SQL Server: {source}")]
    Connect {
        #[source]
        source: bb8::RunError<tiberius::error::Error>,
    },
}

/// Opens the pool's SQL Server connections, following an Azure SQL gateway
/// redirect.
pub struct ConnectionManager {
    config: Config,
}

impl bb8::ManageConnection for ConnectionManager {
    type Connection = tiberius::Client<Compat<TcpStream>>;
    type Error = tiberius::error::Error;

    async fn connect(&self) -> Result<Self::Connection, Self::Error> {
        match open(self.config.clone()).await {
            Err(tiberius::error::Error::Routing { host, port }) => {
                let mut config = self.config.clone();
                config.host(host);
                config.port(port);
                open(config).await
            }
            opened => opened,
        }
    }

    async fn is_valid(&self, connection: &mut Self::Connection) -> Result<(), Self::Error> {
        connection
            .simple_query("SELECT 1")
            .await?
            .into_row()
            .await?;
        Ok(())
    }

    fn has_broken(&self, _connection: &mut Self::Connection) -> bool {
        false
    }
}

/// Opens one connection with `config`.
async fn open(config: Config) -> tiberius::Result<tiberius::Client<Compat<TcpStream>>> {
    let tcp = TcpStream::connect(config.get_addr()).await?;
    tcp.set_nodelay(true)?;
    tiberius::Client::connect(config, tcp.compat_write()).await
}

/// SQL Server client with a connection pool.
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
        let pool = Pool::builder()
            .max_size(max_connections)
            .connection_timeout(connection_timeout)
            .retry_connection(false)
            .build_unchecked(ConnectionManager {
                config: connection_config(credentials, connection_timeout, query_timeout),
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

    /// Maximum time a query waits for a connection and its first result.
    pub fn query_timeout(&self) -> Duration {
        self.query_timeout
    }
}

/// Connection settings for `credentials`. The login waits at most
/// `connection_timeout`, and a query at most `query_timeout` for each answer
/// of the server.
pub fn connection_config(
    credentials: &Credentials,
    connection_timeout: Duration,
    query_timeout: Duration,
) -> Config {
    let mut config = Config::new();
    config.host(&credentials.host);
    config.port(credentials.port);
    config.database(&credentials.database);
    config.authentication(AuthMethod::sql_server(
        &credentials.username,
        credentials.password.expose_secret(),
    ));
    config.encryption(match credentials.encrypt {
        true => EncryptionLevel::Required,
        false => EncryptionLevel::Off,
    });
    if credentials.trust_server_certificate {
        config.trust_cert();
    }
    config.application_name("flowgen");
    config.handshake_timeout(Some(connection_timeout));
    config.command_timeout(Some(query_timeout));
    config
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

/// Whether retrying a query might help: a lost connection, a deadlock, a lock
/// timeout, or a server error Microsoft lists as transient, such as a busy or
/// unavailable database. A query the server left unanswered past
/// `query_timeout` is not retried.
pub fn is_transient(error: &tiberius::error::Error) -> bool {
    match error {
        tiberius::error::Error::Io { kind, .. } => *kind != std::io::ErrorKind::TimedOut,
        tiberius::error::Error::Routing { .. } => true,
        tiberius::error::Error::Server(token) => matches!(
            token.code(),
            1205 | 1222
                | 4060
                | 4221
                | 10053
                | 10054
                | 10060
                | 10928
                | 10929
                | 40143
                | 40197
                | 40501
                | 40613
                | 49918
                | 49919
                | 49920
        ),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_password_with_separators_stays_one_value() {
        let credentials: Credentials = serde_json::from_str(
            r#"{"host": "db", "database": "app", "username": "u", "password": "a;b=c"}"#,
        )
        .unwrap();
        let config = connection_config(
            &credentials,
            Duration::from_secs(30),
            Duration::from_secs(120),
        );
        assert_eq!(config.get_addr(), "db:1433");
    }

    #[test]
    fn a_missing_credentials_file_names_the_path() {
        let error = read_credentials(Path::new("/missing/mssql.json")).unwrap_err();
        assert!(error.to_string().contains("/missing/mssql.json"));
    }

    #[test]
    fn io_errors_are_transient_and_conversions_are_not() {
        let io = tiberius::error::Error::Io {
            kind: std::io::ErrorKind::ConnectionReset,
            message: "reset".to_string(),
        };
        let conversion = tiberius::error::Error::Conversion("bad".into());
        let unanswered = tiberius::error::Error::Io {
            kind: std::io::ErrorKind::TimedOut,
            message: "timed out".to_string(),
        };
        assert!(is_transient(&io));
        assert!(!is_transient(&conversion));
        assert!(!is_transient(&unanswered));
    }
}

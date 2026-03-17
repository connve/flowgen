//! Microsoft SQL Server client with connection pooling.

use bb8::{Pool, PooledConnection};
use bb8_tiberius::ConnectionManager;
use std::sync::Arc;
use tiberius::Config;

/// Errors that can occur during MSSQL client operations.
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum Error {
    #[error("Failed to parse connection string: {source}")]
    ConnectionStringParse {
        #[source]
        source: tiberius::error::Error,
    },

    #[error("Failed to create connection pool: {source}")]
    PoolCreation {
        #[source]
        source: bb8_tiberius::Error,
    },

    #[error("Failed to get connection from pool: {source}")]
    PoolGet {
        #[source]
        source: bb8::RunError<bb8_tiberius::Error>,
    },

    #[error("Query execution failed: {source}")]
    QueryExecution {
        #[source]
        source: tiberius::error::Error,
    },

    #[error("Failed to convert result to Arrow: {source}")]
    ArrowConversion {
        #[source]
        source: arrow::error::ArrowError,
    },
}

type ConnectionPool = Pool<ConnectionManager>;

/// Microsoft SQL Server client with connection pooling.
///
/// Manages a pool of connections to SQL Server using bb8 and tiberius.
/// Connections are automatically returned to the pool when dropped.
pub struct Client {
    pool: Arc<ConnectionPool>,
}

impl Client {
    /// Creates a new MSSQL client with connection pooling.
    ///
    /// # Arguments
    ///
    /// * `connection_string` - SQL Server connection string
    /// * `max_connections` - Maximum number of connections in the pool
    /// * `connection_timeout` - Timeout for establishing connections
    ///
    /// # Returns
    ///
    /// A new client instance with an initialized connection pool.
    pub async fn new(
        connection_string: &str,
        max_connections: u32,
        connection_timeout: std::time::Duration,
    ) -> Result<Self, Error> {
        let config = Config::from_ado_string(connection_string)
            .map_err(|source| Error::ConnectionStringParse { source })?;

        let manager = ConnectionManager::new(config);

        let pool = Pool::builder()
            .max_size(max_connections)
            .connection_timeout(connection_timeout)
            .build(manager)
            .await
            .map_err(|source| Error::PoolCreation { source })?;

        Ok(Self {
            pool: Arc::new(pool),
        })
    }

    /// Gets a connection from the pool.
    ///
    /// Connections are automatically returned to the pool when dropped.
    pub async fn get_connection(&self) -> Result<PooledConnection<'_, ConnectionManager>, Error> {
        self.pool
            .get()
            .await
            .map_err(|source| Error::PoolGet { source })
    }

    /// Returns a reference to the connection pool.
    ///
    /// Useful for advanced pool management or monitoring.
    pub fn pool(&self) -> &ConnectionPool {
        &self.pool
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_connection_string_parsing() {
        let conn_str = "server=tcp:localhost,1433;database=test;user=sa;password=pass;TrustServerCertificate=true";
        let config = Config::from_ado_string(conn_str);
        assert!(config.is_ok());
    }
}

use mongodb::{options::ClientOptions, Client};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;

/// Default MongoDB host.
pub const DEFAULT_MONGO_HOST: &str = "localhost";
/// Default MongoDB port.
pub const DEFAULT_MONGO_PORT: u16 = 27017;

#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum Error {
    #[error("Reading credentials from file failed with error: {source}")]
    CredentialsFileRead {
        #[source]
        source: std::io::Error,
    },
    #[error("Parsing credentials from file failed with error: {source}")]
    CredentialsFileParse {
        #[source]
        source: serde_json::Error,
    },
    #[error("Building connection URI failed with error: {source}")]
    UriBuild {
        #[source]
        source: url::ParseError,
    },
    #[error("Mongo connection parsing failed with error: {source}")]
    MongoConnectionParse {
        #[source]
        source: mongodb::error::Error,
    },
    #[error("Mongo client creation failed with error: {source}")]
    MongoClientCreate {
        #[source]
        source: mongodb::error::Error,
    },
}

/// Default connection string scheme.
pub const DEFAULT_MONGO_SCHEME: &str = "mongodb";

/// MongoDB connection credentials.
///
/// Loaded from JSON file specified in `credentials_path`. All fields are
/// optional: `scheme` defaults to `mongodb`, `host` to `localhost`, `port`
/// to `27017`, and `username`/`password` are omitted for unauthenticated
/// connections. The target database is not part of the connection string
/// — each task sets it independently via its own `db_name` field.
///
/// ```json
/// {
///   "host": "mongo.example.com",
///   "port": 27017,
///   "username": "user",
///   "password": "pass",
///   "options": { "authSource": "admin", "replicaSet": "rs0" }
/// }
/// ```
///
/// MongoDB Atlas uses `mongodb+srv://`, where DNS resolves the actual
/// cluster hosts and port — set `scheme` and omit `port`:
///
/// ```json
/// {
///   "scheme": "mongodb+srv",
///   "host": "cluster0.abcde.mongodb.net",
///   "username": "user",
///   "password": "pass"
/// }
/// ```
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Default)]
pub struct MongoCredentials {
    /// Connection string scheme. Defaults to `mongodb`. Set to
    /// `mongodb+srv` for MongoDB Atlas and other DNS-seedlist deployments;
    /// `port` is ignored in that case.
    #[serde(default)]
    pub scheme: Option<String>,
    /// MongoDB host. Defaults to `localhost`.
    #[serde(default)]
    pub host: Option<String>,
    /// MongoDB port. Defaults to `27017`. Ignored when `scheme` is
    /// `mongodb+srv`.
    #[serde(default)]
    pub port: Option<u16>,
    /// Username for authentication. Omit for unauthenticated connections.
    #[serde(default)]
    pub username: Option<String>,
    /// Password for authentication. Omit for unauthenticated connections.
    #[serde(default)]
    pub password: Option<String>,
    /// Additional connection string options, e.g. `authSource`, `tls`,
    /// `replicaSet`, `retryWrites`, `w`. Passed through verbatim as URI
    /// query parameters — see the MongoDB connection string reference.
    #[serde(default)]
    pub options: HashMap<String, String>,
}

impl MongoCredentials {
    pub fn from_file(path: &PathBuf) -> Result<Self, Error> {
        let content =
            fs::read_to_string(path).map_err(|source| Error::CredentialsFileRead { source })?;

        serde_json::from_str(&content).map_err(|source| Error::CredentialsFileParse { source })
    }

    /// Builds a connection string from the structured fields, with
    /// `options` appended as query parameters.
    pub fn build_connection_string(&self) -> Result<String, Error> {
        let scheme = match self.scheme.as_deref() {
            Some(scheme) => scheme,
            None => DEFAULT_MONGO_SCHEME,
        };
        let auth = match (&self.username, &self.password) {
            (Some(username), Some(password)) => format!("{username}:{password}@"),
            _ => String::new(),
        };
        let host = match self.host.as_deref() {
            Some(host) => host,
            None => DEFAULT_MONGO_HOST,
        };
        let port = match self.port {
            Some(port) => port,
            None => DEFAULT_MONGO_PORT,
        };
        let host = match scheme {
            // SRV-style schemes encode the port in DNS; a port here would
            // produce an invalid connection string.
            DEFAULT_MONGO_SCHEME => format!("{host}:{port}"),
            _ => host.to_string(),
        };

        let base = format!("{scheme}://{auth}{host}/");
        let mut url = url::Url::parse(&base).map_err(|source| Error::UriBuild { source })?;
        if !self.options.is_empty() {
            let mut pairs = url.query_pairs_mut();
            for (key, value) in &self.options {
                pairs.append_pair(key, value);
            }
        }
        Ok(url.into())
    }
}

/// Builder for configuring and creating MongoDB clients.
#[derive(Default)]
pub struct MongoClientBuilder {
    credentials_path: Option<PathBuf>,
}

impl MongoClientBuilder {
    pub fn new() -> Self {
        MongoClientBuilder::default()
    }

    /// Sets the path to the credentials file.
    pub fn credentials_path(mut self, path: PathBuf) -> Self {
        self.credentials_path = Some(path);
        self
    }

    /// Builds a new MongoDB client instance.
    pub fn build(self) -> Result<MongoClient, Error> {
        Ok(MongoClient {
            credentials_path: self.credentials_path,
        })
    }
}

pub struct MongoClient {
    credentials_path: Option<PathBuf>,
}

impl MongoClient {
    pub async fn connect(self) -> Result<Client, Error> {
        let credentials = match &self.credentials_path {
            Some(path) => MongoCredentials::from_file(path)?,
            None => MongoCredentials::default(),
        };
        let uri = credentials.build_connection_string()?;

        let options = ClientOptions::parse(&uri)
            .await
            .map_err(|source| Error::MongoConnectionParse { source })?;

        Client::with_options(options).map_err(|source| Error::MongoClientCreate { source })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_connection_string_defaults() {
        let creds = MongoCredentials::default();
        assert_eq!(
            creds.build_connection_string().unwrap(),
            "mongodb://localhost:27017/"
        );
    }

    #[test]
    fn test_build_connection_string_with_host_and_port() {
        let creds = MongoCredentials {
            host: Some("mongo.example.com".to_string()),
            port: Some(27018),
            ..Default::default()
        };
        assert_eq!(
            creds.build_connection_string().unwrap(),
            "mongodb://mongo.example.com:27018/"
        );
    }

    #[test]
    fn test_build_connection_string_with_auth() {
        let creds = MongoCredentials {
            username: Some("u".to_string()),
            password: Some("p".to_string()),
            ..Default::default()
        };
        assert_eq!(
            creds.build_connection_string().unwrap(),
            "mongodb://u:p@localhost:27017/"
        );
    }

    #[test]
    fn test_build_connection_string_ignores_partial_auth() {
        let creds = MongoCredentials {
            username: Some("u".to_string()),
            ..Default::default()
        };
        assert_eq!(
            creds.build_connection_string().unwrap(),
            "mongodb://localhost:27017/"
        );
    }

    #[test]
    fn test_build_connection_string_appends_options() {
        let creds = MongoCredentials {
            options: HashMap::from([("replicaSet".to_string(), "rs0".to_string())]),
            ..Default::default()
        };
        assert_eq!(
            creds.build_connection_string().unwrap(),
            "mongodb://localhost:27017/?replicaSet=rs0"
        );
    }

    #[test]
    fn test_build_connection_string_srv_scheme_omits_port() {
        let creds = MongoCredentials {
            scheme: Some("mongodb+srv".to_string()),
            host: Some("cluster0.abcde.mongodb.net".to_string()),
            port: Some(27017),
            ..Default::default()
        };
        assert_eq!(
            creds.build_connection_string().unwrap(),
            "mongodb+srv://cluster0.abcde.mongodb.net/"
        );
    }

    #[test]
    fn test_from_file_roundtrip() {
        let mut path = std::env::temp_dir();
        path.push(format!(
            "flowgen_test_mongo_creds_{}.json",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::write(&path, r#"{"host": "example.com", "port": 27018}"#).unwrap();

        let creds = MongoCredentials::from_file(&path).unwrap();
        std::fs::remove_file(&path).ok();

        assert_eq!(creds.host.as_deref(), Some("example.com"));
        assert_eq!(creds.port, Some(27018));
    }

    #[test]
    fn test_from_file_missing_returns_read_error() {
        let path = PathBuf::from("/nonexistent/flowgen_mongo_creds.json");
        assert!(matches!(
            MongoCredentials::from_file(&path),
            Err(Error::CredentialsFileRead { .. })
        ));
    }

    #[test]
    fn test_from_file_invalid_json_returns_parse_error() {
        let mut path = std::env::temp_dir();
        path.push(format!(
            "flowgen_test_mongo_creds_invalid_{}.json",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::write(&path, "not json").unwrap();

        let result = MongoCredentials::from_file(&path);
        std::fs::remove_file(&path).ok();

        assert!(matches!(result, Err(Error::CredentialsFileParse { .. })));
    }

    #[test]
    fn test_builder_without_credentials_path_connects_with_defaults() {
        let client = MongoClientBuilder::new().build().unwrap();
        assert!(client.credentials_path.is_none());
    }

    #[test]
    fn test_builder_with_credentials_path() {
        let client = MongoClientBuilder::new()
            .credentials_path(PathBuf::from("/etc/mongo/credentials.json"))
            .build()
            .unwrap();
        assert_eq!(
            client.credentials_path,
            Some(PathBuf::from("/etc/mongo/credentials.json"))
        );
    }
}

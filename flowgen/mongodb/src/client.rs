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
    #[error("MongoDB connection parsing failed with error: {source}")]
    MongoConnectionParse {
        #[source]
        source: mongodb::error::Error,
    },
    #[error("MongoDB client creation failed with error: {source}")]
    MongoClientCreate {
        #[source]
        source: mongodb::error::Error,
    },
}

/// Default connection string scheme.
pub const DEFAULT_MONGO_SCHEME: &str = "mongodb";

/// One MongoDB host, or a replica-set seed list.
///
/// Accepts either a single string or an array in YAML/JSON — a single host
/// is just a seed list of one, matching how the MongoDB connection string
/// itself represents both cases as one comma-joined authority.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
#[serde(untagged)]
pub enum MongoHost {
    One(String),
    Many(Vec<String>),
}

impl MongoHost {
    fn as_slice(&self) -> &[String] {
        match self {
            MongoHost::One(host) => std::slice::from_ref(host),
            MongoHost::Many(hosts) => hosts,
        }
    }
}

impl Default for MongoHost {
    fn default() -> Self {
        MongoHost::One(DEFAULT_MONGO_HOST.to_string())
    }
}

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
///
/// A self-hosted replica set (no SRV record) needs an explicit seed list
/// instead of a single host — `host` also accepts an array, with `port` as
/// the fallback for any entry that omits its own:
///
/// ```json
/// {
///   "host": ["mongo-0:27017", "mongo-1:27017", "mongo-2:27017"],
///   "username": "user",
///   "password": "pass",
///   "options": { "replicaSet": "rs0" }
/// }
/// ```
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Default)]
pub struct MongoCredentials {
    /// Connection string scheme. Defaults to `mongodb`. Set to
    /// `mongodb+srv` for MongoDB Atlas and other DNS-seedlist deployments;
    /// `port` is ignored in that case.
    #[serde(default)]
    pub scheme: Option<String>,
    /// MongoDB host, or a replica-set seed list. Defaults to `localhost`.
    #[serde(default)]
    pub host: MongoHost,
    /// MongoDB port. Defaults to `27017`. Ignored when `scheme` is
    /// `mongodb+srv`, or for any `host` entry that carries its own port.
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
    ///
    /// A replica-set seed list does not fit `url::Url`, which only parses
    /// a single authority — the authority is built as a plain string
    /// instead, and only the query string goes through the `url` crate's
    /// percent-encoding.
    pub fn build_connection_string(&self) -> Result<String, Error> {
        let scheme = match self.scheme.as_deref() {
            Some(scheme) => scheme,
            None => DEFAULT_MONGO_SCHEME,
        };
        let auth = match (&self.username, &self.password) {
            (Some(username), Some(password)) => format!("{username}:{password}@"),
            _ => String::new(),
        };
        let authority = self.build_authority(scheme);

        let mut uri = format!("{scheme}://{auth}{authority}/");
        if !self.options.is_empty() {
            let query: String = url::form_urlencoded::Serializer::new(String::new())
                .extend_pairs(&self.options)
                .finish();
            uri.push('?');
            uri.push_str(&query);
        }
        Ok(uri)
    }

    /// Renders the host segment: one host, or a comma-joined seed list.
    fn build_authority(&self, scheme: &str) -> String {
        self.host
            .as_slice()
            .iter()
            .map(|entry| self.qualify_host(entry, scheme))
            .collect::<Vec<_>>()
            .join(",")
    }

    /// Appends the default port to a bare host, unless it already has one
    /// or the scheme is SRV-style (port comes from DNS).
    ///
    /// Bracketed hosts (`[::1]`) are checked for `]:` rather than a bare
    /// colon count, since the address itself contains colons.
    fn qualify_host(&self, host: &str, scheme: &str) -> String {
        let has_port = match host.strip_prefix('[') {
            Some(after_bracket) => after_bracket.contains("]:"),
            None => host.matches(':').count() == 1,
        };
        if scheme != DEFAULT_MONGO_SCHEME || has_port {
            return host.to_string();
        }
        let port = self.port.unwrap_or(DEFAULT_MONGO_PORT);
        format!("{host}:{port}")
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
            host: MongoHost::One("mongo.example.com".to_string()),
            port: Some(27018),
            ..Default::default()
        };
        assert_eq!(
            creds.build_connection_string().unwrap(),
            "mongodb://mongo.example.com:27018/"
        );
    }

    #[test]
    fn test_build_connection_string_bracketed_ipv6_without_port_gets_default_port() {
        let creds = MongoCredentials {
            host: MongoHost::One("[::1]".to_string()),
            ..Default::default()
        };
        assert_eq!(
            creds.build_connection_string().unwrap(),
            "mongodb://[::1]:27017/"
        );
    }

    #[test]
    fn test_build_connection_string_bracketed_ipv6_with_port_is_untouched() {
        let creds = MongoCredentials {
            host: MongoHost::One("[::1]:27018".to_string()),
            ..Default::default()
        };
        assert_eq!(
            creds.build_connection_string().unwrap(),
            "mongodb://[::1]:27018/"
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
            host: MongoHost::One("cluster0.abcde.mongodb.net".to_string()),
            port: Some(27017),
            ..Default::default()
        };
        assert_eq!(
            creds.build_connection_string().unwrap(),
            "mongodb+srv://cluster0.abcde.mongodb.net/"
        );
    }

    #[test]
    fn test_build_connection_string_with_host_seed_list() {
        let creds = MongoCredentials {
            host: MongoHost::Many(vec![
                "mongo-0:27017".to_string(),
                "mongo-1:27017".to_string(),
                "mongo-2:27017".to_string(),
            ]),
            ..Default::default()
        };
        assert_eq!(
            creds.build_connection_string().unwrap(),
            "mongodb://mongo-0:27017,mongo-1:27017,mongo-2:27017/"
        );
    }

    #[test]
    fn test_build_connection_string_seed_list_falls_back_to_default_port() {
        let creds = MongoCredentials {
            host: MongoHost::Many(vec!["mongo-0".to_string(), "mongo-1".to_string()]),
            port: Some(27018),
            ..Default::default()
        };
        assert_eq!(
            creds.build_connection_string().unwrap(),
            "mongodb://mongo-0:27018,mongo-1:27018/"
        );
    }

    #[test]
    fn test_host_deserializes_from_string_or_array() {
        let one: MongoHost = serde_json::from_str(r#""mongo.example.com""#).unwrap();
        assert_eq!(one, MongoHost::One("mongo.example.com".to_string()));

        let many: MongoHost = serde_json::from_str(r#"["mongo-0", "mongo-1"]"#).unwrap();
        assert_eq!(
            many,
            MongoHost::Many(vec!["mongo-0".to_string(), "mongo-1".to_string()])
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

        assert_eq!(creds.host, MongoHost::One("example.com".to_string()));
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
            .credentials_path(PathBuf::from("/etc/mongodb/credentials.json"))
            .build()
            .unwrap();
        assert_eq!(
            client.credentials_path,
            Some(PathBuf::from("/etc/mongodb/credentials.json"))
        );
    }
}

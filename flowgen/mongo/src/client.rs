use mongodb::{options::ClientOptions, Client};
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::PathBuf;

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
    #[error("Missing credentials error.")]
    MissingCredentials,

    #[error("Mongo connection parsing failed with error: {source}")]
    MongoConnectionParse {
        #[source]
        source: mongodb::error::Error,
    },
    #[error("Mongo reader failed with error: {source}")]
    MongoReader {
        #[source]
        source: mongodb::error::Error,
    },
}

#[derive(Debug, Deserialize, Serialize)]
pub struct MongoCredentials {
    #[serde(rename = "MONGODB_USERNAME")]
    username: Option<String>,
    #[serde(rename = "MONGODB_PASSWORD")]
    password: Option<String>,
    #[serde(rename = "MONGODB_URI")]
    uri: Option<String>,
}

impl MongoCredentials {
    pub fn from_file(path: &PathBuf) -> Result<Self, Error> {
        let content =
            fs::read_to_string(path).map_err(|source| Error::CredentialsFileRead { source })?;

        serde_json::from_str(&content).map_err(|source| Error::CredentialsFileParse { source })
    }

    pub fn build_connection_string(&self, default_host: &str) -> Result<String, Error> {
        // Try to use MONGODB_URI if present
        if let Some(uri) = &self.uri {
            if !uri.is_empty() {
                return Ok(uri.clone());
            }
        }

        // Otherwise, build URI from username and password
        match (&self.username, &self.password) {
            (Some(username), Some(password)) if !username.is_empty() && !password.is_empty() => Ok(
                format!("mongodb://{}:{}@{}", username, password, default_host),
            ),
            _ => Err(Error::MissingCredentials),
        }
    }
}

pub struct MongoClientBuilder {
    credentials: Option<MongoCredentials>,
    default_host: String,
}

impl Default for MongoClientBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl MongoClientBuilder {
    pub fn new() -> Self {
        Self {
            credentials: None,
            default_host: "localhost:27017".to_string(),
        }
    }

    pub fn credentials_path(mut self, path: PathBuf) -> Result<Self, Error> {
        self.credentials = Some(MongoCredentials::from_file(&path)?);
        Ok(self)
    }

    pub fn default_host(mut self, host: String) -> Self {
        self.default_host = host;
        self
    }

    pub fn build(self) -> Result<MongoClient, Error> {
        let credentials = self.credentials.ok_or(Error::MissingCredentials)?;

        Ok(MongoClient {
            credentials,
            default_host: self.default_host,
        })
    }
}

pub struct MongoClient {
    credentials: MongoCredentials,
    default_host: String,
}

impl MongoClient {
    pub async fn connect(self) -> Result<Client, Error> {
        let uri = self
            .credentials
            .build_connection_string(&self.default_host)?;

        let options = ClientOptions::parse(&uri)
            .await
            .map_err(|source| Error::MongoConnectionParse { source })?;

        Client::with_options(options).map_err(|source| Error::MongoReader { source })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn test_build_connection_string_with_uri() {
        let creds = MongoCredentials {
            username: None,
            password: None,
            uri: Some("mongodb://example.com:27017".to_string()),
        };

        let s = creds.build_connection_string("default").unwrap();
        assert_eq!(s, "mongodb://example.com:27017");
    }

    #[test]
    fn test_build_connection_string_with_user_pass() {
        let creds = MongoCredentials {
            username: Some("u".to_string()),
            password: Some("p".to_string()),
            uri: None,
        };

        let s = creds.build_connection_string("host:123").unwrap();
        assert!(s.contains("mongodb://u:p@host:123"));
    }

    #[test]
    fn test_build_connection_string_missing() {
        let creds = MongoCredentials {
            username: None,
            password: None,
            uri: None,
        };

        assert!(matches!(
            creds.build_connection_string("h"),
            Err(Error::MissingCredentials)
        ));
    }

    #[test]
    fn test_from_file_and_builder() {
        let mut path = std::env::temp_dir();
        let name = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        path.push(format!("flowgen_test_creds_{name}.json"));
        std::fs::write(&path, "{ \"MONGODB_URI\": \"mongodb://tmp:27017\" }").unwrap();

        let creds = MongoCredentials::from_file(&path).unwrap();
        assert_eq!(creds.uri.unwrap(), "mongodb://tmp:27017");

        let builder = MongoClientBuilder::new().credentials_path(path).unwrap();
        let client = builder.build();
        assert!(client.is_ok());
    }

    #[test]
    fn test_mongo_client_builder_missing_credentials() {
        let b = MongoClientBuilder::new();
        assert!(matches!(b.build(), Err(Error::MissingCredentials)));
    }

    #[test]
    fn test_mongo_client_builder_default() {
        let b: MongoClientBuilder = Default::default();
        assert!(matches!(b.build(), Err(Error::MissingCredentials)));
    }

    #[test]
    fn test_mongo_client_builder_default_host() {
        let b = MongoClientBuilder::new().default_host("custom:123".to_string());
        // default host only affects connect string; we can't observe it directly,
        // but verify it builds without error when credentials are present
        let mut path = std::env::temp_dir();
        let name = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        path.push(format!("flowgen_test_default_host_{name}.json"));
        std::fs::write(
            &path,
            "{ \"MONGODB_USERNAME\": \"u\", \"MONGODB_PASSWORD\": \"p\" }",
        )
        .unwrap();
        let client = b.credentials_path(path).unwrap().build().unwrap();
        assert_eq!(client.default_host, "custom:123");
    }

    #[test]
    fn test_build_connection_string_empty_uri_and_creds() {
        let creds = MongoCredentials {
            username: Some("".to_string()),
            password: Some("".to_string()),
            uri: Some("".to_string()),
        };

        assert!(matches!(
            creds.build_connection_string("h"),
            Err(Error::MissingCredentials)
        ));
    }

    #[tokio::test]
    async fn test_connect_with_invalid_uri() {
        // invalid uri should cause parse error
        let creds = MongoCredentials {
            username: None,
            password: None,
            uri: Some("not-a-valid-uri".to_string()),
        };

        let client = MongoClient {
            credentials: creds,
            default_host: "irrelevant".to_string(),
        };

        let res = client.connect().await;
        assert!(matches!(res, Err(Error::MongoConnectionParse { .. })));
    }
}

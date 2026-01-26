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

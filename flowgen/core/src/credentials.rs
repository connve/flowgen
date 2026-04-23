//! Shared credential types for authenticating with external services.
//!
//! Provides a unified credentials format that can be loaded from JSON files
//! and reused across HTTP requests, MCP connections, and other integrations.

use serde::{Deserialize, Serialize};
use std::path::Path;

/// Errors that can occur during credential loading.
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum Error {
    #[error("Failed to read credentials file at {path}: {source}")]
    ReadFile {
        path: String,
        #[source]
        source: std::io::Error,
    },
    #[error("Failed to parse credentials file at {path}: {source}")]
    ParseFile {
        path: String,
        #[source]
        source: serde_json::Error,
    },
}

/// HTTP authentication credentials loaded from a JSON file.
///
/// Supports Bearer token and Basic authentication. The JSON file format:
///
/// ```json
/// {
///   "bearer_auth": "my-secret-token",
///   "basic_auth": {
///     "username": "user",
///     "password": "pass"
///   }
/// }
/// ```
#[derive(PartialEq, Clone, Debug, Default, Deserialize, Serialize)]
pub struct HttpCredentials {
    /// Bearer token for `Authorization: Bearer <token>` header.
    pub bearer_auth: Option<String>,
    /// Basic authentication credentials.
    pub basic_auth: Option<BasicAuth>,
}

/// Basic authentication username and password.
#[derive(PartialEq, Clone, Debug, Default, Deserialize, Serialize)]
pub struct BasicAuth {
    /// Username for basic authentication.
    pub username: String,
    /// Password for basic authentication.
    pub password: String,
}

/// Loads and parses HTTP credentials from a JSON file.
pub async fn load_http_credentials(path: &Path) -> Result<HttpCredentials, Error> {
    let content = tokio::fs::read_to_string(path)
        .await
        .map_err(|source| Error::ReadFile {
            path: path.display().to_string(),
            source,
        })?;
    serde_json::from_str(&content).map_err(|source| Error::ParseFile {
        path: path.display().to_string(),
        source,
    })
}

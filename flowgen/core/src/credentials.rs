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

impl HttpCredentials {
    /// Returns the value for an outgoing `Authorization` header, or `None`
    /// when no credentials are set.
    ///
    /// Bearer wins over Basic when both are present, matching HTTP semantics
    /// (a single request carries one `Authorization` header).
    ///
    /// Used by transports that expose raw header injection rather than a
    /// reqwest `RequestBuilder`, so basic-auth encoding is not re-implemented
    /// per call site.
    pub fn authorization_header(&self) -> Option<String> {
        use base64::Engine;
        match (&self.bearer_auth, &self.basic_auth) {
            (Some(token), _) => Some(format!("Bearer {token}")),
            (None, Some(basic)) => {
                let raw = format!("{}:{}", basic.username, basic.password);
                let encoded = base64::engine::general_purpose::STANDARD.encode(raw);
                Some(format!("Basic {encoded}"))
            }
            (None, None) => None,
        }
    }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn authorization_header_bearer() {
        let creds = HttpCredentials {
            bearer_auth: Some("tok".to_string()),
            basic_auth: None,
        };
        assert_eq!(creds.authorization_header().as_deref(), Some("Bearer tok"));
    }

    #[test]
    fn authorization_header_basic() {
        let creds = HttpCredentials {
            bearer_auth: None,
            basic_auth: Some(BasicAuth {
                username: "Aladdin".to_string(),
                password: "open sesame".to_string(),
            }),
        };
        assert_eq!(
            creds.authorization_header().as_deref(),
            Some("Basic QWxhZGRpbjpvcGVuIHNlc2FtZQ=="),
        );
    }

    #[test]
    fn authorization_header_bearer_wins() {
        let creds = HttpCredentials {
            bearer_auth: Some("tok".to_string()),
            basic_auth: Some(BasicAuth {
                username: "u".to_string(),
                password: "p".to_string(),
            }),
        };
        assert_eq!(creds.authorization_header().as_deref(), Some("Bearer tok"));
    }

    #[test]
    fn authorization_header_none() {
        let creds = HttpCredentials::default();
        assert_eq!(creds.authorization_header(), None);
    }
}

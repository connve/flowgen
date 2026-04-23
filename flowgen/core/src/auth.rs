//! Authentication providers for user identity resolution.
//!
//! Supports JWT (local secret or JWKS), OIDC (discovery-based), and session
//! token validation (external HTTP endpoint). All providers resolve to a
//! `UserContext` containing the user ID and claims.

pub mod jwt;
pub mod oidc;
pub mod session;

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Event meta key for the authenticated user context.
pub const AUTH: &str = "auth";
use std::sync::Arc;

/// Resolved user identity from an auth provider.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserContext {
    /// Unique user identifier (extracted from JWT `sub` claim, OIDC, or session response).
    pub user_id: String,
    /// All claims/attributes from the auth token.
    #[serde(default)]
    pub claims: HashMap<String, serde_json::Value>,
}

/// Auth provider errors.
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum AuthError {
    #[error("No authorization token provided.")]
    NoToken,
    #[error("Invalid or expired token: {source}")]
    InvalidToken {
        #[source]
        source: jsonwebtoken::errors::Error,
    },
    #[error("JWKS fetch failed: {source}")]
    JwksFetchFailed {
        #[source]
        source: reqwest::Error,
    },
    #[error("OIDC discovery failed: {source}")]
    OidcDiscoveryFailed {
        #[source]
        source: reqwest::Error,
    },
    #[error("Session validation request failed: {source}")]
    SessionRequestFailed {
        #[source]
        source: reqwest::Error,
    },
    #[error("Session validation returned status {status}.")]
    SessionRejected { status: u16 },
    #[error("Session validation response parse failed: {source}")]
    SessionParseFailed {
        #[source]
        source: reqwest::Error,
    },
    #[error("Missing user_id claim '{claim}' in token.")]
    MissingUserIdClaim { claim: String },
    #[error("JWT config requires either 'secret' or 'jwks_url'.")]
    MissingDecodingKey,
    #[error("JWT header missing 'kid' for JWKS validation.")]
    MissingKeyId,
    #[error("No matching JWKS key for kid '{kid}'.")]
    UnknownKeyId { kid: String },
    #[error("Failed to build decoding key from JWK: {source}")]
    JwkDecodingKey {
        #[source]
        source: jsonwebtoken::errors::Error,
    },
    #[error("No decoding key configured.")]
    NoDecodingKey,
}

/// Auth provider trait — validates a token and returns user context.
#[async_trait::async_trait]
pub trait AuthProvider: std::fmt::Debug + Send + Sync + 'static {
    /// Validate a bearer token and extract user context.
    async fn validate(&self, token: &str) -> Result<UserContext, AuthError>;
}

/// Auth provider configuration (deserialized from worker YAML).
#[derive(PartialEq, Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum AuthConfig {
    /// JWT validation with local secret or JWKS URL.
    Jwt(jwt::JwtConfig),
    /// OIDC discovery-based JWT validation.
    Oidc(oidc::OidcConfig),
    /// Session token validation via external HTTP endpoint.
    Session(session::SessionConfig),
}

impl AuthConfig {
    /// Build an auth provider from this configuration.
    pub async fn build(self) -> Result<Arc<dyn AuthProvider>, AuthError> {
        match self {
            AuthConfig::Jwt(config) => {
                let provider = jwt::JwtProvider::new(config).await?;
                Ok(Arc::new(provider))
            }
            AuthConfig::Oidc(config) => {
                let provider = oidc::OidcProvider::new(config).await?;
                Ok(Arc::new(provider))
            }
            AuthConfig::Session(config) => {
                let provider = session::SessionProvider::new(config);
                Ok(Arc::new(provider))
            }
        }
    }
}

/// Task-level auth configuration.
#[derive(PartialEq, Clone, Debug, Default, Deserialize, Serialize)]
pub struct TaskAuthConfig {
    /// When true, reject requests without a valid auth token.
    #[serde(default)]
    pub required: bool,
}

/// Extract bearer token from an Authorization header value.
pub fn extract_bearer_token(auth_header: &str) -> Option<&str> {
    auth_header.strip_prefix("Bearer ")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_bearer_token() {
        assert_eq!(extract_bearer_token("Bearer abc123"), Some("abc123"));
        assert_eq!(extract_bearer_token("Basic dXNlcjpwYXNz"), None);
        assert_eq!(extract_bearer_token("bearer abc"), None);
        assert_eq!(extract_bearer_token(""), None);
    }

    #[test]
    fn test_user_context_serialization() {
        let ctx = UserContext {
            user_id: "user-123".to_string(),
            claims: HashMap::from([
                ("email".to_string(), serde_json::json!("user@example.com")),
                ("role".to_string(), serde_json::json!("admin")),
            ]),
        };
        let json = serde_json::to_value(&ctx).unwrap();
        assert_eq!(json["user_id"], "user-123");
        assert_eq!(json["claims"]["email"], "user@example.com");
    }

    #[test]
    fn test_task_auth_config_default() {
        let config = TaskAuthConfig::default();
        assert!(!config.required);
    }
}

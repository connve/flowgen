//! Session token authentication provider.
//!
//! Validates session tokens by calling an external HTTP endpoint.
//! The endpoint receives the token and returns user identity in its response.

use super::{AuthError, AuthProvider, UserContext};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Session provider configuration.
#[derive(PartialEq, Clone, Debug, Deserialize, Serialize)]
pub struct SessionConfig {
    /// External validation endpoint URL.
    /// The token is sent as a Bearer token in the Authorization header.
    pub validation_url: String,
    /// JSON field in the validation response containing the user ID (default: "user_id").
    #[serde(default = "default_user_id_field")]
    pub user_id_field: String,
}

fn default_user_id_field() -> String {
    "user_id".to_string()
}

/// Session token authentication provider.
#[derive(Debug)]
pub struct SessionProvider {
    config: SessionConfig,
    client: reqwest::Client,
}

impl SessionProvider {
    /// Create a new session provider.
    pub fn new(config: SessionConfig) -> Self {
        Self {
            config,
            client: reqwest::Client::new(),
        }
    }
}

#[async_trait::async_trait]
impl AuthProvider for SessionProvider {
    async fn validate(&self, token: &str) -> Result<UserContext, AuthError> {
        let response = self
            .client
            .get(&self.config.validation_url)
            .bearer_auth(token)
            .send()
            .await
            .map_err(|source| AuthError::SessionRequestFailed { source })?;

        if !response.status().is_success() {
            return Err(AuthError::SessionRejected {
                status: response.status().as_u16(),
            });
        }

        let body: HashMap<String, serde_json::Value> = response
            .json()
            .await
            .map_err(|source| AuthError::SessionParseFailed { source })?;

        let user_id = body
            .get(&self.config.user_id_field)
            .and_then(|v| v.as_str())
            .ok_or_else(|| AuthError::MissingUserIdClaim {
                claim: self.config.user_id_field.clone(),
            })?
            .to_string();

        Ok(UserContext {
            user_id,
            claims: body,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_session_config_default_field() {
        let config: SessionConfig =
            serde_json::from_str(r#"{"validation_url": "https://auth.example.com/validate"}"#)
                .unwrap();
        assert_eq!(config.user_id_field, "user_id");
    }

    #[test]
    fn test_session_config_serialization() {
        let config = SessionConfig {
            validation_url: "https://auth.example.com/validate".to_string(),
            user_id_field: "sub".to_string(),
        };
        let json = serde_json::to_string(&config).unwrap();
        let deserialized: SessionConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(config, deserialized);
    }
}

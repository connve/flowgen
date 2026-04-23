//! JWT authentication provider.
//!
//! Validates JWT tokens using either a local HMAC secret (HS256) or
//! remote JWKS keys (RS256/ES256). Extracts user identity from a
//! configurable claim (default: `sub`).

use super::{AuthError, AuthProvider, UserContext};
use jsonwebtoken::{decode, decode_header, jwk::JwkSet, Algorithm, DecodingKey, Validation};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::info;

/// JWT provider configuration.
#[derive(PartialEq, Clone, Debug, Deserialize, Serialize)]
pub struct JwtConfig {
    /// HMAC secret for HS256 validation (mutually exclusive with `jwks_url`).
    pub secret: Option<String>,
    /// JWKS endpoint URL for RS256/ES256 validation (mutually exclusive with `secret`).
    pub jwks_url: Option<String>,
    /// Expected audience claim. When set, tokens without this audience are rejected.
    pub audience: Option<String>,
    /// Expected issuer claim. When set, tokens from other issuers are rejected.
    pub issuer: Option<String>,
    /// Claim name to extract as user_id (default: "sub").
    #[serde(default = "default_user_id_claim")]
    pub user_id_claim: String,
}

fn default_user_id_claim() -> String {
    "sub".to_string()
}

/// JWT authentication provider.
pub struct JwtProvider {
    config: JwtConfig,
    /// Cached JWKS keys (fetched once at init, refreshable).
    jwks: Option<Arc<RwLock<JwkSet>>>,
    /// Pre-built decoding key for HMAC secret.
    hmac_key: Option<DecodingKey>,
}

impl std::fmt::Debug for JwtProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JwtProvider")
            .field("config", &self.config)
            .field("has_jwks", &self.jwks.is_some())
            .field("has_hmac_key", &self.hmac_key.is_some())
            .finish()
    }
}

impl JwtProvider {
    /// Create a new JWT provider from config.
    ///
    /// If `jwks_url` is configured, fetches the JWKS keys at init time.
    pub async fn new(config: JwtConfig) -> Result<Self, AuthError> {
        let hmac_key = config
            .secret
            .as_ref()
            .map(|s| DecodingKey::from_secret(s.as_bytes()));

        let jwks = if let Some(ref url) = config.jwks_url {
            let jwks = fetch_jwks(url).await?;
            info!(url = %url, keys = jwks.keys.len(), "Fetched JWKS keys for JWT validation.");
            Some(Arc::new(RwLock::new(jwks)))
        } else {
            None
        };

        if hmac_key.is_none() && jwks.is_none() {
            return Err(AuthError::MissingDecodingKey);
        }

        Ok(Self {
            config,
            jwks,
            hmac_key,
        })
    }

    /// Build validation parameters from config.
    fn build_validation(&self, algorithms: &[Algorithm]) -> Validation {
        let mut validation = Validation::new(algorithms[0]);
        validation.algorithms = algorithms.to_vec();

        if let Some(ref aud) = self.config.audience {
            validation.set_audience(&[aud]);
        } else {
            validation.validate_aud = false;
        }

        if let Some(ref iss) = self.config.issuer {
            validation.set_issuer(&[iss]);
        }

        validation
    }

    /// Extract UserContext from validated JWT claims.
    fn extract_user_context(
        &self,
        claims: &HashMap<String, serde_json::Value>,
    ) -> Result<UserContext, AuthError> {
        let user_id = claims
            .get(&self.config.user_id_claim)
            .and_then(|v| v.as_str())
            .ok_or_else(|| AuthError::MissingUserIdClaim {
                claim: self.config.user_id_claim.clone(),
            })?
            .to_string();

        Ok(UserContext {
            user_id,
            claims: claims.clone(),
        })
    }
}

#[async_trait::async_trait]
impl AuthProvider for JwtProvider {
    async fn validate(&self, token: &str) -> Result<UserContext, AuthError> {
        // HMAC (HS256) path — use pre-built key.
        if let Some(ref key) = self.hmac_key {
            let validation = self.build_validation(&[Algorithm::HS256]);
            let token_data = decode::<HashMap<String, serde_json::Value>>(token, key, &validation)
                .map_err(|source| AuthError::InvalidToken { source })?;
            return self.extract_user_context(&token_data.claims);
        }

        // JWKS (RS256/ES256) path — match kid from header to JWKS.
        if let Some(ref jwks) = self.jwks {
            let header =
                decode_header(token).map_err(|source| AuthError::InvalidToken { source })?;

            let kid = header.kid.as_deref().ok_or(AuthError::MissingKeyId)?;

            let jwks_read = jwks.read().await;
            let jwk = jwks_read.find(kid).ok_or_else(|| AuthError::UnknownKeyId {
                kid: kid.to_string(),
            })?;

            let key = DecodingKey::from_jwk(jwk)
                .map_err(|source| AuthError::JwkDecodingKey { source })?;

            let algorithm = jwk
                .common
                .key_algorithm
                .and_then(|a| match a {
                    jsonwebtoken::jwk::KeyAlgorithm::RS256 => Some(Algorithm::RS256),
                    jsonwebtoken::jwk::KeyAlgorithm::RS384 => Some(Algorithm::RS384),
                    jsonwebtoken::jwk::KeyAlgorithm::RS512 => Some(Algorithm::RS512),
                    jsonwebtoken::jwk::KeyAlgorithm::ES256 => Some(Algorithm::ES256),
                    jsonwebtoken::jwk::KeyAlgorithm::ES384 => Some(Algorithm::ES384),
                    _ => None,
                })
                .unwrap_or(Algorithm::RS256);

            let validation = self.build_validation(&[algorithm]);
            let token_data = decode::<HashMap<String, serde_json::Value>>(token, &key, &validation)
                .map_err(|source| AuthError::InvalidToken { source })?;
            return self.extract_user_context(&token_data.claims);
        }

        Err(AuthError::NoDecodingKey)
    }
}

/// Fetch JWKS from a remote URL.
pub async fn fetch_jwks(url: &str) -> Result<JwkSet, AuthError> {
    let response = reqwest::get(url)
        .await
        .map_err(|source| AuthError::JwksFetchFailed { source })?;

    response
        .json::<JwkSet>()
        .await
        .map_err(|source| AuthError::JwksFetchFailed { source })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_jwt_config_default_claim() {
        let config: JwtConfig = serde_json::from_str(r#"{"secret": "test-secret"}"#).unwrap();
        assert_eq!(config.user_id_claim, "sub");
    }

    #[test]
    fn test_jwt_config_custom_claim() {
        let config: JwtConfig =
            serde_json::from_str(r#"{"secret": "test-secret", "user_id_claim": "email"}"#).unwrap();
        assert_eq!(config.user_id_claim, "email");
    }

    #[tokio::test]
    async fn test_jwt_provider_requires_secret_or_jwks() {
        let config = JwtConfig {
            secret: None,
            jwks_url: None,
            audience: None,
            issuer: None,
            user_id_claim: "sub".to_string(),
        };
        let result = JwtProvider::new(config).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_jwt_hs256_validation() {
        let secret = "test-secret-key-at-least-32-bytes!!";
        let config = JwtConfig {
            secret: Some(secret.to_string()),
            jwks_url: None,
            audience: None,
            issuer: None,
            user_id_claim: "sub".to_string(),
        };
        let provider = JwtProvider::new(config).await.unwrap();

        // Create a valid token with exp claim.
        let exp = chrono::Utc::now().timestamp() as u64 + 3600;
        let claims = HashMap::from([
            ("sub".to_string(), serde_json::json!("user-42")),
            ("email".to_string(), serde_json::json!("user@test.com")),
            ("exp".to_string(), serde_json::json!(exp)),
        ]);
        let token = jsonwebtoken::encode(
            &jsonwebtoken::Header::new(Algorithm::HS256),
            &claims,
            &jsonwebtoken::EncodingKey::from_secret(secret.as_bytes()),
        )
        .unwrap();

        let user_ctx = provider.validate(&token).await.unwrap();
        assert_eq!(user_ctx.user_id, "user-42");
        assert_eq!(user_ctx.claims["email"], "user@test.com");
    }

    #[tokio::test]
    async fn test_jwt_hs256_invalid_token() {
        let config = JwtConfig {
            secret: Some("my-secret".to_string()),
            jwks_url: None,
            audience: None,
            issuer: None,
            user_id_claim: "sub".to_string(),
        };
        let provider = JwtProvider::new(config).await.unwrap();

        let result = provider.validate("invalid.token.here").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_jwt_missing_user_id_claim() {
        let secret = "test-secret-key-at-least-32-bytes!!";
        let config = JwtConfig {
            secret: Some(secret.to_string()),
            jwks_url: None,
            audience: None,
            issuer: None,
            user_id_claim: "user_id".to_string(),
        };
        let provider = JwtProvider::new(config).await.unwrap();

        // Token with "sub" but not "user_id".
        let exp = chrono::Utc::now().timestamp() as u64 + 3600;
        let claims = HashMap::from([
            ("sub".to_string(), serde_json::json!("user-1")),
            ("exp".to_string(), serde_json::json!(exp)),
        ]);
        let token = jsonwebtoken::encode(
            &jsonwebtoken::Header::new(Algorithm::HS256),
            &claims,
            &jsonwebtoken::EncodingKey::from_secret(secret.as_bytes()),
        )
        .unwrap();

        let result = provider.validate(&token).await;
        assert!(matches!(result, Err(AuthError::MissingUserIdClaim { .. })));
    }
}

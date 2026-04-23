//! OIDC authentication provider.
//!
//! Discovers JWKS endpoint from the OpenID Connect discovery document,
//! then delegates JWT validation to the JWT provider.

use super::jwt::{fetch_jwks, JwtConfig, JwtProvider};
use super::{AuthError, AuthProvider, UserContext};
use serde::{Deserialize, Serialize};
use tracing::info;

/// OIDC provider configuration.
#[derive(PartialEq, Clone, Debug, Deserialize, Serialize)]
pub struct OidcConfig {
    /// OIDC issuer URL (e.g., "https://auth.example.com/realms/myapp").
    /// The discovery document is fetched from `{issuer_url}/.well-known/openid-configuration`.
    pub issuer_url: String,
    /// Expected audience claim.
    pub audience: Option<String>,
    /// Claim name to extract as user_id (default: "sub").
    #[serde(default = "default_user_id_claim")]
    pub user_id_claim: String,
}

fn default_user_id_claim() -> String {
    "sub".to_string()
}

/// Minimal OIDC discovery document (only fields we need).
#[derive(Deserialize)]
struct OidcDiscovery {
    jwks_uri: String,
    issuer: String,
}

/// OIDC authentication provider — thin wrapper around JWT with JWKS discovery.
#[derive(Debug)]
pub struct OidcProvider {
    jwt_provider: JwtProvider,
}

impl OidcProvider {
    /// Create a new OIDC provider by fetching the discovery document.
    pub async fn new(config: OidcConfig) -> Result<Self, AuthError> {
        let discovery_url = format!(
            "{}/.well-known/openid-configuration",
            config.issuer_url.trim_end_matches('/')
        );

        let discovery: OidcDiscovery = reqwest::get(&discovery_url)
            .await
            .map_err(|source| AuthError::OidcDiscoveryFailed { source })?
            .json()
            .await
            .map_err(|source| AuthError::OidcDiscoveryFailed { source })?;

        info!(
            issuer = %discovery.issuer,
            jwks_uri = %discovery.jwks_uri,
            "OIDC discovery complete."
        );

        // Verify the discovered JWKS endpoint is reachable.
        let _jwks = fetch_jwks(&discovery.jwks_uri).await?;

        let jwt_config = JwtConfig {
            secret: None,
            jwks_url: Some(discovery.jwks_uri),
            audience: config.audience,
            issuer: Some(discovery.issuer),
            user_id_claim: config.user_id_claim,
        };

        let jwt_provider = JwtProvider::new(jwt_config).await?;

        Ok(Self { jwt_provider })
    }
}

#[async_trait::async_trait]
impl AuthProvider for OidcProvider {
    async fn validate(&self, token: &str) -> Result<UserContext, AuthError> {
        self.jwt_provider.validate(token).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_oidc_config_serialization() {
        let config = OidcConfig {
            issuer_url: "https://auth.example.com/realms/test".to_string(),
            audience: Some("my-app".to_string()),
            user_id_claim: "sub".to_string(),
        };
        let json = serde_json::to_string(&config).unwrap();
        let deserialized: OidcConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(config, deserialized);
    }

    #[test]
    fn test_oidc_config_default_claim() {
        let config: OidcConfig =
            serde_json::from_str(r#"{"issuer_url": "https://auth.example.com"}"#).unwrap();
        assert_eq!(config.user_id_claim, "sub");
    }
}

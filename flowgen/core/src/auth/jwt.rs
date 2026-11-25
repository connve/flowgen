//! JWT authentication provider.
//!
//! Validates JWT tokens using either a local HMAC secret (HS256) or
//! remote JWKS keys (RS256/ES256). Extracts user identity from a
//! configurable claim (default: `sub`).

use super::{AuthError, AuthProvider, UserContext};
use jsonwebtoken::{
    decode, decode_header,
    jwk::{Jwk, JwkSet},
    Algorithm, DecodingKey, Validation,
};
use secrecy::{ExposeSecret, SecretString};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tracing::info;

/// JWT provider configuration.
///
/// `secret` is a [`SecretString`] so the HMAC key never leaks through
/// `Debug`, logs, or panics. Serialization always emits `"***"` in its
/// place — nothing in the runtime round-trips config back from its
/// serialized form, so the redacted value is only ever seen by the
/// config viewer, never re-parsed.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct JwtConfig {
    /// HMAC secret for HS256 validation (mutually exclusive with `jwks_url`).
    #[serde(default, serialize_with = "serialize_redacted")]
    pub secret: Option<SecretString>,
    /// JWKS endpoint URL for RS256/ES256 validation (mutually exclusive with `secret`).
    pub jwks_url: Option<String>,
    /// Expected audience claim. When set, tokens without this audience are rejected.
    pub audience: Option<String>,
    /// Expected issuer claim. When set, tokens from other issuers are rejected.
    pub issuer: Option<String>,
    /// Claim name to extract as user_id (default: "sub").
    #[serde(default = "default_user_id_claim")]
    pub user_id_claim: String,
    /// How long after fetching the JWKS a token signed with a key it lacks
    /// makes it fetch again, which picks up the provider's next signing key,
    /// e.g. `5m`.
    #[serde(default = "default_jwks_refresh", with = "humantime_serde")]
    pub jwks_refresh: Duration,
}

/// Five minutes between JWKS fetches for unknown keys.
pub fn default_jwks_refresh() -> Duration {
    Duration::from_secs(300)
}

impl PartialEq for JwtConfig {
    /// Compares config for equality, treating the secret by presence only
    /// (`SecretString` deliberately has no `PartialEq` to discourage
    /// timing-sensitive comparisons; config equality never needs the value).
    fn eq(&self, other: &Self) -> bool {
        self.secret.is_some() == other.secret.is_some()
            && self.jwks_url == other.jwks_url
            && self.audience == other.audience
            && self.issuer == other.issuer
            && self.user_id_claim == other.user_id_claim
            && self.jwks_refresh == other.jwks_refresh
    }
}

/// Serializes an optional secret as `"***"` when present, `null` when
/// absent, so a serialized config never carries the plaintext key.
fn serialize_redacted<S>(secret: &Option<SecretString>, s: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    match secret {
        Some(_) => s.serialize_some("***"),
        None => s.serialize_none(),
    }
}

fn default_user_id_claim() -> String {
    "sub".to_string()
}

/// JWT authentication provider.
pub struct JwtProvider {
    config: JwtConfig,
    /// Cached JWKS keys, fetched again for a key they lack.
    jwks: Option<Arc<RwLock<Jwks>>>,
    /// Pre-built decoding key for HMAC secret.
    hmac_key: Option<DecodingKey>,
}

/// The JWKS and when it was last fetched.
struct Jwks {
    keys: JwkSet,
    fetched: Instant,
}

impl Jwks {
    /// The key named `kid`, or that there is none when the keys were fetched
    /// within `refresh`; `None` when they may be fetched again.
    fn cached(&self, kid: &str, refresh: Duration) -> Option<Result<Jwk, AuthError>> {
        match self.keys.find(kid) {
            Some(jwk) => Some(Ok(jwk.clone())),
            None if self.fetched.elapsed() < refresh => Some(Err(AuthError::UnknownKeyId {
                kid: kid.to_string(),
            })),
            None => None,
        }
    }
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
            .map(|s| DecodingKey::from_secret(s.expose_secret().as_bytes()));

        let jwks = if let Some(ref url) = config.jwks_url {
            let jwks = fetch_jwks(url).await?;
            info!(url = %url, keys = jwks.keys.len(), "Fetched JWKS keys for JWT validation");
            Some(Arc::new(RwLock::new(Jwks {
                keys: jwks,
                fetched: Instant::now(),
            })))
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

    /// The key named `kid`. A key the cached JWKS lacks makes it fetch again,
    /// at most once per `jwks_refresh`, so the provider's next signing key is
    /// picked up while made-up key ids can't make every request fetch.
    async fn jwk(&self, jwks: &RwLock<Jwks>, kid: &str) -> Result<Jwk, AuthError> {
        let refresh = self.config.jwks_refresh;
        if let Some(found) = jwks.read().await.cached(kid, refresh) {
            return found;
        }
        let Some(url) = &self.config.jwks_url else {
            return Err(AuthError::UnknownKeyId {
                kid: kid.to_string(),
            });
        };
        {
            let mut cached = jwks.write().await;
            if let Some(found) = cached.cached(kid, refresh) {
                return found;
            }
            // Claims the fetch, failed or not, so requests meanwhile neither
            // start their own nor wait for it.
            cached.fetched = Instant::now();
        }
        let keys = fetch_jwks(url).await?;
        info!(url = %url, keys = keys.keys.len(), "Fetched JWKS keys for JWT validation");
        let mut cached = jwks.write().await;
        cached.keys = keys;
        match cached.keys.find(kid) {
            Some(jwk) => Ok(jwk.clone()),
            None => Err(AuthError::UnknownKeyId {
                kid: kid.to_string(),
            }),
        }
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
            let jwk = self.jwk(jwks, kid).await?;

            let key = DecodingKey::from_jwk(&jwk)
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

    #[test]
    fn test_jwt_secret_redacted_on_serialize() {
        let config: JwtConfig =
            serde_json::from_str(r#"{"secret": "super-secret-hmac-key"}"#).unwrap();
        let out = serde_json::to_string(&config).unwrap();
        assert!(
            !out.contains("super-secret-hmac-key"),
            "plaintext secret leaked into serialized output: {out}"
        );
        assert!(
            out.contains("\"***\""),
            "expected redaction marker in {out}"
        );
    }

    #[tokio::test]
    async fn test_jwt_provider_requires_secret_or_jwks() {
        let config = JwtConfig {
            secret: None,
            jwks_url: None,
            audience: None,
            issuer: None,
            user_id_claim: "sub".to_string(),
            jwks_refresh: default_jwks_refresh(),
        };
        let result = JwtProvider::new(config).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_jwt_hs256_validation() {
        let secret = "test-secret-key-at-least-32-bytes!!";
        let config = JwtConfig {
            secret: Some(secret.to_string().into()),
            jwks_url: None,
            audience: None,
            issuer: None,
            user_id_claim: "sub".to_string(),
            jwks_refresh: default_jwks_refresh(),
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
            secret: Some("my-secret".to_string().into()),
            jwks_url: None,
            audience: None,
            issuer: None,
            user_id_claim: "sub".to_string(),
            jwks_refresh: default_jwks_refresh(),
        };
        let provider = JwtProvider::new(config).await.unwrap();

        let result = provider.validate("invalid.token.here").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_jwt_missing_user_id_claim() {
        let secret = "test-secret-key-at-least-32-bytes!!";
        let config = JwtConfig {
            secret: Some(secret.to_string().into()),
            jwks_url: None,
            audience: None,
            issuer: None,
            user_id_claim: "user_id".to_string(),
            jwks_refresh: default_jwks_refresh(),
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

    fn rsa_jwk(kid: &str) -> Jwk {
        Jwk {
            common: jsonwebtoken::jwk::CommonParameters {
                key_id: Some(kid.to_string()),
                key_algorithm: Some(jsonwebtoken::jwk::KeyAlgorithm::RS256),
                ..Default::default()
            },
            algorithm: jsonwebtoken::jwk::AlgorithmParameters::RSA(
                jsonwebtoken::jwk::RSAKeyParameters {
                    key_type: jsonwebtoken::jwk::RSAKeyType::RSA,
                    n: "AQAB".to_string(),
                    e: "AQAB".to_string(),
                },
            ),
        }
    }

    /// Serves a JWKS whose key is `a` on the first fetch and `b` after it.
    async fn rotating_jwks(fetches: Arc<std::sync::atomic::AtomicUsize>) -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/jwks", listener.local_addr().unwrap());
        let app = axum::Router::new().route(
            "/jwks",
            axum::routing::get(move || {
                let fetches = Arc::clone(&fetches);
                async move {
                    let kid = match fetches.fetch_add(1, std::sync::atomic::Ordering::SeqCst) {
                        0 => "a",
                        _ => "b",
                    };
                    axum::Json(JwkSet {
                        keys: vec![rsa_jwk(kid)],
                    })
                }
            }),
        );
        tokio::spawn(async move { axum::serve(listener, app).await });
        url
    }

    fn token_signed_with(kid: &str) -> String {
        use base64::Engine;
        let encode =
            |bytes: Vec<u8>| base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes);
        let mut header = jsonwebtoken::Header::new(Algorithm::RS256);
        header.kid = Some(kid.to_string());
        let claims = HashMap::from([("sub".to_string(), "u1".to_string())]);
        format!(
            "{}.{}.{}",
            encode(serde_json::to_vec(&header).unwrap()),
            encode(serde_json::to_vec(&claims).unwrap()),
            encode(b"signature".to_vec())
        )
    }

    #[tokio::test]
    async fn a_token_signed_with_a_new_key_fetches_the_keys_again_at_most_once_per_refresh() {
        let fetches = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let url = rotating_jwks(Arc::clone(&fetches)).await;
        let provider = |jwks_refresh| {
            JwtProvider::new(JwtConfig {
                secret: None,
                jwks_url: Some(url.clone()),
                audience: None,
                issuer: None,
                user_id_claim: "sub".to_string(),
                jwks_refresh,
            })
        };

        let refreshing = provider(Duration::ZERO).await.unwrap();
        let rotated = refreshing.validate(&token_signed_with("b")).await;
        assert!(
            matches!(rotated, Err(AuthError::InvalidToken { .. })),
            "{rotated:?}"
        );
        assert_eq!(fetches.load(std::sync::atomic::Ordering::SeqCst), 2);

        let recent = provider(Duration::from_secs(3600)).await.unwrap();
        let unknown = recent.validate(&token_signed_with("c")).await;
        assert!(
            matches!(unknown, Err(AuthError::UnknownKeyId { .. })),
            "{unknown:?}"
        );
        assert_eq!(fetches.load(std::sync::atomic::Ordering::SeqCst), 3);
    }
}

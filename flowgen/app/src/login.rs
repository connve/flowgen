//! Browser-facing OIDC login for the admin web UI: authorization code +
//! PKCE, delegating ID-token verification to
//! [`flowgen_core::auth::oidc::OidcProvider`] so it shares the same
//! discovery/JWKS/JWT-validate path as bearer-token auth on the other
//! servers.
//!
//! This is the *client* half of OIDC (redirect the browser, exchange a code
//! for tokens) — the other servers only ever validate a token someone else
//! already obtained, so this code has no home in `flowgen_core`.
//!
//! Deliberately stateless: flowgen never stores a session server-side. The
//! caller keeps [`LoginState`] (PKCE verifier, state, nonce) in a
//! short-lived encrypted cookie between `authorize_url` and `exchange_code`,
//! and persists the resulting tokens the same way (see `web.rs`'s auth
//! cookie) — this module has no session store of its own.

use flowgen_core::auth::oidc::{OidcConfig, OidcProvider};
use flowgen_core::auth::{AuthProvider, UserContext};
use oauth2::basic::{BasicClient, BasicTokenResponse};
use oauth2::{
    AuthUrl, AuthorizationCode, ClientId, ClientSecret, CsrfToken, EndpointNotSet, EndpointSet,
    PkceCodeChallenge, PkceCodeVerifier, RedirectUrl, Scope, TokenResponse, TokenUrl,
};
use serde::{Deserialize, Serialize};

/// Errors from the interactive login flow. Separate from
/// `flowgen_core::auth::AuthError` — that type covers token *validation*
/// only and is `#[non_exhaustive]` from outside its crate; this flow has
/// failure modes (bad callback state, missing id_token) that belong to the
/// admin UI, not the shared auth-provider abstraction.
#[derive(thiserror::Error, Debug)]
pub enum LoginError {
    #[error("OIDC discovery failed: {0}")]
    Discovery(#[source] reqwest::Error),
    #[error("Invalid issuer, redirect, or endpoint URL: {0}")]
    InvalidUrl(String),
    #[error("Failed to build ID-token validator: {0}")]
    Validator(#[source] flowgen_core::auth::AuthError),
    #[error("Code exchange with the identity provider failed: {0}")]
    Exchange(String),
    #[error("Identity provider did not return an id_token.")]
    MissingIdToken,
    #[error("Callback `state` did not match the value issued at login.")]
    InvalidState,
    #[error("ID token `nonce` did not match the value issued at login.")]
    InvalidNonce,
    #[error("ID token failed validation: {0}")]
    InvalidIdToken(#[source] flowgen_core::auth::AuthError),
}

/// Discovery document fields needed to build the OAuth2 client. Distinct
/// from `OidcProvider`'s internal copy — that one only surfaces
/// `jwks_uri`/`issuer`; this needs the authorize/token endpoints too.
#[derive(Deserialize)]
struct Discovery {
    authorization_endpoint: String,
    token_endpoint: String,
}

/// Config for interactive browser login — the `web.auth` field. Distinct
/// from `flowgen_core::auth::oidc::OidcConfig`, which only carries what's
/// needed to validate a token someone else already has (issuer, audience,
/// claim name); this needs OAuth2 client credentials and a redirect URI to
/// obtain one in the first place.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct LoginConfig {
    /// OIDC issuer URL. Discovery fetched from
    /// `{issuer_url}/.well-known/openid-configuration`. Works with any
    /// standard-compliant IdP (Okta, Zitadel, Auth0, ...) — including one
    /// that itself federates to an upstream customer IdP, since discovery
    /// and token validation look identical either way from here.
    pub issuer_url: String,
    pub client_id: String,
    #[serde(serialize_with = "serialize_redacted")]
    pub client_secret: secrecy::SecretString,
    /// Must exactly match a redirect URI registered with the IdP, e.g.
    /// `https://flowgen.example.com/auth/callback`.
    pub redirect_uri: String,
    /// Additional scopes beyond `openid`, `profile`, `email` (always
    /// requested).
    #[serde(default)]
    pub extra_scopes: Vec<String>,
}

impl PartialEq for LoginConfig {
    /// Compares the secret by presence only, matching `JwtConfig`'s
    /// convention (`SecretString` deliberately has no `PartialEq`, to
    /// discourage timing-sensitive comparisons config equality never needs).
    fn eq(&self, other: &Self) -> bool {
        use secrecy::ExposeSecret;
        self.issuer_url == other.issuer_url
            && self.client_id == other.client_id
            && self.client_secret.expose_secret() == other.client_secret.expose_secret()
            && self.redirect_uri == other.redirect_uri
            && self.extra_scopes == other.extra_scopes
    }
}

fn serialize_redacted<S>(_: &secrecy::SecretString, s: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    s.serialize_str("***")
}

/// Everything that must survive the browser round-trip to the IdP and back.
/// The caller stores this in a short-lived encrypted cookie between
/// `authorize_url` and `exchange_code`.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LoginState {
    pub csrf_state: String,
    pub nonce: String,
    pkce_verifier: String,
}

/// A verified login: the raw tokens (the caller decides how to persist
/// them — see `web.rs`'s encrypted auth cookie) plus the resolved identity.
pub struct LoginResult {
    pub user: UserContext,
    pub id_token: String,
    pub refresh_token: Option<String>,
    pub expires_in: Option<u64>,
}

type OidcOauthClient =
    BasicClient<EndpointSet, EndpointNotSet, EndpointNotSet, EndpointNotSet, EndpointSet>;

/// Browser-facing OIDC login client. Build once at startup (discovery is a
/// network round trip) and share via `Arc`.
pub struct LoginClient {
    client: OidcOauthClient,
    http: oauth2::reqwest::Client,
    scopes: Vec<Scope>,
    /// Validates the ID token returned by the exchange — shares the same
    /// JWKS-backed path used for bearer-token validation on the other
    /// servers, rather than reimplementing JWT verification here.
    id_token_validator: OidcProvider,
}

impl LoginClient {
    /// Fetches the discovery document and builds the OAuth2 + ID-token
    /// validation clients. Call once at startup.
    pub async fn new(config: LoginConfig) -> Result<Self, LoginError> {
        use secrecy::ExposeSecret;

        let discovery_url = format!(
            "{}/.well-known/openid-configuration",
            config.issuer_url.trim_end_matches('/')
        );
        let discovery: Discovery = reqwest::get(&discovery_url)
            .await
            .map_err(LoginError::Discovery)?
            .json()
            .await
            .map_err(LoginError::Discovery)?;

        let auth_url = AuthUrl::new(discovery.authorization_endpoint)
            .map_err(|source| LoginError::InvalidUrl(source.to_string()))?;
        let token_url = TokenUrl::new(discovery.token_endpoint)
            .map_err(|source| LoginError::InvalidUrl(source.to_string()))?;
        let redirect_url = RedirectUrl::new(config.redirect_uri.clone())
            .map_err(|source| LoginError::InvalidUrl(source.to_string()))?;

        let client = BasicClient::new(ClientId::new(config.client_id.clone()))
            .set_client_secret(ClientSecret::new(
                config.client_secret.expose_secret().to_string(),
            ))
            .set_auth_uri(auth_url)
            .set_token_uri(token_url)
            .set_redirect_uri(redirect_url);

        let id_token_validator = OidcProvider::new(OidcConfig {
            issuer_url: config.issuer_url.clone(),
            audience: Some(config.client_id.clone()),
            user_id_claim: "sub".to_string(),
        })
        .await
        .map_err(LoginError::Validator)?;

        let mut scopes = vec![
            Scope::new("openid".to_string()),
            Scope::new("profile".to_string()),
            Scope::new("email".to_string()),
        ];
        scopes.extend(config.extra_scopes.into_iter().map(Scope::new));

        // A redirect on the token-exchange POST would let a malicious or
        // compromised endpoint redirect the request (with our client
        // credentials) to an attacker-controlled host — `oauth2`'s own docs
        // call this out and every example disables it.
        let http = oauth2::reqwest::Client::builder()
            .redirect(oauth2::reqwest::redirect::Policy::none())
            .build()
            .map_err(|source| LoginError::InvalidUrl(source.to_string()))?;

        Ok(Self {
            client,
            http,
            scopes,
            id_token_validator,
        })
    }

    /// Builds the URL to redirect the browser to, plus the state the caller
    /// must stash (in an encrypted cookie) until the callback arrives.
    pub fn authorize_url(&self) -> (String, LoginState) {
        let (pkce_challenge, pkce_verifier) = PkceCodeChallenge::new_random_sha256();
        // No first-class `Nonce` type in `oauth2` (that's an
        // OIDC-specific concept the base OAuth2 crate doesn't model) — mint
        // one the same way the crate mints `state`, and verify it against
        // the ID token's `nonce` claim ourselves in `exchange_code`.
        let nonce = CsrfToken::new_random();

        let mut request = self
            .client
            .authorize_url(CsrfToken::new_random)
            .set_pkce_challenge(pkce_challenge)
            .add_extra_param("nonce", nonce.secret().clone());
        for scope in &self.scopes {
            request = request.add_scope(scope.clone());
        }
        let (url, csrf_token) = request.url();

        (
            url.to_string(),
            LoginState {
                csrf_state: csrf_token.secret().clone(),
                nonce: nonce.secret().clone(),
                pkce_verifier: pkce_verifier.secret().clone(),
            },
        )
    }

    /// Exchanges the callback's `code` for tokens, verifies `state` against
    /// what was stashed, and validates the returned ID token (signature,
    /// issuer, audience, and `nonce` claim against what was stashed).
    pub async fn exchange_code(
        &self,
        code: String,
        returned_state: &str,
        stashed: &LoginState,
    ) -> Result<LoginResult, LoginError> {
        if returned_state != stashed.csrf_state {
            return Err(LoginError::InvalidState);
        }

        let token_response: BasicTokenResponse = self
            .client
            .exchange_code(AuthorizationCode::new(code))
            .set_pkce_verifier(PkceCodeVerifier::new(stashed.pkce_verifier.clone()))
            .request_async(&self.http)
            .await
            .map_err(|source| LoginError::Exchange(source.to_string()))?;

        let id_token = extract_id_token(&token_response).ok_or(LoginError::MissingIdToken)?;

        let user = self
            .id_token_validator
            .validate(&id_token)
            .await
            .map_err(LoginError::InvalidIdToken)?;

        let claim_nonce = user.claims.get("nonce").and_then(|v| v.as_str());
        if claim_nonce != Some(stashed.nonce.as_str()) {
            return Err(LoginError::InvalidNonce);
        }

        Ok(LoginResult {
            user,
            id_token,
            refresh_token: token_response.refresh_token().map(|t| t.secret().clone()),
            expires_in: token_response.expires_in().map(|d| d.as_secs()),
        })
    }

    /// Re-validates a stored `id_token` (signature, issuer, audience,
    /// expiry) — the middleware's cheap path, tried before falling back to
    /// `refresh`.
    pub async fn validate_id_token(&self, id_token: &str) -> Result<UserContext, LoginError> {
        self.id_token_validator
            .validate(id_token)
            .await
            .map_err(LoginError::InvalidIdToken)
    }

    /// Exchanges a refresh token for a new `id_token` (and possibly a new
    /// `refresh_token`, if the IdP rotates them). No `nonce` to check here —
    /// that's only meaningful on the original authorization response.
    pub async fn refresh(&self, refresh_token: &str) -> Result<LoginResult, LoginError> {
        let token_response: BasicTokenResponse = self
            .client
            .exchange_refresh_token(&oauth2::RefreshToken::new(refresh_token.to_string()))
            .request_async(&self.http)
            .await
            .map_err(|source| LoginError::Exchange(source.to_string()))?;

        let id_token = extract_id_token(&token_response).ok_or(LoginError::MissingIdToken)?;
        let user = self
            .id_token_validator
            .validate(&id_token)
            .await
            .map_err(LoginError::InvalidIdToken)?;

        Ok(LoginResult {
            user,
            id_token,
            refresh_token: token_response.refresh_token().map(|t| t.secret().clone()),
            expires_in: token_response.expires_in().map(|d| d.as_secs()),
        })
    }
}

/// The ID token isn't a standard OAuth2 field — `oauth2` only models
/// `access_token`/`refresh_token`/etc. — so it rides in the token
/// response's raw JSON body under `id_token`, which `BasicTokenResponse`
/// doesn't expose as a typed field. Re-serialize and pull it back out.
fn extract_id_token(response: &BasicTokenResponse) -> Option<String> {
    #[derive(Deserialize)]
    struct IdTokenField {
        id_token: Option<String>,
    }
    let value = serde_json::to_value(response).ok()?;
    serde_json::from_value::<IdTokenField>(value).ok()?.id_token
}

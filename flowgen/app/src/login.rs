//! Browser-facing OIDC login for the web UI: authorization code +
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
use oauth2::basic::{
    BasicErrorResponse, BasicRevocationErrorResponse, BasicTokenIntrospectionResponse,
    BasicTokenType,
};
use oauth2::{
    AuthUrl, AuthorizationCode, ClientId, ClientSecret, CsrfToken, EndpointNotSet, EndpointSet,
    ExtraTokenFields, PkceCodeChallenge, PkceCodeVerifier, RedirectUrl, Scope,
    StandardRevocableToken, StandardTokenResponse, TokenResponse, TokenUrl,
};
use serde::{Deserialize, Serialize};

/// Errors from the interactive login flow. Separate from
/// `flowgen_core::auth::AuthError` — that type covers token *validation*
/// only and is `#[non_exhaustive]` from outside its crate; this flow has
/// failure modes (bad callback state, missing id_token) that belong to the
/// web UI, not the shared auth-provider abstraction.
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
    #[error("Failed to load `web.auth.credentials_path`: {0}")]
    Credentials(#[source] flowgen_core::credentials::Error),
    #[error(
        "No OIDC client secret. Set `web.auth.client_secret`, or `client_secret` in the JSON \
         file at `web.auth.credentials_path`."
    )]
    MissingClientSecret,
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
    /// Required unless `credentials_path` supplies it, which takes precedence.
    #[serde(default, serialize_with = "serialize_redacted_option")]
    pub client_secret: Option<secrecy::SecretString>,
    /// JSON file holding `client_secret` and `cookie_secret`, e.g. a mounted
    /// Kubernetes secret. Wins over `client_secret` here and over
    /// `web.cookie_secret`.
    #[serde(default)]
    pub credentials_path: Option<std::path::PathBuf>,
    /// Must exactly match a redirect URI registered with the IdP, e.g.
    /// `https://flowgen.example.com/auth/callback`.
    pub redirect_uri: String,
    /// Additional scopes beyond `openid`, `profile`, `email` (always
    /// requested).
    #[serde(default)]
    pub extra_scopes: Vec<String>,
    /// The provider's logout URL, with whatever return parameter it expects
    /// already embedded and registered, e.g.
    /// `https://example.okta.com/oauth2/<id>/v1/logout?post_logout_redirect_uri=https%3A%2F%2Fexample.com%2Fflowgen%2F`.
    /// flowgen appends `id_token_hint`.
    ///
    /// Given in full because not every provider advertises
    /// `end_session_endpoint` in its discovery document. Unset signs out of
    /// flowgen only, leaving the provider's session intact.
    #[serde(default)]
    pub signout_redirect_url: Option<String>,
}

/// The secrets behind `web.auth`, resolved from `credentials_path` and the
/// inline fields.
pub struct ResolvedSecrets {
    pub client_secret: secrecy::SecretString,
    /// `None` leaves the caller on `web.cookie_secret`, which has no
    /// equivalent under `web.auth` to read instead.
    pub cookie_secret: Option<secrecy::SecretString>,
}

impl LoginConfig {
    /// Reads `credentials_path`, falling back to the inline fields per key.
    pub async fn resolve_secrets(&self) -> Result<ResolvedSecrets, LoginError> {
        let from_file = match &self.credentials_path {
            Some(path) => flowgen_core::credentials::load_web_credentials(path)
                .await
                .map_err(LoginError::Credentials)?,
            None => flowgen_core::credentials::WebCredentials::default(),
        };
        let client_secret = match from_file
            .client_secret
            .or_else(|| self.client_secret.clone())
        {
            Some(secret) => secret,
            None => return Err(LoginError::MissingClientSecret),
        };
        Ok(ResolvedSecrets {
            client_secret,
            cookie_secret: from_file.cookie_secret,
        })
    }
}

impl PartialEq for LoginConfig {
    /// Compares the secret by presence only, matching `JwtConfig`'s
    /// convention (`SecretString` deliberately has no `PartialEq`, to
    /// discourage timing-sensitive comparisons config equality never needs).
    fn eq(&self, other: &Self) -> bool {
        self.issuer_url == other.issuer_url
            && self.client_id == other.client_id
            && self.client_secret.is_some() == other.client_secret.is_some()
            && self.credentials_path == other.credentials_path
            && self.redirect_uri == other.redirect_uri
            && self.extra_scopes == other.extra_scopes
            && self.signout_redirect_url == other.signout_redirect_url
    }
}

fn serialize_redacted_option<S>(
    secret: &Option<secrecy::SecretString>,
    s: S,
) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    match secret {
        Some(_) => s.serialize_some("***"),
        None => s.serialize_none(),
    }
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

/// `id_token` is an OIDC addition to the OAuth2 token response, so `oauth2`
/// does not model it. Declaring it as the response's extra fields is what
/// carries it through deserialization.
#[derive(Clone, Debug, Deserialize, Serialize)]
struct IdTokenField {
    id_token: Option<String>,
}

impl ExtraTokenFields for IdTokenField {}

type OidcTokenResponse = StandardTokenResponse<IdTokenField, BasicTokenType>;

type OidcOauthClient = oauth2::Client<
    BasicErrorResponse,
    OidcTokenResponse,
    BasicTokenIntrospectionResponse,
    StandardRevocableToken,
    BasicRevocationErrorResponse,
    EndpointSet,
    EndpointNotSet,
    EndpointNotSet,
    EndpointNotSet,
    EndpointSet,
>;

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
    /// From `web.auth.signout_redirect_url`, parsed at startup so a malformed
    /// URL fails there rather than on someone's first sign-out.
    signout_redirect_url: Option<url::Url>,
}

impl LoginClient {
    /// Fetches the discovery document and builds the OAuth2 + ID-token
    /// validation clients. Call once at startup.
    ///
    /// Takes `client_secret` separately because it may come from
    /// `credentials_path` — see [`LoginConfig::resolve_secrets`].
    pub async fn new(
        config: LoginConfig,
        client_secret: &secrecy::SecretString,
    ) -> Result<Self, LoginError> {
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

        let signout_redirect_url = match &config.signout_redirect_url {
            Some(url) => Some(
                url::Url::parse(url)
                    .map_err(|source| LoginError::InvalidUrl(source.to_string()))?,
            ),
            None => None,
        };

        let client: OidcOauthClient = oauth2::Client::new(ClientId::new(config.client_id.clone()))
            .set_client_secret(ClientSecret::new(client_secret.expose_secret().to_string()))
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
            signout_redirect_url,
        })
    }

    /// The configured logout URL with `id_token_hint` appended, ending the
    /// provider's session so the next sign-in asks for credentials again.
    ///
    /// `None` signs out of flowgen alone — see
    /// [`LoginConfig::signout_redirect_url`].
    pub fn signout_url(&self, id_token: &str) -> Option<String> {
        Some(with_id_token_hint(
            self.signout_redirect_url.as_ref()?,
            id_token,
        ))
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

        let token_response: OidcTokenResponse = self
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
        let token_response: OidcTokenResponse = self
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

/// Appends `id_token_hint` while preserving any query the configured URL
/// already carries, such as the provider's own `post_logout_redirect_uri`.
fn with_id_token_hint(url: &url::Url, id_token: &str) -> String {
    let mut url = url.clone();
    url.query_pairs_mut().append_pair("id_token_hint", id_token);
    url.to_string()
}

fn extract_id_token(response: &OidcTokenResponse) -> Option<String> {
    response.extra_fields().id_token.clone()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn id_token_is_read_from_a_token_response() {
        let body = r#"{
            "token_type": "Bearer",
            "access_token": "at",
            "expires_in": 3600,
            "scope": "openid profile email",
            "id_token": "header.payload.signature"
        }"#;
        let response: OidcTokenResponse = serde_json::from_str(body).expect("parses");
        assert_eq!(
            extract_id_token(&response).as_deref(),
            Some("header.payload.signature")
        );
    }

    #[test]
    fn id_token_hint_is_added_alongside_the_providers_own_query_params() {
        let configured = url::Url::parse(
            "https://example.okta.com/oauth2/default/v1/logout\
             ?post_logout_redirect_uri=https%3A%2F%2Fexample.com%2Fflowgen%2F",
        )
        .expect("parses");

        let signout = url::Url::parse(&with_id_token_hint(&configured, "the.id.token"))
            .expect("still a valid url");

        let pairs: Vec<(String, String)> = signout
            .query_pairs()
            .map(|(k, v)| (k.into_owned(), v.into_owned()))
            .collect();
        assert_eq!(
            pairs,
            vec![
                (
                    "post_logout_redirect_uri".to_string(),
                    "https://example.com/flowgen/".to_string()
                ),
                ("id_token_hint".to_string(), "the.id.token".to_string()),
            ]
        );
    }

    #[test]
    fn id_token_hint_is_added_to_a_url_with_no_existing_query() {
        let configured =
            url::Url::parse("https://example.okta.com/oauth2/default/v1/logout").expect("parses");
        assert_eq!(
            with_id_token_hint(&configured, "the.id.token"),
            "https://example.okta.com/oauth2/default/v1/logout?id_token_hint=the.id.token"
        );
    }

    #[test]
    fn a_token_response_without_an_id_token_is_reported_as_missing() {
        let body = r#"{
            "token_type": "Bearer",
            "access_token": "at",
            "expires_in": 3600
        }"#;
        let response: OidcTokenResponse = serde_json::from_str(body).expect("parses");
        assert!(extract_id_token(&response).is_none());
    }
}

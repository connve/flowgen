//! Embedded web interface for flowgen.
//!
//! Serves the static SvelteKit UI and its web API (flows, logs, config,
//! resources, and the built-in Agents chat). The static assets are compiled
//! into the binary with `rust-embed`, so the single `flowgen` binary remains
//! self-contained.

use axum::{
    extract::{Path as AxumPath, State},
    http::{HeaderMap, StatusCode, Uri},
    response::sse::{Event as SseEvent, KeepAlive, Sse},
    response::{IntoResponse, Redirect},
    routing::{get, post},
    Json, Router,
};
use flowgen_client::types as api;
use futures::stream::Stream;
use futures_util::StreamExt;
use rust_embed::RustEmbed;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::Duration;
use tracing::{info, warn};

/// Default port for the web server.
pub const DEFAULT_WEB_PORT: u16 = 8080;

/// Default path prefix for the web UI.
pub const DEFAULT_WEB_PATH: &str = "/";

/// Base path the SvelteKit bundle was compiled with (`PUBLIC_BASE`
/// in `web/svelte.config.js`). `serve_embedded` rewrites this to
/// whatever `web.path` was configured.
const BUILT_BASE_PATH: &str = "/flowgen";

/// SSE event name carrying `FlowMetricsSnapshot` payloads on `/api/flows/stream`.
const SSE_EVENT_SNAPSHOT: &str = "snapshot";

/// SSE event name carrying `LogRecord` payloads on `/api/logs/stream`.
const SSE_EVENT_LOG: &str = "log";

// Response types come from `flowgen_client::types` — see openapi.yaml.

/// Errors that can occur while running the web server.
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum Error {
    /// Failed to bind the TCP listener.
    #[error("Error binding web listener on port {port}: {source}")]
    BindListener {
        port: u16,
        #[source]
        source: std::io::Error,
    },
    /// Failed to serve HTTP requests.
    #[error("Error serving web requests: {source}")]
    ServeHttp {
        #[source]
        source: std::io::Error,
    },
}

/// Embedded static assets produced by the SvelteKit build.
#[derive(RustEmbed)]
#[folder = "../../web/build"]
struct WebAssets;

/// State shared with the web API handlers.
pub struct WebState {
    /// Registry of currently running flows.
    pub flow_registry: Arc<RwLock<std::collections::HashMap<String, crate::app::FlowHandle>>>,
    /// Path prefix the UI is mounted at (e.g. "" or "/flowgen"), used to
    /// strip the prefix from asset lookups. Always without a trailing slash.
    pub prefix: String,
    /// Optional resource loader used by the resources endpoints to
    /// list and fetch templates, prompts, SQL files, etc.
    pub resource_loader: Option<flowgen_core::resource::ResourceLoader>,
    /// Shared metrics store populated by the tracing layer. Used by
    /// the flow list, the flow detail, and the SSE stream.
    pub metrics_store: Arc<dyn flowgen_core::flow::activity::MetricsStore>,
    /// Backend-agnostic log query used by the SSE stream and the
    /// history endpoint.
    pub logs_store: Option<Arc<dyn flowgen_core::telemetry::query::LogsStore>>,
    /// Peers sharing their logs and counters, in cluster mode.
    pub cluster_peers: Option<Arc<flowgen_core::telemetry::cluster::ClusterPeers>>,
    /// This pod's identity, for `/api/cluster` outside cluster mode.
    pub this_pod: String,
    /// Flows this pod runs, for `/api/cluster`.
    pub running_flows: Arc<dyn flowgen_core::telemetry::cluster::RunningFlows>,
    /// Running application configuration, surfaced read-only by the
    /// config viewer. Secrets serialize as `"***"` (see `JwtConfig`).
    pub app_config: Arc<crate::config::AppConfig>,
    /// Cache backing the built-in Agents conversation history — the store our
    /// UI reads and writes; a persistence flow can later copy it into a
    /// database. This is the **system** cache (`flowgen_system`), which is out
    /// of flow-script reach, so chats are not exposed to `ctx.cache`. Proxy
    /// traffic stays stateless: conversations are our UI's domain, not the
    /// gateway's.
    pub conversation_cache: Arc<dyn flowgen_core::cache::Cache>,
    /// Whether a dedicated system bucket actually backs `conversation_cache`.
    /// False in single-binary/in-memory mode, where it falls back to the
    /// runtime cache that flow scripts can reach — `start_web_server` warns
    /// once at startup so operators know.
    pub system_bucket_present: bool,
    /// From `web.agents`; `None` disables `/api/agents`.
    pub agents: Option<crate::config::AgentsOptions>,
    /// OIDC login client, built from `web.auth` at startup. `None` leaves
    /// the web UI unauthenticated.
    pub login_client: Option<Arc<crate::login::LoginClient>>,
    /// Key encrypting the browser session cookie — see `crate::login` for
    /// why there's no server-side session store to protect instead.
    /// See [`crate::login::cookie_key`] for where it comes from.
    pub cookie_key: axum_extra::extract::cookie::Key,
    /// Whether login cookies carry `Secure` (browsers require HTTPS to send
    /// them). From `web.cookie_secure`, default `true`.
    pub cookie_secure: bool,
    /// Machine keys `/api/*` accepts as `Authorization: Bearer`, from
    /// `web.api_credentials_path`.
    pub api_keys: Vec<flowgen_core::credentials::ApiKey>,
    /// From `web.authoring`; `None` disables `/api/changes`.
    pub authoring: Option<crate::config::AuthoringOptions>,
    /// Flows callable in-process; approved changes are published through one.
    pub inproc: Arc<flowgen_core::task::inproc::registry::InprocRegistry>,
    /// Bucket the synced flow sources are read from, when flows load from the
    /// cache (`flows.cache`).
    pub flows_cache: Option<Arc<dyn flowgen_core::cache::Cache>>,
    /// Agent replies this pod is writing.
    pub replies: Arc<crate::agent_reply::Replies>,
}

/// Who an `/api/*` request authenticated as, when `web.auth` is set.
#[derive(Clone, Debug)]
pub(crate) enum Caller {
    /// A signed-in user, through the session cookie.
    User(flowgen_core::auth::UserContext),
    /// A machine key from `web.api_credentials_path`, by name.
    Key(String),
}

impl Caller {
    /// How the caller appears in audit fields: a user by their email claim, as
    /// the user id is often opaque, and a machine key as `key:<name>`.
    pub(crate) fn label(&self) -> String {
        match self {
            Caller::User(user) => match user.claims.get("email") {
                Some(serde_json::Value::String(email)) if !email.trim().is_empty() => {
                    email.trim().to_string()
                }
                _ => user.user_id.clone(),
            },
            Caller::Key(name) => format!("key:{name}"),
        }
    }
}

/// Shortest machine key `/api/*` accepts; shorter keys in the file are ignored.
pub(crate) const MIN_API_KEY_LEN: usize = 32;

/// Routes a machine key may call: reading flows, resources, logs and changes,
/// validating files, and proposing and editing changes; the authoring handlers
/// let a key edit only the changes it proposed.
fn key_may_call(api_prefix: &str, method: &axum::http::Method, path: &str) -> bool {
    let route = match path.strip_prefix(api_prefix) {
        Some(route) => route,
        None => return false,
    };
    let under = |base: &str| route == base || route.starts_with(&format!("{base}/"));
    let change = route.strip_prefix("/changes/");
    let single = |id: &str| !id.is_empty() && !id.contains('/');
    match *method {
        axum::http::Method::GET => [
            "/flows",
            "/resources",
            "/logs",
            "/changes",
            "/version",
            "/workspace/file",
        ]
        .iter()
        .any(|base| under(base)),
        axum::http::Method::POST => {
            route == "/changes"
                || route == "/workspace/validate"
                || matches!(change.and_then(|rest| rest.strip_suffix("/files")), Some(id) if single(id))
        }
        axum::http::Method::PATCH => matches!(change, Some(id) if single(id)),
        _ => false,
    }
}

/// The machine key whose value is the request's bearer token, compared in
/// constant time.
fn matching_key<'a>(
    keys: &'a [flowgen_core::credentials::ApiKey],
    headers: &axum::http::HeaderMap,
) -> Option<&'a flowgen_core::credentials::ApiKey> {
    use secrecy::ExposeSecret;
    use subtle::ConstantTimeEq;
    let header = headers
        .get(axum::http::header::AUTHORIZATION)?
        .to_str()
        .ok()?;
    let token = flowgen_core::auth::extract_bearer_token(header)?;
    keys.iter().find(|key| {
        let key = key.key.expose_secret().as_bytes();
        key.len() >= MIN_API_KEY_LEN && bool::from(key.ct_eq(token.as_bytes()))
    })
}

/// Wraps `cookie::Key` so `FromRef<Arc<WebState>>` can be implemented here
/// — the orphan rules block implementing a foreign trait for the foreign
/// `Key` type directly against a foreign `Arc<WebState>`.
#[derive(Clone)]
struct CookieKey(axum_extra::extract::cookie::Key);

impl From<CookieKey> for axum_extra::extract::cookie::Key {
    fn from(k: CookieKey) -> Self {
        k.0
    }
}

impl axum::extract::FromRef<Arc<WebState>> for CookieKey {
    fn from_ref(state: &Arc<WebState>) -> Self {
        CookieKey(state.cookie_key.clone())
    }
}

/// The state's private cookie jar type — used instead of the crate default
/// `PrivateCookieJar<Key>` since `Key` itself can't satisfy `FromRef` here
/// (see [`CookieKey`]).
type AuthJar = axum_extra::extract::cookie::PrivateCookieJar<CookieKey>;

/// Starts the web server on the given port.
///
/// The server mounts the embedded UI at `path` and exposes `GET /api/flows`
/// alongside it. All other requests fall back to `index.html` so the SvelteKit
/// client-side router can handle them.
pub async fn start_web_server(port: u16, path: &str, state: WebState) -> Result<(), Error> {
    // Without a dedicated system cache bucket, conversation history shares the
    // runtime cache that flow scripts can read and write via `ctx.cache`. Warn
    // once at startup so operators know to configure a system bucket when that
    // access matters.
    let warn_conversations_in_runtime_cache =
        state.agents.is_some() && !state.system_bucket_present;

    let app = router(path, state);

    let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{port}"))
        .await
        .map_err(|source| Error::BindListener { port, source })?;

    info!(port, path = %path, "Starting web server");

    if warn_conversations_in_runtime_cache {
        warn!(
            "Agents conversation history is stored in the runtime cache (no system cache bucket \
             configured), which flow scripts can read and write via ctx.cache"
        );
    }

    axum::serve(listener, app)
        .await
        .map_err(|source| Error::ServeHttp { source })
}

fn router(path: &str, mut state: WebState) -> Router {
    let prefix = path.trim_end_matches('/').to_string();
    let api_prefix = if prefix.is_empty() {
        "/api".to_string()
    } else {
        format!("{prefix}/api")
    };
    state.prefix = prefix.clone();
    let state = Arc::new(state);

    let auth_prefix = format!("{prefix}/auth");
    let auth_routes = Router::new()
        .route(&format!("{auth_prefix}/login"), get(auth_login))
        .route(&format!("{auth_prefix}/callback"), get(auth_callback))
        .route(&format!("{auth_prefix}/logout"), get(auth_logout))
        .route(&format!("{auth_prefix}/me"), get(auth_me))
        .with_state(Arc::clone(&state));

    let agents = Router::new()
        .route(&format!("{api_prefix}/agents/models"), get(proxy_models))
        .route(
            &format!("{api_prefix}/agents/tools"),
            get(crate::connections::list_agent_tools),
        )
        .route(
            &format!("{api_prefix}/agents/conversations"),
            get(list_conversations),
        )
        .route(
            &format!("{api_prefix}/agents/conversations/stream"),
            get(stream_conversations),
        )
        .route(
            &format!("{api_prefix}/agents/conversations/{{id}}"),
            get(get_conversation)
                .put(put_conversation)
                .patch(update_conversation)
                .delete(delete_conversation),
        )
        .route(
            &format!("{api_prefix}/agents/conversations/{{id}}/reply"),
            post(crate::agent_reply::start_reply)
                .get(crate::agent_reply::follow_reply)
                .delete(crate::agent_reply::stop_reply),
        )
        .route(
            &format!("{api_prefix}/agents/conversations/{{id}}/read"),
            axum::routing::put(mark_conversation_read).delete(mark_conversation_unread),
        )
        .route_layer(axum::middleware::from_fn_with_state(
            Arc::clone(&state),
            require_agents,
        ));

    let mut api = Router::new()
        .route(&format!("{api_prefix}/flows"), get(list_flows))
        .route(&format!("{api_prefix}/flows/stream"), get(stream_flows))
        .route(&format!("{api_prefix}/flows/{{*path}}"), get(get_flow))
        .route(&format!("{api_prefix}/logs"), get(list_logs))
        .route(&format!("{api_prefix}/logs/stream"), get(stream_logs))
        .route(
            &format!("{api_prefix}/connections"),
            get(crate::connections::list_connections),
        )
        .route(
            &format!("{api_prefix}/connections/{{id}}"),
            get(crate::connections::get_connection),
        )
        .route(
            &format!("{api_prefix}/connectors"),
            get(crate::connections::list_connectors),
        )
        .route(
            &format!("{api_prefix}/connectors/{{name}}/icon"),
            get(crate::connections::connector_icon),
        )
        .route(&format!("{api_prefix}/cluster"), get(get_cluster_status))
        .route(
            &format!("{api_prefix}/cluster/token"),
            post(regenerate_cluster_token),
        )
        .route(&format!("{api_prefix}/version"), get(get_version))
        .route(&format!("{api_prefix}/config"), get(get_config))
        .merge(agents)
        .route(
            &format!("{api_prefix}/workspace/validate"),
            post(crate::authoring::validate_workspace),
        )
        .route(
            &format!("{api_prefix}/workspace/file"),
            get(crate::authoring::workspace_file),
        )
        .route(
            &format!("{api_prefix}/changes"),
            get(crate::authoring::list_changes).post(crate::authoring::propose_change),
        )
        .route(
            &format!("{api_prefix}/changes/{{id}}"),
            get(crate::authoring::get_change).patch(crate::authoring::edit_change),
        )
        .route(
            &format!("{api_prefix}/changes/{{id}}/files"),
            post(crate::authoring::add_change_files),
        )
        .route(
            &format!("{api_prefix}/changes/{{id}}/approve"),
            post(crate::authoring::approve_change),
        )
        .route(
            &format!("{api_prefix}/changes/{{id}}/reject"),
            post(crate::authoring::reject_change),
        )
        .route(&format!("{api_prefix}/openapi.yaml"), get(get_openapi))
        .route(&format!("{api_prefix}/resources"), get(list_resources))
        .route(
            &format!("{api_prefix}/resources/{{*key}}"),
            get(get_resource),
        )
        .with_state(Arc::clone(&state));

    if state.login_client.is_some() {
        api = api.layer(axum::middleware::from_fn_with_state(
            Arc::clone(&state),
            auth_middleware,
        ));
    }

    Router::new()
        .merge(auth_routes)
        .merge(api)
        .fallback(serve_embedded)
        .layer(axum::middleware::map_response(security_headers))
        .with_state(state)
}

async fn security_headers(mut response: axum::response::Response) -> axum::response::Response {
    let headers = response.headers_mut();
    headers.insert(
        axum::http::header::X_CONTENT_TYPE_OPTIONS,
        axum::http::HeaderValue::from_static("nosniff"),
    );
    headers.insert(
        axum::http::header::REFERRER_POLICY,
        axum::http::HeaderValue::from_static("strict-origin-when-cross-origin"),
    );
    response
}

/// The page's Content-Security-Policy: scripts only from the app and the
/// page's own inline scripts, by hash, so injected markup can't run code.
/// Icons load from the Iconify API.
fn content_security_policy(html: &[u8], frame_ancestors: &[String]) -> String {
    use base64::Engine as _;
    use sha2::Digest as _;
    let hashes: String = inline_scripts(html)
        .iter()
        .map(|script| {
            let digest = sha2::Sha256::digest(script);
            format!(
                " 'sha256-{}'",
                base64::engine::general_purpose::STANDARD.encode(digest)
            )
        })
        .collect();
    format!(
        "default-src 'self'; script-src 'self'{hashes}; style-src 'self' 'unsafe-inline'; \
         img-src 'self' data:; font-src 'self' data:; \
         connect-src 'self' https://api.iconify.design https://api.simplesvg.com https://api.unisvg.com; \
         object-src 'none'; base-uri 'self'; frame-ancestors {}",
        frame_ancestors.join(" ")
    )
}

/// The bodies of a page's `<script>` elements without `src`.
fn inline_scripts(html: &[u8]) -> Vec<&[u8]> {
    let mut scripts = Vec::new();
    let mut rest = html;
    while let Some(start) = find(rest, b"<script") {
        let Some(tag_end) = find(&rest[start..], b">") else {
            break;
        };
        let tag = &rest[start..start + tag_end];
        let body_start = start + tag_end + 1;
        let Some(body_len) = find(&rest[body_start..], b"</script>") else {
            break;
        };
        if find(tag, b"src=").is_none() {
            scripts.push(&rest[body_start..body_start + body_len]);
        }
        rest = &rest[body_start + body_len..];
    }
    scripts
}

fn find(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

// No server-side session store (see `crate::login`): the browser's cookie
// *is* the session, encrypted with `WebState::cookie_key` so it can't be
// read or forged client-side. Two cookies, both `HttpOnly; Secure;
// SameSite=Lax`:
//   - `SSO_STATE_COOKIE`: the PKCE verifier/state/nonce, alive only for the
//     few seconds between `/auth/login` and `/auth/callback`.
//   - `SSO_SESSION_COOKIE`: the IdP's tokens, alive for the session.

const SSO_STATE_COOKIE: &str = "flowgen_auth_state";
const SSO_SESSION_COOKIE: &str = "flowgen_auth_session";

/// What's encrypted into `SSO_SESSION_COOKIE`. Not a session record in any
/// store — this struct only ever exists serialized inside the cookie.
#[derive(serde::Serialize, serde::Deserialize)]
struct AuthSession {
    user: flowgen_core::auth::UserContext,
    id_token: String,
    refresh_token: Option<String>,
    /// Unix seconds, as reported by the provider. Informational: expiry is
    /// decided by validating `id_token`, not by reading this.
    expires_at: Option<i64>,
}

/// Builds a login cookie: `HttpOnly`, `SameSite=Lax`, `Path=/`, and `Secure`
/// per `secure` (browsers require HTTPS to send a `Secure` cookie — see
/// `web.cookie_secure`).
fn auth_cookie(
    name: &'static str,
    value: String,
    max_age: Option<time::Duration>,
    secure: bool,
) -> axum_extra::extract::cookie::Cookie<'static> {
    use axum_extra::extract::cookie::{Cookie, SameSite};
    let mut cookie = Cookie::new(name, value);
    cookie.set_http_only(true);
    cookie.set_secure(secure);
    cookie.set_same_site(SameSite::Lax);
    cookie.set_path("/");
    if let Some(max_age) = max_age {
        cookie.set_max_age(max_age);
    }
    cookie
}

/// Builds a cookie that deletes `name` on the browser. Carries the same
/// attributes as [`auth_cookie`]: per RFC 6265 a removal `Set-Cookie`
/// addresses a different cookie once its `Path` differs from the original's.
fn removal_cookie(
    name: &'static str,
    secure: bool,
) -> axum_extra::extract::cookie::Cookie<'static> {
    let mut cookie = auth_cookie(name, String::new(), None, secure);
    cookie.make_removal();
    cookie
}

/// What's encrypted into `SSO_STATE_COOKIE` between `/auth/login` and `/auth/callback`.
#[derive(serde::Serialize, serde::Deserialize)]
struct PendingLogin {
    #[serde(flatten)]
    login: crate::login::LoginState,
    /// Where the callback sends the browser, already checked by [`return_path`].
    #[serde(default)]
    return_to: Option<String>,
}

#[derive(serde::Deserialize)]
struct AuthLoginQuery {
    /// Web UI page to come back to after signing in; dropped unless it is a
    /// page under the UI's path outside the auth routes.
    return_to: Option<String>,
}

/// `return_to` when it is a web UI page on this origin, outside the auth routes.
fn return_path(prefix: &str, return_to: &str) -> Option<String> {
    let path = return_to.strip_prefix(prefix)?;
    let safe = path.starts_with('/')
        && !return_to.starts_with("//")
        && return_to.chars().all(|c| c.is_ascii_graphic() && c != '\\')
        && !has_dot_segment(path)
        && !path.starts_with("/auth/");
    match safe {
        true => Some(return_to.to_string()),
        false => None,
    }
}

/// Whether the path part holds a `.` or `..` segment, plain or percent-encoded,
/// which a browser resolves before following the redirect.
fn has_dot_segment(path_and_query: &str) -> bool {
    let path = match path_and_query.split_once(['?', '#']) {
        Some((path, _)) => path,
        None => path_and_query,
    };
    path.split('/').any(|segment| {
        let decoded = segment.to_ascii_lowercase().replace("%2e", ".");
        decoded == "." || decoded == ".."
    })
}

/// `GET /auth/login` — redirects the browser to the IdP. `return_to` names
/// the page to come back to after the callback.
async fn auth_login(
    State(state): State<Arc<WebState>>,
    jar: AuthJar,
    axum::extract::Query(query): axum::extract::Query<AuthLoginQuery>,
) -> impl IntoResponse {
    let Some(login_client) = &state.login_client else {
        return (StatusCode::NOT_FOUND, "OIDC login is not configured").into_response();
    };
    let (url, login_state) = login_client.authorize_url();
    let return_to = match query.return_to {
        Some(return_to) => return_path(&state.prefix, &return_to),
        None => None,
    };
    let pending = PendingLogin {
        login: login_state,
        return_to,
    };
    let Ok(encoded) = serde_json::to_string(&pending) else {
        return (StatusCode::INTERNAL_SERVER_ERROR, "Failed to start login").into_response();
    };
    let jar = jar.add(auth_cookie(
        SSO_STATE_COOKIE,
        encoded,
        Some(time::Duration::minutes(10)),
        state.cookie_secure,
    ));
    (jar, Redirect::to(&url)).into_response()
}

#[derive(serde::Deserialize)]
struct AuthCallbackQuery {
    code: Option<String>,
    state: Option<String>,
    error: Option<String>,
    error_description: Option<String>,
}

/// Sends the browser to the web UI login page with why signing in failed:
/// `access_denied` when the provider refused the user, `provider` for another
/// provider error, `expired` when the sign-in outlived its state cookie, and
/// `failed` when the response could not be verified. Only these codes, so a
/// crafted link cannot put text on the page.
fn login_failed(state: &WebState, code: &'static str) -> axum::response::Response {
    Redirect::to(&format!("{}login?error={code}", ui_url(&state.prefix))).into_response()
}

/// `GET /auth/callback` — exchanges the code, verifies everything, and sets
/// the session cookie.
async fn auth_callback(
    State(state): State<Arc<WebState>>,
    jar: AuthJar,
    axum::extract::Query(query): axum::extract::Query<AuthCallbackQuery>,
) -> impl IntoResponse {
    let Some(login_client) = &state.login_client else {
        return (StatusCode::NOT_FOUND, "OIDC login is not configured").into_response();
    };

    if let Some(error) = query.error {
        let description = match &query.error_description {
            Some(description) => description.as_str(),
            None => "",
        };
        warn!(error = %error, description = %description, "OIDC provider returned an error at callback");
        return match error.as_str() {
            "access_denied" => login_failed(&state, "access_denied"),
            _ => login_failed(&state, "provider"),
        };
    }
    let (Some(code), Some(returned_state)) = (query.code, query.state) else {
        warn!("OIDC callback has no code or state");
        return login_failed(&state, "failed");
    };

    let stashed = match jar.get(SSO_STATE_COOKIE) {
        Some(stashed) => serde_json::from_str::<PendingLogin>(stashed.value()).ok(),
        None => None,
    };
    let Some(stashed) = stashed else {
        return login_failed(&state, "expired");
    };

    let result = match login_client
        .exchange_code(code, &returned_state, &stashed.login)
        .await
    {
        Ok(result) => result,
        Err(source) => {
            warn!(error = %source, "OIDC login failed");
            return login_failed(&state, "failed");
        }
    };

    let session = AuthSession {
        user: result.user,
        id_token: result.id_token,
        refresh_token: result.refresh_token,
        expires_at: result
            .expires_in
            .map(|secs| chrono::Utc::now().timestamp() + secs as i64),
    };
    let Ok(encoded) = serde_json::to_string(&session) else {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            "Failed to complete login",
        )
            .into_response();
    };

    let jar = jar
        .remove(removal_cookie(SSO_STATE_COOKIE, state.cookie_secure))
        .add(auth_cookie(
            SSO_SESSION_COOKIE,
            encoded,
            None,
            state.cookie_secure,
        ));
    let redirect_to = match stashed.return_to {
        Some(return_to) => return_to,
        None => ui_url(&state.prefix),
    };
    (jar, Redirect::to(&redirect_to)).into_response()
}

/// Where the web UI lives, for redirecting back to it.
///
/// Keeps the trailing slash: the SvelteKit bundle resolves its assets against
/// the document's directory, so a page served at `/flowgen` would look for them
/// under `/` and load nothing.
fn ui_url(prefix: &str) -> String {
    format!("{}/", prefix.trim_end_matches('/'))
}

/// `GET /auth/logout` — clears the session cookie, then hands the browser to
/// the provider's logout URL so its session ends too. Falls back to the web
/// UI when `web.auth.signout_redirect_url` is unset.
///
/// Navigated to rather than fetched, so the browser follows the cross-origin
/// redirect itself.
///
/// Writes the removal header directly rather than through [`AuthJar`], whose
/// `remove` only covers a cookie that decrypted on the way in. Clearing it
/// here works whatever `web.cookie_secret` encrypted it.
async fn auth_logout(State(state): State<Arc<WebState>>, jar: AuthJar) -> impl IntoResponse {
    let Some(login_client) = &state.login_client else {
        return (StatusCode::NOT_FOUND, "OIDC login is not configured").into_response();
    };

    let redirect_to =
        match read_session(&jar).and_then(|session| login_client.signout_url(&session.id_token)) {
            Some(url) => url,
            None => ui_url(&state.prefix),
        };

    let cookie = removal_cookie(SSO_SESSION_COOKIE, state.cookie_secure).to_string();
    match axum::http::HeaderValue::from_str(&cookie) {
        Ok(value) => {
            let mut response = Redirect::to(&redirect_to).into_response();
            response
                .headers_mut()
                .append(axum::http::header::SET_COOKIE, value);
            response
        }
        Err(source) => {
            warn!(error = %source, "Failed to encode session removal cookie");
            (StatusCode::INTERNAL_SERVER_ERROR, "Failed to sign out").into_response()
        }
    }
}

/// `GET /auth/me` — the logged-in user, 401 if not logged in, or 404 if
/// `web.auth` isn't configured (matching `auth_login`/`auth_callback`'s
/// existing convention) — the frontend uses the 404 case to tell "no login
/// offered" apart from "not logged in yet" before deciding whether a 401
/// elsewhere means "go log in".
async fn auth_me(State(state): State<Arc<WebState>>, jar: AuthJar) -> impl IntoResponse {
    if state.login_client.is_none() {
        return (StatusCode::NOT_FOUND, "OIDC login is not configured").into_response();
    }
    let Some(resolved) = resolve_session(&state, &jar).await else {
        return StatusCode::UNAUTHORIZED.into_response();
    };
    let mut response = Json(api::UserContext {
        user_id: resolved.session.user.user_id,
        claims: resolved.session.user.claims.into_iter().collect(),
    })
    .into_response();
    if let Some(jar) = resolved.refreshed {
        apply_refreshed_cookies(&mut response, jar);
    }
    response
}

fn read_session(jar: &AuthJar) -> Option<AuthSession> {
    let cookie = jar.get(SSO_SESSION_COOKIE)?;
    serde_json::from_str(cookie.value()).ok()
}

/// A session that is good to serve, plus the cookie to reissue when it was
/// refreshed on the way through.
struct ResolvedSession {
    session: AuthSession,
    refreshed: Option<AuthJar>,
}

/// Validates the session cookie, silently refreshing it against the identity
/// provider once past `exp`. `None` means the caller must answer 401.
///
/// Shared by `/auth/me` and the `/api/*` middleware so both agree on whether a
/// session is still good — the UI gates its first render on the former and
/// every subsequent call on the latter.
async fn resolve_session(state: &WebState, jar: &AuthJar) -> Option<ResolvedSession> {
    let login_client = state.login_client.as_ref()?;
    let session = read_session(jar)?;

    if login_client
        .validate_id_token(&session.id_token)
        .await
        .is_ok()
    {
        return Some(ResolvedSession {
            session,
            refreshed: None,
        });
    }

    let Some(refresh_token) = session.refresh_token.clone() else {
        info!("Session expired and the provider issued no refresh token, signing out");
        return None;
    };
    let refreshed = match login_client.refresh(&refresh_token).await {
        Ok(refreshed) => refreshed,
        Err(source) => {
            warn!(error = %source, "Session refresh failed");
            return None;
        }
    };
    let session = AuthSession {
        user: refreshed.user,
        id_token: refreshed.id_token,
        refresh_token: refreshed.refresh_token.or(session.refresh_token),
        expires_at: refreshed
            .expires_in
            .map(|secs| chrono::Utc::now().timestamp() + secs as i64),
    };
    let encoded = match serde_json::to_string(&session) {
        Ok(encoded) => encoded,
        Err(source) => {
            warn!(error = %source, "Failed to encode refreshed session");
            return None;
        }
    };
    Some(ResolvedSession {
        refreshed: Some(jar.clone().add(auth_cookie(
            SSO_SESSION_COOKIE,
            encoded,
            None,
            state.cookie_secure,
        ))),
        session,
    })
}

/// Copies a refreshed jar's cookies onto an already-built response.
fn apply_refreshed_cookies(response: &mut axum::response::Response, jar: AuthJar) {
    for cookie in jar.iter() {
        match axum::http::HeaderValue::from_str(&cookie.to_string()) {
            Ok(value) => {
                response
                    .headers_mut()
                    .append(axum::http::header::SET_COOKIE, value);
            }
            // Don't silently serve the request on a cookie the browser will
            // never receive.
            Err(source) => {
                warn!(error = %source, "Failed to encode refreshed session cookie")
            }
        }
    }
}

/// Protects `/api/*` when `web.auth` is configured, on the session
/// [`resolve_session`] resolves or else a machine key. Not layered at all
/// when `web.auth` is unset.
async fn auth_middleware(
    State(state): State<Arc<WebState>>,
    jar: AuthJar,
    mut request: axum::extract::Request,
    next: axum::middleware::Next,
) -> axum::response::Response {
    if state.login_client.is_none() {
        return next.run(request).await;
    }
    match resolve_session(&state, &jar).await {
        Some(resolved) => {
            let user = resolved.session.user.clone();
            request.extensions_mut().insert(Caller::User(user.clone()));
            request
                .extensions_mut()
                .insert(Arc::new(flowgen_core::auth::UserSession::with_token(
                    user.clone(),
                    &resolved.session.id_token,
                )));
            request.extensions_mut().insert(user);
            let mut response = next.run(request).await;
            if let Some(jar) = resolved.refreshed {
                apply_refreshed_cookies(&mut response, jar);
            }
            response
        }
        None => match matching_key(&state.api_keys, request.headers()) {
            Some(_)
                if !key_may_call(
                    &format!("{}/api", state.prefix),
                    request.method(),
                    request.uri().path(),
                ) =>
            {
                StatusCode::FORBIDDEN.into_response()
            }
            Some(key) => {
                request
                    .extensions_mut()
                    .insert(Caller::Key(key.name.clone()));
                next.run(request).await
            }
            None => StatusCode::UNAUTHORIZED.into_response(),
        },
    }
}

/// Returns a list of currently loaded flows.
async fn list_flows(State(state): State<Arc<WebState>>) -> impl IntoResponse {
    // Fetch all metrics up front (no lock held across the await), then do
    // the usual synchronous pass over the flow registry using a lookup.
    let metrics: HashMap<String, flowgen_core::flow::activity::FlowMetricsSnapshot> = state
        .metrics_store
        .snapshot_all()
        .await
        .unwrap_or_default()
        .into_iter()
        .map(|s| (s.flow.clone(), s))
        .collect();

    let flows = match state.flow_registry.read() {
        Ok(registry) => registry
            .values()
            .map(|handle| build_summary(handle, &metrics))
            .collect::<Vec<_>>(),
        Err(_) => {
            warn!("Flow registry is poisoned, returning empty flow list");
            Vec::new()
        }
    };

    Json(flows)
}

/// Merges the registered flow handle (static config-time data) with
/// whatever live metrics the tracing layer has collected so far.
fn build_summary(
    handle: &crate::app::FlowHandle,
    metrics: &HashMap<String, flowgen_core::flow::activity::FlowMetricsSnapshot>,
) -> api::FlowSummary {
    let source = match handle.from_filesystem {
        true => api::FlowSummarySource::Filesystem,
        false => api::FlowSummarySource::Cache,
    };
    let snapshot = metrics.get(handle.identity());
    let (
        last_event_at,
        last_warning_at,
        last_error_at,
        events_total,
        warnings_total,
        errors_total,
        status,
    ) = match snapshot {
        Some(s) => (
            s.last_event_at_ms.and_then(ms_to_datetime),
            s.last_warning_at_ms.and_then(ms_to_datetime),
            s.last_error_at_ms.and_then(ms_to_datetime),
            s.events_total,
            s.warnings_total,
            s.errors_total,
            core_status_to_api(s.status),
        ),
        None => (None, None, None, 0, 0, 0, api::FlowStatus::Idle),
    };
    api::FlowSummary {
        path: handle.identity().to_string(),
        name: handle.identity().to_string(),
        display_name: handle.display_name().map(ToString::to_string),
        description: handle.description().map(ToString::to_string),
        tags: handle.tags().to_vec(),
        require_leader_election: handle.require_leader_election(),
        task_count: handle.task_count() as u64,
        source,
        started_at: system_time_to_datetime(handle.started_at()),
        last_event_at,
        last_warning_at,
        last_error_at,
        events_total: events_total as i64,
        warnings_total: warnings_total as i64,
        errors_total: errors_total as i64,
        status,
    }
}

fn core_status_to_api(s: flowgen_core::flow::activity::FlowStatus) -> api::FlowStatus {
    use flowgen_core::flow::activity::FlowStatus as Core;
    match s {
        Core::Idle => api::FlowStatus::Idle,
        Core::Ok => api::FlowStatus::Ok,
        Core::Warn => api::FlowStatus::Warn,
        Core::Error => api::FlowStatus::Error,
    }
}

fn system_time_to_datetime(t: std::time::SystemTime) -> Option<chrono::DateTime<chrono::Utc>> {
    let d = t.duration_since(std::time::UNIX_EPOCH).ok()?;
    chrono::DateTime::<chrono::Utc>::from_timestamp(d.as_secs() as i64, d.subsec_nanos())
}

fn ms_to_datetime(ms: u64) -> Option<chrono::DateTime<chrono::Utc>> {
    let secs = (ms / 1000) as i64;
    let nsecs = ((ms % 1000) * 1_000_000) as u32;
    chrono::DateTime::<chrono::Utc>::from_timestamp(secs, nsecs)
}

/// Current wall-clock time in epoch milliseconds, for stamping conversation
/// writes. Saturates to 0 before the epoch, which never happens in practice.
pub(crate) fn now_millis() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// Returns the YAML source of a single flow so operators can inspect the
/// loaded flow from the web UI.
async fn get_flow(
    State(state): State<Arc<WebState>>,
    AxumPath(path): AxumPath<String>,
) -> Result<Json<api::FlowDetail>, (StatusCode, String)> {
    let Ok(registry) = state.flow_registry.read() else {
        return Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            "Flow registry is poisoned".into(),
        ));
    };
    match registry.get(&path) {
        Some(handle) => Ok(Json(api::FlowDetail {
            path: handle.identity().to_string(),
            name: handle.identity().to_string(),
            display_name: handle.display_name().map(ToString::to_string),
            yaml: handle.flow_yaml().to_string(),
        })),
        None => Err((StatusCode::NOT_FOUND, format!("Flow '{path}' not found"))),
    }
}

/// Streams live per-flow metrics to the web UI over Server-Sent Events.
///
/// Emits one `snapshot` frame with every flow's current metrics on
/// connect, then a `snapshot` frame carrying a single-element array
/// whenever any flow's counters change — the frontend already merges
/// `snapshot` payloads by `flow`, so a partial array updates just that
/// flow. Event/log history and live tail for a flow come from
/// `/api/logs` and `/api/logs/stream` (with `flow` set) — the same
/// source `/logs` uses — not from this endpoint.
async fn stream_flows(
    State(state): State<Arc<WebState>>,
) -> Sse<impl Stream<Item = Result<SseEvent, axum::Error>>> {
    let initial = state.metrics_store.snapshot_all().await.unwrap_or_default();
    let initial_frame = match SseEvent::default()
        .event(SSE_EVENT_SNAPSHOT)
        .json_data(&initial)
    {
        Ok(ev) => ev,
        Err(source) => {
            warn!(error = %source, "Failed to encode SSE snapshot frame");
            SseEvent::default().data("[]")
        }
    };

    let live = match state.metrics_store.watch_all().await {
        Ok(stream) => stream
            .filter_map(|snapshot| async move {
                match SseEvent::default()
                    .event(SSE_EVENT_SNAPSHOT)
                    .json_data(&[snapshot])
                {
                    Ok(ev) => Some(Ok(ev)),
                    Err(source) => {
                        warn!(error = %source, "Failed to encode SSE snapshot frame");
                        None
                    }
                }
            })
            .boxed(),
        Err(source) => {
            warn!(error = %source, "Metrics store watch subscription failed");
            futures_util::stream::empty().boxed()
        }
    };

    let stream = tokio_stream::once(Ok(initial_frame)).chain(live);
    Sse::new(stream).keep_alive(
        KeepAlive::new()
            .interval(Duration::from_secs(15))
            .text("keep-alive"),
    )
}

/// Default `?limit` for `/api/logs` snapshots.
const LOGS_SNAPSHOT_DEFAULT_LIMIT: usize = 500;

/// Returns retained log records — framework, lifecycle, and per-task
/// activity in one place. The per-flow Activity panel calls this with
/// `flow` set to backfill its history from the same source the global
/// `/logs` viewer uses (unscoped).
async fn list_logs(
    State(state): State<Arc<WebState>>,
    axum_extra::extract::Query(params): axum_extra::extract::Query<LogsQuery>,
) -> Json<Vec<api::LogRecord>> {
    let query = match state.logs_store.as_ref() {
        Some(q) => q,
        None => return Json(Vec::new()),
    };
    let limit = match params.limit {
        Some(n) => n.min(flowgen_core::telemetry::query::MAX_QUERY_LIMIT),
        None => LOGS_SNAPSHOT_DEFAULT_LIMIT,
    };
    let records = match query.query(params.filter(), limit).await {
        Ok(r) => r,
        Err(source) => {
            warn!(error = %source, "Log query history read failed");
            return Json(Vec::new());
        }
    };
    let wire: Vec<api::LogRecord> = records.into_iter().map(stored_to_wire).collect();
    Json(wire)
}

/// Streams log records as they arrive. Same scope as `list_logs`:
/// unscoped by default (the global `/logs` UI passes `levels` and filters
/// free text client-side); the per-flow Activity panel passes `flow` so it
/// only receives that flow's live records.
async fn stream_logs(
    State(state): State<Arc<WebState>>,
    axum_extra::extract::Query(params): axum_extra::extract::Query<LogsQuery>,
) -> Sse<impl Stream<Item = Result<SseEvent, axum::Error>>> {
    // Tail-only: `/api/logs` returns the initial snapshot, this endpoint
    // streams new records as they arrive. Sending history here too would
    // duplicate every retained record for a UI that already loaded them.
    let live = match state.logs_store.as_ref() {
        Some(query) => {
            let tail = match query.tail(params.filter()).await {
                Ok(stream) => stream,
                Err(source) => {
                    warn!(error = %source, "Log query tail subscription failed");
                    futures_util::stream::empty().boxed()
                }
            };
            tail.filter_map(|record| async move {
                let wire = stored_to_wire(record);
                match SseEvent::default().event(SSE_EVENT_LOG).json_data(&wire) {
                    Ok(ev) => Some(Ok(ev)),
                    Err(source) => {
                        warn!(error = %source, "Failed to encode SSE log frame");
                        None
                    }
                }
            })
            .boxed()
        }
        None => {
            warn!("No logs query backend configured; /api/logs/stream is empty");
            futures_util::stream::empty().boxed()
        }
    };
    Sse::new(live).keep_alive(
        KeepAlive::new()
            .interval(Duration::from_secs(15))
            .text("keep-alive"),
    )
}

#[derive(serde::Deserialize)]
struct LogsQuery {
    limit: Option<usize>,
    /// Restrict to one flow's records. Used by the per-flow Activity panel;
    /// omitted by the global `/logs` viewer, which shows every flow.
    flow: Option<String>,
    /// Levels to keep, one `levels` parameter each; omitted keeps every level.
    #[serde(default)]
    levels: Vec<api::LogLevel>,
}

impl LogsQuery {
    fn filter(self) -> flowgen_core::telemetry::query::LogFilter {
        flowgen_core::telemetry::query::LogFilter {
            flow: self.flow,
            levels: self.levels.iter().map(ToString::to_string).collect(),
            ..Default::default()
        }
    }
}

/// The wire level of a stored record's tracing level.
pub(crate) fn wire_level(level: &str) -> api::LogLevel {
    match level {
        "warn" | "warning" => api::LogLevel::Warn,
        "error" => api::LogLevel::Error,
        "debug" => api::LogLevel::Debug,
        "trace" => api::LogLevel::Trace,
        _ => api::LogLevel::Info,
    }
}

/// Converts an internal `StoredLog` to the OpenAPI wire shape.
fn stored_to_wire(record: flowgen_core::telemetry::StoredLog) -> api::LogRecord {
    let spans = record
        .spans
        .into_iter()
        .map(|s| api::LogSpan {
            name: s.name,
            fields: s.fields.into_iter().map(kv_to_wire).collect(),
        })
        .collect();
    let timestamp = match record.timestamp.as_deref() {
        None => None,
        Some(ts) => match chrono::DateTime::parse_from_rfc3339(ts) {
            Ok(dt) => Some(dt.with_timezone(&chrono::Utc)),
            Err(_) => None,
        },
    };
    api::LogRecord {
        body: record.body,
        level: wire_level(&record.level),
        timestamp,
        target: record.target,
        spans,
        fields: record.fields.into_iter().map(kv_to_wire).collect(),
    }
}

fn kv_to_wire((k, v): (String, String)) -> api::KeyValue {
    api::KeyValue { key: k, value: v }
}

/// Returns the list of resources discoverable from the filesystem loader.
/// Cache-backed loaders are not walked today (no listing API on the cache
/// abstraction); those installations get an empty list until we add one.
/// Symlinks are followed and dot entries skipped, so a mounted ConfigMap lists
/// each file once, under its clean name.
async fn list_resources(State(state): State<Arc<WebState>>) -> Json<Vec<api::ResourceSummary>> {
    let Some(loader) = &state.resource_loader else {
        return Json(Vec::new());
    };
    let Some(base) = loader.base_path() else {
        return Json(Vec::new());
    };

    let mut entries: Vec<api::ResourceSummary> = walkdir::WalkDir::new(base)
        .follow_links(true)
        .into_iter()
        .filter_entry(|e| e.depth() == 0 || !e.file_name().to_string_lossy().starts_with('.'))
        .filter_map(Result::ok)
        .filter(|e| e.file_type().is_file())
        .filter_map(|e| {
            let rel = e.path().strip_prefix(base).ok()?;
            let key = rel.to_string_lossy().replace('\\', "/");
            let extension = e
                .path()
                .extension()
                .and_then(|s| s.to_str())
                .map(str::to_string);
            let size = e.metadata().ok().map(|m| m.len() as i64);
            Some(api::ResourceSummary {
                key,
                extension,
                size,
            })
        })
        .collect();
    entries.sort_by(|a, b| a.key.cmp(&b.key));
    Json(entries)
}

/// Returns the content of a single resource by key.
async fn get_resource(
    State(state): State<Arc<WebState>>,
    AxumPath(key): AxumPath<String>,
) -> Result<Json<api::ResourceContent>, (StatusCode, String)> {
    let Some(loader) = &state.resource_loader else {
        return Err((
            StatusCode::NOT_FOUND,
            "Resource loader not configured".into(),
        ));
    };
    // Guard path traversal — the loader itself would resolve `..` against
    // its base, so a hostile key could escape the resources directory.
    if key.split('/').any(|seg| seg == "..") {
        return Err((StatusCode::BAD_REQUEST, "Invalid resource key".into()));
    }
    match loader.load(&key).await {
        Ok(content) => {
            let extension = std::path::Path::new(&key)
                .extension()
                .and_then(|s| s.to_str())
                .map(str::to_string);
            Ok(Json(api::ResourceContent {
                key,
                extension,
                content,
            }))
        }
        Err(source) => Err((StatusCode::NOT_FOUND, source.to_string())),
    }
}

/// Lists the pods behind the web UI with their reachability and flow count;
/// only this pod when pods do not share their logs and counters.
async fn get_cluster_status(
    State(state): State<Arc<WebState>>,
) -> Result<Json<api::ClusterStatus>, (StatusCode, String)> {
    use flowgen_core::telemetry::cluster::{PodStatus, Reachability};
    let flows = state.running_flows.count().await;
    let pods = match state.cluster_peers.as_ref() {
        Some(peers) => peers.status(flows).await,
        None => Ok(vec![PodStatus {
            identity: state.this_pod.clone(),
            address: None,
            reachability: Reachability::Reachable { flows },
        }]),
    };
    match pods {
        Ok(pods) => Ok(Json(api::ClusterStatus {
            pods: pods
                .into_iter()
                .map(|pod| {
                    let (flows, unreachable_reason) = match pod.reachability {
                        Reachability::Reachable { flows } => (Some(flows as u64), None),
                        Reachability::Unreachable { reason } => (None, Some(reason)),
                    };
                    api::PodStatus {
                        identity: pod.identity,
                        address: pod.address,
                        unreachable_reason,
                        flows,
                    }
                })
                .collect(),
        })),
        Err(e) => {
            warn!(error = %e, "Failed to list peers for cluster status");
            Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                "Failed to list peers".into(),
            ))
        }
    }
}

/// Replaces the token pods present to each other, e.g. after a leak.
async fn regenerate_cluster_token(
    State(state): State<Arc<WebState>>,
    user: Option<axum::Extension<flowgen_core::auth::UserContext>>,
) -> Result<StatusCode, (StatusCode, String)> {
    let Some(peers) = state.cluster_peers.as_ref() else {
        return Err((StatusCode::NOT_FOUND, "Cluster mode is off".into()));
    };
    match peers.regenerate_token().await {
        Ok(()) => {
            match user {
                Some(axum::Extension(user)) => {
                    warn!(user_id = %user.user_id, "Cluster token regenerated through the web API")
                }
                None => warn!("Cluster token regenerated through the web API"),
            }
            Ok(StatusCode::NO_CONTENT)
        }
        Err(e) => {
            warn!(error = %e, "Failed to regenerate the cluster token");
            Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                "Failed to regenerate the cluster token".into(),
            ))
        }
    }
}

/// Returns the running flowgen version so the UI can render it in the sidebar.
async fn get_version() -> Json<api::VersionInfo> {
    Json(api::VersionInfo {
        version: env!("CARGO_PKG_VERSION").to_string(),
    })
}

/// Returns the running application configuration as YAML for the web
/// config viewer. Secrets are redacted at serialization time (see
/// `JwtConfig`), so no additional masking is needed here.
async fn get_config(State(state): State<Arc<WebState>>) -> Json<api::ConfigInfo> {
    let yaml = match serde_yaml::to_string(&*state.app_config) {
        Ok(yaml) => yaml,
        Err(source) => {
            warn!(error = %source, "Failed to serialize app config to YAML");
            String::new()
        }
    };
    let authoring_targets = match &state.authoring {
        Some(authoring) => authoring
            .targets
            .iter()
            .map(|target| api::AuthoringTarget {
                name: target.name.clone(),
                paths: target.paths.clone(),
            })
            .collect(),
        None => Vec::new(),
    };
    Json(api::ConfigInfo {
        yaml,
        agents: state.agents.is_some(),
        authoring: state.authoring.is_some(),
        authoring_targets,
    })
}

/// Answers `/api/agents` with 404 unless `web.agents` is enabled.
async fn require_agents(
    State(state): State<Arc<WebState>>,
    request: axum::extract::Request,
    next: axum::middleware::Next,
) -> axum::response::Response {
    match state.agents {
        Some(_) => next.run(request).await,
        None => (StatusCode::NOT_FOUND, "Agents are not enabled (web.agents)").into_response(),
    }
}

/// Resolves the base URL the built-in Agents chat proxies to. Prefers the
/// explicit `web.ai_gateway_url`; otherwise targets the same-process AI
/// gateway on loopback. Returns `None` when no gateway is configured.
pub(crate) fn gateway_base_url(state: &WebState) -> Option<String> {
    if let Some(url) = state
        .app_config
        .web
        .as_ref()
        .and_then(|w| w.ai_gateway_url.as_ref())
    {
        return Some(url.trim_end_matches('/').to_string());
    }
    let gateway = state.app_config.ai_gateway.as_ref()?;
    let path = gateway.path.trim_end_matches('/');
    Some(format!("http://127.0.0.1:{}{path}", gateway.port))
}

/// Builds the outbound headers sent with every proxied request to the AI
/// gateway, from `web.headers`. Used to identify this web server to
/// `llm_proxy`/`mcp_tool` `headers` scoping (e.g. `X-Flowgen-Client:
/// flowgen-ui`). Entries that aren't valid header names/values are skipped.
fn outbound_gateway_headers(state: &WebState) -> reqwest::header::HeaderMap {
    let mut headers = reqwest::header::HeaderMap::new();
    let Some(web) = state.app_config.web.as_ref() else {
        return headers;
    };
    for (name, value) in &web.headers {
        let Ok(header_name) = reqwest::header::HeaderName::try_from(name.as_str()) else {
            continue;
        };
        let Ok(header_value) = reqwest::header::HeaderValue::from_str(value) else {
            continue;
        };
        headers.insert(header_name, header_value);
    }
    headers
}

/// The headers for a request to the AI gateway: `web.headers`, and the
/// user's token when `web.agents.on_behalf_of_user` is set.
pub(crate) fn gateway_headers(
    state: &WebState,
    session: Option<&flowgen_core::auth::UserSession>,
) -> reqwest::header::HeaderMap {
    let mut headers = outbound_gateway_headers(state);
    if let (
        Some(crate::config::AgentsOptions {
            on_behalf_of_user: true,
            ..
        }),
        Some(session),
    ) = (&state.agents, session)
    {
        if let Some(token) = &session.token {
            use secrecy::ExposeSecret;
            match reqwest::header::HeaderValue::from_str(&format!(
                "Bearer {}",
                token.expose_secret()
            )) {
                Ok(mut value) => {
                    value.set_sensitive(true);
                    headers.insert(reqwest::header::AUTHORIZATION, value);
                }
                Err(source) => {
                    warn!(error = %source, "Failed to forward the user's token to the AI gateway");
                }
            }
        }
    }
    headers
}

/// Proxies the gateway model list so the Agents chat can populate its model
/// selector without knowing the gateway URL.
async fn proxy_models(State(state): State<Arc<WebState>>) -> axum::response::Response {
    let Some(base) = gateway_base_url(&state) else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            "AI gateway is not configured",
        )
            .into_response();
    };
    match reqwest::Client::new()
        .get(format!("{base}/models"))
        .headers(outbound_gateway_headers(&state))
        .send()
        .await
    {
        Ok(resp) => {
            let status =
                StatusCode::from_u16(resp.status().as_u16()).unwrap_or(StatusCode::BAD_GATEWAY);
            let text = resp.text().await.unwrap_or_default();
            let mut headers = HeaderMap::new();
            headers.insert(
                axum::http::header::CONTENT_TYPE,
                axum::http::HeaderValue::from_static("application/json"),
            );
            (status, headers, text).into_response()
        }
        Err(source) => {
            warn!(error = %source, "Failed to list AI gateway models for the Agents tab");
            (StatusCode::BAD_GATEWAY, "Failed to reach AI gateway").into_response()
        }
    }
}

// Persistence for the web UI's Agents chat. The gateway proxy stays
// stateless; conversation memory is our UI's domain and lives in the
// configured system cache (see `WebState::conversation_cache`), out of
// user-script reach. A persistence flow can later copy these into a database.
// Types (`api::Conversation`, etc.) are generated from openapi.yaml.

/// What a key under `web.agents.cache_prefix` holds.
#[derive(Clone, Copy)]
pub(crate) enum AgentsKeys {
    Conversations,
    Reads,
    Folders,
}

pub(crate) fn key_prefix(state: &WebState, keys: AgentsKeys) -> String {
    let root = match &state.agents {
        Some(agents) => agents.cache_prefix.clone(),
        None => crate::config::default_agents_cache_prefix(),
    };
    let segment = match keys {
        AgentsKeys::Conversations => "conversations",
        AgentsKeys::Reads => "reads",
        AgentsKeys::Folders => "folders",
    };
    format!("{root}.{segment}.")
}

pub(crate) fn reply_save_interval(state: &WebState) -> Duration {
    match &state.agents {
        Some(agents) => agents.reply_save_interval,
        None => crate::config::default_reply_save_interval(),
    }
}

pub(crate) fn reply_idle_timeout(state: &WebState) -> Duration {
    match &state.agents {
        Some(agents) => agents.reply_idle_timeout,
        None => crate::config::default_reply_idle_timeout(),
    }
}

/// Validates a client-supplied conversation id: `[A-Za-z0-9_-]+`, non-empty.
/// Anything else is rejected rather than silently sanitized — the id is the
/// client's own handle, and `.` would break the dotted KV key namespace.
/// `stream` is taken by the route of the conversation stream.
pub(crate) fn valid_conversation_id(id: &str) -> bool {
    !id.is_empty()
        && id != "stream"
        && id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
}

/// The signed-in user's key segment: the hex SHA-256 of their id, so a flow
/// can derive it from `event.meta.auth.user_id` with the script `sha256` and
/// keys don't show who the user is. Without `web.auth`, or for a machine key,
/// there is none and conversations are shared.
pub(crate) fn conversation_owner(
    user: Option<&axum::Extension<flowgen_core::auth::UserContext>>,
) -> Option<String> {
    use sha2::Digest;
    user.map(|axum::Extension(user)| format!("{:x}", sha2::Sha256::digest(&user.user_id)))
}

/// `agents.conversations.<owner>.<id>` for a user's own conversation, and
/// `agents.conversations.<id>` for a shared one, such as a thread a flow opened.
pub(crate) fn conversation_key(state: &WebState, owner: Option<&str>, id: &str) -> String {
    user_key(&key_prefix(state, AgentsKeys::Conversations), owner, id)
}

/// Whether a stored conversation is the caller's own or shared.
fn visible_to(prefix: &str, owner: Option<&str>, key: &str) -> bool {
    match key.strip_prefix(prefix).map(|rest| rest.split_once('.')) {
        Some(None) => true,
        Some(Some((key_owner, _))) => owner == Some(key_owner),
        None => false,
    }
}

/// Conversation `id` as the caller sees it: their own, else the shared one.
pub(crate) async fn find_conversation(
    state: &WebState,
    owner: Option<&str>,
    id: &str,
) -> Result<Option<(String, bytes::Bytes)>, flowgen_core::cache::Error> {
    let own = owner.map(|owner| conversation_key(state, Some(owner), id));
    for key in own.into_iter().chain([conversation_key(state, None, id)]) {
        if let Some(bytes) = state.conversation_cache.get(&key).await? {
            return Ok(Some((key, bytes)));
        }
    }
    Ok(None)
}

/// The conversation id in a stored key, without the prefix and owner.
fn conversation_id_of<'a>(prefix: &str, key: &'a str) -> Option<&'a str> {
    let rest = key.strip_prefix(prefix)?;
    match rest.split_once('.') {
        Some((_, id)) => Some(id),
        None => Some(rest),
    }
}

/// `<prefix><user>.<id>` for a signed-in user's own key, `<prefix><id>`
/// without `web.auth`, where everyone shares one.
fn user_key(prefix: &str, owner: Option<&str>, id: &str) -> String {
    match owner {
        Some(owner) => format!("{prefix}{owner}.{id}"),
        None => format!("{prefix}{id}"),
    }
}

/// Read state lives apart from the conversation, so reading it doesn't
/// change it for anyone else.
fn read_key(state: &WebState, owner: Option<&str>, id: &str) -> String {
    user_key(&key_prefix(state, AgentsKeys::Reads), owner, id)
}

fn folder_key(state: &WebState, owner: Option<&str>, id: &str) -> String {
    user_key(&key_prefix(state, AgentsKeys::Folders), owner, id)
}

/// The conversation id in a per-user key of `owner`, if the key is theirs.
fn user_key_id<'a>(prefix: &str, owner: Option<&str>, key: &'a str) -> Option<&'a str> {
    let rest = key.strip_prefix(prefix)?;
    match (owner, rest.split_once('.')) {
        (Some(owner), Some((user, id))) if user == owner => Some(id),
        (None, None) => Some(rest),
        _ => None,
    }
}

async fn user_value(state: &WebState, key: &str) -> Option<String> {
    match state.conversation_cache.get(key).await {
        Ok(Some(bytes)) => String::from_utf8(bytes.to_vec()).ok(),
        Ok(None) => None,
        Err(source) => {
            warn!(key = %key, error = %source, "Failed to read conversation state");
            None
        }
    }
}

/// The conversation's `updatedAt` when the reader last read it; `0` marks it
/// unread, none falls back to the conversation's own `unread`.
async fn read_at(state: &WebState, owner: Option<&str>, id: &str) -> Option<i64> {
    user_value(state, &read_key(state, owner, id))
        .await?
        .parse()
        .ok()
}

/// The folder the user filed the conversation in; empty means none, and
/// missing falls back to the folder a flow gave it.
async fn folder_of(state: &WebState, owner: Option<&str>, id: &str) -> Option<String> {
    user_value(state, &folder_key(state, owner, id)).await
}

/// A folder path as `a/b`: segments trimmed, empty ones dropped.
fn normalize_folder(folder: &str) -> String {
    folder
        .split('/')
        .map(str::trim)
        .filter(|segment| !segment.is_empty())
        .collect::<Vec<_>>()
        .join("/")
}

/// How long conversations and their read state live, from `web.agents`.
pub(crate) fn conversation_ttl(state: &WebState) -> Option<u64> {
    match &state.agents {
        Some(crate::config::AgentsOptions {
            conversation_history_ttl: Some(ttl),
            ..
        }) if ttl.as_secs() > 0 => Some(ttl.as_secs()),
        _ => None,
    }
}

pub(crate) async fn write_read_at(
    state: &WebState,
    owner: Option<&str>,
    id: &str,
    at: i64,
) -> Result<(), flowgen_core::cache::Error> {
    state
        .conversation_cache
        .put(
            &read_key(state, owner, id),
            at.to_string().into(),
            conversation_ttl(state),
        )
        .await
}

/// A stored conversation as the reader sees it: unread when it changed since
/// they last read it. A corrupt entry is skipped, not fatal.
pub(crate) async fn conversation_for(
    state: &WebState,
    owner: Option<&str>,
    key: &str,
    bytes: &[u8],
) -> Option<api::Conversation> {
    let mut conversation = match serde_json::from_slice::<api::Conversation>(bytes) {
        Ok(conversation) => conversation,
        Err(source) => {
            warn!(key = %key, error = %source, "Skipping unparseable conversation");
            return None;
        }
    };
    let unread = match read_at(state, owner, &conversation.id).await {
        Some(at) => at < conversation.updated_at,
        None => conversation.unread == Some(true),
    };
    conversation.unread = unread.then_some(true);
    if let Some(folder) = folder_of(state, owner, &conversation.id).await {
        conversation.folder = Some(folder).filter(|folder| !folder.is_empty());
    }
    crate::agent_reply::settle_interrupted(&mut conversation, reply_save_interval(state));
    Some(conversation)
}

fn summary_of(c: api::Conversation) -> api::ConversationSummary {
    api::ConversationSummary {
        id: c.id,
        title: c.title,
        updated_at: c.updated_at,
        message_count: c.messages.len() as i64,
        origin: c.origin,
        unread: c.unread,
        folder: c.folder,
    }
}

/// The caller's own and the shared conversations, newest first.
async fn conversation_summaries(
    state: &WebState,
    owner: Option<&str>,
) -> Result<Vec<api::ConversationSummary>, flowgen_core::cache::Error> {
    let prefix = key_prefix(state, AgentsKeys::Conversations);
    let keys = state.conversation_cache.list_keys(&prefix).await?;
    let mut summaries = Vec::with_capacity(keys.len());
    for key in keys
        .into_iter()
        .filter(|key| visible_to(&prefix, owner, key))
    {
        match state.conversation_cache.get(&key).await {
            Ok(Some(bytes)) => summaries.extend(
                conversation_for(state, owner, &key, &bytes)
                    .await
                    .map(summary_of),
            ),
            Ok(None) => {}
            Err(source) => warn!(key = %key, error = %source, "Failed to read conversation"),
        }
    }
    summaries.sort_by_key(|s| std::cmp::Reverse(s.updated_at));
    Ok(summaries)
}

/// Lists the caller's own and the shared conversations (summaries only),
/// newest first.
async fn list_conversations(
    State(state): State<Arc<WebState>>,
    user: Option<axum::Extension<flowgen_core::auth::UserContext>>,
) -> axum::response::Response {
    let owner = conversation_owner(user.as_ref());
    match conversation_summaries(&state, owner.as_deref()).await {
        Ok(summaries) => Json(serde_json::json!({ "conversations": summaries })).into_response(),
        Err(source) => {
            warn!(error = %source, "Failed to list conversations from cache");
            (
                StatusCode::SERVICE_UNAVAILABLE,
                "Conversation store unavailable",
            )
                .into_response()
        }
    }
}

/// Streams the caller's own and the shared conversations: the list on
/// connect, then each change. The watch starts before the list is read, so
/// no change is missed; one may arrive twice.
async fn stream_conversations(
    State(state): State<Arc<WebState>>,
    user: Option<axum::Extension<flowgen_core::auth::UserContext>>,
) -> axum::response::Response {
    let owner = conversation_owner(user.as_ref());
    let conversations = key_prefix(&state, AgentsKeys::Conversations);
    let reads = key_prefix(&state, AgentsKeys::Reads);
    let folders = key_prefix(&state, AgentsKeys::Folders);
    let watch = |prefix: String| {
        let state = Arc::clone(&state);
        async move {
            match state
                .conversation_cache
                .watch(prefix.trim_end_matches('.'), false)
                .await
            {
                Ok(stream) => stream,
                Err(source) => {
                    warn!(error = %source, "Conversation watch subscription failed");
                    futures_util::stream::empty().boxed()
                }
            }
        }
    };
    let live = futures_util::stream::select_all([
        watch(conversations.clone()).await,
        watch(reads.clone()).await,
        watch(folders.clone()).await,
    ]);
    let initial = match conversation_summaries(&state, owner.as_deref()).await {
        Ok(summaries) => SseEvent::default()
            .event("conversations")
            .json_data(&summaries),
        Err(source) => {
            warn!(error = %source, "Failed to list conversations from cache");
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                "Conversation store unavailable",
            )
                .into_response();
        }
    };

    let changes = live.filter_map(move |event| {
        let owner = owner.clone();
        let state = Arc::clone(&state);
        let (conversations, reads, folders) =
            (conversations.clone(), reads.clone(), folders.clone());
        async move {
            let event = match event {
                Ok(event) => event,
                Err(source) => {
                    warn!(error = %source, "Conversation watch failed");
                    return None;
                }
            };
            let owner = owner.as_deref();
            let changed = |summary: api::ConversationSummary| {
                SseEvent::default()
                    .event("conversation")
                    .json_data(&summary)
            };
            let conversation_key = |key: &str| {
                key.starts_with(&conversations) && visible_to(&conversations, owner, key)
            };
            match event {
                flowgen_core::cache::WatchEvent::Put { key, value } if conversation_key(&key) => {
                    let conversation = conversation_for(&state, owner, &key, &value).await?;
                    Some(changed(summary_of(conversation)))
                }
                flowgen_core::cache::WatchEvent::Delete { key } if conversation_key(&key) => {
                    let id = conversation_id_of(&conversations, &key)?;
                    Some(Ok(SseEvent::default().event("removed").data(id)))
                }
                flowgen_core::cache::WatchEvent::Put { key, .. }
                | flowgen_core::cache::WatchEvent::Delete { key } => {
                    let id = user_key_id(&reads, owner, &key)
                        .or_else(|| user_key_id(&folders, owner, &key))?;
                    let (key, bytes) = match find_conversation(&state, owner, id).await {
                        Ok(Some(found)) => found,
                        Ok(None) => return None,
                        Err(source) => {
                            warn!(id = %id, error = %source, "Failed to read conversation from cache");
                            return None;
                        }
                    };
                    let conversation = conversation_for(&state, owner, &key, &bytes).await?;
                    Some(changed(summary_of(conversation)))
                }
            }
        }
    });

    let stream = tokio_stream::once(initial).chain(changes);
    Sse::new(stream)
        .keep_alive(
            KeepAlive::new()
                .interval(Duration::from_secs(15))
                .text("keep-alive"),
        )
        .into_response()
}

/// Returns a single conversation with its full message history.
async fn get_conversation(
    State(state): State<Arc<WebState>>,
    AxumPath(id): AxumPath<String>,
    user: Option<axum::Extension<flowgen_core::auth::UserContext>>,
) -> axum::response::Response {
    if !valid_conversation_id(&id) {
        return (StatusCode::BAD_REQUEST, "Invalid conversation id").into_response();
    }
    let owner = conversation_owner(user.as_ref());
    match find_conversation(&state, owner.as_deref(), &id).await {
        Ok(Some((key, bytes))) => {
            match conversation_for(&state, owner.as_deref(), &key, &bytes).await {
                Some(c) => Json(c).into_response(),
                None => (StatusCode::INTERNAL_SERVER_ERROR, "Corrupt conversation").into_response(),
            }
        }
        Ok(None) => (StatusCode::NOT_FOUND, "Conversation not found").into_response(),
        Err(source) => {
            warn!(id = %id, error = %source, "Failed to read conversation from cache");
            (
                StatusCode::SERVICE_UNAVAILABLE,
                "Conversation store unavailable",
            )
                .into_response()
        }
    }
}

/// Creates or overwrites a conversation. The path id is authoritative and the
/// `updated_at` is server-stamped; the TTL is refreshed on every write, so the
/// expiry window counts from the last activity. An existing shared
/// conversation stays shared; a new one is the signed-in user's own. The
/// writer has read it; for everyone else it is unread if a flow marked it so.
async fn put_conversation(
    State(state): State<Arc<WebState>>,
    AxumPath(id): AxumPath<String>,
    user: Option<axum::Extension<flowgen_core::auth::UserContext>>,
    Json(body): Json<api::ConversationUpsert>,
) -> axum::response::Response {
    if !valid_conversation_id(&id) {
        return (StatusCode::BAD_REQUEST, "Invalid conversation id").into_response();
    }
    let owner = conversation_owner(user.as_ref());
    let (key, stored) = match find_conversation(&state, owner.as_deref(), &id).await {
        Ok(Some((key, bytes))) => match serde_json::from_slice::<api::Conversation>(&bytes) {
            Ok(stored) => (key, Some(stored)),
            Err(source) => {
                warn!(id = %id, error = %source, "Overwriting an unparseable conversation");
                (key, None)
            }
        },
        Ok(None) => (conversation_key(&state, owner.as_deref(), &id), None),
        Err(source) => {
            warn!(id = %id, error = %source, "Failed to read conversation from cache");
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                "Conversation store unavailable",
            )
                .into_response();
        }
    };

    let conversation = api::Conversation {
        id: id.clone(),
        title: body.title,
        messages: body.messages,
        model: body.model,
        origin: body.origin,
        unread: stored.as_ref().and_then(|c| c.unread),
        folder: stored.and_then(|c| c.folder),
        updated_at: now_millis(),
    };
    if let Err(source) = write_read_at(&state, owner.as_deref(), &id, conversation.updated_at).await
    {
        warn!(id = %id, error = %source, "Failed to mark the conversation read");
    }
    let bytes = match serde_json::to_vec(&conversation) {
        Ok(bytes) => bytes,
        Err(source) => {
            warn!(id = %id, error = %source, "Failed to serialize conversation");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                "Failed to store conversation",
            )
                .into_response();
        }
    };

    match state
        .conversation_cache
        .put(&key, bytes.into(), conversation_ttl(&state))
        .await
    {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(source) => {
            warn!(id = %id, error = %source, "Failed to write conversation to cache");
            (
                StatusCode::SERVICE_UNAVAILABLE,
                "Conversation store unavailable",
            )
                .into_response()
        }
    }
}

/// Renames a conversation or moves it to one of the caller's folders,
/// keeping `updated_at`, so it neither moves up the list nor turns unread
/// for anyone.
async fn update_conversation(
    State(state): State<Arc<WebState>>,
    AxumPath(id): AxumPath<String>,
    user: Option<axum::Extension<flowgen_core::auth::UserContext>>,
    Json(body): Json<api::ConversationUpdate>,
) -> axum::response::Response {
    let title = body.title.as_deref().map(str::trim);
    if !valid_conversation_id(&id) || title == Some("") {
        return (StatusCode::BAD_REQUEST, "Invalid conversation id or title").into_response();
    }
    let owner = conversation_owner(user.as_ref());
    let (key, bytes) = match find_conversation(&state, owner.as_deref(), &id).await {
        Ok(Some(found)) => found,
        Ok(None) => return (StatusCode::NOT_FOUND, "Conversation not found").into_response(),
        Err(source) => {
            warn!(id = %id, error = %source, "Failed to read conversation from cache");
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                "Conversation store unavailable",
            )
                .into_response();
        }
    };
    let mut conversation = match serde_json::from_slice::<api::Conversation>(&bytes) {
        Ok(conversation) => conversation,
        Err(_) => {
            return (StatusCode::INTERNAL_SERVER_ERROR, "Corrupt conversation").into_response()
        }
    };
    let unavailable = |source: flowgen_core::cache::Error| {
        warn!(id = %id, error = %source, "Failed to write conversation to cache");
        (
            StatusCode::SERVICE_UNAVAILABLE,
            "Conversation store unavailable",
        )
            .into_response()
    };
    if let Some(folder) = &body.folder {
        let key = folder_key(&state, owner.as_deref(), &id);
        let folder = normalize_folder(folder);
        if let Err(source) = state
            .conversation_cache
            .put(&key, folder.into(), conversation_ttl(&state))
            .await
        {
            return unavailable(source);
        }
    }
    if let Some(title) = title {
        conversation.title = title.to_string();
        let bytes = match serde_json::to_vec(&conversation) {
            Ok(bytes) => bytes,
            Err(source) => {
                warn!(id = %id, error = %source, "Failed to serialize conversation");
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "Failed to store conversation",
                )
                    .into_response();
            }
        };
        if let Err(source) = state
            .conversation_cache
            .put(&key, bytes.into(), conversation_ttl(&state))
            .await
        {
            return unavailable(source);
        }
    }
    StatusCode::NO_CONTENT.into_response()
}

/// Deletes a conversation with the caller's read state and folder, and with
/// everyone's for a shared one. Idempotent — deleting a missing id still
/// succeeds.
async fn delete_conversation(
    State(state): State<Arc<WebState>>,
    AxumPath(id): AxumPath<String>,
    user: Option<axum::Extension<flowgen_core::auth::UserContext>>,
) -> axum::response::Response {
    if !valid_conversation_id(&id) {
        return (StatusCode::BAD_REQUEST, "Invalid conversation id").into_response();
    }
    let owner = conversation_owner(user.as_ref());
    let (deleted, shared) = match find_conversation(&state, owner.as_deref(), &id).await {
        Ok(Some((key, _))) => (
            state.conversation_cache.delete(&key).await,
            key == conversation_key(&state, None, &id),
        ),
        Ok(None) => (Ok(()), false),
        Err(source) => (Err(source), false),
    };
    let mut keys = vec![
        read_key(&state, owner.as_deref(), &id),
        folder_key(&state, owner.as_deref(), &id),
    ];
    if shared {
        for prefix in [
            key_prefix(&state, AgentsKeys::Reads),
            key_prefix(&state, AgentsKeys::Folders),
        ] {
            match state.conversation_cache.list_keys(&prefix).await {
                Ok(found) => keys.extend(
                    found
                        .into_iter()
                        .filter(|key| conversation_id_of(&prefix, key) == Some(id.as_str())),
                ),
                Err(source) => {
                    warn!(prefix = %prefix, error = %source, "Failed to list conversation state")
                }
            }
        }
    }
    keys.sort();
    keys.dedup();
    for key in keys {
        if let Err(source) = state.conversation_cache.delete(&key).await {
            warn!(key = %key, error = %source, "Failed to delete conversation state");
        }
    }
    match deleted {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(source) => {
            warn!(id = %id, error = %source, "Failed to delete conversation from cache");
            (
                StatusCode::SERVICE_UNAVAILABLE,
                "Conversation store unavailable",
            )
                .into_response()
        }
    }
}

/// Marks a conversation read for the caller only.
async fn mark_conversation_read(
    State(state): State<Arc<WebState>>,
    AxumPath(id): AxumPath<String>,
    user: Option<axum::Extension<flowgen_core::auth::UserContext>>,
) -> axum::response::Response {
    set_conversation_read(&state, &id, user.as_ref(), true).await
}

/// Marks a conversation unread for the caller only.
async fn mark_conversation_unread(
    State(state): State<Arc<WebState>>,
    AxumPath(id): AxumPath<String>,
    user: Option<axum::Extension<flowgen_core::auth::UserContext>>,
) -> axum::response::Response {
    set_conversation_read(&state, &id, user.as_ref(), false).await
}

async fn set_conversation_read(
    state: &WebState,
    id: &str,
    user: Option<&axum::Extension<flowgen_core::auth::UserContext>>,
    read: bool,
) -> axum::response::Response {
    if !valid_conversation_id(id) {
        return (StatusCode::BAD_REQUEST, "Invalid conversation id").into_response();
    }
    let owner = conversation_owner(user);
    let updated_at = match find_conversation(state, owner.as_deref(), id).await {
        Ok(Some((_, bytes))) => match serde_json::from_slice::<api::Conversation>(&bytes) {
            Ok(c) => c.updated_at,
            Err(_) => {
                return (StatusCode::INTERNAL_SERVER_ERROR, "Corrupt conversation").into_response()
            }
        },
        Ok(None) => return (StatusCode::NOT_FOUND, "Conversation not found").into_response(),
        Err(source) => {
            warn!(id = %id, error = %source, "Failed to read conversation from cache");
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                "Conversation store unavailable",
            )
                .into_response();
        }
    };
    let at = match read {
        true => updated_at,
        false => 0,
    };
    match write_read_at(state, owner.as_deref(), id, at).await {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(source) => {
            warn!(id = %id, error = %source, "Failed to mark the conversation read");
            (
                StatusCode::SERVICE_UNAVAILABLE,
                "Conversation store unavailable",
            )
                .into_response()
        }
    }
}

/// Returns the bundled OpenAPI spec.
async fn get_openapi() -> impl IntoResponse {
    let mut headers = HeaderMap::new();
    headers.insert(
        axum::http::header::CONTENT_TYPE,
        axum::http::HeaderValue::from_static("application/yaml"),
    );
    (StatusCode::OK, headers, flowgen_client::OPENAPI_YAML)
}

/// Serves a file from the embedded asset folder.
async fn serve_embedded(State(state): State<Arc<WebState>>, uri: Uri) -> axum::response::Response {
    let raw = uri.path();
    // When a non-empty prefix is configured, requests outside that prefix
    // must not surface the UI — the client would then baked-in a wrong
    // base path and every subsequent API call would 404 into the HTML
    // fallback. Redirect to the mount point so both UI and API share the
    // same prefix.
    let stripped = if state.prefix.is_empty() {
        raw
    } else {
        match raw.strip_prefix(&state.prefix) {
            Some(rest) if rest.is_empty() || rest.starts_with('/') => rest,
            _ => return Redirect::temporary(&format!("{}/", state.prefix)).into_response(),
        }
    };
    let path = match stripped.trim_start_matches('/') {
        "" => "index.html".to_string(),
        rest => rest.to_string(),
    };

    let (asset_path, content) = match WebAssets::get(&path) {
        Some(content) => (path, content),
        None => match WebAssets::get("index.html") {
            Some(content) => ("index.html".to_string(), content),
            None => return (StatusCode::NOT_FOUND, HeaderMap::new(), Vec::new()).into_response(),
        },
    };

    let content_type = mime_guess::from_path(&asset_path).first_or_octet_stream();
    let content_type_header = match axum::http::HeaderValue::from_str(content_type.as_ref()) {
        Ok(v) => v,
        Err(_) => axum::http::HeaderValue::from_static("application/octet-stream"),
    };
    let mut headers = HeaderMap::new();
    headers.insert(axum::http::header::CONTENT_TYPE, content_type_header);

    let body = match rewrite_base_path(&asset_path, &content.data, &state.prefix) {
        Some(rewritten) => rewritten,
        None => content.data.into_owned(),
    };

    if asset_path.ends_with(".html") {
        let default_ancestors = crate::config::default_frame_ancestors();
        let frame_ancestors = match &state.app_config.web {
            Some(web) => &web.frame_ancestors,
            None => &default_ancestors,
        };
        match axum::http::HeaderValue::from_str(&content_security_policy(&body, frame_ancestors)) {
            Ok(policy) => {
                headers.insert(axum::http::header::CONTENT_SECURITY_POLICY, policy);
            }
            Err(source) => warn!(error = %source, "Skipping an invalid Content-Security-Policy"),
        }
    }

    (StatusCode::OK, headers, body).into_response()
}

/// Rewrites `BUILT_BASE_PATH` occurrences in text assets to `prefix`.
/// Returns `None` for binary assets or when `prefix == BUILT_BASE_PATH`.
fn rewrite_base_path(asset_path: &str, bytes: &[u8], prefix: &str) -> Option<Vec<u8>> {
    if prefix == BUILT_BASE_PATH {
        return None;
    }
    let ext = asset_path.rsplit('.').next()?;
    match ext {
        "html" | "js" | "css" | "json" | "map" | "webmanifest" => {}
        _ => return None,
    }
    let text = std::str::from_utf8(bytes).ok()?;
    // Slashed form first so the bare replace does not overwrite
    // asset URLs that share the `/flowgen` prefix.
    let slashed_replacement = match prefix {
        "" => "/".to_string(),
        other => format!("{other}/"),
    };
    let slashed_needle = format!("{BUILT_BASE_PATH}/");
    let rewritten = text
        .replace(&slashed_needle, &slashed_replacement)
        .replace(BUILT_BASE_PATH, prefix);
    Some(rewritten.into_bytes())
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn test_default_constants() {
        assert_eq!(DEFAULT_WEB_PORT, 8080);
        assert_eq!(DEFAULT_WEB_PATH, "/");
    }

    pub(crate) fn test_state() -> WebState {
        let app_config = Arc::new(crate::config::AppConfig {
            cache: None,
            flows: crate::config::FlowOptions {
                path: None,
                cache: None,
            },
            resources: None,
            http_server: None,
            mcp_server: None,
            ai_gateway: None,
            web: None,
            health: Default::default(),
            retry: None,
            telemetry: None,
        });
        let flow_registry = Arc::new(RwLock::new(HashMap::new()));
        WebState {
            flow_registry: Arc::clone(&flow_registry),
            prefix: String::new(),
            resource_loader: None,
            metrics_store: flowgen_core::flow::activity::OtlpMetricsStore::builder().build(),
            logs_store: None,
            cluster_peers: None,
            this_pod: "test-pod".to_string(),
            running_flows: Arc::new(crate::app::RegistryFlows(flow_registry)),
            app_config,
            conversation_cache: Arc::new(flowgen_core::cache::memory::MemoryCache::new()),
            system_bucket_present: false,
            agents: None,
            login_client: None,
            cookie_key: axum_extra::extract::cookie::Key::generate(),
            cookie_secure: true,
            api_keys: Vec::new(),
            authoring: None,
            inproc: Arc::new(flowgen_core::task::inproc::registry::InprocRegistry::new()),
            flows_cache: None,
            replies: Default::default(),
        }
    }

    #[test]
    fn test_web_state_allows_empty_registry() {
        let state = test_state();
        let registry = state.flow_registry.read().unwrap();
        assert!(registry.is_empty());
    }

    #[derive(Clone, serde::Serialize)]
    struct IssuerDiscovery {
        issuer: String,
        jwks_uri: String,
        authorization_endpoint: String,
        token_endpoint: String,
    }

    #[derive(serde::Serialize)]
    struct EmptyJwks {
        keys: Vec<()>,
    }

    async fn serve(app: Router) -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(listener, app).await });
        base
    }

    async fn issuer() -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let discovery = IssuerDiscovery {
            issuer: base.clone(),
            jwks_uri: format!("{base}/jwks"),
            authorization_endpoint: format!("{base}/authorize"),
            token_endpoint: format!("{base}/token"),
        };
        let app = Router::new()
            .route(
                "/.well-known/openid-configuration",
                get(move || {
                    let discovery = discovery.clone();
                    async move { Json(discovery) }
                }),
            )
            .route(
                "/jwks",
                get(|| async { Json(EmptyJwks { keys: Vec::new() }) }),
            );
        tokio::spawn(async move { axum::serve(listener, app).await });
        base
    }

    fn api_routes() -> Vec<(String, String)> {
        let spec: serde_yaml::Value = serde_yaml::from_str(flowgen_client::OPENAPI_YAML).unwrap();
        let mut routes = vec![("GET".to_string(), "openapi.yaml".to_string())];
        for (path, operations) in spec["paths"].as_mapping().unwrap() {
            let Some(route) = path.as_str().unwrap().strip_prefix("/api/") else {
                continue;
            };
            let route = route
                .split('/')
                .map(|segment| {
                    if segment.starts_with('{') {
                        "x"
                    } else {
                        segment
                    }
                })
                .collect::<Vec<_>>()
                .join("/");
            for method in ["get", "put", "post", "delete", "patch"] {
                if operations.get(method).is_some() {
                    routes.push((method.to_uppercase(), route.clone()));
                }
            }
        }
        routes
    }

    async fn statuses(base: &str) -> Vec<(String, u16)> {
        let routes = api_routes();
        assert!(
            routes.len() > 15,
            "OpenAPI spec lists too few /api routes: {routes:?}"
        );
        let client = reqwest::Client::new();
        let mut statuses = Vec::new();
        for (method, route) in routes {
            let status = client
                .request(
                    method.parse().unwrap(),
                    format!("{base}/flowgen/api/{route}"),
                )
                .send()
                .await
                .unwrap()
                .status();
            statuses.push((format!("{method} {route}"), status.as_u16()));
        }
        statuses
    }

    #[tokio::test]
    async fn every_api_route_requires_a_session_with_web_auth() {
        let login_client = crate::login::LoginClient::new(
            crate::login::LoginConfig {
                issuer_url: issuer().await,
                client_id: "flowgen".to_string(),
                client_secret: None,
                credentials_path: None,
                redirect_uri: "http://localhost/flowgen/auth/callback".to_string(),
                extra_scopes: Vec::new(),
                signout_redirect_url: None,
            },
            &secrecy::SecretString::from("client-secret"),
        )
        .await
        .unwrap();
        let mut state = test_state();
        state.login_client = Some(Arc::new(login_client));
        let base = serve(router("/flowgen", state)).await;

        let open: Vec<(String, u16)> = statuses(&base)
            .await
            .into_iter()
            .filter(|(_, status)| *status != 401)
            .collect();

        assert!(open.is_empty(), "reachable without a session: {open:?}");
    }

    #[tokio::test]
    async fn sign_in_failures_redirect_to_the_login_page() {
        let login_client = crate::login::LoginClient::new(
            crate::login::LoginConfig {
                issuer_url: issuer().await,
                client_id: "flowgen".to_string(),
                client_secret: None,
                credentials_path: None,
                redirect_uri: "http://localhost/flowgen/auth/callback".to_string(),
                extra_scopes: Vec::new(),
                signout_redirect_url: None,
            },
            &secrecy::SecretString::from("client-secret"),
        )
        .await
        .unwrap();
        let mut state = test_state();
        state.login_client = Some(Arc::new(login_client));
        let base = serve(router("/flowgen", state)).await;
        let client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .unwrap();
        let landing = |response: reqwest::Response| {
            assert!(response.status().is_redirection(), "{}", response.status());
            response.headers()[reqwest::header::LOCATION]
                .to_str()
                .unwrap()
                .to_string()
        };
        let callback = |query: &str| client.get(format!("{base}/flowgen/auth/callback{query}"));

        for (query, code) in [
            (
                "?error=access_denied&error_description=User+is+not+assigned",
                "access_denied",
            ),
            ("?error=server_error", "provider"),
            ("?error=%3Cb%3Ehi%3C%2Fb%3E", "provider"),
            ("", "failed"),
            ("?code=a&state=b", "expired"),
        ] {
            let location = landing(callback(query).send().await.unwrap());
            assert_eq!(location, format!("/flowgen/login?error={code}"), "{query}");
        }

        let started = client
            .get(format!("{base}/flowgen/auth/login"))
            .send()
            .await
            .unwrap();
        let state_cookie = started
            .headers()
            .get_all(reqwest::header::SET_COOKIE)
            .iter()
            .filter_map(|cookie| cookie.to_str().ok())
            .find(|cookie| cookie.starts_with(SSO_STATE_COOKIE))
            .and_then(|cookie| cookie.split(';').next())
            .unwrap()
            .to_string();
        let forged = callback("?code=a&state=forged")
            .header(reqwest::header::COOKIE, state_cookie)
            .send()
            .await
            .unwrap();
        assert_eq!(landing(forged), "/flowgen/login?error=failed");
    }

    const MACHINE_KEY: &str = "0123456789abcdef0123456789abcdef";

    #[test]
    fn callers_are_labelled_by_email_then_user_id_and_keys_by_name() {
        let user = |claims: &[(&str, serde_json::Value)]| {
            Caller::User(flowgen_core::auth::UserContext {
                user_id: "00u1".to_string(),
                claims: claims
                    .iter()
                    .map(|(name, value)| (name.to_string(), value.clone()))
                    .collect(),
            })
        };
        assert_eq!(
            user(&[("email", serde_json::json!("jane@example.com"))]).label(),
            "jane@example.com"
        );
        assert_eq!(user(&[("email", serde_json::json!(" "))]).label(), "00u1");
        assert_eq!(user(&[]).label(), "00u1");
        assert_eq!(Caller::Key("agent".to_string()).label(), "key:agent");
    }

    #[test]
    fn machine_keys_reach_only_the_reading_and_proposing_routes() {
        use axum::http::Method;
        let allowed = [
            (Method::GET, "/f/api/flows"),
            (Method::GET, "/f/api/flows/a/b"),
            (Method::GET, "/f/api/changes/abc"),
            (Method::GET, "/f/api/workspace/file"),
            (Method::POST, "/f/api/changes"),
            (Method::POST, "/f/api/changes/abc/files"),
            (Method::PATCH, "/f/api/changes/abc"),
            (Method::POST, "/f/api/workspace/validate"),
        ];
        let refused = [
            (Method::POST, "/f/api/changes/abc/approve"),
            (Method::POST, "/f/api/changes//files"),
            (Method::PATCH, "/f/api/changes/abc/approve"),
            (Method::PATCH, "/f/api/flows/a"),
            (Method::POST, "/f/api/cluster/token"),
            (Method::GET, "/f/api/agents/conversations"),
            (Method::GET, "/f/api/config"),
            (Method::GET, "/f/api/flowsx"),
        ];
        for (method, path) in allowed {
            assert!(key_may_call("/f/api", &method, path), "{method} {path}");
        }
        for (method, path) in refused {
            assert!(!key_may_call("/f/api", &method, path), "{method} {path}");
        }
    }

    #[tokio::test]
    async fn a_machine_key_can_propose_a_change_but_not_approve_it() {
        let login_client = crate::login::LoginClient::new(
            crate::login::LoginConfig {
                issuer_url: issuer().await,
                client_id: "flowgen".to_string(),
                client_secret: None,
                credentials_path: None,
                redirect_uri: "http://localhost/flowgen/auth/callback".to_string(),
                extra_scopes: Vec::new(),
                signout_redirect_url: None,
            },
            &secrecy::SecretString::from("client-secret"),
        )
        .await
        .unwrap();
        let mut state = test_state();
        state.login_client = Some(Arc::new(login_client));
        state.api_keys = vec![
            flowgen_core::credentials::ApiKey {
                name: "agent".to_string(),
                key: secrecy::SecretString::from(MACHINE_KEY),
            },
            flowgen_core::credentials::ApiKey {
                name: "blank".to_string(),
                key: secrecy::SecretString::from(""),
            },
        ];
        state.authoring = Some(crate::config::AuthoringOptions {
            enabled: true,
            targets: vec![authoring_target("workspace", &["flows/", "resources/"])],
            groups_claim: "groups".to_string(),
        });
        let base = serve(router("/flowgen", state)).await;
        let client = reqwest::Client::new();
        let proposal = api::ChangeProposal {
            title: "Add a".to_string(),
            description: None,
            files: vec![api::WorkspaceFile {
                path: "flows/a.yaml".to_string(),
                content: Some("flow:\n  tasks:\n    - log:\n        name: a\n".to_string()),
            }],
        };

        for wrong in ["other", ""] {
            let wrong_key = client
                .post(format!("{base}/flowgen/api/changes"))
                .header("authorization", format!("Bearer {wrong}"))
                .json(&proposal)
                .send()
                .await
                .unwrap();
            assert_eq!(wrong_key.status(), 401, "{wrong:?}");
        }

        let rotate = client
            .post(format!("{base}/flowgen/api/cluster/token"))
            .bearer_auth(MACHINE_KEY)
            .send()
            .await
            .unwrap();
        assert_eq!(rotate.status(), 403);

        let proposed: api::Change = client
            .post(format!("{base}/flowgen/api/changes"))
            .bearer_auth(MACHINE_KEY)
            .json(&proposal)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(proposed.proposed_by, "key:agent");
        assert_eq!(proposed.status, api::ChangeStatus::Pending);
        assert!(proposed.issues.is_empty(), "{:?}", proposed.issues);
        assert!(proposed.files[0].diff.contains("--- /dev/null"));

        let approve = client
            .post(format!(
                "{base}/flowgen/api/changes/{}/approve",
                proposed.id
            ))
            .bearer_auth(MACHINE_KEY)
            .send()
            .await
            .unwrap();
        assert_eq!(approve.status(), 403);
    }

    #[tokio::test]
    async fn approving_a_change_runs_the_publish_flow_and_records_its_result() {
        let (tx, mut rx) = tokio::sync::mpsc::channel::<flowgen_core::event::Event>(1);
        let inproc = Arc::new(flowgen_core::task::inproc::registry::InprocRegistry::new());
        inproc.register(
            "system/publish_workspace".to_string(),
            flowgen_core::task::inproc::registry::Registration {
                tx,
                task_name: "on_publish".to_string(),
                task_id: 0,
                task_type: "inproc_endpoint",
                leaf_count: 1,
                ack_timeout: None,
                callers: vec!["system/".to_string()],
            },
        );
        let published = tokio::spawn(async move {
            let event = rx.recv().await.unwrap();
            let data = event.data_as_json().unwrap();
            event
                .completion_tx
                .as_ref()
                .unwrap()
                .signal_completion(Some(serde_json::json!({"commit": "abc"})));
            data
        });
        let resources = tempfile::TempDir::new().unwrap();
        let mut state = test_state();
        state.resource_loader = Some(flowgen_core::resource::ResourceLoader::new(Some(
            resources.path().to_path_buf(),
        )));
        state.authoring = Some(crate::config::AuthoringOptions {
            enabled: true,
            targets: vec![authoring_target("workspace", &["flows/", "resources/"])],
            groups_claim: "groups".to_string(),
        });
        state.inproc = inproc;
        let base = serve(router("/flowgen", state)).await;
        let client = reqwest::Client::new();

        let proposed: api::Change = client
            .post(format!("{base}/flowgen/api/changes"))
            .json(&api::ChangeProposal {
                title: "Add script".to_string(),
                description: None,
                files: vec![api::WorkspaceFile {
                    path: "resources/s.rhai".to_string(),
                    content: Some("event".to_string()),
                }],
            })
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        let approved: api::Change = client
            .post(format!(
                "{base}/flowgen/api/changes/{}/approve",
                proposed.id
            ))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();

        assert_eq!(approved.status, api::ChangeStatus::Published);
        assert_eq!(approved.result["commit"], "abc");
        let sent = published.await.unwrap();
        assert_eq!(sent["title"], "Add script");
        assert_eq!(
            sent["files"],
            serde_json::json!([{"path": "resources/s.rhai", "content": "event", "previous": null}])
        );

        let base_file: api::WorkspaceFile = client
            .get(format!("{base}/flowgen/api/workspace/file"))
            .query(&[("path", "resources/s.rhai")])
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(base_file.content.as_deref(), Some("event"));
        let follow_up: api::Change = client
            .post(format!("{base}/flowgen/api/changes"))
            .json(&api::ChangeProposal {
                title: "Edit script".to_string(),
                description: None,
                files: vec![api::WorkspaceFile {
                    path: "resources/s.rhai".to_string(),
                    content: Some("event; 2".to_string()),
                }],
            })
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(follow_up.files[0].previous.as_deref(), Some("event"));

        let deployment = |content: Option<&str>| {
            let client = client.clone();
            let url = format!("{base}/flowgen/api/changes/{}", proposed.id);
            let file = resources.path().join("s.rhai");
            let content = content.map(str::to_string);
            async move {
                if let Some(content) = content {
                    std::fs::write(file, content).unwrap();
                }
                let change: api::Change =
                    client.get(url).send().await.unwrap().json().await.unwrap();
                change.deployment
            }
        };
        assert_eq!(deployment(None).await, Some(api::ChangeDeployment::Waiting));
        assert_eq!(
            deployment(Some("event\n")).await,
            Some(api::ChangeDeployment::Deployed)
        );
        assert_eq!(
            deployment(Some("event; 1")).await,
            Some(api::ChangeDeployment::Deployed)
        );

        let again = client
            .post(format!(
                "{base}/flowgen/api/changes/{}/approve",
                proposed.id
            ))
            .send()
            .await
            .unwrap();
        assert_eq!(again.status(), 409);
    }

    #[tokio::test]
    async fn a_pending_change_takes_more_files_and_a_new_title_until_it_is_decided() {
        let mut state = test_state();
        state.authoring = Some(crate::config::AuthoringOptions {
            enabled: true,
            targets: vec![authoring_target("workspace", &["flows/", "resources/"])],
            groups_claim: "groups".to_string(),
        });
        let base = serve(router("/flowgen", state)).await;
        let client = reqwest::Client::new();
        let file = |path: &str, content: &str| api::WorkspaceFile {
            path: path.to_string(),
            content: Some(content.to_string()),
        };
        let proposed: api::Change = client
            .post(format!("{base}/flowgen/api/changes"))
            .json(&api::ChangeProposal {
                title: "Add a".to_string(),
                description: None,
                files: vec![file("resources/a.rhai", "1")],
            })
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        let url = format!("{base}/flowgen/api/changes/{}", proposed.id);

        let added: api::Change = client
            .post(format!("{url}/files"))
            .json(&api::WorkspaceFiles {
                files: vec![file("resources/b.rhai", "2"), file("resources/a.rhai", "3")],
            })
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        let contents: Vec<_> = added
            .files
            .iter()
            .map(|file| (file.path.as_str(), file.content.as_deref()))
            .collect();
        assert_eq!(
            contents,
            vec![
                ("resources/a.rhai", Some("3")),
                ("resources/b.rhai", Some("2"))
            ]
        );
        assert!(added.updated_at > proposed.updated_at);

        let edit = |title: &str| {
            client.patch(&url).json(&api::ChangeEdit {
                title: Some(title.to_string()),
                description: Some("Both scripts".to_string()),
            })
        };
        assert_eq!(edit(" ").send().await.unwrap().status(), 400);
        let edited: api::Change = edit("Add scripts")
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(edited.title, "Add scripts");
        assert_eq!(edited.description.as_deref(), Some("Both scripts"));

        let stale = client
            .post(format!("{url}/approve"))
            .query(&[("updatedAt", proposed.updated_at.unwrap())])
            .send()
            .await
            .unwrap();
        assert_eq!(stale.status(), 409);

        let rejected = client.post(format!("{url}/reject")).send().await.unwrap();
        assert_eq!(rejected.status(), 200);
        assert_eq!(edit("Late").send().await.unwrap().status(), 409);
    }

    fn authoring_target(name: &str, paths: &[&str]) -> crate::config::AuthoringTarget {
        crate::config::AuthoringTarget {
            name: name.to_string(),
            paths: paths.iter().map(|path| path.to_string()).collect(),
            publish_flow: "system/publish_workspace".to_string(),
            approver_groups: Vec::new(),
            track_deployment: true,
        }
    }

    fn authoring_options(
        targets: Vec<crate::config::AuthoringTarget>,
    ) -> crate::config::AuthoringOptions {
        crate::config::AuthoringOptions {
            enabled: true,
            targets,
            groups_claim: "groups".to_string(),
        }
    }

    async fn propose(client: &reqwest::Client, base: &str, title: &str, paths: &[&str]) -> u16 {
        client
            .post(format!("{base}/flowgen/api/changes"))
            .json(&api::ChangeProposal {
                title: title.to_string(),
                description: None,
                files: paths
                    .iter()
                    .map(|path| api::WorkspaceFile {
                        path: path.to_string(),
                        content: Some("event".to_string()),
                    })
                    .collect(),
            })
            .send()
            .await
            .unwrap()
            .status()
            .as_u16()
    }

    #[tokio::test]
    async fn proposals_with_unsafe_paths_duplicate_files_or_no_title_are_refused() {
        let mut state = test_state();
        state.authoring = Some(authoring_options(vec![authoring_target(
            "workspace",
            &["flows/", "resources/"],
        )]));
        let base = serve(router("/flowgen", state)).await;
        let client = reqwest::Client::new();

        assert_eq!(
            propose(&client, &base, "Add", &["resources/a.rhai"]).await,
            201
        );
        for path in [
            "resources/a b.rhai",
            "resources/a:b.rhai",
            "resources/é.rhai",
        ] {
            assert_eq!(propose(&client, &base, "Add", &[path]).await, 400, "{path}");
        }
        assert_eq!(
            propose(
                &client,
                &base,
                "Add",
                &["resources/a.rhai", "resources/a.rhai"]
            )
            .await,
            400
        );
        assert_eq!(
            propose(&client, &base, " \n", &["resources/a.rhai"]).await,
            400
        );
    }

    #[tokio::test]
    async fn a_change_whose_files_moved_to_another_target_cannot_be_decided() {
        let mut state = test_state();
        state.authoring = Some(authoring_options(vec![
            authoring_target("workspace", &["flows/", "resources/"]),
            authoring_target("platform", &["flows/platform/"]),
        ]));
        let cache = Arc::clone(&state.conversation_cache);
        let base = serve(router("/flowgen", state)).await;
        let client = reqwest::Client::new();
        let proposed: api::Change = client
            .post(format!("{base}/flowgen/api/changes"))
            .json(&api::ChangeProposal {
                title: "Add a".to_string(),
                description: None,
                files: vec![api::WorkspaceFile {
                    path: "flows/platform/a.yaml".to_string(),
                    content: Some("flow:\n  tasks:\n    - log:\n        name: a\n".to_string()),
                }],
            })
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(proposed.target, "platform");
        assert!(proposed.issues.is_empty(), "{:?}", proposed.issues);
        let key = format!("authoring.changes.{}", proposed.id);
        let mut stored: api::Change =
            serde_json::from_slice(&cache.get(&key).await.unwrap().unwrap()).unwrap();
        stored.target = "workspace".to_string();
        cache
            .put(&key, serde_json::to_vec(&stored).unwrap().into(), None)
            .await
            .unwrap();

        for action in ["approve", "reject"] {
            let decided = client
                .post(format!(
                    "{base}/flowgen/api/changes/{}/{action}",
                    proposed.id
                ))
                .send()
                .await
                .unwrap();
            assert_eq!(decided.status(), 409, "{action}");
        }
    }

    #[tokio::test]
    async fn a_change_rewritten_while_publishing_keeps_the_newer_record() {
        let (tx, mut rx) = tokio::sync::mpsc::channel::<flowgen_core::event::Event>(1);
        let inproc = Arc::new(flowgen_core::task::inproc::registry::InprocRegistry::new());
        inproc.register(
            "system/publish_workspace".to_string(),
            flowgen_core::task::inproc::registry::Registration {
                tx,
                task_name: "on_publish".to_string(),
                task_id: 0,
                task_type: "inproc_endpoint",
                leaf_count: 1,
                ack_timeout: None,
                callers: vec!["system/".to_string()],
            },
        );
        let mut state = test_state();
        state.authoring = Some(authoring_options(vec![authoring_target(
            "workspace",
            &["flows/", "resources/"],
        )]));
        state.inproc = inproc;
        let cache = Arc::clone(&state.conversation_cache);
        tokio::spawn(async move {
            let event = rx.recv().await.unwrap();
            let id = event.data_as_json().unwrap()["id"]
                .as_str()
                .unwrap()
                .to_string();
            let key = format!("authoring.changes.{id}");
            let mut stored: api::Change =
                serde_json::from_slice(&cache.get(&key).await.unwrap().unwrap()).unwrap();
            stored.title = "Newer".to_string();
            cache
                .put(&key, serde_json::to_vec(&stored).unwrap().into(), None)
                .await
                .unwrap();
            event
                .completion_tx
                .as_ref()
                .unwrap()
                .signal_completion(None);
        });
        let base = serve(router("/flowgen", state)).await;
        let client = reqwest::Client::new();
        let proposed: api::Change = client
            .post(format!("{base}/flowgen/api/changes"))
            .json(&api::ChangeProposal {
                title: "Add script".to_string(),
                description: None,
                files: vec![api::WorkspaceFile {
                    path: "resources/s.rhai".to_string(),
                    content: Some("event".to_string()),
                }],
            })
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();

        let approved = client
            .post(format!(
                "{base}/flowgen/api/changes/{}/approve",
                proposed.id
            ))
            .send()
            .await
            .unwrap();
        assert_eq!(approved.status(), 200);
        let stored: api::Change = client
            .get(format!("{base}/flowgen/api/changes/{}", proposed.id))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();

        assert_eq!(stored.title, "Newer");
        assert_eq!(stored.status, api::ChangeStatus::Publishing);
    }

    #[tokio::test]
    async fn api_routes_are_open_without_web_auth() {
        let base = serve(router("/flowgen", test_state())).await;

        let rejected: Vec<(String, u16)> = statuses(&base)
            .await
            .into_iter()
            .filter(|(_, status)| *status == 401)
            .collect();

        assert!(
            rejected.is_empty(),
            "rejected without web.auth: {rejected:?}"
        );
    }

    fn agents_options() -> crate::config::AgentsOptions {
        crate::config::AgentsOptions {
            enabled: true,
            conversation_history_ttl: None,
            on_behalf_of_user: false,
            cache_prefix: crate::config::default_agents_cache_prefix(),
            reply_save_interval: crate::config::default_reply_save_interval(),
            reply_idle_timeout: crate::config::default_reply_idle_timeout(),
        }
    }

    #[test]
    fn the_gateway_gets_the_users_token_only_when_told_to() {
        let session = flowgen_core::auth::UserSession {
            user: flowgen_core::auth::UserContext {
                user_id: "u1".to_string(),
                claims: Default::default(),
            },
            token: Some(secrecy::SecretString::from("id-token")),
        };
        let authorization = |on_behalf_of_user: bool| {
            let mut state = test_state();
            state.agents = Some(crate::config::AgentsOptions {
                on_behalf_of_user,
                ..agents_options()
            });
            gateway_headers(&state, Some(&session))
                .get(reqwest::header::AUTHORIZATION)
                .map(|value| value.to_str().unwrap().to_string())
        };

        assert_eq!(authorization(false), None);
        assert_eq!(authorization(true), Some("Bearer id-token".to_string()));
    }

    #[tokio::test]
    async fn agents_routes_answer_404_unless_web_agents_is_enabled() {
        let off = serve(router("/flowgen", test_state())).await;
        let mut state = test_state();
        state.agents = Some(agents_options());
        let on = serve(router("/flowgen", state)).await;

        let status = |base: String| async move {
            reqwest::get(format!("{base}/flowgen/api/agents/conversations"))
                .await
                .unwrap()
                .status()
        };
        assert_eq!(status(off).await, StatusCode::NOT_FOUND);
        assert_eq!(status(on).await, StatusCode::OK);
    }

    fn signed_in(user_id: &str) -> Option<axum::Extension<flowgen_core::auth::UserContext>> {
        Some(axum::Extension(flowgen_core::auth::UserContext {
            user_id: user_id.to_string(),
            claims: Default::default(),
        }))
    }

    fn upsert(title: &str) -> Json<api::ConversationUpsert> {
        Json(api::ConversationUpsert {
            title: title.to_string(),
            messages: Vec::new(),
            model: None,
            origin: None,
        })
    }

    async fn write_flow_thread(state: &WebState, id: &str) {
        let written_by_flow = serde_json::json!({
            "id": id,
            "title": "a",
            "messages": [],
            "origin": "agent",
            "unread": true,
            "updatedAt": 1
        });
        state
            .conversation_cache
            .put(
                &conversation_key(state, None, id),
                serde_json::to_vec(&written_by_flow).unwrap().into(),
                None,
            )
            .await
            .unwrap();
    }

    async fn unread_for(
        state: &Arc<WebState>,
        user: Option<axum::Extension<flowgen_core::auth::UserContext>>,
    ) -> serde_json::Value {
        let listed = listed_conversations(state, user).await;
        listed["conversations"][0]["unread"].clone()
    }

    async fn listed_conversations(
        state: &Arc<WebState>,
        user: Option<axum::Extension<flowgen_core::auth::UserContext>>,
    ) -> serde_json::Value {
        let response = list_conversations(State(Arc::clone(state)), user).await;
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        serde_json::from_slice(&body).unwrap()
    }

    async fn listed_titles(
        state: &Arc<WebState>,
        user: Option<axum::Extension<flowgen_core::auth::UserContext>>,
    ) -> Vec<String> {
        let listed = listed_conversations(state, user).await;
        let mut titles: Vec<String> = listed["conversations"]
            .as_array()
            .unwrap()
            .iter()
            .map(|c| c["title"].as_str().unwrap().to_string())
            .collect();
        titles.sort();
        titles
    }

    #[tokio::test]
    async fn signed_in_users_see_their_own_and_the_shared_conversations() {
        let state = Arc::new(test_state());
        put_conversation(
            State(Arc::clone(&state)),
            AxumPath("s".into()),
            None,
            upsert("shared"),
        )
        .await;
        put_conversation(
            State(Arc::clone(&state)),
            AxumPath("a".into()),
            signed_in("ann"),
            upsert("ann's"),
        )
        .await;
        put_conversation(
            State(Arc::clone(&state)),
            AxumPath("b".into()),
            signed_in("bob"),
            upsert("bob's"),
        )
        .await;

        let ann = listed_titles(&state, signed_in("ann")).await;
        let bob_opens_ann = get_conversation(
            State(Arc::clone(&state)),
            AxumPath("a".into()),
            signed_in("bob"),
        )
        .await;

        assert_eq!(ann, vec!["ann's", "shared"]);
        assert_eq!(bob_opens_ann.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn a_reply_to_a_shared_conversation_keeps_it_shared() {
        let state = Arc::new(test_state());
        put_conversation(
            State(Arc::clone(&state)),
            AxumPath("s".into()),
            None,
            upsert("shared"),
        )
        .await;

        put_conversation(
            State(Arc::clone(&state)),
            AxumPath("s".into()),
            signed_in("ann"),
            upsert("shared, replied"),
        )
        .await;

        assert_eq!(
            listed_titles(&state, signed_in("bob")).await,
            vec!["shared, replied"]
        );
    }

    async fn move_to(state: &Arc<WebState>, user: &str, folder: &str) {
        update_conversation(
            State(Arc::clone(state)),
            AxumPath("a1".into()),
            signed_in(user),
            Json(api::ConversationUpdate {
                title: None,
                folder: Some(folder.to_string()),
            }),
        )
        .await;
    }

    async fn folder_for(state: &Arc<WebState>, user: &str) -> serde_json::Value {
        listed_conversations(state, signed_in(user)).await["conversations"][0]["folder"].clone()
    }

    #[tokio::test]
    async fn each_user_files_a_shared_conversation_in_their_own_folder() {
        let state = Arc::new(test_state());
        write_flow_thread(&state, "a1").await;

        move_to(&state, "ann", " a / b/ ").await;
        move_to(&state, "bob", "").await;

        assert_eq!(folder_for(&state, "ann").await, "a/b");
        assert_eq!(folder_for(&state, "bob").await, serde_json::Value::Null);
    }

    #[tokio::test]
    async fn without_auth_everyone_shares_the_folders() {
        let state = Arc::new(test_state());
        write_flow_thread(&state, "a1").await;

        update_conversation(
            State(Arc::clone(&state)),
            AxumPath("a1".into()),
            None,
            Json(api::ConversationUpdate {
                title: None,
                folder: Some("a".to_string()),
            }),
        )
        .await;

        let listed = listed_conversations(&state, None).await;
        assert_eq!(listed["conversations"][0]["folder"], "a");
    }

    #[tokio::test]
    async fn renaming_keeps_the_conversation_where_it_was() {
        let state = Arc::new(test_state());
        write_flow_thread(&state, "a1").await;

        let renamed = update_conversation(
            State(Arc::clone(&state)),
            AxumPath("a1".into()),
            signed_in("ann"),
            Json(api::ConversationUpdate {
                title: Some("  b ".to_string()),
                folder: None,
            }),
        )
        .await;

        let listed = listed_conversations(&state, signed_in("bob")).await;
        assert_eq!(renamed.status(), StatusCode::NO_CONTENT);
        assert_eq!(listed["conversations"][0]["title"], "b");
        assert_eq!(listed["conversations"][0]["updatedAt"], 1);
        assert_eq!(listed["conversations"][0]["unread"], true);
    }

    #[tokio::test]
    async fn a_flow_thread_is_unread_for_each_user_until_they_read_it() {
        let state = Arc::new(test_state());
        write_flow_thread(&state, "a1").await;

        mark_conversation_read(
            State(Arc::clone(&state)),
            AxumPath("a1".into()),
            signed_in("ann"),
        )
        .await;

        assert_eq!(
            unread_for(&state, signed_in("ann")).await,
            serde_json::Value::Null
        );
        assert_eq!(unread_for(&state, signed_in("bob")).await, true);
    }

    #[tokio::test]
    async fn a_flow_thread_keyed_by_the_hash_of_a_user_id_is_only_theirs() {
        use sha2::Digest;
        let state = Arc::new(test_state());
        let ann = format!("{:x}", sha2::Sha256::digest("ann"));
        let thread = serde_json::json!({
            "id": "a1", "title": "a", "messages": [], "updatedAt": 1
        });
        state
            .conversation_cache
            .put(
                &conversation_key(&state, Some(&ann), "a1"),
                serde_json::to_vec(&thread).unwrap().into(),
                None,
            )
            .await
            .unwrap();

        assert_eq!(listed_titles(&state, signed_in("ann")).await, vec!["a"]);
        assert!(listed_titles(&state, signed_in("bob")).await.is_empty());
    }

    #[tokio::test]
    async fn marking_unread_affects_only_the_caller() {
        let state = Arc::new(test_state());
        write_flow_thread(&state, "a1").await;
        for user in ["ann", "bob"] {
            mark_conversation_read(
                State(Arc::clone(&state)),
                AxumPath("a1".into()),
                signed_in(user),
            )
            .await;
        }

        mark_conversation_unread(
            State(Arc::clone(&state)),
            AxumPath("a1".into()),
            signed_in("ann"),
        )
        .await;

        assert_eq!(unread_for(&state, signed_in("ann")).await, true);
        assert_eq!(
            unread_for(&state, signed_in("bob")).await,
            serde_json::Value::Null
        );
    }

    #[tokio::test]
    async fn a_reply_marks_a_shared_thread_unread_for_the_others() {
        let state = Arc::new(test_state());
        write_flow_thread(&state, "a1").await;
        for user in ["ann", "bob"] {
            mark_conversation_read(
                State(Arc::clone(&state)),
                AxumPath("a1".into()),
                signed_in(user),
            )
            .await;
        }

        put_conversation(
            State(Arc::clone(&state)),
            AxumPath("a1".into()),
            signed_in("ann"),
            upsert("a"),
        )
        .await;

        assert_eq!(
            unread_for(&state, signed_in("ann")).await,
            serde_json::Value::Null
        );
        assert_eq!(unread_for(&state, signed_in("bob")).await, true);
    }

    #[tokio::test]
    async fn without_auth_everyone_reads_a_conversation_as_one() {
        let state = Arc::new(test_state());
        write_flow_thread(&state, "a1").await;
        let before = unread_for(&state, None).await;

        mark_conversation_read(State(Arc::clone(&state)), AxumPath("a1".into()), None).await;

        assert_eq!(before, true);
        assert_eq!(unread_for(&state, None).await, serde_json::Value::Null);
    }

    async fn gateway() -> String {
        let frames = [
            r#"{"choices":[{"delta":{"content":"Checking."}}]}"#,
            r#"{"choices":[{"delta":{"tool_calls":[{"index":0,"function":{"name":"tool","arguments":"{}"}}]}}]}"#,
            r#"{"choices":[{"delta":{"content":"Done."}}]}"#,
        ];
        let body: String = frames
            .iter()
            .map(|frame| format!("data: {frame}\n\n"))
            .chain(["data: [DONE]\n\n".to_string()])
            .collect();
        let app = Router::new().route(
            "/v1/chat/completions",
            post(move || {
                let body = body.clone();
                async move {
                    (
                        [(axum::http::header::CONTENT_TYPE, "text/event-stream")],
                        body,
                    )
                }
            }),
        );
        format!("{}/v1", serve(app).await)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_reply_runs_on_the_server_and_lands_in_the_conversation() {
        let mut state = test_state();
        let mut config = (*state.app_config).clone();
        config.web = Some(
            serde_yaml::from_str(&format!(
                "enabled: true\nai_gateway_url: {}",
                gateway().await
            ))
            .unwrap(),
        );
        state.app_config = Arc::new(config);
        state.agents = Some(agents_options());
        let base = serve(router("/f", state)).await;

        let started = reqwest::Client::new()
            .post(format!("{base}/f/api/agents/conversations/c1/reply"))
            .json(&serde_json::json!({"content": "hi", "model": "agent/model"}))
            .send()
            .await
            .unwrap();
        let mut done = None;
        for _ in 0..50 {
            tokio::time::sleep(Duration::from_millis(50)).await;
            let conversation: serde_json::Value =
                reqwest::get(format!("{base}/f/api/agents/conversations/c1"))
                    .await
                    .unwrap()
                    .json()
                    .await
                    .unwrap();
            if conversation["messages"][1]["pending"].is_null() {
                done = Some(conversation);
                break;
            }
        }

        assert_eq!(started.status(), StatusCode::ACCEPTED);
        let conversation = done.expect("the reply ends");
        assert_eq!(conversation["title"], "hi");
        assert_eq!(conversation["messages"][0]["content"], "hi");
        assert_eq!(conversation["messages"][1]["content"], "Checking.\n\nDone.");
        assert_eq!(conversation["messages"][1]["steps"][0]["name"], "tool");
    }

    #[tokio::test]
    async fn the_conversation_stream_sends_the_list_then_each_change() {
        let mut state = test_state();
        state.agents = Some(agents_options());
        let cache = Arc::clone(&state.conversation_cache);
        let key = conversation_key(&state, None, "a1");
        let conversation = |title: &str| {
            serde_json::to_vec(&serde_json::json!({
                "id": "a1", "title": title, "messages": [], "updatedAt": 1
            }))
            .unwrap()
        };
        cache
            .put(&key, conversation("first").into(), None)
            .await
            .unwrap();
        let base = serve(router("/flowgen", state)).await;
        let response = reqwest::get(format!("{base}/flowgen/api/agents/conversations/stream"))
            .await
            .unwrap();
        let mut body = response.bytes_stream();
        let mut received = String::new();
        let mut read_until = async |text: &str| {
            while !received.contains(text) {
                let chunk = tokio::time::timeout(Duration::from_secs(5), body.next())
                    .await
                    .unwrap()
                    .unwrap()
                    .unwrap();
                received.push_str(&String::from_utf8_lossy(&chunk));
            }
        };

        read_until("\"first\"").await;
        cache
            .put(&key, conversation("second").into(), None)
            .await
            .unwrap();
        read_until("\"second\"").await;
        cache.delete(&key).await.unwrap();
        read_until("event: removed\ndata: a1").await;

        assert!(received.starts_with("event: conversations\n"));
        assert!(received.contains("event: conversation\n"));
    }

    #[derive(Debug)]
    struct UnavailableCache;

    #[async_trait::async_trait]
    impl flowgen_core::cache::Cache for UnavailableCache {
        async fn put(
            &self,
            _: &str,
            _: bytes::Bytes,
            _: Option<u64>,
        ) -> Result<(), flowgen_core::cache::Error> {
            Err(flowgen_core::cache::Error::StoreNotInitialized)
        }
        async fn get(&self, _: &str) -> Result<Option<bytes::Bytes>, flowgen_core::cache::Error> {
            Err(flowgen_core::cache::Error::StoreNotInitialized)
        }
        async fn delete(&self, _: &str) -> Result<(), flowgen_core::cache::Error> {
            Err(flowgen_core::cache::Error::StoreNotInitialized)
        }
        async fn create(
            &self,
            _: &str,
            _: bytes::Bytes,
            _: Option<u64>,
        ) -> Result<u64, flowgen_core::cache::Error> {
            Err(flowgen_core::cache::Error::StoreNotInitialized)
        }
        async fn update(
            &self,
            _: &str,
            _: bytes::Bytes,
            _: u64,
            _: Option<u64>,
        ) -> Result<u64, flowgen_core::cache::Error> {
            Err(flowgen_core::cache::Error::StoreNotInitialized)
        }
        async fn get_with_revision(
            &self,
            _: &str,
        ) -> Result<Option<(bytes::Bytes, u64)>, flowgen_core::cache::Error> {
            Err(flowgen_core::cache::Error::StoreNotInitialized)
        }
        async fn delete_with_revision(
            &self,
            _: &str,
            _: u64,
        ) -> Result<(), flowgen_core::cache::Error> {
            Err(flowgen_core::cache::Error::StoreNotInitialized)
        }
        async fn get_revision(&self, _: &str) -> Result<Option<u64>, flowgen_core::cache::Error> {
            Err(flowgen_core::cache::Error::StoreNotInitialized)
        }
        async fn list_keys(&self, _: &str) -> Result<Vec<String>, flowgen_core::cache::Error> {
            Err(flowgen_core::cache::Error::StoreNotInitialized)
        }
    }

    #[tokio::test]
    async fn the_conversation_stream_answers_503_while_the_store_is_unavailable() {
        let mut state = test_state();
        state.agents = Some(agents_options());
        state.conversation_cache = Arc::new(UnavailableCache);

        let response = stream_conversations(State(Arc::new(state)), None).await;

        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[test]
    fn stream_is_not_a_conversation_id() {
        assert!(valid_conversation_id("a1"));
        assert!(!valid_conversation_id("stream"));
        assert!(!valid_conversation_id("a.b"));
    }

    async fn fed_gateway() -> (String, tokio::sync::mpsc::UnboundedSender<bytes::Bytes>) {
        let (frames, receiver) = tokio::sync::mpsc::unbounded_channel::<bytes::Bytes>();
        let receiver = Arc::new(std::sync::Mutex::new(Some(receiver)));
        let app = Router::new().route(
            "/v1/chat/completions",
            post(move || {
                let receiver = receiver.lock().unwrap().take();
                async move {
                    let body = match receiver {
                        Some(receiver) => axum::body::Body::from_stream(
                            tokio_stream::wrappers::UnboundedReceiverStream::new(receiver)
                                .map(Ok::<_, std::io::Error>),
                        ),
                        None => axum::body::Body::empty(),
                    };
                    (
                        [(axum::http::header::CONTENT_TYPE, "text/event-stream")],
                        body,
                    )
                }
            }),
        );
        (format!("{}/v1", serve(app).await), frames)
    }

    fn data(json: &str) -> bytes::Bytes {
        format!("data: {json}\n\n").into()
    }

    fn replying_state(gateway: &str) -> WebState {
        let mut state = test_state();
        let mut config = (*state.app_config).clone();
        config.web = Some(
            serde_yaml::from_str(&format!("enabled: true\nai_gateway_url: {gateway}")).unwrap(),
        );
        state.app_config = Arc::new(config);
        state.agents = Some(crate::config::AgentsOptions {
            reply_save_interval: Duration::from_millis(20),
            reply_idle_timeout: Duration::from_secs(2),
            ..agents_options()
        });
        state
    }

    async fn start(
        state: &Arc<WebState>,
        user: Option<axum::Extension<flowgen_core::auth::UserContext>>,
        id: &str,
        content: &str,
    ) -> StatusCode {
        crate::agent_reply::start_reply(
            State(Arc::clone(state)),
            AxumPath(id.to_string()),
            user,
            None,
            Json(api::ReplyRequest {
                content: content.to_string(),
                model: "a/b".to_string(),
            }),
        )
        .await
        .status()
    }

    async fn stored(state: &WebState, id: &str) -> Option<api::Conversation> {
        let (_, bytes) = find_conversation(state, None, id).await.unwrap()?;
        Some(serde_json::from_slice(&bytes).unwrap())
    }

    async fn reply_ended(state: &WebState, id: &str) -> api::ConversationMessage {
        for _ in 0..100 {
            let last = match stored(state, id).await {
                Some(conversation) => conversation.messages.last().cloned(),
                None => None,
            };
            if let Some(last) = last.filter(|last| last.pending.is_none()) {
                return last;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        panic!("the reply did not end");
    }

    async fn store_pending(state: &WebState, id: &str, updated_at: i64) {
        let conversation = serde_json::json!({
            "id": id,
            "title": "a",
            "messages": [
                {"role": "user", "content": "a", "at": 1},
                {"role": "assistant", "content": "", "at": 1, "pending": true}
            ],
            "updatedAt": updated_at
        });
        state
            .conversation_cache
            .put(
                &conversation_key(state, None, id),
                serde_json::to_vec(&conversation).unwrap().into(),
                None,
            )
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn a_gateway_error_frame_ends_the_reply_with_its_message() {
        let (gateway, frames) = fed_gateway().await;
        let state = Arc::new(replying_state(&gateway));

        let started = start(&state, None, "a1", "a").await;
        frames
            .send(data(r#"{"choices":[{"delta":{"content":"b"}}]}"#))
            .unwrap();
        frames
            .send(data(r#"{"error":{"message":"c","type":"server_error"}}"#))
            .unwrap();
        frames.send("data: [DONE]\n\n".into()).unwrap();
        let reply = reply_ended(&state, "a1").await;

        assert_eq!(started, StatusCode::ACCEPTED);
        assert_eq!(reply.content, "b");
        assert_eq!(reply.error.as_deref(), Some("The agent failed: c"));
    }

    #[tokio::test]
    async fn a_stream_that_ends_before_done_ends_the_reply_with_an_error() {
        let (gateway, frames) = fed_gateway().await;
        let state = Arc::new(replying_state(&gateway));

        start(&state, None, "a1", "a").await;
        frames
            .send(data(r#"{"choices":[{"delta":{"content":"b"}}]}"#))
            .unwrap();
        drop(frames);
        let reply = reply_ended(&state, "a1").await;

        assert_eq!(reply.content, "b");
        assert_eq!(
            reply.error.as_deref(),
            Some("The reply ended before the agent finished")
        );
    }

    #[tokio::test]
    async fn a_gateway_silent_for_the_idle_timeout_fails_the_reply() {
        let (gateway, _frames) = fed_gateway().await;
        let mut state = replying_state(&gateway);
        state.agents = Some(crate::config::AgentsOptions {
            reply_idle_timeout: Duration::from_millis(100),
            ..agents_options()
        });
        let state = Arc::new(state);

        start(&state, None, "a1", "a").await;
        let reply = reply_ended(&state, "a1").await;

        assert_eq!(
            reply.error.as_deref(),
            Some("The agent sent nothing for 100ms")
        );
    }

    #[tokio::test]
    async fn an_empty_message_or_the_stream_id_starts_no_reply() {
        let (gateway, _frames) = fed_gateway().await;
        let state = Arc::new(replying_state(&gateway));

        assert_eq!(
            start(&state, None, "a1", " \n").await,
            StatusCode::BAD_REQUEST
        );
        assert_eq!(
            start(&state, None, "stream", "a").await,
            StatusCode::BAD_REQUEST
        );
        assert!(stored(&state, "a1").await.is_none());
    }

    #[tokio::test]
    async fn a_reply_waits_for_one_in_progress_but_settles_an_interrupted_one() {
        let (gateway, _frames) = fed_gateway().await;
        let state = Arc::new(replying_state(&gateway));
        store_pending(&state, "a1", now_millis()).await;
        store_pending(&state, "a2", 1).await;

        let busy = start(&state, None, "a1", "b").await;
        let settled = start(&state, None, "a2", "b").await;

        let a1 = stored(&state, "a1").await.unwrap();
        let a2 = stored(&state, "a2").await.unwrap();
        assert_eq!(busy, StatusCode::CONFLICT);
        assert_eq!(a1.messages.len(), 2);
        assert_eq!(settled, StatusCode::ACCEPTED);
        assert_eq!(a2.messages.len(), 4);
        assert_eq!(a2.messages[1].pending, None);
        assert_eq!(
            a2.messages[1].error.as_deref(),
            Some("The reply was interrupted")
        );
        assert_eq!(a2.messages[2].content, "b");
        assert_eq!(a2.messages[3].pending, Some(true));
    }

    #[tokio::test]
    async fn a_reply_another_pod_writes_is_stopped_through_the_store() {
        let (gateway, frames) = fed_gateway().await;
        let writing = Arc::new(replying_state(&gateway));
        let mut other = replying_state(&gateway);
        other.conversation_cache = Arc::clone(&writing.conversation_cache);
        let other = Arc::new(other);
        let stop = || {
            crate::agent_reply::stop_reply(
                State(Arc::clone(&other)),
                AxumPath("a1".to_string()),
                None,
            )
        };

        start(&writing, None, "a1", "a").await;
        frames
            .send(data(r#"{"choices":[{"delta":{"content":"b"}}]}"#))
            .unwrap();
        let stopped = stop().await;
        let mut released = false;
        for _ in 0..50 {
            let follow = crate::agent_reply::follow_reply(
                State(Arc::clone(&writing)),
                AxumPath("a1".to_string()),
                None,
            )
            .await;
            if follow.status() == StatusCode::NOT_FOUND {
                released = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let again = stop().await;

        let last = stored(&writing, "a1")
            .await
            .unwrap()
            .messages
            .pop()
            .unwrap();
        assert_eq!(stopped.status(), StatusCode::NO_CONTENT);
        assert!(released, "the writing pod stops at its next save");
        assert_eq!(last.pending, None);
        assert_eq!(last.error, None);
        assert_eq!(last.stopped, Some(true));
        assert_eq!(again.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn a_reply_stopped_on_its_pod_keeps_its_text_and_is_marked_stopped() {
        let (gateway, frames) = fed_gateway().await;
        let state = Arc::new(replying_state(&gateway));

        start(&state, None, "a1", "a").await;
        frames
            .send(data(r#"{"choices":[{"delta":{"content":"b"}}]}"#))
            .unwrap();
        let mut stopped = StatusCode::NOT_FOUND;
        for _ in 0..50 {
            if stored(&state, "a1")
                .await
                .and_then(|conversation| conversation.messages.last().cloned())
                .is_some_and(|last| last.content == "b")
            {
                stopped = crate::agent_reply::stop_reply(
                    State(Arc::clone(&state)),
                    AxumPath("a1".to_string()),
                    None,
                )
                .await
                .status();
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let reply = reply_ended(&state, "a1").await;

        assert_eq!(stopped, StatusCode::NO_CONTENT);
        assert_eq!(reply.content, "b");
        assert_eq!(reply.error, None);
        assert_eq!(reply.stopped, Some(true));
    }

    #[tokio::test]
    async fn only_the_owner_follows_or_stops_their_reply() {
        let (gateway, _frames) = fed_gateway().await;
        let state = Arc::new(replying_state(&gateway));
        let follow = |user: &str| {
            crate::agent_reply::follow_reply(
                State(Arc::clone(&state)),
                AxumPath("a1".to_string()),
                signed_in(user),
            )
        };
        let stop = |user: &str| {
            crate::agent_reply::stop_reply(
                State(Arc::clone(&state)),
                AxumPath("a1".to_string()),
                signed_in(user),
            )
        };

        start(&state, signed_in("ann"), "a1", "a").await;

        assert_eq!(follow("bob").await.status(), StatusCode::NOT_FOUND);
        assert_eq!(stop("bob").await.status(), StatusCode::NOT_FOUND);
        assert_eq!(follow("ann").await.status(), StatusCode::OK);
        assert_eq!(stop("ann").await.status(), StatusCode::NO_CONTENT);
    }

    #[tokio::test]
    async fn deleting_a_shared_conversation_removes_everyones_read_state_and_folder() {
        let state = Arc::new(test_state());
        write_flow_thread(&state, "a1").await;
        write_flow_thread(&state, "a2").await;
        for user in ["ann", "bob"] {
            for id in ["a1", "a2"] {
                mark_conversation_read(
                    State(Arc::clone(&state)),
                    AxumPath(id.into()),
                    signed_in(user),
                )
                .await;
            }
            move_to(&state, user, "f").await;
        }

        let deleted = delete_conversation(
            State(Arc::clone(&state)),
            AxumPath("a1".into()),
            signed_in("ann"),
        )
        .await;

        let mut left = Vec::new();
        for keys in [AgentsKeys::Reads, AgentsKeys::Folders] {
            left.extend(
                state
                    .conversation_cache
                    .list_keys(&key_prefix(&state, keys))
                    .await
                    .unwrap(),
            );
        }
        assert_eq!(deleted.status(), StatusCode::NO_CONTENT);
        assert_eq!(left.len(), 2);
        assert!(left.iter().all(|key| key.ends_with(".a2")), "{left:?}");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn resources_of_a_mounted_config_map_are_listed_once_by_clean_name() {
        use std::os::unix::fs::symlink;
        let root = tempfile::tempdir().unwrap();
        let mount = |dir: &std::path::Path, file: &str| {
            std::fs::create_dir_all(dir.join("..version")).unwrap();
            std::fs::write(dir.join("..version").join(file), "content").unwrap();
            symlink("..version", dir.join("..data")).unwrap();
            symlink(format!("..data/{file}"), dir.join(file)).unwrap();
        };
        mount(root.path(), "a.txt");
        mount(&root.path().join("nested"), "b.txt");
        std::fs::write(root.path().join(".hidden"), "content").unwrap();
        let mut state = test_state();
        state.resource_loader = Some(flowgen_core::resource::ResourceLoader::new(Some(
            root.path().to_path_buf(),
        )));

        let Json(resources) = list_resources(State(Arc::new(state))).await;

        let keys: Vec<&str> = resources.iter().map(|r| r.key.as_str()).collect();
        assert_eq!(keys, vec!["a.txt", "nested/b.txt"]);
    }

    #[test]
    fn logs_query_takes_one_levels_parameter_per_level() {
        let query = |query: &str| {
            let uri: Uri = format!("/api/logs?{query}").parse().unwrap();
            axum_extra::extract::Query::<LogsQuery>::try_from_uri(&uri)
        };

        let filter = query("levels=warn&levels=error").unwrap().0.filter();
        let unfiltered = query("limit=10").unwrap().0.filter();
        let rejected = query("levels=warn&levels=loud");

        assert_eq!(filter.levels, vec!["warn", "error"]);
        assert!(unfiltered.levels.is_empty());
        assert!(rejected.is_err());
    }

    #[tokio::test]
    async fn cluster_status_lists_only_this_pod_without_cache() {
        let state = test_state();
        state.flow_registry.write().unwrap().insert(
            "orders".to_string(),
            crate::app::FlowHandle {
                identity: "orders".to_string(),
                flow_display_name: None,
                flow_description: None,
                flow_tags: Vec::new(),
                require_leader_election: false,
                task_count: 1,
                connections: Vec::new(),
                started_at: std::time::SystemTime::now(),
                flow_yaml: String::new(),
                cancellation_token: tokio_util::sync::CancellationToken::new(),
                join_handle: tokio::spawn(std::future::pending()),
                from_filesystem: true,
                task_manager: None,
            },
        );

        let Json(status) = get_cluster_status(State(Arc::new(state))).await.unwrap();

        assert_eq!(status.pods.len(), 1);
        assert_eq!(status.pods[0].identity, "test-pod");
        assert_eq!(status.pods[0].flows, Some(1));
        assert_eq!(status.pods[0].unreachable_reason, None);
    }

    #[test]
    fn the_policy_lets_only_the_pages_own_inline_scripts_run() {
        use base64::Engine as _;
        use sha2::Digest as _;
        let html = br#"<script>a()</script><script src="b.js"></script><script type="module">c()</script>"#;

        let policy = content_security_policy(html, &["'self'".to_string()]);

        for script in ["a()", "c()"] {
            let hash = base64::engine::general_purpose::STANDARD
                .encode(sha2::Sha256::digest(script.as_bytes()));
            assert!(policy.contains(&format!("'sha256-{hash}'")), "{policy}");
        }
        assert_eq!(policy.matches("'sha256-").count(), 2, "{policy}");
        assert!(policy.ends_with("frame-ancestors 'self'"), "{policy}");
    }

    #[tokio::test]
    async fn the_ui_page_carries_its_policy_and_every_response_nosniff() {
        let base = serve(router("/flowgen", test_state())).await;

        let page = reqwest::get(format!("{base}/flowgen/")).await.unwrap();
        let api = reqwest::get(format!("{base}/flowgen/api/version"))
            .await
            .unwrap();

        let policy = page.headers()["content-security-policy"].to_str().unwrap();
        assert!(policy.contains("script-src 'self' 'sha256-"), "{policy}");
        assert!(policy.contains("frame-ancestors 'self'"), "{policy}");
        for response in [&page, &api] {
            assert_eq!(response.headers()["x-content-type-options"], "nosniff");
        }
        assert!(api.headers().get("content-security-policy").is_none());
    }

    #[test]
    fn a_frame_ancestor_that_would_break_the_policy_is_rejected() {
        let config = |frame_ancestors: &str| -> crate::config::AppConfig {
            serde_json::from_value(serde_json::json!({
                "flows": {},
                "web": { "enabled": true, "frame_ancestors": [frame_ancestors] }
            }))
            .unwrap()
        };

        assert!(config("https://console.example.com").validate().is_ok());
        for broken in ["", "a b", "'self'; script-src *", "a,b"] {
            assert!(config(broken).validate().is_err(), "{broken:?}");
        }
    }

    #[test]
    fn rewrite_base_path_is_noop_when_prefix_matches_build() {
        let html = br#"<script src="/flowgen/_app/foo.js"></script>"#;
        let out = rewrite_base_path("index.html", html, BUILT_BASE_PATH);
        assert!(out.is_none(), "no rewrite needed when prefix == built base");
    }

    #[test]
    fn rewrite_base_path_replaces_prefix_in_html() {
        let html = br#"<script src="/flowgen/_app/foo.js"></script>"#;
        let out = rewrite_base_path("index.html", html, "/ortofan").expect("rewrite");
        let text = std::str::from_utf8(&out).unwrap();
        assert_eq!(text, r#"<script src="/ortofan/_app/foo.js"></script>"#);
    }

    #[test]
    fn rewrite_base_path_replaces_prefix_in_js() {
        let js = br#"const base = "/flowgen"; fetch("/flowgen/api/flows");"#;
        let out = rewrite_base_path("app.js", js, "/nested/path").expect("rewrite");
        let text = std::str::from_utf8(&out).unwrap();
        assert!(text.contains(r#"fetch("/nested/path/api/flows")"#));
        assert!(text.contains(r#"const base = "/nested/path""#));
    }

    #[test]
    fn rewrite_base_path_replaces_both_bare_and_slashed_forms() {
        let html = br#"<script>base="/flowgen"</script><link href="/flowgen/style.css">"#;
        let out = rewrite_base_path("index.html", html, "/test").expect("rewrite");
        let text = std::str::from_utf8(&out).unwrap();
        assert_eq!(
            text,
            r#"<script>base="/test"</script><link href="/test/style.css">"#
        );
    }

    #[test]
    fn rewrite_base_path_maps_empty_prefix_to_root() {
        let html = br#"<link href="/flowgen/style.css">"#;
        let out = rewrite_base_path("index.html", html, "").expect("rewrite");
        let text = std::str::from_utf8(&out).unwrap();
        assert_eq!(text, r#"<link href="/style.css">"#);
    }

    #[test]
    fn rewrite_base_path_skips_binary_assets() {
        let png = &[0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a];
        let out = rewrite_base_path("logo.png", png, "/anything");
        assert!(out.is_none(), "binary assets must not be rewritten");
    }

    #[test]
    fn rewrite_base_path_returns_input_when_no_hits() {
        let css = br#"body { color: red; }"#;
        let out = rewrite_base_path("style.css", css, "/other").expect("rewrite");
        assert_eq!(out, css);
    }

    #[test]
    fn removal_cookie_matches_the_path_and_secure_flag_of_the_cookie_it_clears() {
        let removal = removal_cookie(SSO_SESSION_COOKIE, true).to_string();
        let original = auth_cookie(SSO_SESSION_COOKIE, "value".to_string(), None, true).to_string();
        assert!(original.contains("Path=/"), "{original}");
        assert!(removal.contains("Path=/"), "{removal}");
        assert!(original.contains("Secure"), "{original}");
        assert!(removal.contains("Secure"), "{removal}");
        assert!(removal.contains("Max-Age=0"), "{removal}");
    }

    #[test]
    fn removal_cookie_omits_secure_when_cookie_secure_is_off() {
        let removal = removal_cookie(SSO_SESSION_COOKIE, false).to_string();
        assert!(!removal.contains("Secure"), "{removal}");
    }

    #[test]
    fn return_path_accepts_pages_under_the_prefix() {
        assert_eq!(
            return_path("/flowgen", "/flowgen/agents/abc?x=1#top").as_deref(),
            Some("/flowgen/agents/abc?x=1#top")
        );
        assert_eq!(
            return_path("/flowgen", "/flowgen/").as_deref(),
            Some("/flowgen/")
        );
        assert_eq!(return_path("", "/logs").as_deref(), Some("/logs"));
        assert_eq!(
            return_path("/flowgen", "/flowgen/logs?q=..").as_deref(),
            Some("/flowgen/logs?q=..")
        );
    }

    #[test]
    fn return_path_rejects_anything_that_could_leave_the_ui() {
        for rejected in [
            "https://evil.example/",
            "//evil.example/",
            "/\\evil.example/",
            "/flowgen\\..\\x",
            "/flowgenx/agents",
            "/flowgen",
            "/other/page",
            "/flowgen/auth/login",
            "/flowgen/../other/",
            "/flowgen/%2e%2E/other/",
            "/flowgen/./agents",
            "/flowgen/a b",
            "/flowgen/\r\nSet-Cookie:x",
        ] {
            assert_eq!(return_path("/flowgen", rejected), None, "{rejected:?}");
        }
        assert_eq!(return_path("", "//evil.example/"), None);
        assert_eq!(return_path("", "evil.example"), None);
    }

    #[test]
    fn a_login_state_cookie_without_return_to_still_parses() {
        let encoded = r#"{"csrf_state":"s","nonce":"n","pkce_verifier":"v"}"#;
        let pending: PendingLogin = serde_json::from_str(encoded).unwrap();
        assert_eq!(pending.login.csrf_state, "s");
        assert_eq!(pending.return_to, None);
    }

    #[test]
    fn ui_url_keeps_one_trailing_slash() {
        assert_eq!(ui_url("/flowgen"), "/flowgen/");
        assert_eq!(ui_url("/flowgen/"), "/flowgen/");
        assert_eq!(ui_url("/nested/path"), "/nested/path/");
        assert_eq!(ui_url(""), "/");
    }

    #[test]
    fn the_private_jar_cannot_remove_a_cookie_it_failed_to_decrypt() {
        let mut headers = axum::http::HeaderMap::new();
        headers.insert(
            axum::http::header::COOKIE,
            axum::http::HeaderValue::from_static("flowgen_auth_session=not-encrypted-with-our-key"),
        );
        let key = axum_extra::extract::cookie::Key::generate();
        let jar = axum_extra::extract::cookie::PrivateCookieJar::from_headers(&headers, key)
            .remove(removal_cookie(SSO_SESSION_COOKIE, true));

        let response = (jar, StatusCode::NO_CONTENT).into_response();
        assert!(
            !response
                .headers()
                .contains_key(axum::http::header::SET_COOKIE),
            "jar-based removal emits no Set-Cookie, so auth_logout writes the header itself"
        );
    }
}

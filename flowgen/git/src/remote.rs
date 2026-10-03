//! Remote access shared by git tasks: HTTPS credentials, URL checks, and
//! shallow fetches with an interrupt flag and time budget.

use secrecy::{ExposeSecret, SecretString};
use serde::Deserialize;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

/// Username sent for basic auth when the user did not override it. Accepted
/// by GitHub Personal Access Tokens and App installation tokens, GitLab
/// personal and deploy tokens, and Bitbucket app passwords.
const DEFAULT_TOKEN_USERNAME: &str = "x-access-token";

/// Git credentials loaded from the credentials JSON file.
///
/// The token is sent as basic auth: `git_push` sends it with every request,
/// `git_sync` in answer to the server's `WWW-Authenticate` challenge. It
/// never appears in the repository URL, in `.git/config`, or in logs.
///
/// `username` defaults to `x-access-token`, which works for GitHub Personal
/// Access Tokens and GitHub App installation tokens, and is accepted as the
/// basic-auth user by GitLab personal and deploy tokens and Bitbucket app
/// passwords. Override it for hosts that require a specific literal
/// username (GitLab OAuth tokens expect `oauth2`, Bitbucket Cloud
/// token-auth expects `x-token-auth`).
#[derive(Clone, Debug, Deserialize)]
pub struct Credentials {
    /// HTTPS token, e.g. a GitHub Personal Access Token, a GitLab deploy
    /// token, or a Bitbucket app password.
    pub token: SecretString,
    /// Optional username paired with the token for basic auth. Defaults to
    /// `x-access-token` when omitted.
    #[serde(default)]
    pub username: Option<String>,
}

impl Credentials {
    /// The basic-auth username paired with the token.
    pub fn username(&self) -> &str {
        match &self.username {
            Some(username) => username,
            None => DEFAULT_TOKEN_USERNAME,
        }
    }
}

/// Loads the credentials file when one is configured.
pub async fn load_credentials(
    path: Option<&Path>,
) -> Result<Option<Credentials>, flowgen_core::credentials::Error> {
    match path {
        Some(path) => flowgen_core::credentials::load_credentials(path)
            .await
            .map(Some),
        None => Ok(None),
    }
}

/// Failure to clone a repository.
#[derive(thiserror::Error, Debug)]
pub enum CloneError {
    #[error("Git clone failed for {url}: {source}")]
    Clone {
        url: String,
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },
    #[error("Failed to checkout worktree: {source}")]
    Checkout {
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },
}

/// Static credential helper: responds to gix's auth callback with our
/// token, never embedding it in the repository URL or `.git/config`.
///
/// On `Get`, returns `{username, password=token}`. On `Store`/`Erase`,
/// returns `None` so gix treats those as no-ops — we don't persist
/// anything outside the in-memory copy.
#[derive(Clone)]
struct CredentialHelper {
    username: String,
    password: SecretString,
}

impl CredentialHelper {
    fn new(credentials: &Credentials) -> Self {
        Self {
            username: credentials.username().to_string(),
            password: credentials.token.clone(),
        }
    }

    fn invoke(
        &self,
        action: gix::credentials::helper::Action,
    ) -> Option<gix::credentials::protocol::Outcome> {
        match action {
            gix::credentials::helper::Action::Get(ctx) => {
                Some(gix::credentials::protocol::Outcome {
                    identity: gix::sec::identity::Account {
                        username: self.username.clone(),
                        password: self.password.expose_secret().to_string(),
                        oauth_refresh_token: None,
                    },
                    next: ctx.into(),
                })
            }
            gix::credentials::helper::Action::Store(_)
            | gix::credentials::helper::Action::Erase(_) => None,
        }
    }

    /// Builds a gix `set_credentials` callback. The return type is
    /// dictated by the gix API; `Err` is unreachable in practice
    /// because [`Self::invoke`] never fails.
    #[expect(
        clippy::result_large_err,
        reason = "gix::Connection::set_credentials fixes the Result shape"
    )]
    fn into_gix_callback(
        self,
    ) -> impl FnMut(
        gix::credentials::helper::Action,
    ) -> Result<
        Option<gix::credentials::protocol::Outcome>,
        gix::credentials::protocol::Error,
    > {
        move |action| Ok(self.invoke(action))
    }
}

fn clone_error(url: &str, source: impl std::error::Error + Send + Sync + 'static) -> CloneError {
    CloneError::Clone {
        url: url.to_string(),
        source: Box::new(source),
    }
}

/// Points a prepared clone at the tip of `branch` only, with credentials.
fn single_commit_of(
    prepare: gix::clone::PrepareFetch,
    url: &str,
    branch: &str,
    credentials: Option<&Credentials>,
) -> Result<gix::clone::PrepareFetch, CloneError> {
    let prepare = prepare
        .with_ref_name(Some(branch))
        .map_err(|e| clone_error(url, e))?
        .with_shallow(gix::remote::fetch::Shallow::DepthAtRemote(
            std::num::NonZeroU32::MIN,
        ));
    match credentials {
        Some(credentials) => {
            let helper = CredentialHelper::new(credentials);
            Ok(prepare.configure_connection(move |connection| {
                connection.set_credentials(helper.clone().into_gix_callback());
                Ok(())
            }))
        }
        None => Ok(prepare),
    }
}

/// Shallow-clones a single branch into `path` and checks out its worktree.
pub(crate) fn shallow_clone(
    url: &str,
    branch: &str,
    path: &Path,
    credentials: Option<&Credentials>,
    interrupt: &AtomicBool,
) -> Result<(), CloneError> {
    let prepare = gix::prepare_clone(url, path).map_err(|e| clone_error(url, e))?;
    let mut prepare = single_commit_of(prepare, url, branch, credentials)?;
    let (mut checkout, _outcome) = prepare
        .fetch_then_checkout(gix::progress::Discard, interrupt)
        .map_err(|e| clone_error(url, e))?;
    checkout
        .main_worktree(gix::progress::Discard, interrupt)
        .map_err(|e| CloneError::Checkout {
            source: Box::new(e),
        })?;
    Ok(())
}

/// Shallow-fetches a single branch into a bare repository at `path`, with
/// `HEAD` on the branch tip.
pub(crate) fn shallow_fetch_bare(
    url: &str,
    branch: &str,
    path: &Path,
    credentials: Option<&Credentials>,
    interrupt: &AtomicBool,
) -> Result<gix::Repository, CloneError> {
    let prepare = gix::prepare_clone_bare(url, path).map_err(|e| clone_error(url, e))?;
    let mut prepare = single_commit_of(prepare, url, branch, credentials)?;
    let (repo, _outcome) = prepare
        .fetch_only(gix::progress::Discard, interrupt)
        .map_err(|e| clone_error(url, e))?;
    Ok(repo)
}

/// A repository URL the git tasks must not use.
#[derive(thiserror::Error, Debug)]
pub enum UrlError {
    #[error("SSH URLs are not supported, use HTTPS with a token via credentials_path: {url}")]
    Ssh { url: String },
    #[error("Credentials are only sent over HTTPS, or over HTTP to a loopback host: {url}")]
    Insecure { url: String },
    #[error("Invalid repository URL '{url}': {source}")]
    Parse {
        url: String,
        #[source]
        source: url::ParseError,
    },
}

/// Rejects SSH URLs, and with credentials any URL but HTTPS or HTTP to a
/// loopback host, because the push sends the token with its first request.
pub fn check_url(url: &str, credentials: Option<&Credentials>) -> Result<(), UrlError> {
    let ssh = url.starts_with("git@") || url.starts_with("ssh://");
    match (ssh, credentials) {
        (true, _) => Err(UrlError::Ssh {
            url: url.to_string(),
        }),
        (false, None) => Ok(()),
        (false, Some(_)) => {
            let parsed = url::Url::parse(url).map_err(|source| UrlError::Parse {
                url: url.to_string(),
                source,
            })?;
            match (parsed.scheme(), is_loopback(&parsed)) {
                ("https", _) | ("http", true) => Ok(()),
                _ => Err(UrlError::Insecure {
                    url: url.to_string(),
                }),
            }
        }
    }
}

fn is_loopback(url: &url::Url) -> bool {
    match url.host() {
        Some(url::Host::Domain(domain)) => domain.eq_ignore_ascii_case("localhost"),
        Some(url::Host::Ipv4(ip)) => ip.is_loopback(),
        Some(url::Host::Ipv6(ip)) => ip.is_loopback(),
        None => false,
    }
}

/// Why a blocking git operation produced no result.
#[derive(thiserror::Error, Debug)]
pub enum BlockingError {
    #[error("Git operation did not finish within {timeout:?}")]
    Timeout {
        timeout: Duration,
        #[source]
        source: tokio::time::error::Elapsed,
    },
    #[error("Git operation panicked or was cancelled: {source}")]
    Join {
        #[source]
        source: tokio::task::JoinError,
    },
}

/// Sets the flag when dropped, so a blocking git operation stops once its
/// caller has given up on it.
struct InterruptOnDrop(Arc<AtomicBool>);

impl Drop for InterruptOnDrop {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Relaxed);
    }
}

/// Runs `operation` on a blocking thread with an interrupt flag that is set
/// when `timeout` elapses or the returned future is dropped.
pub(crate) async fn run_blocking<T, F>(
    timeout: Option<Duration>,
    operation: F,
) -> Result<T, BlockingError>
where
    T: Send + 'static,
    F: FnOnce(&AtomicBool) -> T + Send + 'static,
{
    let interrupt = Arc::new(AtomicBool::new(false));
    let _interrupt_on_drop = InterruptOnDrop(Arc::clone(&interrupt));
    let span = tracing::Span::current();
    let task = tokio::task::spawn_blocking(move || {
        let _entered = span.enter();
        operation(&interrupt)
    });
    let joined = match timeout {
        Some(timeout) => tokio::time::timeout(timeout, task)
            .await
            .map_err(|source| BlockingError::Timeout { timeout, source })?,
        None => task.await,
    };
    joined.map_err(|source| BlockingError::Join { source })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn credentials(json: &str) -> Credentials {
        serde_json::from_str(json).unwrap()
    }

    #[test]
    fn test_credentials() {
        let creds = credentials(r#"{"token": "your-password"}"#);
        assert_eq!(creds.token.expose_secret(), "your-password");
        assert_eq!(creds.username(), "x-access-token");
    }

    #[test]
    fn test_credentials_with_username() {
        let creds = credentials(r#"{"token": "glpat-xxxx", "username": "oauth2"}"#);
        assert_eq!(creds.token.expose_secret(), "glpat-xxxx");
        assert_eq!(creds.username(), "oauth2");
    }

    #[test]
    fn the_token_is_redacted_from_debug_output() {
        let creds = credentials(r#"{"token": "ghp_secret"}"#);
        assert!(!format!("{creds:?}").contains("ghp_secret"));
    }

    #[test]
    fn error_display_clone() {
        let err = CloneError::Clone {
            url: "https://github.com/org/repo.git".to_string(),
            source: "network timeout".into(),
        };
        assert_eq!(
            err.to_string(),
            "Git clone failed for https://github.com/org/repo.git: network timeout"
        );
    }

    #[test]
    fn error_display_checkout() {
        let err = CloneError::Checkout {
            source: "conflict".into(),
        };
        assert_eq!(err.to_string(), "Failed to checkout worktree: conflict");
    }

    #[test]
    fn credential_helper_default_username() {
        let helper = CredentialHelper::new(&credentials(r#"{"token": "ghp_secret"}"#));
        let action = gix::credentials::helper::Action::get_for_url("https://github.com/org/repo");
        let outcome = helper.invoke(action).expect("Get must return Some");
        assert_eq!(outcome.identity.username, DEFAULT_TOKEN_USERNAME);
        assert_eq!(outcome.identity.password, "ghp_secret");
    }

    #[test]
    fn credential_helper_explicit_username() {
        let helper = CredentialHelper::new(&credentials(
            r#"{"token": "glpat-xyz", "username": "oauth2"}"#,
        ));
        let action = gix::credentials::helper::Action::get_for_url("https://gitlab.com/org/repo");
        let outcome = helper.invoke(action).expect("Get must return Some");
        assert_eq!(outcome.identity.username, "oauth2");
        assert_eq!(outcome.identity.password, "glpat-xyz");
    }

    #[test]
    fn credential_helper_store_and_erase_are_noops() {
        let helper = CredentialHelper::new(&credentials(r#"{"token": "tok"}"#));
        let store = gix::credentials::helper::Action::Store("payload".into());
        assert!(helper.invoke(store).is_none(), "Store must be a no-op");
        let erase = gix::credentials::helper::Action::Erase("payload".into());
        assert!(helper.invoke(erase).is_none(), "Erase must be a no-op");
    }

    #[tokio::test]
    async fn a_missing_credentials_file_names_its_path() {
        let err = load_credentials(Some(Path::new("/nonexistent/git.json")))
            .await
            .unwrap_err();
        assert!(err.to_string().contains("/nonexistent/git.json"));
    }

    #[tokio::test]
    async fn no_credentials_path_loads_no_credentials() {
        assert!(load_credentials(None).await.unwrap().is_none());
    }

    #[test]
    fn ssh_urls_are_rejected_with_or_without_credentials() {
        let creds = credentials(r#"{"token": "t"}"#);
        for url in [
            "git@github.com:org/repo.git",
            "ssh://git@github.com/org/repo",
        ] {
            assert!(matches!(check_url(url, None), Err(UrlError::Ssh { .. })));
            assert!(matches!(
                check_url(url, Some(&creds)),
                Err(UrlError::Ssh { .. })
            ));
        }
    }

    #[test]
    fn credentials_require_https_or_http_to_a_loopback_host() {
        let creds = credentials(r#"{"token": "t"}"#);
        for url in [
            "https://git.example.com/a.git",
            "http://127.0.0.1:8080/a.git",
            "http://[::1]/a.git",
            "http://localhost/a.git",
        ] {
            assert!(check_url(url, Some(&creds)).is_ok(), "{url}");
        }
        for url in [
            "http://git.example.com/a.git",
            "http://10.0.0.1/a.git",
            "file:///srv/a.git",
        ] {
            assert!(
                matches!(check_url(url, Some(&creds)), Err(UrlError::Insecure { .. })),
                "{url}"
            );
        }
        assert!(matches!(
            check_url("not a url", Some(&creds)),
            Err(UrlError::Parse { .. })
        ));
    }

    #[test]
    fn without_credentials_any_non_ssh_url_is_allowed() {
        for url in ["http://git.example.com/a.git", "file:///srv/a.git"] {
            assert!(check_url(url, None).is_ok(), "{url}");
        }
    }

    #[tokio::test]
    async fn a_blocking_operation_past_its_budget_times_out_and_is_interrupted() {
        let (seen_tx, seen_rx) = std::sync::mpsc::channel();
        let result = run_blocking(Some(Duration::from_millis(50)), move |interrupt| {
            while !interrupt.load(Ordering::Relaxed) {
                std::thread::sleep(Duration::from_millis(5));
            }
            seen_tx.send(()).unwrap();
        })
        .await;
        assert!(matches!(result, Err(BlockingError::Timeout { .. })));
        assert!(seen_rx.recv_timeout(Duration::from_secs(5)).is_ok());
    }
}

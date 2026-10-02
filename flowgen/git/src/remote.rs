//! Remote access shared by git tasks: HTTPS credentials and shallow clones.

use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

/// Username sent for basic auth when the user did not override it. Accepted
/// by GitHub Personal Access Tokens and App installation tokens, GitLab
/// personal and deploy tokens, and Bitbucket app passwords.
const DEFAULT_TOKEN_USERNAME: &str = "x-access-token";

/// Git credentials loaded from the credentials JSON file.
///
/// The token is sent over HTTPS via a gix credential helper that responds
/// to the server's `WWW-Authenticate` challenge — the token never appears
/// in the repository URL, in `.git/config`, or in logs.
///
/// `username` defaults to `x-access-token`, which works for GitHub Personal
/// Access Tokens and GitHub App installation tokens, and is accepted as the
/// basic-auth user by GitLab personal and deploy tokens and Bitbucket app
/// passwords. Override it for hosts that require a specific literal
/// username (GitLab OAuth tokens expect `oauth2`, Bitbucket Cloud
/// token-auth expects `x-token-auth`).
#[derive(Clone, Deserialize, Serialize, PartialEq)]
pub struct Credentials {
    /// HTTPS token, e.g. a GitHub Personal Access Token, a GitLab deploy
    /// token, or a Bitbucket app password.
    pub token: String,
    /// Optional username paired with the token for basic auth. Defaults to
    /// `x-access-token` when omitted.
    #[serde(default)]
    pub username: Option<String>,
}

/// Failure to load a credentials file.
#[derive(thiserror::Error, Debug)]
pub enum CredentialsError {
    #[error("Failed to read credentials file '{path}': {source}")]
    Read {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("Failed to parse credentials file '{path}': {source}")]
    Parse {
        path: PathBuf,
        #[source]
        source: serde_json::Error,
    },
}

impl std::fmt::Debug for Credentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Credentials")
            .field("token", &"***")
            .field("username", &self.username)
            .finish()
    }
}

impl Credentials {
    /// Reads credentials from a JSON file.
    pub async fn load(path: &Path) -> Result<Self, CredentialsError> {
        let content =
            tokio::fs::read_to_string(path)
                .await
                .map_err(|source| CredentialsError::Read {
                    path: path.to_path_buf(),
                    source,
                })?;
        serde_json::from_str(&content).map_err(|source| CredentialsError::Parse {
            path: path.to_path_buf(),
            source,
        })
    }

    /// The basic-auth username paired with the token.
    pub fn username(&self) -> &str {
        match &self.username {
            Some(username) => username,
            None => DEFAULT_TOKEN_USERNAME,
        }
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
    password: String,
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
                        password: self.password.clone(),
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

/// Performs a shallow clone of a single branch into `path`.
pub(crate) fn shallow_clone(
    url: &str,
    branch: &str,
    path: &Path,
    credentials: Option<&Credentials>,
) -> Result<(), CloneError> {
    let clone_error = |source: Box<dyn std::error::Error + Send + Sync>| CloneError::Clone {
        url: url.to_string(),
        source,
    };
    let mut prepare = gix::prepare_clone(url, path)
        .map_err(|e| clone_error(Box::new(e)))?
        .with_ref_name(Some(branch))
        .map_err(|e| clone_error(Box::new(e)))?
        .with_shallow(gix::remote::fetch::Shallow::DepthAtRemote(
            std::num::NonZeroU32::MIN,
        ));

    if let Some(creds) = credentials {
        let helper = CredentialHelper::new(creds);
        prepare = prepare.configure_connection(move |connection| {
            connection.set_credentials(helper.clone().into_gix_callback());
            Ok(())
        });
    }

    let (mut checkout, _outcome) = prepare
        .fetch_then_checkout(gix::progress::Discard, &gix::interrupt::IS_INTERRUPTED)
        .map_err(|e| clone_error(Box::new(e)))?;

    checkout
        .main_worktree(gix::progress::Discard, &gix::interrupt::IS_INTERRUPTED)
        .map_err(|e| CloneError::Checkout {
            source: Box::new(e),
        })?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_credentials() {
        let creds: Credentials = serde_json::from_str(r#"{"token": "your-password"}"#).unwrap();
        assert_eq!(creds.token, "your-password");
        assert_eq!(creds.username(), "x-access-token");
    }

    #[test]
    fn test_credentials_with_username() {
        let creds: Credentials =
            serde_json::from_str(r#"{"token": "glpat-xxxx", "username": "oauth2"}"#).unwrap();
        assert_eq!(creds.token, "glpat-xxxx");
        assert_eq!(creds.username(), "oauth2");
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
        let creds = Credentials {
            token: "ghp_secret".to_string(),
            username: None,
        };
        let helper = CredentialHelper::new(&creds);
        let action = gix::credentials::helper::Action::get_for_url("https://github.com/org/repo");
        let outcome = helper.invoke(action).expect("Get must return Some");
        assert_eq!(outcome.identity.username, DEFAULT_TOKEN_USERNAME);
        assert_eq!(outcome.identity.password, "ghp_secret");
    }

    #[test]
    fn credential_helper_explicit_username() {
        let creds = Credentials {
            token: "glpat-xyz".to_string(),
            username: Some("oauth2".to_string()),
        };
        let helper = CredentialHelper::new(&creds);
        let action = gix::credentials::helper::Action::get_for_url("https://gitlab.com/org/repo");
        let outcome = helper.invoke(action).expect("Get must return Some");
        assert_eq!(outcome.identity.username, "oauth2");
        assert_eq!(outcome.identity.password, "glpat-xyz");
    }

    #[test]
    fn credential_helper_store_and_erase_are_noops() {
        let creds = Credentials {
            token: "tok".to_string(),
            username: None,
        };
        let helper = CredentialHelper::new(&creds);
        let store = gix::credentials::helper::Action::Store("payload".into());
        assert!(helper.invoke(store).is_none(), "Store must be a no-op");
        let erase = gix::credentials::helper::Action::Erase("payload".into());
        assert!(helper.invoke(erase).is_none(), "Erase must be a no-op");
    }

    #[tokio::test]
    async fn a_missing_credentials_file_names_its_path() {
        let err = Credentials::load(Path::new("/nonexistent/git.json"))
            .await
            .unwrap_err();
        assert!(err.to_string().contains("/nonexistent/git.json"));
    }
}

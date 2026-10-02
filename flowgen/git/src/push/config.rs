//! Configuration for the git push task.

use flowgen_core::config::ConfigExt;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::time::Duration;

fn default_branch() -> String {
    "main".to_string()
}

fn default_timeout() -> Option<Duration> {
    Some(Duration::from_secs(120))
}

fn default_connect_timeout() -> Option<Duration> {
    Some(Duration::from_secs(10))
}

/// Git push processor configuration.
///
/// Commits the files in `event.data.files` on top of `branch` and pushes the
/// commit. Each file is `{path, content}`; a `null` content deletes it, and an
/// optional `previous` (the content the change was prepared against, or
/// `null` for a new file) makes the push fail with a conflict when the branch
/// holds something else.
///
/// # Example
///
/// ```yaml
/// - git_push:
///     name: commit_changes
///     repository_url: "https://git.example.com/team/configs.git"
///     path: "configs/"
///     credentials_path: /etc/git/credentials.json
///     author:
///       name: "{{event.data.author.name}}"
///       email: "{{event.data.author.email}}"
///     message: "{{event.data.title}}"
/// ```
#[derive(PartialEq, Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Processor {
    /// Task name.
    pub name: String,
    /// HTTPS repository URL.
    pub repository_url: String,
    /// Branch to push to. Defaults to `main`.
    #[serde(default = "default_branch")]
    pub branch: String,
    /// Directory within the repository that file paths are relative to.
    #[serde(default)]
    pub path: Option<String>,
    /// JSON file with `{token, username?}`, as for `git_sync`.
    #[serde(default)]
    pub credentials_path: Option<PathBuf>,
    /// Commit author.
    pub author: Author,
    /// Commit message.
    pub message: String,
    /// Time budget for one request to the git server, including the pack
    /// upload. Defaults to 120s.
    #[serde(default = "default_timeout", with = "humantime_serde")]
    pub timeout: Option<Duration>,
    /// TCP/TLS connect timeout. Defaults to 10s.
    #[serde(default = "default_connect_timeout", with = "humantime_serde")]
    pub connect_timeout: Option<Duration>,
    /// Optional list of upstream task names this task depends on.
    #[serde(default)]
    pub depends_on: Option<Vec<String>>,
    /// Optional retry configuration (overrides app-level retry config).
    #[serde(default)]
    pub retry: Option<flowgen_core::retry::RetryConfig>,
}

/// Commit author.
#[derive(PartialEq, Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Author {
    pub name: String,
    pub email: String,
}

impl ConfigExt for Processor {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn branch_defaults_to_main_and_unknown_fields_are_rejected() {
        let config: Processor = serde_json::from_str(
            r#"{
                "name": "push",
                "repository_url": "https://git.example.com/a.git",
                "author": {"name": "a", "email": "a@example.com"},
                "message": "m"
            }"#,
        )
        .unwrap();
        assert_eq!(config.branch, "main");

        let unknown = serde_json::from_str::<Processor>(
            r#"{
                "name": "push",
                "repository_url": "https://git.example.com/a.git",
                "author": {"name": "a", "email": "a@example.com"},
                "message": "m",
                "brnach": "dev"
            }"#,
        );
        assert!(unknown.is_err());
    }
}

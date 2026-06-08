//! Configuration for the git sync task.

use flowgen_core::config::ConfigExt;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

fn default_branch() -> String {
    "main".to_string()
}

/// Git sync processor configuration.
///
/// Clones or pulls a Git repository and emits one event per file found
/// under the configured `path` within the repository. Downstream tasks
/// decide what to do with the files (parse, store, transform).
///
/// # Example
///
/// ```yaml
/// - git_sync:
///     name: sync_flows
///     repository_url: "https://github.com/org/configs.git"
///     branch: main
///     path: "flows/"
///     credentials_path: /etc/flowgen/credentials/git.json
/// ```
#[derive(PartialEq, Clone, Debug, Deserialize, Serialize)]
pub struct Processor {
    /// Task name.
    pub name: String,
    /// Git repository URL (HTTPS).
    pub repository_url: String,
    /// Branch to track (defaults to "main").
    #[serde(default = "default_branch")]
    pub branch: String,
    /// Path within the repository to scan for files.
    /// All files under this path are emitted as events.
    #[serde(default)]
    pub path: Option<String>,
    /// Local path to clone the repository into.
    ///
    /// Defaults to `<system_temp>/<flow_name>/<task_name>` so that multiple
    /// `git_sync` tasks in the same worker do not collide on the same
    /// working tree. Override only when you need a stable path on a
    /// persistent volume.
    #[serde(default)]
    pub clone_path: Option<PathBuf>,
    /// Path to credentials JSON file for HTTPS token authentication.
    #[serde(default)]
    pub credentials_path: Option<PathBuf>,
    /// Optional list of upstream task names this task depends on.
    #[serde(default)]
    pub depends_on: Option<Vec<String>>,
    /// Optional retry configuration.
    #[serde(default)]
    pub retry: Option<flowgen_core::retry::RetryConfig>,
}

impl ConfigExt for Processor {}

/// Git credentials loaded from the credentials JSON file.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
pub struct Credentials {
    /// HTTPS token (e.g., GitHub PAT, GitLab deploy token).
    pub token: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_defaults() {
        let json = r#"{
            "name": "sync",
            "repository_url": "https://github.com/org/repo.git"
        }"#;
        let config: Processor = serde_json::from_str(json).unwrap();
        assert_eq!(config.branch, "main");
        assert!(config.path.is_none());
        assert!(config.clone_path.is_none());
        assert!(config.credentials_path.is_none());
    }

    #[test]
    fn test_credentials() {
        let json = r#"{
            "token": "ghp_xxxxxxxxxxxx"
        }"#;
        let creds: Credentials = serde_json::from_str(json).unwrap();
        assert_eq!(creds.token, "ghp_xxxxxxxxxxxx");
    }
}

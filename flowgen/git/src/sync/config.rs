//! Configuration for the git sync task.

use flowgen_core::config::ConfigExt;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

fn default_branch() -> String {
    "main".to_string()
}

fn default_flows_path() -> String {
    "flows".to_string()
}

fn default_clone_path() -> PathBuf {
    PathBuf::from("/tmp/flowgen-repo")
}

fn default_flows_prefix() -> String {
    "flowgen.flows".to_string()
}

fn default_resources_prefix() -> String {
    "flowgen.resources".to_string()
}

fn default_metadata_db_name() -> String {
    "flowgen_metadata".to_string()
}

/// Git sync processor configuration.
#[derive(PartialEq, Clone, Debug, Deserialize, Serialize)]
pub struct Processor {
    /// Task name.
    pub name: String,
    /// Git repository URL (SSH or HTTPS).
    pub repository_url: String,
    /// Branch to track (defaults to "main").
    #[serde(default = "default_branch")]
    pub branch: String,
    /// Path within the repository to scan for flow YAML files (defaults to "flows").
    #[serde(default = "default_flows_path")]
    pub flows_path: String,
    /// Path within the repository to scan for resource files (optional).
    pub resources_path: Option<String>,
    /// Local path to clone the repository into (defaults to "/tmp/flowgen-repo").
    #[serde(default = "default_clone_path")]
    pub clone_path: PathBuf,
    /// Authentication configuration.
    #[serde(default)]
    pub auth: GitAuth,
    /// Cache key prefix for flows (defaults to "flowgen.flows").
    #[serde(default = "default_flows_prefix")]
    pub flows_prefix: String,
    /// Cache key prefix for resources (defaults to "flowgen.resources").
    #[serde(default = "default_resources_prefix")]
    pub resources_prefix: String,
    /// Metadata cache bucket name (defaults to "flowgen_metadata").
    #[serde(default = "default_metadata_db_name")]
    pub db_name: String,
    /// Optional list of upstream task names this task depends on.
    #[serde(default)]
    pub depends_on: Option<Vec<String>>,
    /// Optional retry configuration.
    #[serde(default)]
    pub retry: Option<flowgen_core::retry::RetryConfig>,
}

impl ConfigExt for Processor {}

/// Git authentication configuration.
#[derive(PartialEq, Clone, Debug, Default, Deserialize, Serialize)]
pub struct GitAuth {
    /// Authentication type.
    #[serde(default, rename = "type")]
    pub auth_type: GitAuthType,
    /// Path to SSH private key file.
    pub ssh_key_path: Option<PathBuf>,
    /// Path to SSH known_hosts file.
    pub ssh_known_hosts_path: Option<PathBuf>,
    /// Token for HTTPS authentication.
    pub token: Option<String>,
}

/// Supported Git authentication types.
#[derive(PartialEq, Clone, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum GitAuthType {
    #[default]
    None,
    Ssh,
    Token,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_defaults() {
        let json = r#"{
            "name": "sync",
            "repository_url": "git@github.com:org/repo.git"
        }"#;
        let config: Processor = serde_json::from_str(json).unwrap();
        assert_eq!(config.branch, "main");
        assert_eq!(config.flows_path, "flows");
        assert_eq!(config.flows_prefix, "flowgen.flows");
        assert_eq!(config.db_name, "flowgen_metadata");
        assert!(config.resources_path.is_none());
    }

    #[test]
    fn test_auth_ssh() {
        let json = r#"{
            "type": "ssh",
            "ssh_key_path": "/etc/git/key"
        }"#;
        let auth: GitAuth = serde_json::from_str(json).unwrap();
        assert_eq!(auth.auth_type, GitAuthType::Ssh);
    }
}

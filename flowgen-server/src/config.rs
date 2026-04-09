//! Configuration structures for flowgen-server.
//!
//! Defines the server application configuration including cache, Git sync,
//! and flow/resource discovery settings.

use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::time::Duration;

/// Default NATS server URL function for serde.
fn default_nats_url() -> String {
    flowgen_nats::client::DEFAULT_NATS_URL.to_string()
}

/// Default sync interval (60 seconds).
fn default_sync_interval() -> Duration {
    Duration::from_secs(60)
}

/// Default Git branch name.
fn default_branch() -> String {
    "main".to_string()
}

/// Default flows path within the Git repository.
fn default_flows_path() -> String {
    "flows".to_string()
}

/// Default resources path within the Git repository.
fn default_resources_path() -> String {
    "resources".to_string()
}

/// Default cache key prefix for flows.
fn default_flows_cache_prefix() -> String {
    "flowgen.flows".to_string()
}

/// Default cache key prefix for resources.
fn default_resources_cache_prefix() -> String {
    "flowgen.resources".to_string()
}

/// Default local clone path for the Git repository.
fn default_clone_path() -> PathBuf {
    PathBuf::from("/tmp/flowgen-repo")
}

/// Top-level server application configuration.
#[derive(PartialEq, Clone, Debug, Deserialize, Serialize)]
pub struct ServerAppConfig {
    /// Cache backend configuration.
    pub cache: CacheOptions,
    /// Flow synchronization configuration.
    pub flows: FlowSyncConfig,
    /// Resource synchronization configuration.
    #[serde(default)]
    pub flow_resources: Option<ResourceSyncConfig>,
}

/// Cache backend type.
#[derive(PartialEq, Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum CacheType {
    /// NATS JetStream Key-Value store.
    Nats,
}

/// Cache configuration options.
#[derive(PartialEq, Clone, Debug, Deserialize, Serialize)]
pub struct CacheOptions {
    /// Whether caching is enabled.
    pub enabled: bool,
    /// Cache backend type.
    #[serde(rename = "type")]
    pub cache_type: CacheType,
    /// Path to cache credentials file.
    pub credentials_path: PathBuf,
    /// NATS server URL. Defaults to "localhost:4222".
    #[serde(default = "default_nats_url")]
    pub url: String,
    /// Cache database name (defaults to "flowgen_cache" if not provided).
    pub db_name: Option<String>,
}

/// Flow synchronization configuration.
#[derive(PartialEq, Clone, Debug, Deserialize, Serialize)]
pub struct FlowSyncConfig {
    /// Cache key prefix for flows.
    #[serde(default = "default_flows_cache_prefix")]
    pub cache_prefix: String,
    /// Git repository configuration.
    pub git: GitOptions,
}

/// Resource synchronization configuration.
#[derive(PartialEq, Clone, Debug, Deserialize, Serialize)]
pub struct ResourceSyncConfig {
    /// Cache key prefix for resources.
    #[serde(default = "default_resources_cache_prefix")]
    pub cache_prefix: String,
    /// Path within the Git repository to scan for resources.
    #[serde(default = "default_resources_path")]
    pub resources_path: String,
}

/// Git repository synchronization options.
#[derive(PartialEq, Clone, Debug, Deserialize, Serialize)]
pub struct GitOptions {
    /// Git repository URL (SSH or HTTPS).
    pub repository_url: String,
    /// Branch to track.
    #[serde(default = "default_branch")]
    pub branch: String,
    /// Path within the repository to scan for flow YAML files.
    #[serde(default = "default_flows_path")]
    pub flows_path: String,
    /// Interval between sync cycles.
    #[serde(default = "default_sync_interval", with = "humantime_serde")]
    pub sync_interval: Duration,
    /// Authentication configuration.
    #[serde(default)]
    pub auth: GitAuth,
    /// Local path to clone the repository into.
    #[serde(default = "default_clone_path")]
    pub clone_path: PathBuf,
}

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
    /// No authentication.
    #[default]
    None,
    /// SSH key-based authentication.
    Ssh,
    /// Token-based HTTPS authentication.
    Token,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_git_options() {
        let json = r#"{
            "repository_url": "git@github.com:connve/flowgen-configs.git"
        }"#;
        let opts: GitOptions = serde_json::from_str(json).unwrap();
        assert_eq!(opts.branch, "main");
        assert_eq!(opts.flows_path, "flows");
        assert_eq!(opts.sync_interval, Duration::from_secs(60));
        assert_eq!(opts.clone_path, PathBuf::from("/tmp/flowgen-repo"));
    }

    #[test]
    fn test_git_auth_ssh() {
        let json = r#"{
            "type": "ssh",
            "ssh_key_path": "/etc/flowgen/git-ssh-key",
            "ssh_known_hosts_path": "/etc/flowgen/known_hosts"
        }"#;
        let auth: GitAuth = serde_json::from_str(json).unwrap();
        assert_eq!(auth.auth_type, GitAuthType::Ssh);
        assert_eq!(
            auth.ssh_key_path,
            Some(PathBuf::from("/etc/flowgen/git-ssh-key"))
        );
    }

    #[test]
    fn test_git_auth_token() {
        let json = r#"{
            "type": "token",
            "token": "ghp_abc123"
        }"#;
        let auth: GitAuth = serde_json::from_str(json).unwrap();
        assert_eq!(auth.auth_type, GitAuthType::Token);
        assert_eq!(auth.token, Some("ghp_abc123".to_string()));
    }

    #[test]
    fn test_git_auth_default() {
        let auth = GitAuth::default();
        assert_eq!(auth.auth_type, GitAuthType::None);
        assert!(auth.ssh_key_path.is_none());
        assert!(auth.token.is_none());
    }
}

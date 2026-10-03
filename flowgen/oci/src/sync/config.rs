//! Configuration for the `oci_sync` task.

use flowgen_core::config::ConfigExt;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// OCI artifact sync processor configuration.
///
/// Pulls an OCI artifact (manifest + layers) from a registry on each trigger
/// event and emits one downstream event per layer with the file content.
/// Each emitted event matches the [`super::processor::FileEvent`] shape so
/// the same downstream pipeline can ingest output from either `oci_sync` or
/// `git_sync`.
///
/// # Example
///
/// ```yaml
/// - oci_sync:
///     name: pull_configs
///     artifact: "registry.example.com/team/configs:prod"
///     credentials_path: /etc/registry/credentials.json
/// ```
#[derive(PartialEq, Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Processor {
    /// Task name.
    pub name: String,
    /// Full OCI reference, e.g. `registry.example.com/team/configs:prod` or `…@sha256:…`.
    pub artifact: String,
    /// Optional path to a credentials file. Two formats are auto-detected:
    /// a `{ "username", "password" }` JSON file, or the
    /// standard Docker `config.json` (`kubernetes.io/dockerconfigjson`
    /// Secret payload) with multiple `auths` entries keyed by registry
    /// host. For the latter, the entry matching the artifact's registry
    /// host is picked automatically; this lets the same secret used as
    /// the pod's `imagePullSecrets` also authenticate `oci_sync`.
    #[serde(default)]
    pub credentials_path: Option<PathBuf>,
    /// Bypass the manifest-digest cache and re-pull every tick. Use when
    /// downstream cache was mutated out of band and needs re-seeding.
    #[serde(default)]
    pub force_pull: bool,
    /// Maximum uncompressed size for any single file extracted from a
    /// tar/tar+gzip layer. Guards against tar-bomb layers. Default: 10 MB.
    #[serde(default = "default_max_file_size")]
    pub max_file_size: u64,
    /// Maximum total uncompressed size across all files pulled from a
    /// single artifact. Default: 100 MB.
    #[serde(default = "default_max_total_size")]
    pub max_total_size: u64,
    /// Optional list of upstream task names this task depends on.
    #[serde(default)]
    pub depends_on: Option<Vec<String>>,
    /// Optional retry configuration.
    #[serde(default)]
    pub retry: Option<flowgen_core::retry::RetryConfig>,
}

fn default_max_file_size() -> u64 {
    10 * 1024 * 1024
}

fn default_max_total_size() -> u64 {
    100 * 1024 * 1024
}

impl Default for Processor {
    fn default() -> Self {
        Self {
            name: String::new(),
            artifact: String::new(),
            credentials_path: None,
            force_pull: false,
            max_file_size: default_max_file_size(),
            max_total_size: default_max_total_size(),
            depends_on: None,
            retry: None,
        }
    }
}

impl ConfigExt for Processor {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_deser_minimal() {
        let json = r#"{
            "name": "pull",
            "artifact": "ghcr.io/org/flows:prod"
        }"#;
        let config: Processor = serde_json::from_str(json).unwrap();
        assert_eq!(config.name, "pull");
        assert_eq!(config.artifact, "ghcr.io/org/flows:prod");
        assert!(config.credentials_path.is_none());
        assert!(config.depends_on.is_none());
        assert!(config.retry.is_none());
        assert_eq!(config.max_file_size, 10 * 1024 * 1024);
        assert_eq!(config.max_total_size, 100 * 1024 * 1024);
    }

    #[test]
    fn config_deser_size_caps_override() {
        let json = r#"{
            "name": "pull",
            "artifact": "ghcr.io/org/flows:prod",
            "max_file_size": 1024,
            "max_total_size": 4096
        }"#;
        let config: Processor = serde_json::from_str(json).unwrap();
        assert_eq!(config.max_file_size, 1024);
        assert_eq!(config.max_total_size, 4096);
    }

    #[test]
    fn processor_default_matches_serde_defaults() {
        let p = Processor::default();
        assert_eq!(p.max_file_size, 10 * 1024 * 1024);
        assert_eq!(p.max_total_size, 100 * 1024 * 1024);
    }

    #[test]
    fn config_deser_full() {
        let json = r#"{
            "name": "pull_all",
            "artifact": "ghcr.io/org/flows@sha256:abcd",
            "credentials_path": "/etc/flowgen/credentials/registry.json",
            "depends_on": ["trigger"],
            "retry": { "max_attempts": 2, "initial_backoff": "500ms" }
        }"#;
        let config: Processor = serde_json::from_str(json).unwrap();
        assert_eq!(
            config.credentials_path,
            Some(PathBuf::from("/etc/flowgen/credentials/registry.json"))
        );
        assert_eq!(config.depends_on, Some(vec!["trigger".to_string()]));
        assert!(config.retry.is_some());
    }

    #[test]
    fn config_deser_missing_name_fails() {
        let json = r#"{ "artifact": "ghcr.io/org/flows:prod" }"#;
        let result = serde_json::from_str::<Processor>(json);
        assert!(result.is_err());
    }

    #[test]
    fn config_deser_missing_artifact_fails() {
        let json = r#"{ "name": "sync" }"#;
        let result = serde_json::from_str::<Processor>(json);
        assert!(result.is_err());
    }

    #[test]
    fn config_roundtrip_serde() {
        let json = r#"{
            "name": "rt",
            "artifact": "ghcr.io/org/flows:prod",
            "credentials_path": "/etc/creds.json"
        }"#;
        let config: Processor = serde_json::from_str(json).unwrap();
        let serialized = serde_json::to_string(&config).unwrap();
        let deserialized: Processor = serde_json::from_str(&serialized).unwrap();
        assert_eq!(config, deserialized);
    }
}

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
///     name: pull_flows
///     artifact: "ghcr.io/connve/flows-tenant-connve:prod"
///     credentials_path: /etc/flowgen/credentials/registry.json
/// ```
#[derive(PartialEq, Clone, Debug, Default, Deserialize, Serialize)]
pub struct Processor {
    /// Task name.
    pub name: String,
    /// Full OCI reference, e.g. `ghcr.io/org/flows:prod` or `…@sha256:…`.
    pub artifact: String,
    /// Optional path to a credentials file. Two formats are auto-detected:
    /// the flowgen-native `{ "username", "password" }` shape, or the
    /// standard Docker `config.json` (`kubernetes.io/dockerconfigjson`
    /// Secret payload) with multiple `auths` entries keyed by registry
    /// host. For the latter, the entry matching the artifact's registry
    /// host is picked automatically; this lets the same secret used as
    /// the pod's `imagePullSecrets` also authenticate `oci_sync`.
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

/// Registry credentials loaded from the credentials JSON file.
#[derive(PartialEq, Clone, Debug, Default, Deserialize, Serialize)]
pub struct Credentials {
    /// Registry username. For GHCR with a Personal Access Token, this is
    /// the GitHub username; with a GitHub Actions token, it's the actor.
    pub username: String,
    /// Registry password or token.
    pub password: String,
}

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
    }

    #[test]
    fn config_deser_full() {
        let json = r#"{
            "name": "pull_all",
            "artifact": "ghcr.io/org/flows@sha256:abcd",
            "credentials_path": "/etc/flowgen/credentials/registry.json",
            "depends_on": ["trigger"],
            "retry": { "max_retries": 2, "initial_interval": "500ms" }
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
    fn credentials_deser() {
        let json = r#"{ "username": "robot", "password": "tok123" }"#;
        let creds: Credentials = serde_json::from_str(json).unwrap();
        assert_eq!(creds.username, "robot");
        assert_eq!(creds.password, "tok123");
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

    #[test]
    fn credentials_default_values() {
        let creds = Credentials::default();
        assert_eq!(creds.username, "");
        assert_eq!(creds.password, "");
    }
}

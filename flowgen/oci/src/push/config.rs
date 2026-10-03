//! Configuration for the OCI push task.

use flowgen_core::config::ConfigExt;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// OCI push processor configuration.
///
/// Packs the files in `event.data.files` (`{path, content}`) into a
/// single-layer tar+gzip artifact and pushes it under every tag. The emitted
/// event keeps the incoming data, with `files` replaced by `digest` and
/// `references`.
///
/// # Example
///
/// ```yaml
/// - oci_push:
///     name: release
///     repository: "registry.example.com/team/configs"
///     tags: ["{{event.data.commit}}", "latest"]
///     credentials_path: /etc/registry/credentials.json
/// ```
#[derive(PartialEq, Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Processor {
    /// Task name.
    ///
    /// ```yaml
    /// name: release
    /// ```
    pub name: String,
    /// Registry and repository without a tag, rendered per event.
    ///
    /// ```yaml
    /// repository: "registry.example.com/team/configs"
    /// ```
    pub repository: String,
    /// Tags to push the artifact under, rendered per event; at least one is
    /// required.
    ///
    /// ```yaml
    /// tags: ["{{event.data.commit}}", "latest"]
    /// ```
    pub tags: Vec<String>,
    /// Registry credentials: `{username, password}` or a Docker `config.json`.
    ///
    /// ```yaml
    /// credentials_path: /etc/registry/credentials.json
    /// ```
    #[serde(default)]
    pub credentials_path: Option<PathBuf>,
    /// Optional list of upstream task names this task depends on.
    ///
    /// ```yaml
    /// depends_on: ["build_files"]
    /// ```
    #[serde(default)]
    pub depends_on: Option<Vec<String>>,
    /// Optional retry configuration (overrides app-level retry config).
    ///
    /// ```yaml
    /// retry:
    ///   max_attempts: 3
    ///   initial_backoff: "2s"
    /// ```
    #[serde(default)]
    pub retry: Option<flowgen_core::retry::RetryConfig>,
}

impl ConfigExt for Processor {}

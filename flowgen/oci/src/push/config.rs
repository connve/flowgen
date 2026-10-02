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
    pub name: String,
    /// Registry and repository without a tag.
    pub repository: String,
    /// Tags to push the artifact under.
    pub tags: Vec<String>,
    /// Registry credentials: `{username, password}` or a Docker `config.json`.
    #[serde(default)]
    pub credentials_path: Option<PathBuf>,
    /// Optional list of upstream task names this task depends on.
    #[serde(default)]
    pub depends_on: Option<Vec<String>>,
    /// Optional retry configuration (overrides app-level retry config).
    #[serde(default)]
    pub retry: Option<flowgen_core::retry::RetryConfig>,
}

impl ConfigExt for Processor {}

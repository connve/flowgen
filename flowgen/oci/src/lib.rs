//! OCI registry tasks.
//!
//! `oci_sync` pulls an artifact from an OCI registry (GHCR, ECR, GAR,
//! Artifactory, Harbor, etc.) when its manifest digest changes and emits one
//! event per file, in the same shape as `git_sync`.
//!
//! `oci_push` packs the files of an event into a single-layer tar+gzip
//! artifact and pushes it to a registry under one or more tags, in the
//! layout `oci_sync` reads back.

use flowgen_core::task::meta::{Connection, Connector, Icon, TaskMeta};

/// OCI push: push files to a registry as an artifact.
pub mod push {
    /// Artifact packing and the registry push.
    pub mod client;
    /// Configuration for the OCI push task.
    pub mod config;
    /// OCI push processor implementation.
    pub mod processor;
}
/// Registry credentials and client setup shared by both tasks.
pub mod registry;
/// OCI sync: pull an artifact from a registry and emit its files.
pub mod sync {
    /// Configuration for the OCI sync task.
    pub mod config;
    /// OCI sync processor implementation.
    pub mod processor;
}

/// The connector of this crate's task types.
pub const CONNECTOR: Connector = Connector {
    name: "oci",
    label: "OCI",
    icon: Icon::Svg(include_str!("../icons/oci.svg")),
    describe_credentials: None,
};

impl TaskMeta for push::config::Processor {
    const CONNECTOR: Connector = CONNECTOR;
    const CPU_INTENSIVE: bool = true;

    fn connections(&self) -> Vec<Connection> {
        vec![Connection::new(CONNECTOR, self.credentials_path.as_deref())
            .target(&[&self.repository])]
    }
}

impl TaskMeta for sync::config::Processor {
    const CONNECTOR: Connector = CONNECTOR;
    const CPU_INTENSIVE: bool = true;

    fn connections(&self) -> Vec<Connection> {
        vec![Connection::new(CONNECTOR, self.credentials_path.as_deref()).target(&[&self.artifact])]
    }
}

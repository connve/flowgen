//! OCI registry tasks.
//!
//! `oci_sync` pulls an artifact from an OCI registry (GHCR, ECR, GAR,
//! Artifactory, Harbor, etc.) when its manifest digest changes and emits one
//! event per file, in the same shape as `git_sync`.
//!
//! `oci_push` packs the files of an event into a single-layer tar+gzip
//! artifact and pushes it to a registry under one or more tags, in the
//! layout `oci_sync` reads back.

use flowgen_core::task::context::TaskContext;
use flowgen_core::task::meta::{Connection, Connector, Icon};
use flowgen_core::task::runner::Runner;
use flowgen_core::task::{Task, TaskError, TaskIo, TaskMeta};
use std::sync::Arc;

pub mod push;
/// Registry credentials and client setup shared by both tasks.
pub mod registry;
pub mod sync;

/// The connector of this crate's task types.
pub const CONNECTOR: Connector = Connector {
    name: "oci",
    label: "OCI",
    icon: Icon::Svg(include_str!("../icons/oci.svg")),
    describe_credentials: None,
};

impl TaskMeta for push::OciPush {
    const CONNECTOR: Connector = CONNECTOR;
    const CPU_INTENSIVE: bool = true;

    fn connections(&self) -> Vec<Connection> {
        vec![Connection::new(CONNECTOR, self.credentials_path.as_deref())
            .target(&[&self.repository])]
    }
}

impl TaskMeta for sync::OciSync {
    const CONNECTOR: Connector = CONNECTOR;
    const CPU_INTENSIVE: bool = true;

    fn connections(&self) -> Vec<Connection> {
        vec![Connection::new(CONNECTOR, self.credentials_path.as_deref()).target(&[&self.artifact])]
    }
}

impl Task for push::OciPush {
    async fn run(self, io: TaskIo, context: Arc<TaskContext>) -> Result<(), TaskError> {
        let mut builder = push::ProcessorBuilder::new()
            .config(Arc::new(self))
            .task_id(io.task_id)
            .task_type(io.task_type)
            .task_context(context);
        if let Some(rx) = io.rx {
            builder = builder.receiver(rx);
        }
        if let Some(tx) = io.tx {
            builder = builder.sender(tx);
        }
        builder.build().await?.run().await?;
        Ok(())
    }
}

impl Task for sync::OciSync {
    async fn run(self, io: TaskIo, context: Arc<TaskContext>) -> Result<(), TaskError> {
        let mut builder = sync::ProcessorBuilder::new()
            .config(Arc::new(self))
            .task_id(io.task_id)
            .task_type(io.task_type)
            .task_context(context);
        if let Some(rx) = io.rx {
            builder = builder.receiver(rx);
        }
        if let Some(tx) = io.tx {
            builder = builder.sender(tx);
        }
        builder.build().await?.run().await?;
        Ok(())
    }
}

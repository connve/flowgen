//! Git tasks.
//!
//! `git_sync` clones a branch and emits its files as events; `git_push`
//! commits the files of an event onto a branch and pushes the commit over
//! HTTPS.

use flowgen_core::task::context::TaskContext;
use flowgen_core::task::meta::{Connection, Connector, Icon};
use flowgen_core::task::runner::Runner;
use flowgen_core::task::{Task, TaskError, TaskIo, TaskMeta};
use std::sync::Arc;

pub mod push;
/// HTTPS credentials and shallow clones shared by the git tasks.
pub mod remote;
pub mod sync;

/// The connector of this crate's task types.
pub const CONNECTOR: Connector = Connector {
    name: "git",
    label: "Git",
    icon: Icon::Svg(include_str!("../icons/git.svg")),
    describe_credentials: None,
};

impl TaskMeta for push::GitPush {
    const CONNECTOR: Connector = CONNECTOR;
    const CPU_INTENSIVE: bool = true;

    fn connections(&self) -> Vec<Connection> {
        vec![Connection::new(CONNECTOR, self.credentials_path.as_deref())
            .target(&[&self.repository_url])]
    }
}

impl TaskMeta for sync::GitSync {
    const CONNECTOR: Connector = CONNECTOR;
    const CPU_INTENSIVE: bool = true;

    fn connections(&self) -> Vec<Connection> {
        vec![Connection::new(CONNECTOR, self.credentials_path.as_deref())
            .target(&[&self.repository_url])]
    }
}

impl Task for push::GitPush {
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

impl Task for sync::GitSync {
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

//! Key-value tasks.
//!
//! `kv_store` gets, puts, lists and deletes keys in a store chosen by its
//! URL: a redb file on this machine or a NATS server. `kv_watch` starts a
//! flow when a key changes.

use flowgen_core::task::context::TaskContext;
use flowgen_core::task::meta::{Connection, Connector, Icon};
use flowgen_core::task::runner::Runner;
use flowgen_core::task::{Task, TaskError, TaskIo, TaskMeta};
use std::path::Path;
use std::sync::Arc;

pub mod store;
pub mod watch;

/// The connector of this crate's task types.
pub const CONNECTOR: Connector = Connector {
    name: "kv",
    label: "Key-Value Store",
    icon: Icon::Svg(include_str!("../icons/kv.svg")),
    describe_credentials: None,
    credentials: &[],
};

impl TaskMeta for store::KvStore {
    const CONNECTOR: Connector = CONNECTOR;

    fn validate(&self) -> Result<(), TaskError> {
        self.check_config()?;
        Ok(())
    }

    fn connections(&self) -> Vec<Connection> {
        connections(&self.url, self.credentials_path.as_deref())
    }
}

impl TaskMeta for watch::KvWatch {
    const CONNECTOR: Connector = CONNECTOR;
    const RUNS_UNTIL_STOPPED: bool = true;
    const DRAINS: bool = true;

    fn validate(&self) -> Result<(), TaskError> {
        self.check_config()?;
        Ok(())
    }

    fn connections(&self) -> Vec<Connection> {
        connections(&self.url, self.credentials_path.as_deref())
    }
}

/// A NATS store shares its connection with the other NATS tasks on the same
/// credentials; a redb store shows its file.
fn connections(url: &str, credentials: Option<&Path>) -> Vec<Connection> {
    match store::backend(url, credentials) {
        Ok(store::Backend::Redb(path)) => {
            vec![Connection::new(CONNECTOR, credentials).target(&[&path.to_string_lossy()])]
        }
        Ok(store::Backend::Nats(url)) => {
            vec![Connection::new(flowgen_nats::CONNECTOR, credentials).target(&[url])]
        }
        Err(_) => vec![Connection::new(CONNECTOR, credentials)],
    }
}

impl Task for store::KvStore {
    async fn run(self, io: TaskIo, context: Arc<TaskContext>) -> Result<(), TaskError> {
        let mut builder = store::ProcessorBuilder::new()
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

impl Task for watch::KvWatch {
    async fn run(self, io: TaskIo, context: Arc<TaskContext>) -> Result<(), TaskError> {
        let mut builder = watch::WatcherBuilder::new()
            .config(Arc::new(self))
            .task_id(io.task_id)
            .task_type(io.task_type)
            .task_context(context);
        if let Some(tx) = io.tx {
            builder = builder.sender(tx);
        }
        builder.build().await?.run().await?;
        Ok(())
    }
}

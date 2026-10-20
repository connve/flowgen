//! Braze tasks: exporting user profiles, and sending messages, campaigns,
//! and Canvases through the Braze REST API.

#![deny(missing_docs)]

use flowgen_core::task::context::TaskContext;
use flowgen_core::task::meta::{Connection, Connector, Icon};
use flowgen_core::task::runner::Runner;
use flowgen_core::task::{Task, TaskError, TaskIo, TaskMeta};
use std::sync::Arc;

pub mod export_user_data;
pub mod messaging;
pub mod processor;

/// The connector of this crate's task types.
pub const CONNECTOR: Connector = Connector {
    name: "braze",
    label: "Braze",
    icon: Icon::Png(include_bytes!("../icons/braze.png")),
    describe_credentials: None,
};

macro_rules! braze_task {
    ($($config:ty),+) => {
        $(
            impl TaskMeta for $config {
                const CONNECTOR: Connector = CONNECTOR;

                fn connections(&self) -> Vec<Connection> {
                    vec![Connection::new(CONNECTOR, Some(&self.credentials_path))
                        .target(&[&self.base_url])]
                }
            }

            impl Task for $config {
                async fn run(self, io: TaskIo, context: Arc<TaskContext>) -> Result<(), TaskError> {
                    let mut builder = processor::ProcessorBuilder::new()
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
                    builder.build()?.run().await?;
                    Ok(())
                }
            }
        )+
    };
}

braze_task!(export_user_data::ExportUserData, messaging::Messaging);

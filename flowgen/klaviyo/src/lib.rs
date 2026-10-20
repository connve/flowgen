//! Klaviyo tasks: templates, campaigns, lists, events and profiles.
//!
//! A private API key belongs to one Klaviyo account, so each task talks to
//! the account of its `credentials_path`.

use flowgen_core::task::context::TaskContext;
use flowgen_core::task::meta::{Connection, Connector, Icon};
use flowgen_core::task::runner::Runner;
use flowgen_core::task::{Task, TaskError, TaskIo, TaskMeta};
use std::sync::Arc;

pub mod campaigns;
pub mod events;
pub mod lists;
pub mod processor;
pub mod profiles;
pub mod templates;

/// The connector of this crate's task types.
pub const CONNECTOR: Connector = Connector {
    name: "klaviyo",
    label: "Klaviyo",
    icon: Icon::Png(include_bytes!("../icons/klaviyo.png")),
    describe_credentials: None,
};

macro_rules! klaviyo_task {
    ($($config:ty),+) => {
        $(
            impl TaskMeta for $config {
                const CONNECTOR: Connector = CONNECTOR;

                fn connections(&self) -> Vec<Connection> {
                    let connection = Connection::new(CONNECTOR, Some(&self.credentials_path));
                    match &self.base_url {
                        Some(base_url) => vec![connection.target(&[base_url])],
                        None => vec![connection],
                    }
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

klaviyo_task!(
    templates::Templates,
    campaigns::Campaigns,
    lists::Lists,
    events::Events,
    profiles::Profiles
);

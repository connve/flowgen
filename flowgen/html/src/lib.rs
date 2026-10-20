//! HTML processing tasks for flowgen.
//!
//! Provides HTML parsing and extraction capabilities. Tasks in this crate operate
//! on HTML content received in event data (typically fetched by an upstream
//! `http_request` task).

use flowgen_core::task::context::TaskContext;
use flowgen_core::task::meta::{Connector, Icon};
use flowgen_core::task::runner::Runner;
use flowgen_core::task::{Task, TaskError, TaskIo, TaskMeta};
use std::sync::Arc;

/// HTML scrape task for extracting structured data from HTML documents.
pub mod scrape;

/// The connector of this crate's task types.
pub const CONNECTOR: Connector = Connector {
    name: "html",
    label: "HTML",
    icon: Icon::Svg(include_str!("../icons/html.svg")),
    describe_credentials: None,
};

impl TaskMeta for scrape::Scrape {
    const CONNECTOR: Connector = CONNECTOR;
    const CPU_INTENSIVE: bool = true;
}

impl Task for scrape::Scrape {
    async fn run(self, io: TaskIo, context: Arc<TaskContext>) -> Result<(), TaskError> {
        let mut builder = scrape::ProcessorBuilder::new()
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

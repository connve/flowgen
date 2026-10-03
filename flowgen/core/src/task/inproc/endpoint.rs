//! `inproc_endpoint`: makes its flow callable in-process at the flow's identity.

use super::registry::{default_callers, InprocRegistry, Registration};
use crate::event::Event;
use std::sync::Arc;
use tokio::sync::mpsc::Sender;

/// Errors from the in-process endpoint.
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum Error {
    #[error("Missing required builder attribute: {}", _0)]
    MissingBuilderAttribute(String),
}

/// Registers the flow in the in-process registry; calls enter the flow after this task.
#[derive(Debug)]
pub struct Processor {
    config: Arc<super::config::Endpoint>,
    tx: Sender<Event>,
    task_id: usize,
    task_type: &'static str,
    task_context: Arc<crate::task::context::TaskContext>,
}

#[async_trait::async_trait]
impl crate::task::runner::Runner for Processor {
    type Error = Error;
    type EventHandler = ();

    async fn init(&self) -> Result<(), Error> {
        Ok(())
    }

    #[tracing::instrument(skip(self), name = "task.run", fields(task = %self.config.name, task_id = self.task_id, task_type = %self.task_type))]
    async fn run(self) -> Result<(), Error> {
        let registry: &InprocRegistry = &self.task_context.inproc;
        let flow = self.task_context.flow.identity().to_string();
        let callers = match &self.config.callers {
            Some(callers) => callers.clone(),
            None => default_callers(&flow),
        };
        registry.register(
            flow,
            Registration {
                tx: self.tx,
                task_name: self.config.name.clone(),
                task_id: self.task_id,
                task_type: self.task_type,
                leaf_count: self.task_context.leaf_count,
                ack_timeout: self.config.ack_timeout,
                callers,
            },
        );
        Ok(())
    }
}

/// Builder for the in-process endpoint.
#[derive(Debug, Default)]
pub struct ProcessorBuilder {
    config: Option<Arc<super::config::Endpoint>>,
    tx: Option<Sender<Event>>,
    task_id: usize,
    task_type: Option<&'static str>,
    task_context: Option<Arc<crate::task::context::TaskContext>>,
}

impl ProcessorBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn config(mut self, config: Arc<super::config::Endpoint>) -> Self {
        self.config = Some(config);
        self
    }

    pub fn sender(mut self, tx: Sender<Event>) -> Self {
        self.tx = Some(tx);
        self
    }

    pub fn task_id(mut self, task_id: usize) -> Self {
        self.task_id = task_id;
        self
    }

    pub fn task_type(mut self, task_type: &'static str) -> Self {
        self.task_type = Some(task_type);
        self
    }

    pub fn task_context(mut self, task_context: Arc<crate::task::context::TaskContext>) -> Self {
        self.task_context = Some(task_context);
        self
    }

    pub async fn build(self) -> Result<Processor, Error> {
        Ok(Processor {
            config: self
                .config
                .ok_or_else(|| Error::MissingBuilderAttribute("config".to_string()))?,
            tx: self
                .tx
                .ok_or_else(|| Error::MissingBuilderAttribute("sender".to_string()))?,
            task_id: self.task_id,
            task_type: self
                .task_type
                .ok_or_else(|| Error::MissingBuilderAttribute("task_type".to_string()))?,
            task_context: self
                .task_context
                .ok_or_else(|| Error::MissingBuilderAttribute("task_context".to_string()))?,
        })
    }
}

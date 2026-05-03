//! LLM proxy processor — registers a flow as an LLM proxy backend.
//!
//! At flow start the processor builds an `LlmProxyRegistration` and inserts
//! it into the shared `AiGatewayServer` dispatch table keyed by the proxy
//! `name`. The actual HTTP entry point, request parsing, protocol translation
//! and response streaming all live in `super::server`; this module only
//! handles the registration lifecycle.

use super::config;
use super::server::AiGatewayServer;
use flowgen_core::config::ConfigExt;
use flowgen_core::credentials::HttpCredentials;
use flowgen_core::event::Event;
use flowgen_core::registry::ResponseRegistry;
use std::{fs, sync::Arc};
use tokio::sync::mpsc;

/// Errors that can occur during AI gateway processor lifecycle.
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum Error {
    #[error("JSON error: {source}")]
    SerdeJson {
        #[source]
        source: serde_json::Error,
    },
    #[error("Error reading credentials file at {path}: {source}")]
    ReadCredentials {
        path: std::path::PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("Missing required builder attribute: {0}")]
    MissingBuilderAttribute(String),
    #[error("Config template rendering error: {source}")]
    ConfigRender {
        #[source]
        source: flowgen_core::config::Error,
    },
    #[error("Task failed after all retry attempts: {source}")]
    RetryExhausted {
        #[source]
        source: Box<Error>,
    },
}

/// AI gateway processor.
#[derive(Debug)]
pub struct Processor {
    /// Processor configuration.
    config: Arc<config::Processor>,
    /// Event sender channel into the pipeline.
    tx: Option<mpsc::Sender<Event>>,
    /// Task identifier.
    task_id: usize,
    /// Task execution context.
    task_context: Arc<flowgen_core::task::context::TaskContext>,
    /// Task type for logging.
    task_type: &'static str,
    /// Shared AI gateway server.
    ai_gateway_server: Arc<AiGatewayServer>,
}

#[async_trait::async_trait]
impl flowgen_core::task::runner::Runner for Processor {
    type Error = Error;
    type EventHandler = ();

    async fn init(&self) -> Result<(), Error> {
        Ok(())
    }

    #[tracing::instrument(skip(self), name = "task.run", fields(task = %self.config.name, task_id = self.task_id, task_type = %self.task_type))]
    async fn run(self) -> Result<(), Error> {
        let init_config = self
            .config
            .render(&serde_json::json!({}))
            .map_err(|source| Error::ConfigRender { source })?;

        let credentials = match &init_config.credentials_path {
            Some(path) => {
                let content = fs::read_to_string(path).map_err(|e| Error::ReadCredentials {
                    path: path.clone(),
                    source: e,
                })?;
                Some(
                    serde_json::from_str::<HttpCredentials>(&content)
                        .map_err(|source| Error::SerdeJson { source })?,
                )
            }
            None => None,
        };

        let auth_provider = self.ai_gateway_server.auth_provider();

        let response_registry = self
            .task_context
            .response_registry
            .clone()
            .unwrap_or_else(|| Arc::new(ResponseRegistry::new()));

        let tx = self
            .tx
            .clone()
            .ok_or_else(|| Error::MissingBuilderAttribute("sender".to_string()))?;

        let registration = super::server::LlmProxyRegistration {
            flow_name: self.task_context.flow.name.clone(),
            protocol: self.config.protocol,
            config: Arc::clone(&self.config),
            credentials,
            auth_provider,
            tx,
            task_id: self.task_id,
            task_type: self.task_type,
            response_registry,
            leaf_count: self.task_context.leaf_count,
            cancellation_token: self.task_context.cancellation_token.clone(),
        };

        self.ai_gateway_server
            .register(self.config.name.clone(), registration);

        Ok(())
    }
}

/// Builder for AI gateway processor.
#[derive(Debug, Default)]
pub struct ProcessorBuilder {
    config: Option<Arc<config::Processor>>,
    tx: Option<mpsc::Sender<Event>>,
    task_id: usize,
    task_context: Option<Arc<flowgen_core::task::context::TaskContext>>,
    task_type: Option<&'static str>,
    ai_gateway_server: Option<Arc<AiGatewayServer>>,
}

impl ProcessorBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn config(mut self, config: Arc<config::Processor>) -> Self {
        self.config = Some(config);
        self
    }

    pub fn sender(mut self, sender: mpsc::Sender<Event>) -> Self {
        self.tx = Some(sender);
        self
    }

    pub fn task_id(mut self, task_id: usize) -> Self {
        self.task_id = task_id;
        self
    }

    pub fn task_context(
        mut self,
        task_context: Arc<flowgen_core::task::context::TaskContext>,
    ) -> Self {
        self.task_context = Some(task_context);
        self
    }

    pub fn task_type(mut self, task_type: &'static str) -> Self {
        self.task_type = Some(task_type);
        self
    }

    /// Sets the shared AI gateway server.
    pub fn ai_gateway_server(mut self, server: Arc<AiGatewayServer>) -> Self {
        self.ai_gateway_server = Some(server);
        self
    }

    pub async fn build(self) -> Result<Processor, Error> {
        Ok(Processor {
            config: self
                .config
                .ok_or_else(|| Error::MissingBuilderAttribute("config".to_string()))?,
            tx: self.tx,
            task_id: self.task_id,
            task_context: self
                .task_context
                .ok_or_else(|| Error::MissingBuilderAttribute("task_context".to_string()))?,
            task_type: self
                .task_type
                .ok_or_else(|| Error::MissingBuilderAttribute("task_type".to_string()))?,
            ai_gateway_server: self
                .ai_gateway_server
                .ok_or_else(|| Error::MissingBuilderAttribute("ai_gateway_server".to_string()))?,
        })
    }
}

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

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> Arc<config::Processor> {
        Arc::new(config::Processor {
            name: "test_proxy".to_string(),
            protocol: config::Protocol::Openai,
            credentials_path: None,
            auth: None,
            ack_timeout: None,
            depends_on: None,
            retry: None,
        })
    }

    fn test_task_context() -> Arc<flowgen_core::task::context::TaskContext> {
        Arc::new(flowgen_core::task::context::TaskContext {
            flow: flowgen_core::task::context::FlowOptions {
                name: "test_flow".to_string(),
                labels: None,
            },
            task_manager: Arc::new(
                flowgen_core::task::manager::TaskManagerBuilder::new()
                    .build()
                    .unwrap(),
            ),
            cache: Arc::new(flowgen_core::cache::memory::MemoryCache::new()),
            response_registry: None,
            resource_loader: None,
            retry: None,
            cancellation_token: tokio_util::sync::CancellationToken::new(),
            leaf_count: 1,
            startup_delay: None,
            client_registry: Arc::new(flowgen_core::client_registry::ClientRegistry::new()),
        })
    }

    fn test_ai_gateway_server() -> Arc<AiGatewayServer> {
        Arc::new(AiGatewayServer::new("/v1".to_string()))
    }

    #[test]
    fn builder_new_starts_with_all_none() {
        let builder = ProcessorBuilder::new();
        assert!(builder.config.is_none());
        assert!(builder.tx.is_none());
        assert_eq!(builder.task_id, 0);
        assert!(builder.task_context.is_none());
        assert!(builder.task_type.is_none());
        assert!(builder.ai_gateway_server.is_none());
    }

    #[test]
    fn builder_methods_set_fields() {
        let (tx, _rx) = mpsc::channel(1);
        let builder = ProcessorBuilder::new()
            .config(test_config())
            .sender(tx)
            .task_id(42)
            .task_context(test_task_context())
            .task_type("llm_proxy")
            .ai_gateway_server(test_ai_gateway_server());

        assert!(builder.config.is_some());
        assert!(builder.tx.is_some());
        assert_eq!(builder.task_id, 42);
        assert!(builder.task_context.is_some());
        assert_eq!(builder.task_type, Some("llm_proxy"));
        assert!(builder.ai_gateway_server.is_some());
    }

    #[tokio::test]
    async fn build_fails_with_missing_config() {
        let result = ProcessorBuilder::new()
            .task_context(test_task_context())
            .task_type("llm_proxy")
            .ai_gateway_server(test_ai_gateway_server())
            .build()
            .await;
        let err = result.unwrap_err();
        assert!(err.to_string().contains("config"));
    }

    #[tokio::test]
    async fn build_fails_with_missing_task_context() {
        let result = ProcessorBuilder::new()
            .config(test_config())
            .task_type("llm_proxy")
            .ai_gateway_server(test_ai_gateway_server())
            .build()
            .await;
        let err = result.unwrap_err();
        assert!(err.to_string().contains("task_context"));
    }

    #[tokio::test]
    async fn build_fails_with_missing_task_type() {
        let result = ProcessorBuilder::new()
            .config(test_config())
            .task_context(test_task_context())
            .ai_gateway_server(test_ai_gateway_server())
            .build()
            .await;
        let err = result.unwrap_err();
        assert!(err.to_string().contains("task_type"));
    }

    #[tokio::test]
    async fn build_fails_with_missing_ai_gateway_server() {
        let result = ProcessorBuilder::new()
            .config(test_config())
            .task_context(test_task_context())
            .task_type("llm_proxy")
            .build()
            .await;
        let err = result.unwrap_err();
        assert!(err.to_string().contains("ai_gateway_server"));
    }

    #[tokio::test]
    async fn build_succeeds_with_all_required_fields() {
        let result = ProcessorBuilder::new()
            .config(test_config())
            .task_context(test_task_context())
            .task_type("llm_proxy")
            .ai_gateway_server(test_ai_gateway_server())
            .build()
            .await;
        assert!(result.is_ok());
    }
}

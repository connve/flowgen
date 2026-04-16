//! MCP tool task processor.
//!
//! Registers a flow as an MCP tool with the MCP server during setup (blocking task).
//! The MCP server injects events into the tool's sender channel when `tools/call`
//! requests arrive. The processor does not handle events directly — downstream
//! tasks in the pipeline process the injected events.

use flowgen_core::{
    config::ConfigExt,
    event::Event,
    task::{context::TaskContext, runner::Runner},
};
use std::{fs, sync::Arc};
use tokio::sync::mpsc::Sender;
use tracing::{error, info};

/// Errors that can occur during MCP tool processor operations.
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum Error {
    #[error("Error sending event to channel: {source}")]
    SendMessage {
        #[source]
        source: flowgen_core::event::Error,
    },
    #[error("Failed to render configuration template: {source}")]
    ConfigRender {
        #[source]
        source: flowgen_core::config::Error,
    },
    #[error("Failed to read credentials from {path}: {source}")]
    ReadCredentials {
        path: std::path::PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("JSON deserialization error: {source}")]
    SerdeJson {
        #[source]
        source: serde_json::Error,
    },
    #[error("MCP server not configured in task context.")]
    McpServerNotConfigured,
    #[error("Failed to downcast MCP server to concrete type.")]
    McpServerDowncast,
    #[error("Failed to register MCP tool: {source}")]
    ToolRegistration {
        #[source]
        source: super::server::Error,
    },
    #[error("Missing required builder attribute: {}", _0)]
    MissingBuilderAttribute(String),
    #[error("Task failed after all retry attempts: {source}")]
    RetryExhausted {
        #[source]
        source: Box<Error>,
    },
}

/// Event handler for the MCP tool task.
///
/// Constructed during `init()` and used in `run()` to register the tool
/// with the MCP server. Contains the rendered configuration and pipeline
/// channel needed for tool registration. The handler does not process
/// incoming events directly; the MCP server injects events into the
/// pipeline channel.
#[derive(Clone, Debug)]
pub struct EventHandler {
    /// Rendered processor configuration.
    config: Arc<super::config::Processor>,
    /// Event sender channel for the downstream pipeline.
    tx: Option<Sender<Event>>,
}

/// MCP tool processor that registers a tool with the MCP server.
#[derive(Debug)]
pub struct Processor {
    /// Processor configuration.
    config: Arc<super::config::Processor>,
    /// Event sender channel for the downstream pipeline.
    tx: Option<Sender<Event>>,
    /// Task identifier.
    task_id: usize,
    /// Task execution context providing shared resources.
    task_context: Arc<TaskContext>,
    /// Task type identifier for logging and categorization.
    task_type: &'static str,
}

#[async_trait::async_trait]
impl Runner for Processor {
    type Error = Error;
    type EventHandler = EventHandler;

    async fn init(&self) -> Result<EventHandler, Error> {
        let init_config = self
            .config
            .render(&serde_json::json!({}))
            .map_err(|source| Error::ConfigRender { source })?;

        Ok(EventHandler {
            config: Arc::new(init_config),
            tx: self.tx.clone(),
        })
    }

    #[tracing::instrument(skip(self), name = "task.run", fields(task = %self.config.name, task_id = self.task_id, task_type = %self.task_type))]
    async fn run(self) -> Result<(), Error> {
        let retry_config =
            flowgen_core::retry::RetryConfig::merge(&self.task_context.retry, &self.config.retry);

        let event_handler = match tokio_retry::Retry::spawn(retry_config.strategy(), || async {
            match self.init().await {
                Ok(handler) => Ok(handler),
                Err(e) => {
                    error!(error = %e, "Failed to initialize MCP tool processor.");
                    Err(tokio_retry::RetryError::transient(e))
                }
            }
        })
        .await
        {
            Ok(handler) => handler,
            Err(e) => return Err(e),
        };

        // Load tool-specific credentials if a path is configured.
        let credentials = match &event_handler.config.credentials_path {
            Some(path) => {
                let content = fs::read_to_string(path).map_err(|e| Error::ReadCredentials {
                    path: path.clone(),
                    source: e,
                })?;
                Some(
                    serde_json::from_str::<super::config::Credentials>(&content)
                        .map_err(|e| Error::SerdeJson { source: e })?,
                )
            }
            None => None,
        };

        // Retrieve the MCP server from the task context.
        let mcp_server = self
            .task_context
            .mcp_server
            .as_ref()
            .ok_or(Error::McpServerNotConfigured)?;

        let server = mcp_server
            .as_any()
            .downcast_ref::<super::server::McpServer>()
            .ok_or(Error::McpServerDowncast)?;

        // Use the tool name directly. Collisions are detected at registration time.
        let tool_name = event_handler.config.name.clone();

        // The sender channel connects the MCP server to the downstream pipeline.
        let tool_tx = event_handler
            .tx
            .clone()
            .ok_or_else(|| {
                Error::MissingBuilderAttribute(
                    "mcp_tool must have an output channel (cannot be the last task in a flow)."
                        .to_string(),
                )
            })?;

        server
            .register_tool(
                tool_name.clone(),
                super::server::ToolRegistration {
                    description: event_handler.config.description.clone(),
                    input_schema: event_handler.config.input_schema.clone(),
                    tx: tool_tx,
                    credentials,
                    ack_timeout: event_handler.config.ack_timeout,
                },
            )
            .map_err(|source| Error::ToolRegistration { source })?;

        info!("MCP tool registered: {}", tool_name);

        Ok(())
    }
}

/// Builder for constructing MCP tool processor instances.
#[derive(Debug, Default)]
pub struct ProcessorBuilder {
    /// Optional processor configuration.
    config: Option<Arc<super::config::Processor>>,
    /// Optional event sender channel.
    tx: Option<Sender<Event>>,
    /// Task identifier.
    task_id: usize,
    /// Optional task execution context.
    task_context: Option<Arc<TaskContext>>,
    /// Optional task type identifier.
    task_type: Option<&'static str>,
}

impl ProcessorBuilder {
    /// Creates a new processor builder with default values.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the processor configuration.
    pub fn config(mut self, config: Arc<super::config::Processor>) -> Self {
        self.config = Some(config);
        self
    }

    /// Sets the event sender channel for the downstream pipeline.
    pub fn sender(mut self, sender: Sender<Event>) -> Self {
        self.tx = Some(sender);
        self
    }

    /// Sets the task identifier.
    pub fn task_id(mut self, task_id: usize) -> Self {
        self.task_id = task_id;
        self
    }

    /// Sets the task execution context.
    pub fn task_context(mut self, task_context: Arc<TaskContext>) -> Self {
        self.task_context = Some(task_context);
        self
    }

    /// Sets the task type identifier for logging.
    pub fn task_type(mut self, task_type: &'static str) -> Self {
        self.task_type = Some(task_type);
        self
    }

    /// Builds the processor, validating all required attributes.
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
        })
    }
}

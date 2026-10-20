//! The `mcp_tool` task: exposes a flow as an MCP tool callable by LLMs.
//!
//! Registers the tool with the MCP server during setup (blocking task).
//! The MCP server injects events into the tool's sender channel when `tools/call`
//! requests arrive. The processor does not handle events directly — downstream
//! tasks in the pipeline process the injected events.

use flowgen_core::{
    config::ConfigExt,
    event::Event,
    task::{context::TaskContext, runner::Runner, TaskConfig},
};
use serde::{Deserialize, Serialize};
use std::{fs, path::PathBuf, sync::Arc};
use tokio::sync::mpsc::Sender;
use tracing::{error, info};

/// MCP tool task configuration.
///
/// # Example YAML
/// ```yaml
/// - mcp_tool:
///     name: send_welcome
///     description: "Send personalized welcome campaign with CRM sync"
///     credentials_path: /etc/mcp/api-keys.json  # optional, overrides global
///     input_schema:
///       type: object
///       properties:
///         user_id:
///           type: string
///           description: "User ID to send welcome to"
///       required: [user_id]
/// ```
#[derive(PartialEq, Clone, Debug, Default, Deserialize, Serialize, TaskConfig)]
#[serde(deny_unknown_fields)]
pub struct Tool {
    /// Unique tool name. Combined with flow name for the full tool identifier:
    /// `{flow_name}.{name}` (e.g., `crm_welcome_flow.send_welcome`).
    pub name: String,
    /// Human-readable description shown to LLMs for tool selection.
    pub description: String,
    /// JSON Schema describing the tool's input parameters.
    /// Follows the standard JSON Schema format used by MCP.
    #[serde(default)]
    pub input_schema: serde_json::Value,
    /// Optional path to credentials file for this specific tool.
    /// Overrides the global `worker.mcp_server.credentials_path` if set.
    pub credentials_path: Option<PathBuf>,
    /// HTTP headers a request must carry, with matching values, for this
    /// tool to be visible on `tools/list` and callable at all. All declared
    /// headers must match (AND). An empty map (the default) means every
    /// caller can see and call it. A caller missing a required header gets
    /// the same "unknown tool" error as a nonexistent tool name.
    #[serde(default)]
    pub headers: std::collections::HashMap<String, String>,
    /// Timeout for waiting on flow completion before responding to the MCP client.
    /// If not specified, waits indefinitely for flow completion.
    /// Accepts human-readable durations (e.g., "30s", "5m", "1h").
    #[serde(default, with = "humantime_serde")]
    pub ack_timeout: Option<std::time::Duration>,
    /// Optional user authentication configuration.
    /// When `auth.required` is true, requests must include a valid bearer token
    /// validated by the worker-level auth provider (JWT, OIDC, or session).
    /// The resolved user context is injected into `event.meta.auth`.
    #[serde(default)]
    pub auth: Option<flowgen_core::auth::TaskAuthConfig>,
    /// Optional list of upstream task names this task depends on.
    /// When set, this task only receives events from the named tasks.
    /// When not set, the task receives from the previous task in the list (linear chain).
    #[serde(default)]
    pub depends_on: Option<Vec<String>>,
    /// Optional retry configuration (overrides app-level retry config).
    #[serde(default)]
    pub retry: Option<flowgen_core::retry::RetryConfig>,
}

impl ConfigExt for Tool {}

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
    #[error("Invalid input_schema at '{location}': {source}")]
    InputSchema {
        location: String,
        #[source]
        source: Box<jsonschema::ValidationError<'static>>,
    },
    #[error("Failed to register MCP tool: {source}")]
    ToolRegistration {
        #[source]
        source: crate::server::Error,
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
    config: Arc<Tool>,
    /// Event sender channel for the downstream pipeline.
    tx: Option<Sender<Event>>,
}

/// MCP tool processor that registers a tool with the MCP server.
#[derive(Debug)]
pub struct Processor {
    /// Processor configuration.
    config: Arc<Tool>,
    /// Event sender channel for the downstream pipeline.
    tx: Option<Sender<Event>>,
    /// Task identifier.
    task_id: usize,
    /// Task execution context providing shared resources.
    task_context: Arc<TaskContext>,
    /// Task type identifier for logging and categorization.
    task_type: &'static str,
    /// Shared MCP server. The processor inserts a `ToolRegistration` into
    /// this server during `run()`; the dispatcher routes inbound JSON-RPC
    /// `tools/call` requests for this tool name to it.
    mcp_server: Arc<crate::server::McpServer>,
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

        let event_handler = match tokio_retry::Retry::spawn(
            retry_config.init_strategy(self.task_context.startup_delay),
            || async {
                match self.init().await {
                    Ok(handler) => Ok(handler),
                    Err(e) => {
                        error!(error = %e, "Failed to initialize MCP tool processor");
                        Err(tokio_retry::RetryError::transient(e))
                    }
                }
            },
        )
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
                    serde_json::from_str::<crate::config::Credentials>(&content)
                        .map_err(|e| Error::SerdeJson { source: e })?,
                )
            }
            None => None,
        };

        // Use the tool name directly. Collisions are detected at registration time.
        let tool_name = event_handler.config.name.clone();

        // The sender channel connects the MCP server to the downstream pipeline.
        let tool_tx = event_handler.tx.clone().ok_or_else(|| {
            Error::MissingBuilderAttribute(
                "mcp_tool must have an output channel (cannot be the last task in a flow)."
                    .to_string(),
            )
        })?;

        let input_validator = match &event_handler.config.input_schema {
            serde_json::Value::Null => None,
            schema => Some(Arc::new(jsonschema::validator_for(schema).map_err(
                |source| Error::InputSchema {
                    location: source.instance_path().to_string(),
                    source: Box::new(source),
                },
            )?)),
        };

        crate::server::register_tool(
            self.mcp_server.as_ref(),
            tool_name.clone(),
            crate::server::ToolRegistration {
                flow_name: self.task_context.flow.identity().to_string(),
                description: event_handler.config.description.clone(),
                input_schema: event_handler.config.input_schema.clone(),
                input_validator,
                tx: tool_tx,
                credentials,
                ack_timeout: event_handler.config.ack_timeout,
                auth_required: event_handler
                    .config
                    .auth
                    .as_ref()
                    .map(|a| a.required)
                    .unwrap_or(false),
                leaf_count: self.task_context.leaf_count,
                headers: event_handler.config.headers.clone(),
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
    config: Option<Arc<Tool>>,
    /// Optional event sender channel.
    tx: Option<Sender<Event>>,
    /// Task identifier.
    task_id: usize,
    /// Optional task execution context.
    task_context: Option<Arc<TaskContext>>,
    /// Optional task type identifier.
    task_type: Option<&'static str>,
    /// Optional shared MCP server.
    mcp_server: Option<Arc<crate::server::McpServer>>,
}

impl ProcessorBuilder {
    /// Creates a new processor builder with default values.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the processor configuration.
    pub fn config(mut self, config: Arc<Tool>) -> Self {
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

    /// Sets the shared MCP server.
    pub fn mcp_server(mut self, server: Arc<crate::server::McpServer>) -> Self {
        self.mcp_server = Some(server);
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
            mcp_server: self
                .mcp_server
                .ok_or_else(|| Error::MissingBuilderAttribute("mcp_server".to_string()))?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_config_default() {
        let config = Tool::default();
        assert_eq!(config.name, String::new());
        assert_eq!(config.description, String::new());
        assert_eq!(config.input_schema, serde_json::Value::default());
        assert!(config.credentials_path.is_none());
        assert!(config.retry.is_none());
    }

    #[test]
    fn test_config_serialization() {
        let config = Tool {
            name: "send_welcome".to_string(),
            description: "Send welcome campaign".to_string(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "user_id": { "type": "string" }
                },
                "required": ["user_id"]
            }),
            credentials_path: Some(std::path::PathBuf::from("/etc/mcp/keys.json")),
            headers: std::collections::HashMap::new(),
            ack_timeout: None,
            auth: None,
            depends_on: None,
            retry: None,
        };

        let json = serde_json::to_string(&config).unwrap();
        let deserialized: Tool = serde_json::from_str(&json).unwrap();
        assert_eq!(config, deserialized);
    }
}

//! AI completion processor for generating text responses using LLMs.
//!
//! Provides both standard and streaming completion modes with support for
//! multiple AI providers through the Rig framework.

use flowgen_core::event::{Event, EventBuilder, EventData, EventExt, SharedCompletionTx};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::mpsc::{Receiver, Sender};
use tracing::{error, Instrument};

/// Errors that can occur during completion processing.
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum Error {
    #[error("Error sending event to channel: {source}")]
    SendMessage {
        #[source]
        source: flowgen_core::event::Error,
    },
    #[error("Error building event: {source}")]
    EventBuilder {
        #[source]
        source: flowgen_core::event::Error,
    },
    #[error("JSON serialization error: {source}")]
    SerdeJson {
        #[source]
        source: serde_json::Error,
    },
    #[error("Config template rendering error: {source}")]
    ConfigRender {
        #[source]
        source: flowgen_core::config::Error,
    },
    #[error("Missing required builder attribute: {}", _0)]
    MissingBuilderAttribute(String),
    #[error("Task failed after all retry attempts: {source}")]
    RetryExhausted {
        #[source]
        source: Box<Error>,
    },
    #[error("Missing credentials path for provider: {provider}")]
    MissingCredentialsPath { provider: String },
    #[error("Failed to read credentials file at {path:?}: {source}")]
    ReadCredentials {
        path: std::path::PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("Failed to parse credentials file at {path:?}: {source}")]
    ParseCredentials {
        path: std::path::PathBuf,
        #[source]
        source: serde_json::Error,
    },
    #[error("Rig client error: {}", _0)]
    RigClient(String),
    #[error("Completion generation failed: {}", _0)]
    CompletionFailed(String),
    #[error("Agent client creation failed: {source}")]
    AgentClient {
        #[source]
        source: crate::agent::Error,
    },
    #[error("Resource loading failed: {source}")]
    ResourceLoad {
        #[source]
        source: flowgen_core::resource::Error,
    },
}

/// Response structure for completion events.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompletionResponse {
    /// Generated text response.
    pub text: String,
    /// Model used for generation.
    pub model: String,
    /// Provider that generated the response.
    pub provider: String,
    /// Optional usage statistics.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub usage: Option<CompletionUsage>,
}

/// Streaming chunk for completion events.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompletionChunk {
    /// Text content of this chunk.
    pub text: String,
    /// Whether this is the final chunk.
    pub is_final: bool,
    /// Index of this chunk in the sequence.
    pub index: usize,
}

/// Token usage statistics for completions.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompletionUsage {
    /// Number of tokens in the prompt.
    pub prompt_tokens: usize,
    /// Number of tokens in the completion.
    pub completion_tokens: usize,
    /// Total tokens used.
    pub total_tokens: usize,
}

/// API credentials for AI providers.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
pub struct Credentials {
    /// API key for the provider (required for most providers).
    pub api_key: String,
    /// Optional organization ID (OpenAI, Anthropic).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub organization_id: Option<String>,
    /// Optional project ID (Google/Gemini).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub project_id: Option<String>,
    /// Optional region (AWS Bedrock).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub region: Option<String>,
}

/// Loads credentials from a JSON file.
async fn load_credentials(path: &std::path::Path) -> Result<Credentials, Error> {
    let content = tokio::fs::read_to_string(path)
        .await
        .map_err(|e| Error::ReadCredentials {
            path: path.to_path_buf(),
            source: e,
        })?;

    serde_json::from_str::<Credentials>(&content).map_err(|e| Error::ParseCredentials {
        path: path.to_path_buf(),
        source: e,
    })
}

/// Handles individual completion operations.
pub struct EventHandler {
    /// Processor configuration settings.
    config: Arc<super::config::Processor>,
    /// Current task identifier for event filtering.
    task_id: usize,
    /// Event sender for passing completion responses.
    tx: Option<Sender<Event>>,
    /// Task type identifier.
    task_type: &'static str,
    /// Task execution context providing metadata and runtime configuration.
    task_context: Arc<flowgen_core::task::context::TaskContext>,
    /// AI agent client for generating completions.
    client: crate::agent::AgentClient,
}

impl EventHandler {
    /// Processes an event by generating a completion.
    async fn handle(&self, event: Event) -> Result<(), Error> {
        if self.task_context.cancellation_token.is_cancelled() {
            return Ok(());
        }

        let event = Arc::new(event);
        let completion_tx_arc = Arc::clone(&event).completion_tx.clone();

        flowgen_core::event::with_event_context(&Arc::clone(&event), async move {
            // Render config with event data for template substitution.
            let event_value = serde_json::value::Value::try_from(event.as_ref())
                .map_err(|source| Error::EventBuilder { source })?;

            // Render prompt from Source (inline or external file).
            let rendered_prompt = self
                .config
                .prompt
                .render(self.task_context.resource_loader.as_ref(), &event_value)
                .await
                .map_err(|source| Error::ResourceLoad { source })?;

            // Render optional system prompt from Source.
            let rendered_system_prompt = if let Some(ref system_prompt) = self.config.system_prompt
            {
                Some(
                    system_prompt
                        .render(self.task_context.resource_loader.as_ref(), &event_value)
                        .await
                        .map_err(|source| Error::ResourceLoad { source })?,
                )
            } else {
                None
            };

            // Generate completion using the Rig agent client.
            let completion_text = self
                .client
                .complete(&rendered_prompt, rendered_system_prompt.as_deref())
                .await
                .map_err(|source| Error::AgentClient { source })?;

            // Build response with completion result.
            // Note: Usage statistics are not available from Rig's non-streaming prompt() method.
            let response = CompletionResponse {
                text: completion_text,
                model: self.config.model.clone(),
                provider: format!("{:?}", self.config.provider),
                usage: None,
            };

            // Send the completion response as an event.
            self.send_response(response, completion_tx_arc).await
        })
        .await
    }

    /// Sends a completion response as an event.
    async fn send_response(
        &self,
        response: CompletionResponse,
        completion_tx_arc: Option<SharedCompletionTx>,
    ) -> Result<(), Error> {
        let response_value =
            serde_json::to_value(&response).map_err(|source| Error::SerdeJson { source })?;

        let mut event = EventBuilder::new()
            .data(EventData::Json(response_value))
            .task_id(self.task_id)
            .task_type(self.task_type)
            .build()
            .map_err(|source| Error::EventBuilder { source })?;

        // Signal completion or pass through to next task.
        match self.tx {
            None => {
                // Final task, signal completion.
                if let Some(arc) = completion_tx_arc.as_ref() {
                    if let Ok(mut guard) = arc.lock() {
                        if let Some(tx) = guard.take() {
                            tx.send(Ok(())).ok();
                        }
                    }
                }
            }
            Some(_) => {
                // Pass through completion_tx to next task.
                event.completion_tx = completion_tx_arc.clone();
            }
        }

        event
            .send_with_logging(self.tx.as_ref())
            .await
            .map_err(|source| Error::SendMessage { source })?;

        Ok(())
    }
}

/// AI completion processor.
#[derive(Debug)]
pub struct Processor {
    /// Completion task configuration.
    config: Arc<super::config::Processor>,
    /// Channel sender for passing completion events.
    tx: Option<Sender<Event>>,
    /// Channel receiver for incoming events.
    rx: Receiver<Event>,
    /// Current task identifier for event filtering.
    task_id: usize,
    /// Task execution context providing metadata and runtime configuration.
    task_context: Arc<flowgen_core::task::context::TaskContext>,
    /// Task type for event categorization and logging.
    task_type: &'static str,
}

#[async_trait::async_trait]
impl flowgen_core::task::runner::Runner for Processor {
    type Error = Error;
    type EventHandler = EventHandler;

    /// Initializes the processor by loading credentials and setting up the client.
    async fn init(&self) -> Result<Self::EventHandler, Self::Error> {
        // Load credentials based on provider requirements.
        let credentials = match self.config.provider {
            super::config::Provider::Custom => {
                // Custom providers are optional.
                if let Some(path) = &self.config.credentials_path {
                    Some(load_credentials(path).await?)
                } else {
                    None // Local providers like Ollama may not need credentials.
                }
            }
            _ => {
                // OpenAI, Anthropic, Google, Cohere all require credentials.
                let path = self.config.credentials_path.as_ref().ok_or_else(|| {
                    Error::MissingCredentialsPath {
                        provider: format!("{:?}", self.config.provider),
                    }
                })?;

                Some(load_credentials(path).await?)
            }
        };

        // Build the agent client with provider, model, and optional parameters.
        let mut builder = crate::agent::ClientBuilder::new(
            self.config.provider.clone(),
            self.config.model.clone(),
        );

        if let Some(creds) = credentials {
            builder = builder.credentials(creds);
        }

        if let Some(endpoint) = &self.config.endpoint {
            builder = builder.endpoint(endpoint.clone());
        }

        if let Some(temp) = self.config.temperature {
            builder = builder.temperature(temp);
        }

        if let Some(tokens) = self.config.max_tokens {
            builder = builder.max_tokens(tokens);
        }

        // Load static context documents for RAG (without event data - truly static).
        if let Some(ref contexts) = self.config.static_context {
            for (idx, context_source) in contexts.iter().enumerate() {
                let resolved_text = context_source
                    .resolve(self.task_context.resource_loader.as_ref())
                    .await
                    .map_err(|source| Error::ResourceLoad { source })?;

                builder = builder.static_context_document(rig::completion::Document {
                    id: format!("context_{idx}"),
                    text: resolved_text,
                    additional_props: std::collections::HashMap::new(),
                });
            }
        }

        // Set maximum agent turns for multi-turn conversations.
        if let Some(max_turns) = self.config.max_turns {
            builder = builder.max_turns(max_turns);
        }

        let client = builder
            .build()
            .map_err(|source| Error::AgentClient { source })?;

        let event_handler = EventHandler {
            config: Arc::clone(&self.config),
            task_id: self.task_id,
            tx: self.tx.clone(),
            task_type: self.task_type,
            task_context: Arc::clone(&self.task_context),
            client,
        };

        Ok(event_handler)
    }

    #[tracing::instrument(skip(self), name = "task.run", fields(flow = %self.task_context.flow.name, task = %self.config.name, task_id = self.task_id, task_type = %self.task_type))]
    async fn run(mut self) -> Result<(), Error> {
        let retry_config =
            flowgen_core::retry::RetryConfig::merge(&self.task_context.retry, &self.config.retry);

        let event_handler = match tokio_retry::Retry::spawn(retry_config.strategy(), || async {
            match self.init().await {
                Ok(handler) => Ok(handler),
                Err(e) => {
                    error!(error = %e, "Failed to initialize completion processor");
                    Err(tokio_retry::RetryError::transient(e))
                }
            }
        })
        .await
        {
            Ok(handler) => Arc::new(handler),
            Err(e) => {
                return Err(e);
            }
        };

        loop {
            match self.rx.recv().await {
                Some(event) => {
                    let event_handler = Arc::clone(&event_handler);
                    let retry_strategy = retry_config.strategy();
                    tokio::spawn(
                        async move {
                            let result = tokio_retry::Retry::spawn(retry_strategy, || async {
                                match event_handler.handle(event.clone()).await {
                                    Ok(result) => Ok(result),
                                    Err(e) => {
                                        error!(error = %e, "Failed to generate completion");
                                        Err(tokio_retry::RetryError::transient(e))
                                    }
                                }
                            })
                            .await;

                            if let Err(err) = result {
                                error!(error = %err, "Completion failed after all retry attempts");
                            }
                        }
                        .instrument(tracing::Span::current()),
                    );
                }
                None => return Ok(()),
            }
        }
    }
}

/// Builder for constructing Processor instances with validation.
#[derive(Debug, Default)]
pub struct ProcessorBuilder {
    /// Processor configuration (required for build).
    config: Option<Arc<super::config::Processor>>,
    /// Event sender for passing events to next task (optional if this is the last task).
    tx: Option<Sender<Event>>,
    /// Event receiver for incoming events (required for build).
    rx: Option<Receiver<Event>>,
    /// Current task identifier for event filtering.
    task_id: usize,
    /// Task execution context providing metadata and runtime configuration.
    task_context: Option<Arc<flowgen_core::task::context::TaskContext>>,
    /// Task type for event categorization and logging.
    task_type: Option<&'static str>,
}

impl ProcessorBuilder {
    pub fn new() -> ProcessorBuilder {
        ProcessorBuilder {
            ..Default::default()
        }
    }

    pub fn config(mut self, config: Arc<super::config::Processor>) -> Self {
        self.config = Some(config);
        self
    }

    pub fn receiver(mut self, receiver: Receiver<Event>) -> Self {
        self.rx = Some(receiver);
        self
    }

    pub fn sender(mut self, sender: Sender<Event>) -> Self {
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

    pub async fn build(self) -> Result<Processor, Error> {
        Ok(Processor {
            config: self
                .config
                .ok_or_else(|| Error::MissingBuilderAttribute("config".to_string()))?,
            rx: self
                .rx
                .ok_or_else(|| Error::MissingBuilderAttribute("receiver".to_string()))?,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_completion_response_serialization() {
        let response = CompletionResponse {
            text: "Hello, world!".to_string(),
            model: "gpt-4".to_string(),
            provider: "OpenAi".to_string(),
            usage: Some(CompletionUsage {
                prompt_tokens: 10,
                completion_tokens: 5,
                total_tokens: 15,
            }),
        };

        let json = serde_json::to_string(&response).unwrap();
        let deserialized: CompletionResponse = serde_json::from_str(&json).unwrap();
        assert_eq!(response.text, deserialized.text);
        assert_eq!(response.model, deserialized.model);
    }

    #[test]
    fn test_completion_chunk_serialization() {
        let chunk = CompletionChunk {
            text: "Hello".to_string(),
            is_final: false,
            index: 0,
        };

        let json = serde_json::to_string(&chunk).unwrap();
        let deserialized: CompletionChunk = serde_json::from_str(&json).unwrap();
        assert_eq!(chunk.text, deserialized.text);
        assert_eq!(chunk.is_final, deserialized.is_final);
        assert_eq!(chunk.index, deserialized.index);
    }

    #[test]
    fn test_credentials_serialization() {
        let creds = Credentials {
            api_key: "sk-test123".to_string(),
            organization_id: Some("org-123".to_string()),
            project_id: None,
            region: None,
        };

        let json = serde_json::to_string(&creds).unwrap();
        let deserialized: Credentials = serde_json::from_str(&json).unwrap();
        assert_eq!(creds, deserialized);
    }
}

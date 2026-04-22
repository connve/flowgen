//! LLM proxy configuration and OpenAI-compatible types.
//!
//! Provides task configuration for the LLM proxy endpoint and the OpenAI
//! request/response types used for protocol translation.

use flowgen_core::config::ConfigExt;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

// --- OpenAI protocol constants ---

/// OpenAI object type for non-streaming responses.
pub const OBJECT_CHAT_COMPLETION: &str = "chat.completion";
/// OpenAI object type for streaming chunks.
pub const OBJECT_CHAT_COMPLETION_CHUNK: &str = "chat.completion.chunk";
/// Role for assistant messages.
pub const ROLE_ASSISTANT: &str = "assistant";
/// Role for system messages.
pub const ROLE_SYSTEM: &str = "system";
/// Finish reason when generation completes normally.
pub const FINISH_REASON_STOP: &str = "stop";
/// SSE terminator per OpenAI streaming specification.
pub const SSE_DONE: &str = "data: [DONE]\n\n";

// --- Task configuration ---

/// LLM proxy processor configuration.
///
/// Thin HTTP layer that accepts OpenAI-format chat completion requests,
/// translates them into pipeline events, and streams back OpenAI-format
/// responses. Registers at the exact path (bypasses worker path prefix)
/// so OpenAI clients can connect directly.
///
/// # Example
///
/// ```yaml
/// tasks:
///   - llm_proxy:
///       name: proxy
///       path: /v1/chat/completions
///       credentials_path: /etc/proxy/credentials.json
///
///   - ai_completion:
///       name: complete
///       provider: google_vertex
///       model: gemini-2.0-flash
///       stream: true
///       prompt: "{{event.data.prompt}}"
/// ```
#[derive(PartialEq, Clone, Debug, Deserialize, Serialize)]
pub struct Processor {
    /// The unique name / identifier of the task.
    pub name: String,
    /// HTTP path to register the endpoint on (e.g., "/v1/chat/completions").
    /// Registered at the root level, not under the worker path prefix.
    pub path: String,
    /// Optional path to credentials file for authenticating incoming requests.
    pub credentials_path: Option<PathBuf>,
    /// Optional user authentication configuration.
    /// When `auth.required` is true, requests must include a valid bearer token
    /// validated by the worker-level auth provider (JWT, OIDC, or session).
    #[serde(default)]
    pub auth: Option<flowgen_core::auth::TaskAuthConfig>,
    /// Timeout for waiting on pipeline completion before responding.
    #[serde(default, with = "humantime_serde")]
    pub ack_timeout: Option<std::time::Duration>,
    /// Optional list of upstream task names this task depends on.
    #[serde(default)]
    pub depends_on: Option<Vec<String>>,
    /// Optional retry configuration.
    #[serde(default)]
    pub retry: Option<flowgen_core::retry::RetryConfig>,
}

impl ConfigExt for Processor {}

// --- OpenAI request types ---

/// OpenAI chat completion request body.
#[derive(Debug, Clone, Deserialize)]
pub struct ChatCompletionRequest {
    /// Model identifier requested by the client.
    pub model: Option<String>,
    /// Conversation messages (system, user, assistant).
    pub messages: Vec<Message>,
    /// Sampling temperature (0.0-2.0).
    pub temperature: Option<f32>,
    /// Maximum tokens to generate.
    pub max_tokens: Option<u32>,
    /// Whether to stream the response as SSE chunks.
    #[serde(default)]
    pub stream: bool,
}

/// Chat message with role and content.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Message {
    /// Message role: "system", "user", or "assistant".
    pub role: String,
    /// Message text content.
    pub content: String,
}

impl Message {
    /// Create an assistant response message.
    pub fn assistant(content: String) -> Self {
        Self {
            role: ROLE_ASSISTANT.to_string(),
            content,
        }
    }

    /// Check whether this message has the system role.
    pub fn is_system(&self) -> bool {
        self.role == ROLE_SYSTEM
    }
}

// --- OpenAI response types (non-streaming) ---

/// OpenAI chat completion response.
#[derive(Debug, Clone, Serialize)]
pub struct ChatCompletionResponse {
    /// Unique response identifier.
    pub id: String,
    /// Object type (always "chat.completion").
    pub object: &'static str,
    /// Unix timestamp of response creation.
    pub created: i64,
    /// Model that produced the response.
    pub model: String,
    /// Response choices (typically one).
    pub choices: Vec<Choice>,
    /// Token usage statistics.
    pub usage: Option<Usage>,
}

impl ChatCompletionResponse {
    /// Create a response with a single assistant message.
    pub fn new(id: String, created: i64, model: String, content: String) -> Self {
        Self {
            id,
            object: OBJECT_CHAT_COMPLETION,
            created,
            model,
            choices: vec![Choice {
                index: 0,
                message: Message::assistant(content),
                finish_reason: FINISH_REASON_STOP,
            }],
            usage: None,
        }
    }
}

/// Non-streaming response choice.
#[derive(Debug, Clone, Serialize)]
pub struct Choice {
    /// Choice index (0-based).
    pub index: u32,
    /// Assistant message.
    pub message: Message,
    /// Reason the model stopped generating.
    pub finish_reason: &'static str,
}

/// Token usage statistics.
#[derive(Debug, Clone, Serialize)]
pub struct Usage {
    /// Tokens consumed by the prompt.
    pub prompt_tokens: u32,
    /// Tokens generated in the completion.
    pub completion_tokens: u32,
    /// Total tokens used.
    pub total_tokens: u32,
}

// --- OpenAI streaming types ---

/// OpenAI streaming chunk sent as an SSE event.
#[derive(Debug, Clone, Serialize)]
pub struct ChatCompletionChunk {
    /// Unique response identifier (same across all chunks).
    pub id: String,
    /// Object type (always "chat.completion.chunk").
    pub object: &'static str,
    /// Unix timestamp of response creation.
    pub created: i64,
    /// Model that produced the response.
    pub model: String,
    /// Streaming choices with delta content.
    pub choices: Vec<StreamChoice>,
}

impl ChatCompletionChunk {
    /// Create a content delta chunk.
    pub fn content(id: &str, created: i64, model: &str, content: String) -> Self {
        Self {
            id: id.to_string(),
            object: OBJECT_CHAT_COMPLETION_CHUNK,
            created,
            model: model.to_string(),
            choices: vec![StreamChoice {
                index: 0,
                delta: Delta {
                    role: None,
                    content: Some(content),
                },
                finish_reason: None,
            }],
        }
    }

    /// Create the initial chunk that announces the assistant role.
    pub fn role(id: &str, created: i64, model: &str) -> Self {
        Self {
            id: id.to_string(),
            object: OBJECT_CHAT_COMPLETION_CHUNK,
            created,
            model: model.to_string(),
            choices: vec![StreamChoice {
                index: 0,
                delta: Delta {
                    role: Some(ROLE_ASSISTANT.to_string()),
                    content: None,
                },
                finish_reason: None,
            }],
        }
    }

    /// Create the final chunk with stop finish reason.
    pub fn stop(id: &str, created: i64, model: &str) -> Self {
        Self {
            id: id.to_string(),
            object: OBJECT_CHAT_COMPLETION_CHUNK,
            created,
            model: model.to_string(),
            choices: vec![StreamChoice {
                index: 0,
                delta: Delta {
                    role: None,
                    content: None,
                },
                finish_reason: Some(FINISH_REASON_STOP),
            }],
        }
    }

    /// Format this chunk as an SSE data line.
    pub fn to_sse(&self) -> Result<String, serde_json::Error> {
        Ok(format!("data: {}\n\n", serde_json::to_string(self)?))
    }
}

/// Streaming choice with incremental delta content.
#[derive(Debug, Clone, Serialize)]
pub struct StreamChoice {
    /// Choice index (0-based).
    pub index: u32,
    /// Incremental content update.
    pub delta: Delta,
    /// Present only on the final chunk.
    pub finish_reason: Option<&'static str>,
}

/// Incremental content delta in a streaming chunk.
#[derive(Debug, Clone, Serialize)]
pub struct Delta {
    /// Present only in the first chunk to announce the role.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub role: Option<String>,
    /// Incremental text content.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content: Option<String>,
}

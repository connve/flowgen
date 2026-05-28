//! AI gateway configuration and OpenAI-compatible types.
//!
//! Provides task configuration for the AI gateway endpoint and the OpenAI
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

/// AI gateway processor configuration.
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
///   - ai_gateway:
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

#[cfg(test)]
mod tests {
    use super::*;

    // --- Processor config deserialization ---

    #[test]
    fn processor_deser_minimal() {
        let json = r#"{
            "name": "proxy",
            "path": "/v1/chat/completions"
        }"#;
        let cfg: Processor = serde_json::from_str(json).unwrap();
        assert_eq!(cfg.name, "proxy");
        assert_eq!(cfg.path, "/v1/chat/completions");
        assert_eq!(cfg.credentials_path, None);
        assert_eq!(cfg.auth, None);
        assert_eq!(cfg.ack_timeout, None);
        assert_eq!(cfg.depends_on, None);
        assert_eq!(cfg.retry, None);
    }

    #[test]
    fn processor_deser_with_credentials_and_auth() {
        let json = r#"{
            "name": "secure_proxy",
            "path": "/v1/chat/completions",
            "credentials_path": "/etc/proxy/creds.json",
            "auth": { "required": true }
        }"#;
        let cfg: Processor = serde_json::from_str(json).unwrap();
        assert_eq!(
            cfg.credentials_path,
            Some(PathBuf::from("/etc/proxy/creds.json"))
        );
        let auth = cfg.auth.unwrap();
        assert!(auth.required);
    }

    #[test]
    fn processor_deser_with_ack_timeout() {
        let json = r#"{
            "name": "proxy",
            "path": "/v1/chat/completions",
            "ack_timeout": "30s"
        }"#;
        let cfg: Processor = serde_json::from_str(json).unwrap();
        assert_eq!(cfg.ack_timeout, Some(std::time::Duration::from_secs(30)));
    }

    #[test]
    fn processor_deser_with_depends_on() {
        let json = r#"{
            "name": "proxy",
            "path": "/api",
            "depends_on": ["auth_check", "rate_limit"]
        }"#;
        let cfg: Processor = serde_json::from_str(json).unwrap();
        assert_eq!(
            cfg.depends_on,
            Some(vec!["auth_check".to_string(), "rate_limit".to_string()])
        );
    }

    #[test]
    fn processor_roundtrip() {
        let cfg = Processor {
            name: "gw".to_string(),
            path: "/v1/chat/completions".to_string(),
            credentials_path: Some(PathBuf::from("/c.json")),
            auth: None,
            ack_timeout: Some(std::time::Duration::from_secs(60)),
            depends_on: None,
            retry: None,
        };
        let json = serde_json::to_string(&cfg).unwrap();
        let back: Processor = serde_json::from_str(&json).unwrap();
        assert_eq!(cfg, back);
    }

    // --- ChatCompletionRequest deserialization ---

    #[test]
    fn chat_request_deser_minimal() {
        let json = r#"{
            "messages": [{"role": "user", "content": "Hello"}]
        }"#;
        let req: ChatCompletionRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.model, None);
        assert_eq!(req.messages.len(), 1);
        assert_eq!(req.messages[0].role, "user");
        assert_eq!(req.messages[0].content, "Hello");
        assert_eq!(req.temperature, None);
        assert_eq!(req.max_tokens, None);
        assert!(!req.stream);
    }

    #[test]
    fn chat_request_deser_full() {
        let json = r#"{
            "model": "gpt-4",
            "messages": [
                {"role": "system", "content": "You are helpful"},
                {"role": "user", "content": "Hi"}
            ],
            "temperature": 0.7,
            "max_tokens": 1000,
            "stream": true
        }"#;
        let req: ChatCompletionRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.model, Some("gpt-4".to_string()));
        assert_eq!(req.messages.len(), 2);
        assert!((req.temperature.unwrap() - 0.7).abs() < f32::EPSILON);
        assert_eq!(req.max_tokens, Some(1000));
        assert!(req.stream);
    }

    #[test]
    fn chat_request_stream_defaults_false() {
        let json = r#"{"messages": [{"role": "user", "content": "test"}]}"#;
        let req: ChatCompletionRequest = serde_json::from_str(json).unwrap();
        assert!(!req.stream);
    }

    // --- Message helpers ---

    #[test]
    fn message_assistant_factory() {
        let msg = Message::assistant("Hello there".to_string());
        assert_eq!(msg.role, ROLE_ASSISTANT);
        assert_eq!(msg.content, "Hello there");
    }

    #[test]
    fn message_is_system() {
        let system = Message {
            role: ROLE_SYSTEM.to_string(),
            content: "system msg".to_string(),
        };
        assert!(system.is_system());

        let user = Message {
            role: "user".to_string(),
            content: "user msg".to_string(),
        };
        assert!(!user.is_system());

        let assistant = Message::assistant("asst msg".to_string());
        assert!(!assistant.is_system());
    }

    // --- ChatCompletionResponse::new ---

    #[test]
    fn chat_response_new_structure() {
        let resp = ChatCompletionResponse::new(
            "resp-123".to_string(),
            1700000000,
            "gpt-4".to_string(),
            "Hello!".to_string(),
        );
        assert_eq!(resp.id, "resp-123");
        assert_eq!(resp.object, OBJECT_CHAT_COMPLETION);
        assert_eq!(resp.created, 1700000000);
        assert_eq!(resp.model, "gpt-4");
        assert_eq!(resp.choices.len(), 1);
        assert_eq!(resp.choices[0].index, 0);
        assert_eq!(resp.choices[0].message.role, ROLE_ASSISTANT);
        assert_eq!(resp.choices[0].message.content, "Hello!");
        assert_eq!(resp.choices[0].finish_reason, FINISH_REASON_STOP);
        assert!(resp.usage.is_none());
    }

    #[test]
    fn chat_response_serialization_matches_openai_shape() {
        let resp = ChatCompletionResponse::new(
            "id-1".to_string(),
            1700000000,
            "gpt-4".to_string(),
            "ok".to_string(),
        );
        let v = serde_json::to_value(&resp).unwrap();
        assert_eq!(v["object"], "chat.completion");
        assert_eq!(v["choices"][0]["finish_reason"], "stop");
        assert_eq!(v["choices"][0]["message"]["role"], "assistant");
        assert_eq!(v["choices"][0]["message"]["content"], "ok");
    }

    // --- ChatCompletionChunk factories ---

    #[test]
    fn chunk_content_structure() {
        let chunk = ChatCompletionChunk::content("id-1", 1700000000, "gpt-4", "Hello".to_string());
        assert_eq!(chunk.id, "id-1");
        assert_eq!(chunk.object, OBJECT_CHAT_COMPLETION_CHUNK);
        assert_eq!(chunk.created, 1700000000);
        assert_eq!(chunk.model, "gpt-4");
        assert_eq!(chunk.choices.len(), 1);
        assert_eq!(chunk.choices[0].index, 0);
        assert_eq!(chunk.choices[0].delta.content, Some("Hello".to_string()));
        assert_eq!(chunk.choices[0].delta.role, None);
        assert_eq!(chunk.choices[0].finish_reason, None);
    }

    #[test]
    fn chunk_role_structure() {
        let chunk = ChatCompletionChunk::role("id-1", 1700000000, "gpt-4");
        assert_eq!(
            chunk.choices[0].delta.role,
            Some(ROLE_ASSISTANT.to_string())
        );
        assert_eq!(chunk.choices[0].delta.content, None);
        assert_eq!(chunk.choices[0].finish_reason, None);
    }

    #[test]
    fn chunk_stop_structure() {
        let chunk = ChatCompletionChunk::stop("id-1", 1700000000, "gpt-4");
        assert_eq!(chunk.choices[0].delta.role, None);
        assert_eq!(chunk.choices[0].delta.content, None);
        assert_eq!(chunk.choices[0].finish_reason, Some(FINISH_REASON_STOP));
    }

    // --- to_sse formatting ---

    #[test]
    fn to_sse_format() {
        let chunk = ChatCompletionChunk::content("id-1", 1700000000, "gpt-4", "Hi".to_string());
        let sse = chunk.to_sse().unwrap();
        assert!(sse.starts_with("data: "));
        assert!(sse.ends_with("\n\n"));
        // Parse the JSON portion back to verify it round-trips.
        let json_str = sse.strip_prefix("data: ").unwrap().trim();
        let v: serde_json::Value = serde_json::from_str(json_str).unwrap();
        assert_eq!(v["object"], "chat.completion.chunk");
        assert_eq!(v["choices"][0]["delta"]["content"], "Hi");
    }

    #[test]
    fn sse_done_sentinel() {
        // Verify the SSE_DONE constant matches OpenAI spec.
        assert_eq!(SSE_DONE, "data: [DONE]\n\n");
    }

    // --- Delta skip_serializing_if ---

    #[test]
    fn delta_skips_none_fields() {
        let delta = Delta {
            role: None,
            content: Some("text".to_string()),
        };
        let v = serde_json::to_value(&delta).unwrap();
        assert!(!v.as_object().unwrap().contains_key("role"));
        assert_eq!(v["content"], "text");
    }

    #[test]
    fn delta_both_none_empty_object() {
        let delta = Delta {
            role: None,
            content: None,
        };
        let v = serde_json::to_value(&delta).unwrap();
        assert!(v.as_object().unwrap().is_empty());
    }
}

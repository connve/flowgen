//! OpenAI Chat Completions API wire types.
//!
//! The gateway speaks this protocol on `POST <prefix>/chat/completions`
//! and `GET <prefix>/models`. Types are shared with the leaf
//! `ai_completion` task via `completion::passthrough`, which converts
//! them to rig's provider-agnostic shape before hitting the upstream
//! provider.
//!
//! For the internal event payload the dispatcher writes into
//! `event.data` see `ai_gateway::config::EventPayload` — it is
//! protocol-neutral and shared with the Anthropic Messages API path.

use serde::{Deserialize, Serialize};

// --- OpenAI protocol constants ---

/// OpenAI object type for non-streaming responses.
pub const OBJECT_CHAT_COMPLETION: &str = "chat.completion";
/// OpenAI object type for streaming chunks.
pub const OBJECT_CHAT_COMPLETION_CHUNK: &str = "chat.completion.chunk";
/// Role for assistant messages.
pub const ROLE_ASSISTANT: &str = "assistant";
/// Role for system messages.
pub const ROLE_SYSTEM: &str = "system";
/// Role for tool result messages sent back to the model.
pub const ROLE_TOOL: &str = "tool";
/// Finish reason when generation completes normally.
pub const FINISH_REASON_STOP: &str = "stop";
/// Finish reason when the model stops because it wants to invoke tools.
pub const FINISH_REASON_TOOL_CALLS: &str = "tool_calls";
/// Tool `type` value per OpenAI tool-use spec.
pub const TOOL_TYPE_FUNCTION: &str = "function";
/// SSE terminator per OpenAI streaming specification.
pub const SSE_DONE: &str = "data: [DONE]\n\n";

// --- OpenAI request types ---

/// OpenAI chat completion request body.
#[derive(Debug, Clone, Deserialize)]
pub struct ChatCompletionRequest {
    /// Model identifier requested by the client.
    pub model: Option<String>,
    /// Conversation messages (system, user, assistant, tool).
    pub messages: Vec<Message>,
    /// Sampling temperature (0.0-2.0).
    pub temperature: Option<f32>,
    /// Maximum tokens to generate.
    pub max_tokens: Option<u32>,
    /// Whether to stream the response as SSE chunks.
    #[serde(default)]
    pub stream: bool,
    /// Client-supplied tool definitions the model is allowed to invoke.
    /// Forwarded verbatim to the upstream provider when the gateway is
    /// configured for tool passthrough.
    #[serde(default)]
    pub tools: Option<Vec<ToolDefinition>>,
    /// Tool selection strategy (`"auto"`, `"none"`, `"required"`, or an
    /// object referencing a specific function). Passed through unchanged.
    #[serde(default)]
    pub tool_choice: Option<serde_json::Value>,
    /// Streaming-only options. When `stream_options.include_usage` is
    /// true the gateway emits an extra SSE chunk with `usage` populated
    /// before the terminating `[DONE]` — matches OpenAI's behaviour.
    #[serde(default)]
    pub stream_options: Option<StreamOptions>,
}

/// OpenAI streaming options.
#[derive(Debug, Clone, Deserialize, Default)]
pub struct StreamOptions {
    /// When true, the final SSE frame before `[DONE]` carries
    /// `usage` with prompt/completion/total token counts.
    #[serde(default)]
    pub include_usage: bool,
}

/// Chat message with role and content.
///
/// `content` is optional because OpenAI permits assistant messages that
/// carry only `tool_calls` (no text), and tool-role messages carry results
/// separately. On the wire an omitted / `null` content is legitimate.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Message {
    /// Message role: `system`, `user`, `assistant`, or `tool`.
    pub role: String,
    /// Message text content. `None` when `tool_calls` is present, or on a
    /// tool-role message before the caller inlines the tool result.
    ///
    /// Accepts both OpenAI wire forms: a bare string, or an array of typed
    /// content parts (`[{"type":"text","text":"..."}]`). Array form is
    /// flattened by concatenating all `text` parts — non-text parts (image,
    /// input_audio) are ignored since the gateway does not forward them.
    #[serde(
        default,
        deserialize_with = "deserialize_message_content",
        skip_serializing_if = "Option::is_none"
    )]
    pub content: Option<String>,
    /// Tool invocations the assistant wants to perform. Present only on
    /// assistant messages returned by a model in tool-use mode.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tool_calls: Vec<ToolCall>,
    /// Identifier of the tool call this message answers. Present only on
    /// tool-role follow-up messages.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool_call_id: Option<String>,
}

/// Accepts `content` as either a bare string or an OpenAI content-parts
/// array. Rig, the OpenAI SDK, and several third-party clients emit the
/// array form even for pure-text turns; the gateway then flattens back to a
/// single string since it only forwards text.
fn deserialize_message_content<'de, D>(deserializer: D) -> Result<Option<String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::de::Error;

    #[derive(Deserialize)]
    #[serde(untagged)]
    enum ContentInput {
        Text(String),
        Parts(Vec<ContentPart>),
        Null,
    }

    #[derive(Deserialize)]
    #[serde(tag = "type", rename_all = "snake_case")]
    enum ContentPart {
        Text {
            text: String,
        },
        #[serde(other)]
        Other,
    }

    match Option::<ContentInput>::deserialize(deserializer).map_err(D::Error::custom)? {
        None | Some(ContentInput::Null) => Ok(None),
        Some(ContentInput::Text(s)) => Ok(Some(s)),
        Some(ContentInput::Parts(parts)) => {
            let joined: String = parts
                .into_iter()
                .filter_map(|p| match p {
                    ContentPart::Text { text } => Some(text),
                    ContentPart::Other => None,
                })
                .collect::<Vec<_>>()
                .join("");
            if joined.is_empty() {
                Ok(None)
            } else {
                Ok(Some(joined))
            }
        }
    }
}

impl Message {
    /// Create an assistant response message carrying text content.
    pub fn assistant(content: String) -> Self {
        Self {
            role: ROLE_ASSISTANT.to_string(),
            content: Some(content),
            tool_calls: Vec::new(),
            tool_call_id: None,
        }
    }

    /// Check whether this message has the system role.
    pub fn is_system(&self) -> bool {
        self.role == ROLE_SYSTEM
    }
}

/// Client-supplied tool definition, matching OpenAI's function-calling
/// schema. The gateway forwards these to the upstream provider unchanged.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ToolDefinition {
    /// Tool kind. OpenAI currently defines only `"function"`.
    #[serde(rename = "type")]
    pub kind: String,
    /// Function metadata (name, description, JSON Schema parameters).
    pub function: ToolFunction,
}

/// Function definition inside a `ToolDefinition`.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ToolFunction {
    /// Function name the model uses to invoke this tool.
    pub name: String,
    /// Optional natural-language description shown to the model.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// JSON Schema describing the function's parameters.
    pub parameters: serde_json::Value,
}

/// Model-emitted tool invocation returned inside an assistant `Message`.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ToolCall {
    /// Unique identifier the client echoes back in the tool-role reply.
    pub id: String,
    /// Tool kind — matches `ToolDefinition::kind`.
    #[serde(rename = "type")]
    pub kind: String,
    /// Called function (name + JSON-encoded arguments string).
    pub function: ToolCallFunction,
}

/// Function invocation payload inside a `ToolCall`.
///
/// `arguments` is a JSON-encoded **string** per the OpenAI spec, not a
/// parsed object; downstream code parses it lazily when it needs the args.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ToolCallFunction {
    /// Function name selected by the model.
    pub name: String,
    /// JSON-encoded argument object as a raw string.
    pub arguments: String,
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

    /// Create a response whose assistant message only carries tool
    /// invocations (no text). `finish_reason` is set to `"tool_calls"`
    /// so tool-aware clients treat the turn as a request for tool
    /// execution rather than a normal completion.
    pub fn with_tool_calls(
        id: String,
        created: i64,
        model: String,
        tool_calls: Vec<ToolCall>,
    ) -> Self {
        Self {
            id,
            object: OBJECT_CHAT_COMPLETION,
            created,
            model,
            choices: vec![Choice {
                index: 0,
                message: Message {
                    role: ROLE_ASSISTANT.to_string(),
                    content: None,
                    tool_calls,
                    tool_call_id: None,
                },
                finish_reason: FINISH_REASON_TOOL_CALLS,
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
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Usage {
    /// Tokens consumed by the prompt.
    pub prompt_tokens: u64,
    /// Tokens generated in the completion.
    pub completion_tokens: u64,
    /// Total tokens used.
    pub total_tokens: u64,
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
    /// Token usage. Present only on the terminal usage frame emitted
    /// when the client sets `stream_options.include_usage: true`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub usage: Option<Usage>,
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
                    tool_calls: Vec::new(),
                },
                finish_reason: None,
            }],
            usage: None,
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
                    tool_calls: Vec::new(),
                },
                finish_reason: None,
            }],
            usage: None,
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
                    tool_calls: Vec::new(),
                },
                finish_reason: Some(FINISH_REASON_STOP),
            }],
            usage: None,
        }
    }

    /// Create a chunk that opens a tool-call delta with `id` and
    /// function name. Emitted once per tool call at the start of the
    /// call's argument stream.
    pub fn tool_call_open(
        id: &str,
        created: i64,
        model: &str,
        index: u32,
        call_id: String,
        function_name: String,
    ) -> Self {
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
                    tool_calls: vec![ToolCallDelta {
                        index,
                        id: Some(call_id),
                        kind: Some(TOOL_TYPE_FUNCTION.to_string()),
                        function: Some(ToolCallDeltaFunction {
                            name: Some(function_name),
                            arguments: String::new(),
                        }),
                    }],
                },
                finish_reason: None,
            }],
            usage: None,
        }
    }

    /// Create a chunk that carries an arguments fragment for a
    /// previously-opened tool call (identified by `index`).
    pub fn tool_call_arguments(
        id: &str,
        created: i64,
        model: &str,
        index: u32,
        arguments: String,
    ) -> Self {
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
                    tool_calls: vec![ToolCallDelta {
                        index,
                        id: None,
                        kind: None,
                        function: Some(ToolCallDeltaFunction {
                            name: None,
                            arguments,
                        }),
                    }],
                },
                finish_reason: None,
            }],
            usage: None,
        }
    }

    /// Create the final chunk with `finish_reason: "tool_calls"`,
    /// signalling the client that the model stopped because it wants
    /// tools invoked.
    pub fn stop_tool_calls(id: &str, created: i64, model: &str) -> Self {
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
                    tool_calls: Vec::new(),
                },
                finish_reason: Some(FINISH_REASON_TOOL_CALLS),
            }],
            usage: None,
        }
    }

    /// Terminal usage frame emitted when the client set
    /// `stream_options.include_usage: true`. Carries the token counts
    /// in the OpenAI `usage: {...}` shape and no delta choices, per
    /// spec.
    pub fn usage_frame(id: &str, created: i64, model: &str, usage: Usage) -> Self {
        Self {
            id: id.to_string(),
            object: OBJECT_CHAT_COMPLETION_CHUNK,
            created,
            model: model.to_string(),
            choices: Vec::new(),
            usage: Some(usage),
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
    /// Incremental tool-call additions carried inside this delta.
    /// Present only on passthrough streams where the model returns
    /// tool invocations. Multiple calls disambiguated by `index`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tool_calls: Vec<ToolCallDelta>,
}

/// Incremental tool-call fragment inside a streaming `Delta`.
///
/// Rig hands us a fully-formed `ToolCall` in a single item, so in
/// practice we emit two deltas per call: one carrying `id` + `name`
/// with empty `arguments`, then one carrying the JSON-encoded
/// `arguments` string. Clients aggregate by `index`.
#[derive(Debug, Clone, Serialize)]
pub struct ToolCallDelta {
    /// Zero-based index disambiguating parallel tool calls in a
    /// single assistant turn.
    pub index: u32,
    /// Present on the opening delta for a tool call.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    /// Present on the opening delta; matches `ToolCall::kind`.
    #[serde(skip_serializing_if = "Option::is_none", rename = "type")]
    pub kind: Option<String>,
    /// Present when carrying either the function name (opening
    /// delta) or an arguments fragment.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub function: Option<ToolCallDeltaFunction>,
}

/// Function payload inside a `ToolCallDelta`.
///
/// Either field can be absent depending on which fragment the delta
/// represents; the wire format skips missing ones.
#[derive(Debug, Clone, Serialize)]
pub struct ToolCallDeltaFunction {
    /// Function name, present only on the opening delta of a tool call.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// Fragment of JSON-encoded arguments. Empty string on the
    /// opening delta; carries the full JSON payload on the
    /// arguments-carrying delta.
    pub arguments: String,
}

// ---------------------------------------------------------------------------
// Protocol adapter
// ---------------------------------------------------------------------------

use super::{ProtocolAdapter, StopReason, StreamWriter};
use axum::response::{IntoResponse, Response};

/// OpenAI Chat Completions API adapter — glues the shared dispatcher
/// to the `ChatCompletionResponse` / `ChatCompletionChunk` wire shape.
pub struct OpenAiAdapter;

impl ProtocolAdapter for OpenAiAdapter {
    type StreamWriter = OpenAiStreamWriter;

    const REQUEST_ID_PREFIX: &'static str = "chatcmpl";
    const PROTOCOL_NAME: &'static str = "openai";
    const EMIT_DONE_SENTINEL: bool = true;

    fn build_blocking_response(
        request_id: String,
        model: String,
        _created: i64,
        text: String,
        tool_calls: Vec<ToolCall>,
        usage: Option<Usage>,
    ) -> Response {
        let created = chrono::Utc::now().timestamp();
        let mut response = match tool_calls.is_empty() {
            true => ChatCompletionResponse::new(request_id, created, model, text),
            false => {
                ChatCompletionResponse::with_tool_calls(request_id, created, model, tool_calls)
            }
        };
        response.usage = usage;
        axum::Json(response).into_response()
    }

    fn new_stream_writer(request_id: String, model: String, created: i64) -> Self::StreamWriter {
        OpenAiStreamWriter {
            request_id,
            model,
            created,
            emitted_tool_calls: false,
        }
    }

    fn error_sse_frame(message: &str) -> String {
        #[derive(serde::Serialize)]
        struct OpenAiErrorBody<'a> {
            error: OpenAiErrorDetail<'a>,
        }
        #[derive(serde::Serialize)]
        struct OpenAiErrorDetail<'a> {
            message: &'a str,
            #[serde(rename = "type")]
            error_type: &'static str,
        }
        let payload = OpenAiErrorBody {
            error: OpenAiErrorDetail {
                message,
                error_type: "server_error",
            },
        };
        match serde_json::to_string(&payload) {
            Ok(json) => format!("data: {json}\n\n"),
            Err(_) => String::new(),
        }
    }
}

/// OpenAI streaming state machine. Emits `chat.completion.chunk`
/// frames matching the OpenAI wire format.
pub struct OpenAiStreamWriter {
    request_id: String,
    model: String,
    created: i64,
    emitted_tool_calls: bool,
}

impl OpenAiStreamWriter {
    fn frame(&self, chunk: &ChatCompletionChunk) -> Option<String> {
        chunk.to_sse().ok()
    }
}

impl StreamWriter for OpenAiStreamWriter {
    fn open(&mut self) -> Vec<String> {
        let chunk = ChatCompletionChunk::role(&self.request_id, self.created, &self.model);
        self.frame(&chunk).into_iter().collect()
    }

    fn text_delta(&mut self, text: String) -> Vec<String> {
        let chunk = ChatCompletionChunk::content(&self.request_id, self.created, &self.model, text);
        self.frame(&chunk).into_iter().collect()
    }

    fn tool_calls(&mut self, calls: Vec<ToolCall>) -> Vec<String> {
        let mut frames = Vec::new();
        for (idx, call) in calls.into_iter().enumerate() {
            self.emitted_tool_calls = true;
            let idx = idx as u32;
            let open = ChatCompletionChunk::tool_call_open(
                &self.request_id,
                self.created,
                &self.model,
                idx,
                call.id.clone(),
                call.function.name.clone(),
            );
            if let Some(f) = self.frame(&open) {
                frames.push(f);
            }
            let args = ChatCompletionChunk::tool_call_arguments(
                &self.request_id,
                self.created,
                &self.model,
                idx,
                call.function.arguments,
            );
            if let Some(f) = self.frame(&args) {
                frames.push(f);
            }
        }
        frames
    }

    fn close(
        &mut self,
        stop: StopReason,
        usage: Option<Usage>,
        include_usage: bool,
    ) -> Vec<String> {
        let mut frames = Vec::new();
        let final_chunk = match (stop, self.emitted_tool_calls) {
            (StopReason::ToolUse, _) | (_, true) => {
                ChatCompletionChunk::stop_tool_calls(&self.request_id, self.created, &self.model)
            }
            _ => ChatCompletionChunk::stop(&self.request_id, self.created, &self.model),
        };
        if let Some(f) = self.frame(&final_chunk) {
            frames.push(f);
        }
        // OpenAI emits the usage frame only when the client set
        // `stream_options.include_usage: true`.
        if let (true, Some(u)) = (include_usage, usage) {
            let usage_chunk =
                ChatCompletionChunk::usage_frame(&self.request_id, self.created, &self.model, u);
            if let Some(f) = self.frame(&usage_chunk) {
                frames.push(f);
            }
        }
        frames
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
        assert_eq!(req.messages[0].content.as_deref(), Some("Hello"));
        assert!(req.messages[0].tool_calls.is_empty());
        assert_eq!(req.messages[0].tool_call_id, None);
        assert_eq!(req.temperature, None);
        assert_eq!(req.max_tokens, None);
        assert!(!req.stream);
        assert!(req.tools.is_none());
        assert!(req.tool_choice.is_none());
    }

    #[test]
    fn message_content_accepts_openai_parts_array() {
        let json = r#"{
            "role": "user",
            "content": [
                {"type": "text", "text": "hello "},
                {"type": "text", "text": "world"}
            ]
        }"#;
        let msg: Message = serde_json::from_str(json).unwrap();
        assert_eq!(msg.content.as_deref(), Some("hello world"));
    }

    #[test]
    fn message_content_skips_non_text_parts() {
        let json = r#"{
            "role": "user",
            "content": [
                {"type": "text", "text": "prompt"},
                {"type": "image_url", "image_url": {"url": "http://x"}}
            ]
        }"#;
        let msg: Message = serde_json::from_str(json).unwrap();
        assert_eq!(msg.content.as_deref(), Some("prompt"));
    }

    #[test]
    fn message_content_null_and_missing() {
        let msg: Message =
            serde_json::from_str(r#"{"role": "assistant", "content": null}"#).unwrap();
        assert!(msg.content.is_none());
        let msg: Message = serde_json::from_str(r#"{"role": "assistant"}"#).unwrap();
        assert!(msg.content.is_none());
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
        assert_eq!(msg.content.as_deref(), Some("Hello there"));
        assert!(msg.tool_calls.is_empty());
        assert_eq!(msg.tool_call_id, None);
    }

    #[test]
    fn message_is_system() {
        let system = Message {
            role: ROLE_SYSTEM.to_string(),
            content: Some("system msg".to_string()),
            tool_calls: Vec::new(),
            tool_call_id: None,
        };
        assert!(system.is_system());

        let user = Message {
            role: "user".to_string(),
            content: Some("user msg".to_string()),
            tool_calls: Vec::new(),
            tool_call_id: None,
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
        assert_eq!(resp.choices[0].message.content.as_deref(), Some("Hello!"));
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
        let json_str = sse.strip_prefix("data: ").unwrap().trim();
        let v: serde_json::Value = serde_json::from_str(json_str).unwrap();
        assert_eq!(v["object"], "chat.completion.chunk");
        assert_eq!(v["choices"][0]["delta"]["content"], "Hi");
    }

    #[test]
    fn sse_done_sentinel() {
        assert_eq!(SSE_DONE, "data: [DONE]\n\n");
    }

    // --- Delta skip_serializing_if ---

    #[test]
    fn delta_skips_none_fields() {
        let delta = Delta {
            role: None,
            content: Some("text".to_string()),
            tool_calls: Vec::new(),
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
            tool_calls: Vec::new(),
        };
        let v = serde_json::to_value(&delta).unwrap();
        assert!(v.as_object().unwrap().is_empty());
    }

    // --- Tool-use passthrough types ---

    #[test]
    fn chat_request_deser_with_tools() {
        let json = r#"{
            "model": "gpt-4",
            "messages": [{"role": "user", "content": "list files"}],
            "tools": [{
                "type": "function",
                "function": {
                    "name": "bash",
                    "description": "run a shell command",
                    "parameters": {"type": "object", "properties": {"cmd": {"type": "string"}}}
                }
            }],
            "tool_choice": "auto"
        }"#;
        let req: ChatCompletionRequest = serde_json::from_str(json).unwrap();
        let tools = req.tools.expect("tools present");
        assert_eq!(tools.len(), 1);
        assert_eq!(tools[0].kind, TOOL_TYPE_FUNCTION);
        assert_eq!(tools[0].function.name, "bash");
        assert_eq!(
            tools[0].function.description.as_deref(),
            Some("run a shell command")
        );
        assert_eq!(req.tool_choice, Some(serde_json::json!("auto")));
    }

    #[test]
    fn assistant_message_with_tool_calls_and_no_content_roundtrips() {
        let json = r#"{
            "role": "assistant",
            "tool_calls": [{
                "id": "call_1",
                "type": "function",
                "function": {"name": "bash", "arguments": "{\"cmd\":\"ls\"}"}
            }]
        }"#;
        let msg: Message = serde_json::from_str(json).unwrap();
        assert_eq!(msg.role, ROLE_ASSISTANT);
        assert_eq!(msg.content, None);
        assert_eq!(msg.tool_calls.len(), 1);
        assert_eq!(msg.tool_calls[0].id, "call_1");
        assert_eq!(msg.tool_calls[0].kind, TOOL_TYPE_FUNCTION);
        assert_eq!(msg.tool_calls[0].function.name, "bash");
        assert_eq!(msg.tool_calls[0].function.arguments, r#"{"cmd":"ls"}"#);

        let back = serde_json::to_value(&msg).unwrap();
        assert!(back.get("content").is_none(), "null content is skipped");
        assert!(back.get("tool_call_id").is_none());
    }

    #[test]
    fn tool_role_reply_deserializes() {
        let json = r#"{
            "role": "tool",
            "tool_call_id": "call_1",
            "content": "file1\nfile2"
        }"#;
        let msg: Message = serde_json::from_str(json).unwrap();
        assert_eq!(msg.role, ROLE_TOOL);
        assert_eq!(msg.tool_call_id.as_deref(), Some("call_1"));
        assert_eq!(msg.content.as_deref(), Some("file1\nfile2"));
        assert!(msg.tool_calls.is_empty());
    }

    // --- stream_options / include_usage ---

    #[test]
    fn chat_request_deser_without_stream_options_defaults_to_none() {
        let json = r#"{"messages":[{"role":"user","content":"hi"}]}"#;
        let req: ChatCompletionRequest = serde_json::from_str(json).unwrap();
        assert!(req.stream_options.is_none());
    }

    #[test]
    fn chat_request_deser_with_include_usage() {
        let json = r#"{
            "messages":[{"role":"user","content":"hi"}],
            "stream": true,
            "stream_options": {"include_usage": true}
        }"#;
        let req: ChatCompletionRequest = serde_json::from_str(json).unwrap();
        let opts = req.stream_options.expect("stream_options present");
        assert!(opts.include_usage);
    }

    #[test]
    fn stream_options_include_usage_defaults_to_false() {
        let json = "{}";
        let opts: StreamOptions = serde_json::from_str(json).unwrap();
        assert!(!opts.include_usage);
    }

    // --- Usage deserialization (leaf → gateway path) ---

    #[test]
    fn usage_roundtrip() {
        let usage = Usage {
            prompt_tokens: 42,
            completion_tokens: 7,
            total_tokens: 49,
        };
        let json = serde_json::to_string(&usage).unwrap();
        let back: Usage = serde_json::from_str(&json).unwrap();
        assert_eq!(back.prompt_tokens, 42);
        assert_eq!(back.completion_tokens, 7);
        assert_eq!(back.total_tokens, 49);
    }

    // --- ChatCompletionChunk usage frame ---

    #[test]
    fn chunk_usage_frame_carries_usage_and_no_choices() {
        let chunk = ChatCompletionChunk::usage_frame(
            "id-1",
            1700000000,
            "gpt-4",
            Usage {
                prompt_tokens: 10,
                completion_tokens: 5,
                total_tokens: 15,
            },
        );
        assert!(
            chunk.choices.is_empty(),
            "usage frame carries empty choices per OpenAI spec"
        );
        let usage = chunk.usage.expect("usage present");
        assert_eq!(usage.prompt_tokens, 10);
        assert_eq!(usage.completion_tokens, 5);
        assert_eq!(usage.total_tokens, 15);
    }

    #[test]
    fn chunk_usage_frame_serialises_to_openai_wire_shape() {
        let chunk = ChatCompletionChunk::usage_frame(
            "id-1",
            1700000000,
            "gpt-4",
            Usage {
                prompt_tokens: 10,
                completion_tokens: 5,
                total_tokens: 15,
            },
        );
        let v = serde_json::to_value(&chunk).unwrap();
        assert_eq!(v["object"], "chat.completion.chunk");
        assert!(v["choices"].as_array().unwrap().is_empty());
        assert_eq!(v["usage"]["prompt_tokens"], 10);
        assert_eq!(v["usage"]["completion_tokens"], 5);
        assert_eq!(v["usage"]["total_tokens"], 15);
    }

    #[test]
    fn chunk_content_omits_usage_field_on_wire() {
        let chunk = ChatCompletionChunk::content("id-1", 1700000000, "gpt-4", "hi".to_string());
        let v = serde_json::to_value(&chunk).unwrap();
        assert!(
            !v.as_object().unwrap().contains_key("usage"),
            "intermediate chunks must not carry a usage field on the wire"
        );
    }

    #[test]
    fn chat_response_with_tool_calls_shape() {
        let tool_calls = vec![ToolCall {
            id: "call_1".to_string(),
            kind: TOOL_TYPE_FUNCTION.to_string(),
            function: ToolCallFunction {
                name: "bash".to_string(),
                arguments: r#"{"cmd":"ls"}"#.to_string(),
            },
        }];
        let resp = ChatCompletionResponse::with_tool_calls(
            "resp-1".to_string(),
            1700000000,
            "gpt-4".to_string(),
            tool_calls,
        );
        assert_eq!(resp.choices[0].finish_reason, FINISH_REASON_TOOL_CALLS);
        assert_eq!(resp.choices[0].message.content, None);
        assert_eq!(resp.choices[0].message.tool_calls.len(), 1);

        let v = serde_json::to_value(&resp).unwrap();
        assert_eq!(v["choices"][0]["finish_reason"], "tool_calls");
        assert!(
            v["choices"][0]["message"].get("content").is_none(),
            "assistant tool-call message serialises without a content field",
        );
        assert_eq!(
            v["choices"][0]["message"]["tool_calls"][0]["function"]["name"],
            "bash"
        );
    }
}

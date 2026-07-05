//! Anthropic Messages API wire types + translation to the shared
//! `EventPayload` and back.
//!
//! Claude Code and other clients honouring `ANTHROPIC_BASE_URL` speak
//! this protocol. The gateway accepts it on `POST <prefix>/messages`,
//! translates the request into the same `EventPayload` the OpenAI
//! path feeds into the pipeline, and translates the leaf's completion
//! back into an Anthropic `MessagesResponse` (or the Anthropic SSE
//! event stream). The leaf `ai_completion` task stays protocol-
//! agnostic — everything Anthropic-shaped lives in this module.
//!
//! Follow-ups not implemented today:
//! - `stop_reason: max_tokens | stop_sequence` — needs the leaf to
//!   surface rig's real stop reason. Today we emit `end_turn` /
//!   `tool_use` only.
//! - `cache_control` prompt-caching blocks — round-tripped inside the
//!   passthrough payload but not honoured downstream.
//! - Image content blocks — accepted, not currently forwarded.

use super::completions::{ROLE_ASSISTANT, ROLE_SYSTEM, ROLE_TOOL, TOOL_TYPE_FUNCTION};
use super::{ProtocolAdapter, StopReason, StreamWriter};
use crate::ai_gateway::config::{
    Message, ToolCall, ToolCallFunction, ToolDefinition, ToolFunction, Usage as OpenAiUsage,
};
use axum::response::{IntoResponse, Response};
use serde::{Deserialize, Serialize};

// --- Anthropic protocol constants ---

/// Response `type` field for a completed message.
pub const OBJECT_MESSAGE: &str = "message";
/// Stop reason when the model finishes a normal turn.
pub const STOP_REASON_END_TURN: &str = "end_turn";
/// Stop reason when the model stops because it wants tools invoked.
pub const STOP_REASON_TOOL_USE: &str = "tool_use";

// --- Request types ---

/// Anthropic Messages API request body.
///
/// Only the fields Claude Code actually sends are modelled; unknown
/// fields are tolerated so we don't reject requests over a spec
/// extension we haven't picked up yet.
#[derive(Debug, Clone, Deserialize)]
pub struct MessagesRequest {
    /// Model identifier of the form `<proxy-name>/<downstream-model>`
    /// matching the OpenAI routing convention.
    pub model: String,
    /// Conversation messages (user, assistant). The system prompt
    /// lives in the top-level `system` field per Anthropic spec.
    pub messages: Vec<InputMessage>,
    /// Optional system prompt. Accepts a bare string or an array of
    /// text blocks per spec.
    #[serde(default)]
    pub system: Option<SystemPrompt>,
    /// Maximum tokens to generate. Required by Anthropic — a missing
    /// field deserialises with a clear error message.
    pub max_tokens: u32,
    /// Sampling temperature (0.0–1.0 per Anthropic).
    #[serde(default)]
    pub temperature: Option<f32>,
    /// Whether to stream the response as Anthropic SSE events.
    #[serde(default)]
    pub stream: bool,
    /// Client-supplied tool definitions.
    #[serde(default)]
    pub tools: Option<Vec<AnthropicToolDefinition>>,
    /// Tool selection strategy — passed through unchanged.
    #[serde(default)]
    pub tool_choice: Option<serde_json::Value>,
    /// Stop sequences — accepted but not honoured downstream today.
    #[serde(default)]
    pub stop_sequences: Option<Vec<String>>,
}

/// Top-level system prompt. Anthropic accepts either a bare string or
/// a list of text blocks — the untagged enum picks the string variant
/// first because serde tries variants in declaration order.
#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum SystemPrompt {
    /// Bare-string form.
    Text(String),
    /// Block form.
    Blocks(Vec<SystemTextBlock>),
}

/// Text block inside a `SystemPrompt::Blocks` array.
#[derive(Debug, Clone, Deserialize)]
pub struct SystemTextBlock {
    /// Block type (always `"text"` today).
    #[serde(rename = "type")]
    pub kind: String,
    /// Block text content.
    pub text: String,
}

/// One entry in the `messages` array.
#[derive(Debug, Clone, Deserialize)]
pub struct InputMessage {
    /// Message role — `"user"` or `"assistant"`.
    pub role: String,
    /// Content — bare string or a list of typed content blocks.
    pub content: MessageContent,
}

/// Content shape for an `InputMessage`.
#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum MessageContent {
    /// Bare-string content.
    Text(String),
    /// Typed content blocks.
    Blocks(Vec<ContentBlock>),
}

/// One block inside a `MessageContent::Blocks` array.
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ContentBlock {
    /// Plain text block.
    Text {
        /// Block text content.
        text: String,
    },
    /// Assistant-emitted tool invocation.
    ToolUse {
        /// Unique invocation id echoed back in the matching `tool_result`.
        id: String,
        /// Tool name selected by the model.
        name: String,
        /// Parsed arguments object (already JSON, not a string).
        input: serde_json::Value,
    },
    /// User-supplied result of a prior `tool_use` invocation.
    ToolResult {
        /// Identifier of the `tool_use` block this result answers.
        tool_use_id: String,
        /// Result payload — bare string or a nested block list.
        content: ToolResultContent,
        /// Optional error flag; passed through informationally.
        #[serde(default)]
        is_error: Option<bool>,
    },
}

/// Payload carried inside a `ContentBlock::ToolResult`.
#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum ToolResultContent {
    /// Bare-string result.
    Text(String),
    /// Nested content-block list (only text blocks are flattened).
    Blocks(Vec<ContentBlock>),
}

/// Tool definition in the Anthropic shape — differs from OpenAI only
/// by the JSON Schema field name (`input_schema` vs `parameters`).
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct AnthropicToolDefinition {
    /// Function name the model invokes.
    pub name: String,
    /// Optional natural-language description.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// JSON Schema describing the function's parameters.
    pub input_schema: serde_json::Value,
}

// --- Response types (non-streaming) ---

/// Anthropic Messages API non-streaming response body.
#[derive(Debug, Clone, Serialize)]
pub struct MessagesResponse {
    /// Unique message id (`msg_<uuid>`).
    pub id: String,
    /// Object type — always `"message"`.
    #[serde(rename = "type")]
    pub kind: &'static str,
    /// Role — always `"assistant"` on responses.
    pub role: &'static str,
    /// Model that produced the response (downstream model name).
    pub model: String,
    /// Assistant-emitted content blocks (text and/or tool_use).
    pub content: Vec<OutputContentBlock>,
    /// Stop reason — `"end_turn"` for normal completions,
    /// `"tool_use"` when the model wants tools invoked.
    pub stop_reason: &'static str,
    /// Always `None` today (leaf doesn't surface the sequence trigger).
    pub stop_sequence: Option<String>,
    /// Token usage.
    pub usage: AnthropicUsage,
}

/// One assistant-emitted content block on the response.
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum OutputContentBlock {
    /// Plain text response fragment.
    Text {
        /// Text content.
        text: String,
    },
    /// Tool the model wants invoked.
    ToolUse {
        /// Invocation id the client echoes back in the next turn's
        /// `tool_result` block.
        id: String,
        /// Selected tool name.
        name: String,
        /// Parsed arguments object.
        input: serde_json::Value,
    },
}

/// Anthropic-shaped token counts. Drops OpenAI's `total_tokens`
/// because Anthropic doesn't carry a total field.
#[derive(Debug, Clone, Serialize)]
pub struct AnthropicUsage {
    /// Prompt tokens (Anthropic name for OpenAI's `prompt_tokens`).
    pub input_tokens: u64,
    /// Completion tokens (Anthropic name for `completion_tokens`).
    pub output_tokens: u64,
}

impl AnthropicUsage {
    /// Build from an OpenAI `Usage`. Missing usage becomes zeros —
    /// Claude Code tolerates that but expects the object to be present.
    pub fn from_openai(usage: Option<OpenAiUsage>) -> Self {
        match usage {
            Some(u) => Self {
                input_tokens: u.prompt_tokens,
                output_tokens: u.completion_tokens,
            },
            None => Self {
                input_tokens: 0,
                output_tokens: 0,
            },
        }
    }
}

// --- Streaming event types ---

/// One event on the Anthropic SSE stream. Anthropic streams are
/// stateful — each event carries an explicit `event: <type>` line
/// and blocks open / close explicitly (unlike OpenAI's flat deltas).
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum StreamEvent {
    /// Opening event — envelope with empty content and best-effort
    /// usage (`input_tokens: 0`, corrected in `message_delta`).
    MessageStart {
        /// Envelope shape mirroring `MessagesResponse` at time zero.
        message: MessageStartEnvelope,
    },
    /// Opens a new content block at `index`.
    ContentBlockStart {
        /// Zero-based block index inside `content`.
        index: u32,
        /// Initial block shape (empty text or empty-input tool_use).
        content_block: OutputContentBlock,
    },
    /// Incremental delta for the block at `index`.
    ContentBlockDelta {
        /// Zero-based block index inside `content`.
        index: u32,
        /// Delta payload (text fragment or JSON-arguments fragment).
        delta: BlockDelta,
    },
    /// Closes the block at `index`.
    ContentBlockStop {
        /// Zero-based block index inside `content`.
        index: u32,
    },
    /// Terminal envelope delta — stop_reason + final usage.
    MessageDelta {
        /// Terminal message-level delta body.
        delta: MessageDeltaBody,
        /// Final usage counters.
        usage: AnthropicUsage,
    },
    /// Terminating event; stream simply closes after this frame.
    MessageStop,
}

/// Envelope carried inside a `MessageStart` event.
#[derive(Debug, Clone, Serialize)]
pub struct MessageStartEnvelope {
    /// Unique message id (`msg_<uuid>`).
    pub id: String,
    /// Object type — always `"message"`.
    #[serde(rename = "type")]
    pub kind: &'static str,
    /// Role — always `"assistant"`.
    pub role: &'static str,
    /// Model producing the response.
    pub model: String,
    /// Empty at start; blocks stream in via `content_block_*` events.
    pub content: Vec<OutputContentBlock>,
    /// Always `None` at start.
    pub stop_reason: Option<&'static str>,
    /// Always `None`.
    pub stop_sequence: Option<String>,
    /// Best-effort usage (both fields zero at start).
    pub usage: AnthropicUsage,
}

/// Delta payload inside a `ContentBlockDelta`.
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum BlockDelta {
    /// Incremental text fragment for a text block.
    TextDelta {
        /// Fragment of text.
        text: String,
    },
    /// Incremental JSON fragment for a tool_use block's `input`.
    InputJsonDelta {
        /// Fragment of JSON-encoded arguments.
        partial_json: String,
    },
}

/// Terminal delta body carried inside a `MessageDelta`.
#[derive(Debug, Clone, Serialize)]
pub struct MessageDeltaBody {
    /// Populated stop reason (`"end_turn"` or `"tool_use"`).
    pub stop_reason: &'static str,
    /// Always `None` today.
    pub stop_sequence: Option<String>,
}

// --- Anthropic-shaped error body ---

/// Wire shape for an Anthropic-side error response.
#[derive(Debug, Clone, Serialize)]
pub struct AnthropicErrorResponse {
    /// Always `"error"`.
    #[serde(rename = "type")]
    pub kind: &'static str,
    /// Error detail.
    pub error: AnthropicErrorDetail,
}

/// Inner detail carried inside an `AnthropicErrorResponse`.
#[derive(Debug, Clone, Serialize)]
pub struct AnthropicErrorDetail {
    /// Machine-readable error category.
    #[serde(rename = "type")]
    pub kind: &'static str,
    /// Human-readable error message.
    pub message: String,
}

// --- Request translation ---

/// Result of translating a `MessagesRequest` into the internal payload
/// shape. Carries both the flat prompt / system_prompt strings the
/// non-passthrough flows expect and the reconstituted OpenAI-shape
/// `messages` + `tools` for passthrough flows.
#[derive(Debug, Clone)]
pub struct TranslatedRequest {
    /// Flat concatenated user prompt.
    pub prompt: String,
    /// Lifted system prompt, or `None` when omitted.
    pub system_prompt: Option<String>,
    /// OpenAI-shape messages, populated only when tools or tool
    /// traffic are present.
    pub messages: Option<Vec<Message>>,
    /// OpenAI-shape tool definitions.
    pub tools: Option<Vec<ToolDefinition>>,
    /// Tool selection strategy — passed through untouched.
    pub tool_choice: Option<serde_json::Value>,
}

/// Translate an Anthropic `MessagesRequest` into a `TranslatedRequest`.
pub fn translate_request(request: &MessagesRequest) -> TranslatedRequest {
    let system_prompt = request.system.as_ref().map(flatten_system);

    let mut prompt_parts: Vec<String> = Vec::new();
    let mut messages: Vec<Message> = Vec::new();

    for input in &request.messages {
        match input.role.as_str() {
            "user" => translate_user_message(&input.content, &mut prompt_parts, &mut messages),
            "assistant" => translate_assistant_message(&input.content, &mut messages),
            other => {
                messages.push(Message {
                    role: other.to_string(),
                    content: flatten_text_content(&input.content),
                    tool_calls: Vec::new(),
                    tool_call_id: None,
                });
            }
        }
    }

    let tools: Option<Vec<ToolDefinition>> = request.tools.as_ref().map(|list| {
        list.iter()
            .map(|t| ToolDefinition {
                kind: TOOL_TYPE_FUNCTION.to_string(),
                function: ToolFunction {
                    name: t.name.clone(),
                    description: t.description.clone(),
                    parameters: t.input_schema.clone(),
                },
            })
            .collect()
    });

    let has_tools = matches!(&tools, Some(list) if !list.is_empty());
    let has_tool_traffic = messages
        .iter()
        .any(|m| !m.tool_calls.is_empty() || m.tool_call_id.is_some());

    let include_messages = has_tools || has_tool_traffic;

    TranslatedRequest {
        prompt: prompt_parts.join("\n"),
        system_prompt: system_prompt.clone(),
        messages: match include_messages {
            true => Some(prepend_system(system_prompt, messages)),
            false => None,
        },
        tools,
        tool_choice: request.tool_choice.clone(),
    }
}

fn flatten_system(system: &SystemPrompt) -> String {
    match system {
        SystemPrompt::Text(s) => s.clone(),
        SystemPrompt::Blocks(blocks) => blocks
            .iter()
            .map(|b| b.text.as_str())
            .collect::<Vec<_>>()
            .join("\n\n"),
    }
}

fn flatten_text_content(content: &MessageContent) -> Option<String> {
    match content {
        MessageContent::Text(s) => Some(s.clone()),
        MessageContent::Blocks(blocks) => {
            let text: Vec<String> = blocks
                .iter()
                .filter_map(|b| match b {
                    ContentBlock::Text { text } => Some(text.clone()),
                    _ => None,
                })
                .collect();
            match text.is_empty() {
                true => None,
                false => Some(text.join("\n")),
            }
        }
    }
}

fn translate_user_message(
    content: &MessageContent,
    prompt_parts: &mut Vec<String>,
    messages: &mut Vec<Message>,
) {
    match content {
        MessageContent::Text(s) => {
            prompt_parts.push(s.clone());
            messages.push(Message {
                role: "user".to_string(),
                content: Some(s.clone()),
                tool_calls: Vec::new(),
                tool_call_id: None,
            });
        }
        MessageContent::Blocks(blocks) => {
            let mut text_buf: Vec<String> = Vec::new();
            for block in blocks {
                match block {
                    ContentBlock::Text { text } => text_buf.push(text.clone()),
                    ContentBlock::ToolResult {
                        tool_use_id,
                        content,
                        ..
                    } => {
                        if !text_buf.is_empty() {
                            let joined = text_buf.join("\n");
                            prompt_parts.push(joined.clone());
                            messages.push(Message {
                                role: "user".to_string(),
                                content: Some(joined),
                                tool_calls: Vec::new(),
                                tool_call_id: None,
                            });
                            text_buf.clear();
                        }
                        messages.push(Message {
                            role: ROLE_TOOL.to_string(),
                            content: Some(flatten_tool_result(content)),
                            tool_calls: Vec::new(),
                            tool_call_id: Some(tool_use_id.clone()),
                        });
                    }
                    ContentBlock::ToolUse { .. } => {
                        // `tool_use` on a user message isn't valid
                        // Anthropic; skip so the transcript stays
                        // coherent for the model.
                    }
                }
            }
            if !text_buf.is_empty() {
                let joined = text_buf.join("\n");
                prompt_parts.push(joined.clone());
                messages.push(Message {
                    role: "user".to_string(),
                    content: Some(joined),
                    tool_calls: Vec::new(),
                    tool_call_id: None,
                });
            }
        }
    }
}

fn translate_assistant_message(content: &MessageContent, messages: &mut Vec<Message>) {
    match content {
        MessageContent::Text(s) => {
            messages.push(Message {
                role: ROLE_ASSISTANT.to_string(),
                content: Some(s.clone()),
                tool_calls: Vec::new(),
                tool_call_id: None,
            });
        }
        MessageContent::Blocks(blocks) => {
            let mut text_buf: Vec<String> = Vec::new();
            let mut tool_calls: Vec<ToolCall> = Vec::new();
            for block in blocks {
                match block {
                    ContentBlock::Text { text } => text_buf.push(text.clone()),
                    ContentBlock::ToolUse { id, name, input } => {
                        tool_calls.push(ToolCall {
                            id: id.clone(),
                            kind: TOOL_TYPE_FUNCTION.to_string(),
                            function: ToolCallFunction {
                                name: name.clone(),
                                arguments: input.to_string(),
                            },
                        });
                    }
                    ContentBlock::ToolResult { .. } => {
                        // `tool_result` belongs on user messages; skip.
                    }
                }
            }
            let text = match text_buf.is_empty() {
                true => None,
                false => Some(text_buf.join("\n")),
            };
            messages.push(Message {
                role: ROLE_ASSISTANT.to_string(),
                content: text,
                tool_calls,
                tool_call_id: None,
            });
        }
    }
}

fn flatten_tool_result(content: &ToolResultContent) -> String {
    match content {
        ToolResultContent::Text(s) => s.clone(),
        ToolResultContent::Blocks(blocks) => blocks
            .iter()
            .filter_map(|b| match b {
                ContentBlock::Text { text } => Some(text.clone()),
                _ => None,
            })
            .collect::<Vec<_>>()
            .join("\n"),
    }
}

fn prepend_system(system: Option<String>, mut messages: Vec<Message>) -> Vec<Message> {
    if let Some(s) = system {
        messages.insert(
            0,
            Message {
                role: ROLE_SYSTEM.to_string(),
                content: Some(s),
                tool_calls: Vec::new(),
                tool_call_id: None,
            },
        );
    }
    messages
}

// --- Response translation ---

/// Build an Anthropic `MessagesResponse` from the leaf's completion.
///
/// `stop_reason` mapping is mechanical: `tool_use` when any tool
/// calls are present, otherwise `end_turn`. `max_tokens` /
/// `stop_sequence` mapping is a follow-up.
pub fn build_response(
    request_id: String,
    model: String,
    text: String,
    tool_calls: Vec<ToolCall>,
    usage: Option<OpenAiUsage>,
) -> MessagesResponse {
    let mut content: Vec<OutputContentBlock> = Vec::new();
    if !text.is_empty() {
        content.push(OutputContentBlock::Text { text });
    }

    let has_tool_calls = !tool_calls.is_empty();
    for call in tool_calls {
        // Anthropic clients expect `input` as a parsed JSON object, but
        // OpenAI-shape `tool_calls.function.arguments` is a JSON-encoded
        // string. Fall back to an empty object when the string is not
        // valid JSON — tool_calls with malformed args shouldn't abort
        // the whole response.
        let input = match serde_json::from_str::<serde_json::Value>(&call.function.arguments) {
            Ok(v) => v,
            Err(_) => serde_json::Value::Object(Default::default()),
        };
        content.push(OutputContentBlock::ToolUse {
            id: call.id,
            name: call.function.name,
            input,
        });
    }

    let stop_reason = match has_tool_calls {
        true => STOP_REASON_TOOL_USE,
        false => STOP_REASON_END_TURN,
    };

    MessagesResponse {
        id: request_id,
        kind: OBJECT_MESSAGE,
        role: ROLE_ASSISTANT,
        model,
        content,
        stop_reason,
        stop_sequence: None,
        usage: AnthropicUsage::from_openai(usage),
    }
}

/// Format an Anthropic SSE frame. Anthropic's SSE requires a leading
/// `event: <type>` line in addition to the `data:` line — the key
/// on-the-wire difference from OpenAI's stream.
pub fn format_sse(event_type: &str, body: &StreamEvent) -> Option<String> {
    let json = serde_json::to_string(body).ok()?;
    Some(format!("event: {event_type}\ndata: {json}\n\n"))
}

/// SSE event-type string carried on the `event:` line for a given
/// `StreamEvent` variant.
pub fn event_type_for(event: &StreamEvent) -> &'static str {
    match event {
        StreamEvent::MessageStart { .. } => "message_start",
        StreamEvent::ContentBlockStart { .. } => "content_block_start",
        StreamEvent::ContentBlockDelta { .. } => "content_block_delta",
        StreamEvent::ContentBlockStop { .. } => "content_block_stop",
        StreamEvent::MessageDelta { .. } => "message_delta",
        StreamEvent::MessageStop => "message_stop",
    }
}

// --- Protocol adapter ---

/// Anthropic Messages API adapter for the shared dispatcher.
pub struct MessagesAdapter;

impl ProtocolAdapter for MessagesAdapter {
    type StreamWriter = MessagesStreamWriter;

    const REQUEST_ID_PREFIX: &'static str = "msg";
    const PROTOCOL_NAME: &'static str = "anthropic";
    const EMIT_DONE_SENTINEL: bool = false;

    fn build_blocking_response(
        request_id: String,
        model: String,
        _created: i64,
        text: String,
        tool_calls: Vec<ToolCall>,
        usage: Option<OpenAiUsage>,
    ) -> Response {
        let response = build_response(request_id, model, text, tool_calls, usage);
        axum::Json(response).into_response()
    }

    fn new_stream_writer(request_id: String, model: String, _created: i64) -> Self::StreamWriter {
        MessagesStreamWriter::new(request_id, model)
    }

    fn error_sse_frame(message: &str) -> String {
        let body = AnthropicErrorResponse {
            kind: "error",
            error: AnthropicErrorDetail {
                kind: "api_error",
                message: message.to_string(),
            },
        };
        match serde_json::to_string(&body) {
            Ok(json) => format!("event: error\ndata: {json}\n\n"),
            Err(_) => String::new(),
        }
    }
}

/// Streaming state machine emitting Anthropic SSE frames.
pub struct MessagesStreamWriter {
    request_id: String,
    model: String,
    next_block_index: u32,
    text_block_open: bool,
    text_block_index: u32,
}

impl MessagesStreamWriter {
    /// Build a new writer scoped to one response.
    pub fn new(request_id: String, model: String) -> Self {
        Self {
            request_id,
            model,
            next_block_index: 0,
            text_block_open: false,
            text_block_index: 0,
        }
    }

    fn emit(events: Vec<StreamEvent>) -> Vec<String> {
        events
            .iter()
            .filter_map(|e| format_sse(event_type_for(e), e))
            .collect()
    }
}

impl StreamWriter for MessagesStreamWriter {
    fn open(&mut self) -> Vec<String> {
        let start = StreamEvent::MessageStart {
            message: MessageStartEnvelope {
                id: self.request_id.clone(),
                kind: OBJECT_MESSAGE,
                role: ROLE_ASSISTANT,
                model: self.model.clone(),
                content: Vec::new(),
                stop_reason: None,
                stop_sequence: None,
                usage: AnthropicUsage {
                    input_tokens: 0,
                    output_tokens: 0,
                },
            },
        };
        Self::emit(vec![start])
    }

    fn text_delta(&mut self, text: String) -> Vec<String> {
        let mut events = Vec::new();
        if !self.text_block_open {
            self.text_block_index = self.next_block_index;
            self.next_block_index += 1;
            self.text_block_open = true;
            events.push(StreamEvent::ContentBlockStart {
                index: self.text_block_index,
                content_block: OutputContentBlock::Text {
                    text: String::new(),
                },
            });
        }
        events.push(StreamEvent::ContentBlockDelta {
            index: self.text_block_index,
            delta: BlockDelta::TextDelta { text },
        });
        Self::emit(events)
    }

    fn tool_calls(&mut self, calls: Vec<ToolCall>) -> Vec<String> {
        let mut events = Vec::new();
        if self.text_block_open {
            events.push(StreamEvent::ContentBlockStop {
                index: self.text_block_index,
            });
            self.text_block_open = false;
        }
        for call in calls {
            let block_index = self.next_block_index;
            self.next_block_index += 1;
            events.push(StreamEvent::ContentBlockStart {
                index: block_index,
                content_block: OutputContentBlock::ToolUse {
                    id: call.id.clone(),
                    name: call.function.name.clone(),
                    input: serde_json::Value::Object(Default::default()),
                },
            });
            events.push(StreamEvent::ContentBlockDelta {
                index: block_index,
                delta: BlockDelta::InputJsonDelta {
                    partial_json: call.function.arguments,
                },
            });
            events.push(StreamEvent::ContentBlockStop { index: block_index });
        }
        Self::emit(events)
    }

    fn close(
        &mut self,
        stop: StopReason,
        usage: Option<OpenAiUsage>,
        _include_usage: bool,
    ) -> Vec<String> {
        let mut events = Vec::new();
        if self.text_block_open {
            events.push(StreamEvent::ContentBlockStop {
                index: self.text_block_index,
            });
            self.text_block_open = false;
        }
        let stop_reason = match stop {
            StopReason::End => STOP_REASON_END_TURN,
            StopReason::ToolUse => STOP_REASON_TOOL_USE,
        };
        events.push(StreamEvent::MessageDelta {
            delta: MessageDeltaBody {
                stop_reason,
                stop_sequence: None,
            },
            usage: AnthropicUsage::from_openai(usage),
        });
        events.push(StreamEvent::MessageStop);
        Self::emit(events)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- Request deserialisation ---

    #[test]
    fn request_deser_plain_text() {
        let json = r#"{
            "model": "ai/kimi",
            "messages": [{"role": "user", "content": "hi"}],
            "max_tokens": 1024
        }"#;
        let req: MessagesRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.model, "ai/kimi");
        assert_eq!(req.max_tokens, 1024);
        assert!(matches!(req.messages[0].content, MessageContent::Text(_)));
    }

    #[test]
    fn request_missing_max_tokens_fails() {
        let json = r#"{
            "model": "ai/kimi",
            "messages": [{"role": "user", "content": "hi"}]
        }"#;
        let err = serde_json::from_str::<MessagesRequest>(json).unwrap_err();
        assert!(err.to_string().contains("max_tokens"));
    }

    #[test]
    fn request_deser_system_as_string() {
        let json = r#"{
            "model": "ai/kimi",
            "messages": [{"role": "user", "content": "hi"}],
            "system": "be nice",
            "max_tokens": 8
        }"#;
        let req: MessagesRequest = serde_json::from_str(json).unwrap();
        assert!(matches!(req.system, Some(SystemPrompt::Text(_))));
    }

    #[test]
    fn request_deser_system_as_blocks() {
        let json = r#"{
            "model": "ai/kimi",
            "messages": [{"role": "user", "content": "hi"}],
            "system": [
                {"type": "text", "text": "part one"},
                {"type": "text", "text": "part two"}
            ],
            "max_tokens": 8
        }"#;
        let req: MessagesRequest = serde_json::from_str(json).unwrap();
        assert!(matches!(req.system, Some(SystemPrompt::Blocks(_))));
    }

    #[test]
    fn request_deser_content_blocks_with_tool_use() {
        let json = r#"{
            "model": "ai/kimi",
            "messages": [{
                "role": "assistant",
                "content": [
                    {"type": "text", "text": "ok"},
                    {"type": "tool_use", "id": "call_1", "name": "bash", "input": {"cmd": "ls"}}
                ]
            }],
            "max_tokens": 8
        }"#;
        let req: MessagesRequest = serde_json::from_str(json).unwrap();
        match &req.messages[0].content {
            MessageContent::Blocks(blocks) => {
                assert_eq!(blocks.len(), 2);
                assert!(matches!(blocks[0], ContentBlock::Text { .. }));
                assert!(matches!(blocks[1], ContentBlock::ToolUse { .. }));
            }
            _ => panic!("expected blocks"),
        }
    }

    #[test]
    fn request_deser_tool_result_block() {
        let json = r#"{
            "model": "ai/kimi",
            "messages": [{
                "role": "user",
                "content": [
                    {"type": "tool_result", "tool_use_id": "call_1", "content": "file1"}
                ]
            }],
            "max_tokens": 8
        }"#;
        let req: MessagesRequest = serde_json::from_str(json).unwrap();
        match &req.messages[0].content {
            MessageContent::Blocks(blocks) => match &blocks[0] {
                ContentBlock::ToolResult {
                    tool_use_id,
                    content,
                    ..
                } => {
                    assert_eq!(tool_use_id, "call_1");
                    assert!(matches!(content, ToolResultContent::Text(_)));
                }
                _ => panic!("expected tool_result"),
            },
            _ => panic!("expected blocks"),
        }
    }

    // --- translate_request ---

    #[test]
    fn translate_request_plain_text_no_messages_field() {
        let req = MessagesRequest {
            model: "ai/kimi".into(),
            messages: vec![InputMessage {
                role: "user".into(),
                content: MessageContent::Text("hello".into()),
            }],
            system: None,
            max_tokens: 8,
            temperature: None,
            stream: false,
            tools: None,
            tool_choice: None,
            stop_sequences: None,
        };
        let t = translate_request(&req);
        assert_eq!(t.prompt, "hello");
        assert!(
            t.messages.is_none(),
            "no tools + no tool traffic → skip messages passthrough"
        );
        assert!(t.tools.is_none());
    }

    #[test]
    fn translate_request_lifts_system() {
        let req = MessagesRequest {
            model: "ai/kimi".into(),
            messages: vec![InputMessage {
                role: "user".into(),
                content: MessageContent::Text("hi".into()),
            }],
            system: Some(SystemPrompt::Text("be nice".into())),
            max_tokens: 8,
            temperature: None,
            stream: false,
            tools: None,
            tool_choice: None,
            stop_sequences: None,
        };
        let t = translate_request(&req);
        assert_eq!(t.system_prompt.as_deref(), Some("be nice"));
    }

    #[test]
    fn translate_request_with_tools_populates_messages_and_prepends_system() {
        let req = MessagesRequest {
            model: "ai/kimi".into(),
            messages: vec![InputMessage {
                role: "user".into(),
                content: MessageContent::Text("hi".into()),
            }],
            system: Some(SystemPrompt::Text("be nice".into())),
            max_tokens: 8,
            temperature: None,
            stream: false,
            tools: Some(vec![AnthropicToolDefinition {
                name: "bash".into(),
                description: None,
                input_schema: serde_json::json!({"type": "object"}),
            }]),
            tool_choice: Some(serde_json::json!("auto")),
            stop_sequences: None,
        };
        let t = translate_request(&req);
        let messages = t.messages.expect("messages present when tools set");
        assert_eq!(messages[0].role, ROLE_SYSTEM);
        assert_eq!(messages[0].content.as_deref(), Some("be nice"));
        assert_eq!(messages[1].role, "user");
        let tools = t.tools.expect("tools translated");
        assert_eq!(tools[0].function.name, "bash");
        assert_eq!(
            tools[0].function.parameters,
            serde_json::json!({"type": "object"})
        );
    }

    #[test]
    fn translate_request_tool_result_becomes_tool_role_message() {
        let req = MessagesRequest {
            model: "ai/kimi".into(),
            messages: vec![InputMessage {
                role: "user".into(),
                content: MessageContent::Blocks(vec![ContentBlock::ToolResult {
                    tool_use_id: "call_1".into(),
                    content: ToolResultContent::Text("file1\nfile2".into()),
                    is_error: None,
                }]),
            }],
            system: None,
            max_tokens: 8,
            temperature: None,
            stream: false,
            tools: None,
            tool_choice: None,
            stop_sequences: None,
        };
        let t = translate_request(&req);
        let messages = t.messages.expect("tool traffic forces messages");
        assert_eq!(messages[0].role, ROLE_TOOL);
        assert_eq!(messages[0].tool_call_id.as_deref(), Some("call_1"));
        assert_eq!(messages[0].content.as_deref(), Some("file1\nfile2"));
    }

    #[test]
    fn translate_request_assistant_tool_use_becomes_tool_call() {
        let req = MessagesRequest {
            model: "ai/kimi".into(),
            messages: vec![InputMessage {
                role: "assistant".into(),
                content: MessageContent::Blocks(vec![
                    ContentBlock::Text {
                        text: "let me check".into(),
                    },
                    ContentBlock::ToolUse {
                        id: "call_1".into(),
                        name: "bash".into(),
                        input: serde_json::json!({"cmd": "ls"}),
                    },
                ]),
            }],
            system: None,
            max_tokens: 8,
            temperature: None,
            stream: false,
            tools: None,
            tool_choice: None,
            stop_sequences: None,
        };
        let t = translate_request(&req);
        let messages = t.messages.expect("assistant tool_use forces messages");
        let asst = &messages[0];
        assert_eq!(asst.role, ROLE_ASSISTANT);
        assert_eq!(asst.content.as_deref(), Some("let me check"));
        assert_eq!(asst.tool_calls.len(), 1);
        assert_eq!(asst.tool_calls[0].id, "call_1");
        assert_eq!(asst.tool_calls[0].function.name, "bash");
        assert_eq!(asst.tool_calls[0].function.arguments, r#"{"cmd":"ls"}"#);
    }

    // --- Response translation ---

    #[test]
    fn build_response_plain_text_end_turn() {
        let resp = build_response(
            "msg_1".into(),
            "kimi".into(),
            "hello".into(),
            Vec::new(),
            None,
        );
        assert_eq!(resp.stop_reason, STOP_REASON_END_TURN);
        assert_eq!(resp.content.len(), 1);
        assert!(matches!(resp.content[0], OutputContentBlock::Text { .. }));
        assert_eq!(resp.usage.input_tokens, 0);
    }

    #[test]
    fn build_response_with_tool_calls_stop_reason_tool_use() {
        let resp = build_response(
            "msg_1".into(),
            "kimi".into(),
            "let me check".into(),
            vec![ToolCall {
                id: "call_1".into(),
                kind: TOOL_TYPE_FUNCTION.into(),
                function: ToolCallFunction {
                    name: "bash".into(),
                    arguments: r#"{"cmd":"ls"}"#.into(),
                },
            }],
            Some(OpenAiUsage {
                prompt_tokens: 42,
                completion_tokens: 7,
                total_tokens: 49,
            }),
        );
        assert_eq!(resp.stop_reason, STOP_REASON_TOOL_USE);
        assert_eq!(resp.content.len(), 2);
        match &resp.content[1] {
            OutputContentBlock::ToolUse { id, name, input } => {
                assert_eq!(id, "call_1");
                assert_eq!(name, "bash");
                assert_eq!(input, &serde_json::json!({"cmd": "ls"}));
            }
            _ => panic!("expected tool_use block"),
        }
        assert_eq!(resp.usage.input_tokens, 42);
        assert_eq!(resp.usage.output_tokens, 7);
    }

    #[test]
    fn build_response_serialises_openai_field_names_dropped() {
        let resp = build_response(
            "msg_1".into(),
            "kimi".into(),
            "hi".into(),
            Vec::new(),
            Some(OpenAiUsage {
                prompt_tokens: 1,
                completion_tokens: 2,
                total_tokens: 3,
            }),
        );
        let v = serde_json::to_value(&resp).unwrap();
        assert_eq!(v["type"], "message");
        assert_eq!(v["role"], "assistant");
        assert_eq!(v["usage"]["input_tokens"], 1);
        assert_eq!(v["usage"]["output_tokens"], 2);
        assert!(v["usage"].get("total_tokens").is_none());
        assert!(v["usage"].get("prompt_tokens").is_none());
    }

    // --- StreamWriter ---

    #[test]
    fn stream_writer_plain_text_sequence() {
        let mut w = MessagesStreamWriter::new("msg_1".into(), "kimi".into());
        let open = w.open();
        assert_eq!(open.len(), 1);
        assert!(open[0].starts_with("event: message_start\n"));

        let first = w.text_delta("hello".into());
        assert_eq!(first.len(), 2);
        assert!(first[0].starts_with("event: content_block_start\n"));
        assert!(first[1].starts_with("event: content_block_delta\n"));

        let more = w.text_delta(" world".into());
        assert_eq!(more.len(), 1);
        assert!(more[0].starts_with("event: content_block_delta\n"));

        let close = w.close(StopReason::End, None, false);
        assert_eq!(close.len(), 3);
        assert!(close[0].starts_with("event: content_block_stop\n"));
        assert!(close[1].starts_with("event: message_delta\n"));
        assert!(close[2].starts_with("event: message_stop\n"));
    }

    #[test]
    fn stream_writer_tool_use_after_text_closes_text_block() {
        let mut w = MessagesStreamWriter::new("msg_1".into(), "kimi".into());
        let _ = w.open();
        let _ = w.text_delta("let me check".into());

        let calls = vec![ToolCall {
            id: "call_1".into(),
            kind: TOOL_TYPE_FUNCTION.into(),
            function: ToolCallFunction {
                name: "bash".into(),
                arguments: r#"{"cmd":"ls"}"#.into(),
            },
        }];
        let events = w.tool_calls(calls);
        assert_eq!(events.len(), 4);
        assert!(events[0].starts_with("event: content_block_stop\n"));
        assert!(events[1].starts_with("event: content_block_start\n"));
        assert!(events[2].starts_with("event: content_block_delta\n"));
        assert!(events[3].starts_with("event: content_block_stop\n"));

        let close = w.close(StopReason::ToolUse, None, false);
        assert_eq!(close.len(), 2);
        assert!(close[0].starts_with("event: message_delta\n"));
        assert!(close[1].starts_with("event: message_stop\n"));
    }

    #[test]
    fn format_sse_carries_event_line() {
        let ev = StreamEvent::MessageStop;
        let sse = format_sse(event_type_for(&ev), &ev).unwrap();
        assert!(sse.starts_with("event: message_stop\n"));
        assert!(sse.contains("data: "));
        assert!(sse.ends_with("\n\n"));
    }
}

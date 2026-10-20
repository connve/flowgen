//! What the AI gateway's protocols share: the `Protocol` selector and the
//! `EventPayload` the gateway writes into `event.data` for a flow.

use serde::{Deserialize, Serialize};

// Re-exports so downstream crates keep resolving OpenAI wire types
// through the well-known `ai_gateway::config::*` path. New code
// should reach for `ai_gateway::protocol::completions::*` directly.
pub use super::protocol::completions::{
    ChatCompletionChunk, ChatCompletionRequest, ChatCompletionResponse, Choice, Delta, Message,
    StreamChoice, StreamOptions, ToolCall, ToolCallDelta, ToolCallDeltaFunction, ToolCallFunction,
    ToolDefinition, ToolFunction, Usage, FINISH_REASON_STOP, FINISH_REASON_TOOL_CALLS,
    OBJECT_CHAT_COMPLETION, OBJECT_CHAT_COMPLETION_CHUNK, ROLE_ASSISTANT, ROLE_SYSTEM, ROLE_TOOL,
    SSE_DONE, TOOL_TYPE_FUNCTION,
};

/// LLM proxy protocol shape.
///
/// Determines the URL layout the AI gateway server mounts for this task. The
/// task's `name` is used as the per-protocol routing key (e.g. for OpenAI it
/// is the prefix in the request body's `model` field).
#[derive(PartialEq, Clone, Copy, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Protocol {
    /// OpenAI-compatible chat completions API.
    /// Mounts `POST <ai_gateway.path>/chat/completions` and
    /// `GET <ai_gateway.path>/models`. Tasks are picked by the request
    /// body's `model` field, sent as `<task-name>/<downstream-model>`.
    #[default]
    Openai,
    /// Anthropic Messages API.
    /// Mounts `POST <ai_gateway.path>/messages`. Same routing
    /// convention as OpenAI — the request body's `model` field is
    /// `<task-name>/<downstream-model>`. Added so clients honouring
    /// `ANTHROPIC_BASE_URL` (Claude Code, official Anthropic SDKs)
    /// can reach downstream OpenAI-compatible providers through the
    /// gateway.
    Anthropic,
}

impl Protocol {
    /// Stable lowercase wire name (`"openai"` / `"anthropic"`), matching the
    /// `PROTOCOL_NAME` the adapters use for logs and the `/models` entry.
    pub fn as_str(&self) -> &'static str {
        match self {
            Protocol::Openai => "openai",
            Protocol::Anthropic => "anthropic",
        }
    }
}

/// Payload the AI gateway server writes into `event.data` before
/// dispatching an incoming request into the flow pipeline.
///
/// Shared across protocols — the OpenAI and Anthropic handlers each
/// translate their wire request into this shape before the pipeline
/// runs, so the leaf `ai_completion` task never learns which protocol
/// the client is using.
///
/// The flat `prompt` / `system_prompt` shape stays canonical for
/// legacy non-passthrough flows. `messages` / `tools` / `tool_choice`
/// carry the full OpenAI-shape request when the client sends tools,
/// so an `ai_completion` task in tool-passthrough mode can rebuild
/// the upstream request without loss. They are skipped on the wire
/// when absent so a tool-less request doesn't pump a redundant
/// message list through Rhai / template rendering.
#[derive(Debug, Clone, Serialize)]
pub struct EventPayload<'a> {
    pub prompt: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub system_prompt: Option<&'a str>,
    /// Downstream model the client picked, or `None` for a bare proxy id —
    /// the routing script then chooses. Absent from `event.data` when `None`,
    /// so scripts branch on presence rather than a sentinel value.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub temperature: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_tokens: Option<u32>,
    pub stream: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub messages: Option<&'a [Message]>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tools: Option<&'a [ToolDefinition]>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tool_choice: Option<&'a serde_json::Value>,
}

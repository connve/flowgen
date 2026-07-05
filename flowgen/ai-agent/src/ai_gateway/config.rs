//! AI gateway task configuration and the internal event payload.
//!
//! The OpenAI wire types live in `super::protocol::completions`; the
//! Anthropic wire types live in `super::protocol::messages`. This
//! module keeps only what is shared across protocols: the `Processor`
//! task config, the `Protocol` selector, and the `EventPayload` the
//! dispatcher writes into `event.data` before dispatching a request
//! into the flow pipeline.
//!
//! Re-exports of the OpenAI wire types are kept here so downstream
//! crates (`app`, `completion::passthrough`, `completion::processor`,
//! integration tests) continue to reach them through
//! `ai_gateway::config::*` without touching their imports.

use flowgen_core::config::ConfigExt;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

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

// --- Task configuration ---

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

/// LLM proxy task configuration.
///
/// Registers a flow as a backend on the shared AI gateway server (configured
/// under `worker.ai_gateway`). The `protocol` selects which URL layout the
/// server exposes; the task's `name` is the routing key clients use to pick
/// this backend within that protocol.
///
/// # Example
///
/// ```yaml
/// tasks:
///   - llm_proxy:
///       name: proxy
///       # protocol: openai  # default; mounts /v1/chat/completions
///       credentials_path: /etc/proxy/credentials.json
///
///   - ai_completion:
///       name: complete
///       provider: google_vertex
///       model: gemini-2.0-flash
///       stream: true
///       prompt: "{{event.data.prompt}}"
/// ```
///
/// Clients then send `model: "proxy/<downstream-model>"` to reach this proxy.
#[derive(PartialEq, Clone, Debug, Deserialize, Serialize)]
pub struct Processor {
    /// The unique name / identifier of the task. Used as the routing key
    /// inside the chosen protocol (e.g. the prefix of OpenAI's `model` field).
    pub name: String,
    /// Wire protocol exposed for this task. Defaults to OpenAI-compatible.
    #[serde(default)]
    pub protocol: Protocol,
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

// --- Pipeline event payload ---

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
    pub model: &'a str,
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

#[cfg(test)]
mod tests {
    use super::*;

    // --- Processor config deserialization ---

    #[test]
    fn processor_deser_minimal() {
        let json = r#"{
            "name": "proxy"
        }"#;
        let cfg: Processor = serde_json::from_str(json).unwrap();
        assert_eq!(cfg.name, "proxy");
        assert_eq!(cfg.protocol, Protocol::Openai);
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
            "ack_timeout": "30s"
        }"#;
        let cfg: Processor = serde_json::from_str(json).unwrap();
        assert_eq!(cfg.ack_timeout, Some(std::time::Duration::from_secs(30)));
    }

    #[test]
    fn processor_deser_with_depends_on() {
        let json = r#"{
            "name": "proxy",
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
            protocol: Protocol::Openai,
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
}

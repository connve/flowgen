//! LLM proxy processor configuration.
//!
//! Provides an OpenAI-compatible `/v1/chat/completions` endpoint that routes
//! to any configured AI provider via the Rig framework.

use crate::completion::config::{McpServerConfig, Provider};
use flowgen_core::config::ConfigExt;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// LLM proxy processor configuration.
///
/// Exposes an OpenAI-compatible chat completions endpoint that routes requests
/// to the configured AI provider. Supports streaming and non-streaming responses.
///
/// # Example
///
/// ```yaml
/// llm_proxy:
///   name: vertex_proxy
///   path: /v1/chat/completions
///   provider: google_vertex
///   model: gemini-2.0-flash
///   credentials_path: /etc/gcp/credentials.json
///   mcp_servers:
///     - url: "http://localhost:3001/mcp"
/// ```
#[derive(PartialEq, Clone, Debug, Deserialize, Serialize)]
pub struct Processor {
    /// The unique name / identifier of the task.
    pub name: String,
    /// HTTP path to register the endpoint on (e.g., "/v1/chat/completions").
    pub path: String,
    /// AI provider to use.
    pub provider: Provider,
    /// Default model identifier. Can be overridden per-request via the request body.
    pub model: String,
    /// Optional path to credentials file containing API keys.
    pub credentials_path: Option<PathBuf>,
    /// Optional custom endpoint URL (for Ollama, LM Studio, or other OpenAI-compatible servers).
    pub endpoint: Option<String>,
    /// Optional default system prompt. Can be overridden per-request via messages.
    pub system_prompt: Option<String>,
    /// Optional default temperature (0.0-1.0). Can be overridden per-request.
    pub temperature: Option<f32>,
    /// Optional default max tokens. Can be overridden per-request.
    pub max_tokens: Option<u32>,
    /// Optional maximum number of recursive agent turns for tool calling.
    pub max_turns: Option<usize>,
    /// Optional MCP server URLs to connect to for tool discovery.
    #[serde(default)]
    pub mcp_servers: Vec<McpServerConfig>,
    /// Optional path to credentials file for authenticating incoming requests.
    /// Uses the same format as http_webhook credentials (bearer_auth / basic_auth).
    pub auth_credentials_path: Option<PathBuf>,
    /// Optional list of upstream task names this task depends on.
    #[serde(default)]
    pub depends_on: Option<Vec<String>>,
    /// Optional retry configuration.
    #[serde(default)]
    pub retry: Option<flowgen_core::retry::RetryConfig>,
}

impl ConfigExt for Processor {}

//! Completion processor configuration structures.
//!
//! Provides configuration for AI completion processors that generate text
//! responses from prompts, with support for streaming, RAG (Retrieval-Augmented
//! Generation), and multi-turn agent conversations.

use flowgen_core::config::ConfigExt;
use flowgen_core::resource::Source;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// AI completion processor configuration.
///
/// Supports standard and streaming completion modes, with RAG (static context documents),
/// multi-turn agent conversations, and resource loading for prompts.
///
/// # Examples
///
/// Basic completion with inline prompt:
/// ```yaml
/// ai_completion:
///   name: summarizer
///   provider: openai
///   model: gpt-4
///   credentials_path: /secrets/openai.json
///   system_prompt: "You are a concise summarizer"
///   prompt: "Summarize this data: {{event.data}}"
///   temperature: 0.3
///   max_tokens: 500
/// ```
///
/// Using external prompt files with resource loader:
/// ```yaml
/// ai_completion:
///   name: analyst
///   provider: anthropic
///   model: claude-3-5-sonnet-20241022
///   credentials_path: /secrets/anthropic.json
///   system_prompt:
///     resource: "prompts/analyst_system.md"
///   prompt:
///     resource: "prompts/analyze_data.md"
/// ```
///
/// RAG with static context documents (inline and resource files):
/// ```yaml
/// ai_completion:
///   name: qa_bot
///   provider: openai
///   model: gpt-4-turbo
///   credentials_path: /secrets/openai.json
///   system_prompt: "Answer questions based on the provided context"
///   prompt: "Question: {{event.data.question}}"
///   static_context:
///     - resource: "context/product_docs.md"
///     - resource: "context/pricing.md"
///     - "Inline context: Support email is support@example.com"
///   max_turns: 3
/// ```
#[derive(PartialEq, Clone, Debug, Deserialize, Serialize)]
pub struct Processor {
    /// The unique name / identifier of the task.
    pub name: String,
    /// AI provider to use (e.g., "openai", "anthropic", "cohere").
    pub provider: Provider,
    /// Model identifier (e.g., "gpt-4", "claude-3-5-sonnet-20241022").
    pub model: String,
    /// User prompt template (supports handlebars templating and resource loading).
    /// Can be inline string or external file via resource loader.
    pub prompt: Source,
    /// Optional path to credentials file containing API keys.
    pub credentials_path: Option<PathBuf>,
    /// Optional custom endpoint URL (for custom or self-hosted providers).
    /// Example: "http://localhost:11434/v1" for Ollama, "http://localhost:1234/v1" for LM Studio.
    pub endpoint: Option<String>,
    /// Optional system prompt to set context for the AI (supports resource loading).
    /// Can be inline string or external file (e.g., .md, .txt) via resource loader.
    pub system_prompt: Option<Source>,
    /// Optional temperature setting for response randomness (0.0-1.0).
    pub temperature: Option<f32>,
    /// Optional maximum tokens for the response.
    pub max_tokens: Option<u32>,
    /// Optional static context documents for RAG (Retrieval-Augmented Generation).
    /// These documents are always available to the agent for answering questions.
    /// Can be inline strings or external files via resource loader.
    #[serde(default)]
    pub static_context: Option<Vec<Source>>,
    /// Optional maximum number of recursive agent turns (prevents infinite loops in multi-turn conversations).
    /// Default is unlimited. Set to prevent excessive API calls in agentic workflows.
    pub max_turns: Option<usize>,
    /// Enable streaming mode (sends chunks as separate events).
    #[serde(default)]
    pub stream: bool,
    /// Bypass rig's `Agent` tool-execution loop and forward
    /// caller-supplied `tools`/`tool_choice` straight to the upstream
    /// provider. Tool invocations returned by the model are surfaced
    /// verbatim as `tool_calls` on the response instead of being
    /// executed in-process. Use this when a client (e.g. opencode,
    /// Claude Desktop) will execute the tools itself and only needs
    /// the AI gateway as a protocol proxy. Requires the client to
    /// supply `tools: [...]` in the request body; a request without
    /// tools follows the normal completion path even when this flag
    /// is on.
    #[serde(default)]
    pub tool_passthrough: bool,
    /// Optional MCP server URLs to connect to for tool discovery.
    /// The agent will connect to each MCP server, discover available tools,
    /// and make them callable during completions. Supports both flowgen's
    /// own MCP server and external MCP-compatible servers.
    ///
    /// Example:
    /// ```yaml
    /// mcp_servers:
    ///   - url: "http://localhost:3001/mcp"
    ///   - url: "http://external-tools:8080/mcp"
    /// ```
    #[serde(default)]
    pub mcp_servers: Vec<McpServerConfig>,
    /// Optional sandbox configuration for agent tool execution.
    /// When Some, tools are sandboxed for security. When None, tools run without sandbox.
    #[serde(default)]
    pub sandbox: Option<flowgen_core::nsjail::SandboxConfig>,
    /// Optional list of upstream task names this task depends on.
    /// When set, this task only receives events from the named tasks.
    /// When not set, the task receives from the previous task in the list (linear chain).
    #[serde(default)]
    pub depends_on: Option<Vec<String>>,
    /// Optional retry configuration (overrides app-level retry config).
    #[serde(default)]
    pub retry: Option<flowgen_core::retry::RetryConfig>,
    /// Optional extended-thinking / reasoning configuration.
    #[serde(default)]
    pub thinking: Option<ThinkingConfig>,
}

/// Extended-thinking / reasoning configuration.
///
/// `effort` is the portable knob; it maps to `reasoning_effort` for
/// OpenAI-shape providers and to a token budget for Anthropic/Gemini.
#[derive(PartialEq, Clone, Debug, Deserialize, Serialize)]
pub struct ThinkingConfig {
    pub effort: Effort,
    /// Whether to receive the reasoning trace in the stream.
    #[serde(default = "default_include_trace")]
    pub include_trace: bool,
}

fn default_include_trace() -> bool {
    true
}

/// Reasoning intensity levels.
#[derive(PartialEq, Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Effort {
    Low,
    Medium,
    High,
}

impl ThinkingConfig {
    /// Renders `additional_params` for the given provider; `None` when
    /// the provider does not expose extended thinking.
    pub fn to_additional_params(
        &self,
        provider: &Provider,
    ) -> Result<Option<serde_json::Value>, serde_json::Error> {
        match provider {
            Provider::Anthropic => {
                let params = AnthropicThinkingParams {
                    thinking: AnthropicThinking {
                        r#type: "enabled",
                        budget_tokens: self.anthropic_budget_tokens(),
                    },
                    include_thoughts: (!self.include_trace).then_some(false),
                };
                serde_json::to_value(params).map(Some)
            }
            Provider::Google | Provider::VertexAi => {
                let params = GeminiThinkingParams {
                    generation_config: GeminiGenerationConfig {
                        thinking_config: GeminiThinkingBlock {
                            thinking_budget: self.gemini_budget_tokens(),
                            include_thoughts: self.include_trace,
                        },
                    },
                };
                serde_json::to_value(params).map(Some)
            }
            Provider::OpenAi | Provider::Xai | Provider::OpenRouter => {
                let params = ReasoningEffortParams {
                    reasoning_effort: self.effort,
                };
                serde_json::to_value(params).map(Some)
            }
            _ => Ok(None),
        }
    }

    fn anthropic_budget_tokens(&self) -> u32 {
        match self.effort {
            Effort::Low => 1024,
            Effort::Medium => 4096,
            Effort::High => 16000,
        }
    }

    fn gemini_budget_tokens(&self) -> u32 {
        match self.effort {
            Effort::Low => 1024,
            Effort::Medium => 8192,
            Effort::High => 24576,
        }
    }
}

#[derive(Serialize)]
struct AnthropicThinkingParams {
    thinking: AnthropicThinking,
    // Only emit when the caller opted out; Anthropic defaults to on.
    #[serde(skip_serializing_if = "Option::is_none")]
    include_thoughts: Option<bool>,
}

#[derive(Serialize)]
struct AnthropicThinking {
    r#type: &'static str,
    budget_tokens: u32,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct GeminiThinkingParams {
    generation_config: GeminiGenerationConfig,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct GeminiGenerationConfig {
    thinking_config: GeminiThinkingBlock,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct GeminiThinkingBlock {
    thinking_budget: u32,
    include_thoughts: bool,
}

#[derive(Serialize)]
struct ReasoningEffortParams {
    reasoning_effort: Effort,
}

/// Configuration for connecting to an MCP server.
#[derive(PartialEq, Clone, Debug, Deserialize, Serialize)]
pub struct McpServerConfig {
    /// MCP server endpoint URL (e.g., "http://localhost:3001/mcp").
    pub url: String,
    /// Optional path to credentials file for authenticating with the MCP server.
    /// Uses the same format as http_request credentials (JSON file with
    /// `bearer_auth` and/or `basic_auth` fields).
    pub credentials_path: Option<std::path::PathBuf>,
}

impl ConfigExt for Processor {}

/// AI provider options.
///
/// Providers not listed here can be reached via `Provider::Custom` with
/// an OpenAI-compatible endpoint in front.
#[derive(PartialEq, Clone, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Provider {
    /// OpenAI provider (GPT models).
    #[default]
    OpenAi,
    /// Anthropic provider (Claude models).
    Anthropic,
    /// Cohere provider.
    Cohere,
    /// Google AI provider (Gemini API).
    Google,
    /// Google Vertex AI provider (uses Application Default Credentials).
    VertexAi,
    /// Groq provider.
    Groq,
    /// Hugging Face provider.
    HuggingFace,
    /// Mistral provider.
    Mistral,
    /// Ollama provider (OpenAI-compatible; requires `endpoint`).
    Ollama,
    /// OpenRouter provider.
    OpenRouter,
    /// Perplexity provider.
    Perplexity,
    /// Together AI provider.
    Together,
    /// xAI (Grok) provider.
    Xai,
    /// Custom OpenAI-compatible provider (requires `endpoint`).
    Custom,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_processor_basic() {
        let processor = Processor {
            name: "test".to_string(),
            provider: Provider::OpenAi,
            model: "gpt-4".to_string(),
            prompt: Source::Inline("Test prompt".to_string()),
            credentials_path: None,
            endpoint: None,
            system_prompt: None,
            temperature: None,
            max_tokens: None,
            static_context: None,
            max_turns: None,
            stream: false,
            tool_passthrough: false,
            mcp_servers: vec![],
            sandbox: Default::default(),
            depends_on: None,
            retry: None,
            thinking: None,
        };

        assert_eq!(processor.name, "test");
        assert_eq!(processor.provider, Provider::OpenAi);
        assert_eq!(processor.model, "gpt-4");
        assert!(matches!(processor.prompt, Source::Inline(_)));
        assert_eq!(processor.credentials_path, None);
        assert_eq!(processor.endpoint, None);
        assert_eq!(processor.system_prompt, None);
        assert_eq!(processor.temperature, None);
        assert_eq!(processor.max_tokens, None);
        assert_eq!(processor.static_context, None);
        assert_eq!(processor.max_turns, None);
        assert!(!processor.stream);
        assert_eq!(processor.retry, None);
    }

    #[test]
    fn test_processor_serialization() {
        let processor = Processor {
            name: "test_completion".to_string(),
            provider: Provider::Anthropic,
            model: "claude-3-5-sonnet-20241022".to_string(),
            prompt: Source::Inline("Test prompt: {{event.data}}".to_string()),
            credentials_path: Some(PathBuf::from("/secrets/anthropic.json")),
            endpoint: None,
            system_prompt: Some(Source::Inline("You are a helpful assistant.".to_string())),
            temperature: Some(0.7),
            max_tokens: Some(1000),
            static_context: Some(vec![
                Source::Inline("Context doc 1".to_string()),
                Source::Inline("Context doc 2".to_string()),
            ]),
            max_turns: Some(5),
            stream: true,
            tool_passthrough: false,
            mcp_servers: vec![],
            sandbox: Default::default(),
            depends_on: None,
            retry: None,
            thinking: None,
        };

        let json = serde_json::to_string(&processor).unwrap();
        let deserialized: Processor = serde_json::from_str(&json).unwrap();
        assert_eq!(processor, deserialized);
    }

    #[test]
    fn test_provider_variants() {
        assert_eq!(Provider::default(), Provider::OpenAi);

        let providers = vec![
            Provider::OpenAi,
            Provider::Anthropic,
            Provider::Cohere,
            Provider::Google,
            Provider::Custom,
        ];

        for provider in providers {
            let json = serde_json::to_string(&provider).unwrap();
            let deserialized: Provider = serde_json::from_str(&json).unwrap();
            assert_eq!(provider, deserialized);
        }
    }

    #[test]
    fn test_config_ext_trait() {
        let processor = Processor {
            name: "test".to_string(),
            provider: Provider::OpenAi,
            model: "gpt-4".to_string(),
            prompt: Source::Inline("Test".to_string()),
            credentials_path: None,
            endpoint: None,
            system_prompt: None,
            temperature: None,
            max_tokens: None,
            static_context: None,
            max_turns: None,
            stream: false,
            tool_passthrough: false,
            mcp_servers: vec![],
            sandbox: Default::default(),
            depends_on: None,
            retry: None,
            thinking: None,
        };
        let _: &dyn ConfigExt = &processor;
    }

    #[test]
    fn test_custom_provider_with_endpoint() {
        let processor = Processor {
            name: "ollama_completion".to_string(),
            provider: Provider::Custom,
            model: "llama2".to_string(),
            prompt: Source::Inline("Process: {{event.data}}".to_string()),
            credentials_path: None, // Local providers may not need credentials.
            endpoint: Some("http://localhost:11434/v1".to_string()),
            system_prompt: None,
            temperature: Some(0.8),
            max_tokens: Some(2000),
            static_context: None,
            max_turns: None,
            stream: true,
            tool_passthrough: false,
            mcp_servers: vec![],
            sandbox: Default::default(),
            depends_on: None,
            retry: None,
            thinking: None,
        };

        let json = serde_json::to_string(&processor).unwrap();
        let deserialized: Processor = serde_json::from_str(&json).unwrap();
        assert_eq!(processor, deserialized);
    }

    #[test]
    fn test_resource_prompt() {
        let processor = Processor {
            name: "resource_test".to_string(),
            provider: Provider::OpenAi,
            model: "gpt-4".to_string(),
            prompt: Source::Resource {
                resource: "prompts/analyze.md".to_string(),
            },
            credentials_path: Some(PathBuf::from("/secrets/openai.json")),
            endpoint: None,
            system_prompt: Some(Source::Resource {
                resource: "prompts/system.md".to_string(),
            }),
            temperature: Some(0.5),
            max_tokens: Some(1500),
            static_context: None,
            max_turns: None,
            stream: false,
            tool_passthrough: false,
            mcp_servers: vec![],
            sandbox: Default::default(),
            depends_on: None,
            retry: None,
            thinking: None,
        };

        let json = serde_json::to_string(&processor).unwrap();
        let deserialized: Processor = serde_json::from_str(&json).unwrap();
        assert_eq!(processor, deserialized);
    }

    #[test]
    fn test_rag_with_resource_context() {
        let processor = Processor {
            name: "rag_test".to_string(),
            provider: Provider::OpenAi,
            model: "gpt-4-turbo".to_string(),
            prompt: Source::Inline("Question: {{event.data.question}}".to_string()),
            credentials_path: Some(PathBuf::from("/secrets/openai.json")),
            endpoint: None,
            system_prompt: Some(Source::Inline("Answer based on context".to_string())),
            temperature: Some(0.3),
            max_tokens: Some(500),
            static_context: Some(vec![
                Source::Resource {
                    resource: "context/product_docs.md".to_string(),
                },
                Source::Resource {
                    resource: "context/pricing.md".to_string(),
                },
                Source::Inline("Support: support@example.com".to_string()),
            ]),
            max_turns: Some(3),
            stream: false,
            tool_passthrough: false,
            mcp_servers: vec![],
            sandbox: Default::default(),
            depends_on: None,
            retry: None,
            thinking: None,
        };

        let json = serde_json::to_string(&processor).unwrap();
        let deserialized: Processor = serde_json::from_str(&json).unwrap();
        assert_eq!(processor, deserialized);
    }

    #[test]
    fn thinking_params_anthropic() {
        let cfg = ThinkingConfig {
            effort: Effort::Medium,
            include_trace: true,
        };
        let v = cfg
            .to_additional_params(&Provider::Anthropic)
            .unwrap()
            .unwrap();
        assert_eq!(v["thinking"]["type"], "enabled");
        assert_eq!(v["thinking"]["budget_tokens"], 4096);
        assert!(v.get("include_thoughts").is_none());
    }

    #[test]
    fn thinking_params_anthropic_no_trace() {
        let cfg = ThinkingConfig {
            effort: Effort::Low,
            include_trace: false,
        };
        let v = cfg
            .to_additional_params(&Provider::Anthropic)
            .unwrap()
            .unwrap();
        assert_eq!(v["thinking"]["budget_tokens"], 1024);
        assert_eq!(v["include_thoughts"], false);
    }

    #[test]
    fn thinking_params_gemini() {
        let cfg = ThinkingConfig {
            effort: Effort::High,
            include_trace: true,
        };
        let v = cfg
            .to_additional_params(&Provider::Google)
            .unwrap()
            .unwrap();
        let block = &v["generationConfig"]["thinkingConfig"];
        assert_eq!(block["thinkingBudget"], 24576);
        assert_eq!(block["includeThoughts"], true);
    }

    #[test]
    fn thinking_params_openai_effort() {
        let cfg = ThinkingConfig {
            effort: Effort::Medium,
            include_trace: true,
        };
        let v = cfg
            .to_additional_params(&Provider::OpenAi)
            .unwrap()
            .unwrap();
        assert_eq!(v["reasoning_effort"], "medium");
    }

    #[test]
    fn thinking_params_unsupported_provider() {
        let cfg = ThinkingConfig {
            effort: Effort::High,
            include_trace: true,
        };
        let cohere = cfg.to_additional_params(&Provider::Cohere).unwrap();
        let groq = cfg.to_additional_params(&Provider::Groq).unwrap();
        assert!(cohere.is_none());
        assert!(groq.is_none());
    }

    #[test]
    fn thinking_include_trace_defaults_to_true() {
        let cfg: ThinkingConfig = serde_json::from_str(r#"{"effort":"low"}"#).unwrap();
        assert!(cfg.include_trace);
    }
}

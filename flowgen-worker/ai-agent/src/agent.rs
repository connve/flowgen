//! AI provider client builders and abstractions.
//!
//! Provides unified client creation for different AI providers through the Rig framework.
//!
//! Uses an enum-based pattern as recommended by Rig's official examples for handling
//! multiple providers without the deprecated CompletionClientDyn trait.

use crate::completion::config::Provider;
use crate::completion::processor::Credentials;
use futures_util::stream::Stream;
use futures_util::StreamExt;
use rig::agent::{Agent, MultiTurnStreamItem};
use rig::client::CompletionClient as RigCompletionClientTrait;
use rig::completion::{Document, Prompt};
use rig::message::ToolChoice;
use rig::streaming::StreamedAssistantContent;
use rig::streaming::StreamingPrompt;
use rig::tool::server::ToolServerHandle;
use rig::vector_store::VectorStoreIndexDyn;
use schemars::Schema;
use serde_json::Value;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Type alias for dynamic context storage (vector store indexes with sample sizes).
type DynamicContext = Arc<RwLock<Vec<(usize, Box<dyn VectorStoreIndexDyn + Send + Sync>)>>>;

/// Streaming chunk from completion.
#[derive(Debug, Clone)]
pub enum CompletionChunk {
    /// Text chunk from the stream.
    Text(String),
    /// Final response with complete text.
    Final(String),
    /// Error during streaming.
    Error(String),
}

/// Errors that can occur during client creation and completion.
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum Error {
    #[error("Missing credentials for provider")]
    MissingCredentials,
    #[error("Custom provider requires endpoint configuration")]
    MissingEndpoint,
    #[error("Provider not yet implemented: {provider}")]
    ProviderNotImplemented { provider: String },
    #[error("OpenAI client creation failed: {source}")]
    OpenAIClient {
        #[source]
        source: rig::http_client::Error,
    },
    #[error("Anthropic client creation failed: {source}")]
    AnthropicClient {
        #[source]
        source: rig::http_client::Error,
    },
    #[error("Cohere client creation failed: {source}")]
    CohereClient {
        #[source]
        source: rig::http_client::Error,
    },
    #[error("Google/Gemini client creation failed: {source}")]
    GeminiClient {
        #[source]
        source: rig::http_client::Error,
    },
    #[error("Mistral client creation failed: {source}")]
    MistralClient {
        #[source]
        source: rig::http_client::Error,
    },
    #[error("Groq client creation failed: {source}")]
    GroqClient {
        #[source]
        source: rig::http_client::Error,
    },
    #[error("Together client creation failed: {source}")]
    TogetherClient {
        #[source]
        source: rig::http_client::Error,
    },
    #[error("xAI client creation failed: {source}")]
    XaiClient {
        #[source]
        source: rig::http_client::Error,
    },
    #[error("OpenRouter client creation failed: {source}")]
    OpenRouterClient {
        #[source]
        source: rig::http_client::Error,
    },
    #[error("Perplexity client creation failed: {source}")]
    PerplexityClient {
        #[source]
        source: rig::http_client::Error,
    },
    #[error("HuggingFace client creation failed: {source}")]
    HuggingFaceClient {
        #[source]
        source: rig::http_client::Error,
    },
    #[error("Completion request failed: {source}")]
    CompletionRequest {
        #[source]
        source: rig::completion::PromptError,
    },
}

/// Client builder for creating AI provider clients.
#[derive(Default)]
pub struct ClientBuilder {
    /// AI provider to use.
    provider: Provider,
    /// Model identifier.
    model: String,
    /// Optional API credentials.
    credentials: Option<Credentials>,
    /// Optional custom endpoint URL.
    endpoint: Option<String>,
    /// Optional agent name (for logging and debugging).
    name: Option<String>,
    /// Optional agent description (useful for sub-agents and workflows).
    description: Option<String>,
    /// Optional temperature setting (0.0-1.0).
    temperature: Option<f32>,
    /// Optional maximum tokens for response.
    max_tokens: Option<u32>,
    /// Static context documents always available to the agent (RAG).
    static_context: Vec<Document>,
    /// Dynamic context from vector stores (RAG).
    dynamic_context: DynamicContext,
    /// Optional tool server handle for providing tools to the agent.
    tool_server_handle: Option<ToolServerHandle>,
    /// Optional tool choice strategy (auto, required, specific tool).
    tool_choice: Option<ToolChoice>,
    /// Optional maximum number of recursive agent turns (prevents infinite loops).
    max_turns: Option<usize>,
    /// Optional JSON schema for structured output (provider must support it).
    output_schema: Option<Schema>,
    /// Optional additional parameters passed directly to the model.
    additional_params: Option<Value>,
}

impl ClientBuilder {
    /// Creates a new client builder for the specified provider and model.
    pub fn new(provider: Provider, model: String) -> Self {
        Self {
            provider,
            model,
            credentials: None,
            endpoint: None,
            name: None,
            description: None,
            temperature: None,
            max_tokens: None,
            static_context: Vec::new(),
            dynamic_context: Arc::new(RwLock::new(Vec::new())),
            tool_server_handle: None,
            tool_choice: None,
            max_turns: None,
            output_schema: None,
            additional_params: None,
        }
    }

    /// Sets the API credentials.
    pub fn credentials(mut self, credentials: Credentials) -> Self {
        self.credentials = Some(credentials);
        self
    }

    /// Sets a custom endpoint URL (for Ollama, LM Studio, or other OpenAI-compatible servers).
    pub fn endpoint(mut self, endpoint: String) -> Self {
        self.endpoint = Some(endpoint);
        self
    }

    /// Sets the temperature for response randomness (0.0-1.0).
    pub fn temperature(mut self, temperature: f32) -> Self {
        self.temperature = Some(temperature);
        self
    }

    /// Sets the maximum number of tokens in the response.
    pub fn max_tokens(mut self, max_tokens: u32) -> Self {
        self.max_tokens = Some(max_tokens);
        self
    }

    /// Sets the agent name (used for logging and debugging).
    pub fn name(mut self, name: String) -> Self {
        self.name = Some(name);
        self
    }

    /// Sets the agent description (useful for sub-agents and workflows).
    pub fn description(mut self, description: String) -> Self {
        self.description = Some(description);
        self
    }

    /// Adds a static context document to the agent (RAG).
    pub fn static_context_document(mut self, document: Document) -> Self {
        self.static_context.push(document);
        self
    }

    /// Sets multiple static context documents for the agent (RAG).
    pub fn static_context(mut self, documents: Vec<Document>) -> Self {
        self.static_context = documents;
        self
    }

    /// Adds a dynamic context vector store index to the agent (RAG).
    pub fn dynamic_context_index(
        self,
        sample_size: usize,
        index: Box<dyn VectorStoreIndexDyn + Send + Sync>,
    ) -> Self {
        if let Ok(mut context) = self.dynamic_context.try_write() {
            context.push((sample_size, index));
        }
        self
    }

    /// Sets the tool server handle that provides tools to the agent.
    ///
    /// Tools are managed through a ToolServerHandle which registers and executes tools.
    pub fn tool_server_handle(mut self, handle: ToolServerHandle) -> Self {
        self.tool_server_handle = Some(handle);
        self
    }

    /// Sets the tool choice strategy (auto, required, or specific tool).
    pub fn tool_choice(mut self, choice: ToolChoice) -> Self {
        self.tool_choice = Some(choice);
        self
    }

    /// Sets the maximum number of recursive agent turns (prevents infinite loops in tool calling).
    pub fn max_turns(mut self, max_turns: usize) -> Self {
        self.max_turns = Some(max_turns);
        self
    }

    /// Sets the JSON schema for structured output (provider must support structured outputs).
    pub fn output_schema(mut self, schema: Schema) -> Self {
        self.output_schema = Some(schema);
        self
    }

    /// Sets additional parameters to pass directly to the model.
    pub fn additional_params(mut self, params: Value) -> Self {
        self.additional_params = Some(params);
        self
    }

    /// Builds an agent client for the configured provider.
    pub fn build(self) -> Result<AgentClient, Error> {
        // Helper macro to reduce boilerplate for standard providers.
        macro_rules! build_provider {
            ($provider_mod:ident, $variant:ident, $error:ident) => {{
                let credentials = self.credentials.ok_or(Error::MissingCredentials)?;
                let client = rig::providers::$provider_mod::Client::new(&credentials.api_key)
                    .map_err(|source| Error::$error { source })?;
                Ok(AgentClient {
                    provider: self.provider,
                    model: self.model,
                    name: self.name,
                    description: self.description,
                    temperature: self.temperature.map(|t| t as f64),
                    max_tokens: self.max_tokens.map(|t| t as u64),
                    static_context: self.static_context,
                    dynamic_context: self.dynamic_context,
                    tool_server_handle: self.tool_server_handle,
                    tool_choice: self.tool_choice,
                    max_turns: self.max_turns,
                    output_schema: self.output_schema,
                    additional_params: self.additional_params,
                    client: ProviderClient::$variant(client),
                })
            }};
        }

        match self.provider {
            Provider::OpenAi => build_provider!(openai, OpenAi, OpenAIClient),
            Provider::Anthropic => build_provider!(anthropic, Anthropic, AnthropicClient),
            Provider::Cohere => build_provider!(cohere, Cohere, CohereClient),
            Provider::Google => build_provider!(gemini, Gemini, GeminiClient),
            Provider::Mistral => build_provider!(mistral, Mistral, MistralClient),
            Provider::Groq => build_provider!(groq, Groq, GroqClient),
            Provider::Together => build_provider!(together, Together, TogetherClient),
            Provider::Xai => build_provider!(xai, Xai, XaiClient),
            Provider::OpenRouter => build_provider!(openrouter, OpenRouter, OpenRouterClient),
            Provider::Perplexity => build_provider!(perplexity, Perplexity, PerplexityClient),
            Provider::HuggingFace => build_provider!(huggingface, HuggingFace, HuggingFaceClient),
            Provider::Ollama | Provider::Custom => self.build_openai_compatible_provider(),
            _ => Err(Error::ProviderNotImplemented {
                provider: format!("{:?}", self.provider),
            }),
        }
    }

    fn build_openai_compatible_provider(mut self) -> Result<AgentClient, Error> {
        // Custom and Ollama providers use OpenAI-compatible endpoints (requires explicit endpoint).
        let endpoint = self.endpoint.take().ok_or(Error::MissingEndpoint)?;
        self.build_openai_compatible(endpoint)
    }

    fn build_openai_compatible(self, endpoint: String) -> Result<AgentClient, Error> {
        // Helper for building OpenAI-compatible clients (Ollama, LM Studio, custom endpoints).
        // Credentials are optional for local providers that don't require authentication.
        let api_key = self.credentials.map(|c| c.api_key).unwrap_or_default();

        let client = rig::providers::openai::Client::builder()
            .api_key(&api_key)
            .base_url(&endpoint)
            .build()
            .map_err(|source| Error::OpenAIClient { source })?;

        Ok(AgentClient {
            provider: self.provider,
            model: self.model,
            name: self.name,
            description: self.description,
            temperature: self.temperature.map(|t| t as f64),
            max_tokens: self.max_tokens.map(|t| t as u64),
            static_context: self.static_context,
            dynamic_context: self.dynamic_context,
            tool_server_handle: self.tool_server_handle,
            tool_choice: self.tool_choice,
            max_turns: self.max_turns,
            output_schema: self.output_schema,
            additional_params: self.additional_params,
            client: ProviderClient::OpenAi(client), // OpenAI-compatible API
        })
    }
}

/// Macro to configure agent builder with common parameters (without tools).
macro_rules! build_agent_no_tools {
    (
        $builder:expr,
        $name:expr,
        $description:expr,
        $temp:expr,
        $tokens:expr,
        $prompt:expr,
        $static_context:expr,
        $dynamic_context:expr,
        $tool_choice:expr,
        $max_turns:expr,
        $output_schema:expr,
        $additional_params:expr
    ) => {{
        let mut builder = $builder;
        if let Some(temp) = $temp {
            builder = builder.temperature(temp);
        }
        if let Some(tokens) = $tokens {
            builder = builder.max_tokens(tokens);
        }
        if let Some(preamble) = $prompt {
            builder = builder.preamble(preamble);
        }
        if let Some(choice) = $tool_choice.as_ref() {
            builder = builder.tool_choice(choice.clone());
        }
        if let Some(params) = $additional_params.as_ref() {
            builder = builder.additional_params(params.clone());
        }

        // Build the agent, then set fields that don't have builder methods.
        let mut agent = builder.build();
        agent.name = $name.clone();
        agent.description = $description.clone();
        agent.static_context = $static_context.to_vec();
        agent.dynamic_context = Arc::clone($dynamic_context);
        if let Some(turns) = $max_turns {
            agent.default_max_turns = Some(turns);
        }
        if let Some(schema) = $output_schema.as_ref() {
            agent.output_schema = Some(schema.clone());
        }
        agent
    }};
}

/// Macro to configure agent builder with common parameters (with tools).
macro_rules! build_agent_with_tools {
    (
        $builder:expr,
        $name:expr,
        $description:expr,
        $temp:expr,
        $tokens:expr,
        $prompt:expr,
        $static_context:expr,
        $dynamic_context:expr,
        $tool_server_handle:expr,
        $tool_choice:expr,
        $max_turns:expr,
        $output_schema:expr,
        $additional_params:expr
    ) => {{
        let mut builder = $builder;
        if let Some(temp) = $temp {
            builder = builder.temperature(temp);
        }
        if let Some(tokens) = $tokens {
            builder = builder.max_tokens(tokens);
        }
        if let Some(preamble) = $prompt {
            builder = builder.preamble(preamble);
        }

        // Add tool server handle (changes typestate).
        let mut builder = builder.tool_server_handle($tool_server_handle.clone());

        if let Some(choice) = $tool_choice.as_ref() {
            builder = builder.tool_choice(choice.clone());
        }
        if let Some(params) = $additional_params.as_ref() {
            builder = builder.additional_params(params.clone());
        }

        // Build the agent, then set fields that don't have builder methods.
        let mut agent = builder.build();
        agent.name = $name.clone();
        agent.description = $description.clone();
        agent.static_context = $static_context.to_vec();
        agent.dynamic_context = Arc::clone($dynamic_context);
        if let Some(turns) = $max_turns {
            agent.default_max_turns = Some(turns);
        }
        if let Some(schema) = $output_schema.as_ref() {
            agent.output_schema = Some(schema.clone());
        }
        agent
    }};
}

/// Parameters for building an agent on-demand.
struct AgentParams<'a> {
    model: &'a str,
    name: &'a Option<String>,
    description: &'a Option<String>,
    temperature: Option<f64>,
    max_tokens: Option<u64>,
    system_prompt: Option<&'a str>,
    static_context: &'a [Document],
    dynamic_context: &'a DynamicContext,
    tool_server_handle: &'a Option<ToolServerHandle>,
    tool_choice: &'a Option<ToolChoice>,
    max_turns: Option<usize>,
    output_schema: &'a Option<Schema>,
    additional_params: &'a Option<Value>,
}

/// Provider client enum - stores the client (built once), not the agent.
///
/// We build agents on-demand per request so we can apply dynamic system prompts.
enum ProviderClient {
    OpenAi(rig::providers::openai::Client),
    Anthropic(rig::providers::anthropic::Client),
    Cohere(rig::providers::cohere::Client),
    Gemini(rig::providers::gemini::Client),
    Mistral(rig::providers::mistral::Client),
    Groq(rig::providers::groq::Client),
    Together(rig::providers::together::Client),
    Xai(rig::providers::xai::Client),
    OpenRouter(rig::providers::openrouter::Client),
    Perplexity(rig::providers::perplexity::Client),
    HuggingFace(rig::providers::huggingface::Client),
}

/// Macro to dispatch to the correct build macro based on tool presence.
macro_rules! dispatch_build_agent {
    ($variant:ident, $client:expr, $params:expr) => {
        if let Some(handle) = $params.tool_server_handle {
            AgentEnum::$variant(build_agent_with_tools!(
                $client.agent($params.model),
                $params.name,
                $params.description,
                $params.temperature,
                $params.max_tokens,
                $params.system_prompt,
                $params.static_context,
                $params.dynamic_context,
                handle,
                $params.tool_choice,
                $params.max_turns,
                $params.output_schema,
                $params.additional_params
            ))
        } else {
            AgentEnum::$variant(build_agent_no_tools!(
                $client.agent($params.model),
                $params.name,
                $params.description,
                $params.temperature,
                $params.max_tokens,
                $params.system_prompt,
                $params.static_context,
                $params.dynamic_context,
                $params.tool_choice,
                $params.max_turns,
                $params.output_schema,
                $params.additional_params
            ))
        }
    };
}

impl ProviderClient {
    /// Builds an agent on-demand with the given parameters.
    fn build_agent(&self, params: &AgentParams) -> AgentEnum {
        match self {
            Self::OpenAi(client) => dispatch_build_agent!(OpenAi, client, params),
            Self::Anthropic(client) => dispatch_build_agent!(Anthropic, client, params),
            Self::Cohere(client) => dispatch_build_agent!(Cohere, client, params),
            Self::Gemini(client) => dispatch_build_agent!(Gemini, client, params),
            Self::Mistral(client) => dispatch_build_agent!(Mistral, client, params),
            Self::Groq(client) => dispatch_build_agent!(Groq, client, params),
            Self::Together(client) => dispatch_build_agent!(Together, client, params),
            Self::Xai(client) => dispatch_build_agent!(Xai, client, params),
            Self::OpenRouter(client) => dispatch_build_agent!(OpenRouter, client, params),
            Self::Perplexity(client) => dispatch_build_agent!(Perplexity, client, params),
            Self::HuggingFace(client) => dispatch_build_agent!(HuggingFace, client, params),
        }
    }
}

/// Agent enum for executing prompts (built on-demand per request).
enum AgentEnum {
    OpenAi(Agent<rig::providers::openai::responses_api::ResponsesCompletionModel>),
    Anthropic(Agent<rig::providers::anthropic::completion::CompletionModel>),
    Cohere(Agent<rig::providers::cohere::CompletionModel>),
    Gemini(Agent<rig::providers::gemini::CompletionModel>),
    Mistral(Agent<rig::providers::mistral::CompletionModel>),
    Groq(Agent<rig::providers::groq::CompletionModel>),
    Together(Agent<rig::providers::together::CompletionModel>),
    Xai(Agent<rig::providers::xai::CompletionModel>),
    OpenRouter(Agent<rig::providers::openrouter::CompletionModel>),
    Perplexity(Agent<rig::providers::perplexity::CompletionModel>),
    HuggingFace(Agent<rig::providers::huggingface::completion::CompletionModel>),
}

impl AgentEnum {
    /// Prompts the agent and returns the completion.
    async fn prompt(self, prompt: &str) -> Result<String, rig::completion::PromptError> {
        match self {
            Self::OpenAi(agent) => agent.prompt(prompt).await,
            Self::Anthropic(agent) => agent.prompt(prompt).await,
            Self::Cohere(agent) => agent.prompt(prompt).await,
            Self::Gemini(agent) => agent.prompt(prompt).await,
            Self::Mistral(agent) => agent.prompt(prompt).await,
            Self::Groq(agent) => agent.prompt(prompt).await,
            Self::Together(agent) => agent.prompt(prompt).await,
            Self::Xai(agent) => agent.prompt(prompt).await,
            Self::OpenRouter(agent) => agent.prompt(prompt).await,
            Self::Perplexity(agent) => agent.prompt(prompt).await,
            Self::HuggingFace(agent) => agent.prompt(prompt).await,
        }
    }

    /// Streams the agent prompt and returns a unified stream of completion chunks.
    async fn stream_prompt(
        self,
        prompt: &str,
    ) -> std::pin::Pin<Box<dyn Stream<Item = CompletionChunk> + Send>> {
        // Macro to reduce boilerplate for converting stream items.
        macro_rules! map_stream {
            ($agent:expr, $prompt:expr) => {{
                let stream = $agent.stream_prompt($prompt).await;
                Box::pin(stream.map(|item| match item {
                    Ok(MultiTurnStreamItem::StreamAssistantItem(
                        StreamedAssistantContent::Text(text),
                    )) => CompletionChunk::Text(text.text),
                    Ok(MultiTurnStreamItem::FinalResponse(resp)) => {
                        CompletionChunk::Final(resp.response().to_string())
                    }
                    Err(e) => CompletionChunk::Error(e.to_string()),
                    _ => CompletionChunk::Text(String::new()),
                }))
            }};
        }

        match self {
            Self::OpenAi(agent) => map_stream!(agent, prompt),
            Self::Anthropic(agent) => map_stream!(agent, prompt),
            Self::Cohere(agent) => map_stream!(agent, prompt),
            Self::Gemini(agent) => map_stream!(agent, prompt),
            Self::Mistral(agent) => map_stream!(agent, prompt),
            Self::Groq(agent) => map_stream!(agent, prompt),
            Self::Together(agent) => map_stream!(agent, prompt),
            Self::Xai(agent) => map_stream!(agent, prompt),
            Self::OpenRouter(agent) => map_stream!(agent, prompt),
            Self::Perplexity(agent) => map_stream!(agent, prompt),
            Self::HuggingFace(agent) => map_stream!(agent, prompt),
        }
    }
}

/// Agent client wrapper following Rig's enum dispatch pattern.
///
/// Stores the provider client (built once) and parameters, then builds
/// Rig agents on-demand per request to support dynamic system prompts.
pub struct AgentClient {
    /// AI provider being used.
    provider: Provider,
    /// Model identifier.
    model: String,
    /// Agent name for logging and debugging.
    name: Option<String>,
    /// Agent description for sub-agents and workflows.
    description: Option<String>,
    /// Optional temperature setting for response randomness.
    temperature: Option<f64>,
    /// Optional maximum tokens for response.
    max_tokens: Option<u64>,
    /// Static context documents (RAG).
    static_context: Vec<Document>,
    /// Dynamic context from vector stores (RAG).
    dynamic_context: DynamicContext,
    /// Tool server handle for managing tools.
    tool_server_handle: Option<ToolServerHandle>,
    /// Tool choice strategy.
    tool_choice: Option<ToolChoice>,
    /// Maximum number of recursive agent turns.
    max_turns: Option<usize>,
    /// JSON schema for structured output.
    output_schema: Option<Schema>,
    /// Additional parameters for the model.
    additional_params: Option<Value>,
    /// Provider client instance (built once, reused for all requests).
    client: ProviderClient,
}

impl AgentClient {
    /// Generates a completion from a prompt with optional system prompt.
    ///
    /// Builds a Rig agent on-demand with the given system prompt, then prompts it.
    pub async fn complete(
        &self,
        prompt: &str,
        system_prompt: Option<&str>,
    ) -> Result<String, Error> {
        // Build agent on-demand to allow dynamic system prompts per request.
        let params = AgentParams {
            model: &self.model,
            name: &self.name,
            description: &self.description,
            temperature: self.temperature,
            max_tokens: self.max_tokens,
            system_prompt,
            static_context: &self.static_context,
            dynamic_context: &self.dynamic_context,
            tool_server_handle: &self.tool_server_handle,
            tool_choice: &self.tool_choice,
            max_turns: self.max_turns,
            output_schema: &self.output_schema,
            additional_params: &self.additional_params,
        };

        let agent = self.client.build_agent(&params);

        agent
            .prompt(prompt)
            .await
            .map_err(|source| Error::CompletionRequest { source })
    }

    /// Generates a streaming completion from a prompt with optional system prompt.
    ///
    /// Returns a stream of completion chunks (text or final response).
    pub async fn complete_stream(
        &self,
        prompt: &str,
        system_prompt: Option<&str>,
    ) -> std::pin::Pin<Box<dyn Stream<Item = CompletionChunk> + Send>> {
        // Build agent on-demand to allow dynamic system prompts per request.
        let params = AgentParams {
            model: &self.model,
            name: &self.name,
            description: &self.description,
            temperature: self.temperature,
            max_tokens: self.max_tokens,
            system_prompt,
            static_context: &self.static_context,
            dynamic_context: &self.dynamic_context,
            tool_server_handle: &self.tool_server_handle,
            tool_choice: &self.tool_choice,
            max_turns: self.max_turns,
            output_schema: &self.output_schema,
            additional_params: &self.additional_params,
        };

        let agent = self.client.build_agent(&params);

        agent.stream_prompt(prompt).await
    }

    /// Returns the AI provider being used.
    pub fn provider(&self) -> &Provider {
        &self.provider
    }

    /// Returns the model identifier.
    pub fn model(&self) -> &str {
        &self.model
    }
}

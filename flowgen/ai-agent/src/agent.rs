//! AI provider client builders and abstractions.
//!
//! Provides unified client creation for different AI providers through the Rig framework.
//!
//! Each provider's completion model is erased to a `DynModel` once at build
//! time, so one `Agent` serves every provider: the agent runs the tool loop,
//! and the erased model also answers tool-use passthrough directly.

use crate::completion::{Credentials, Provider};
use crate::tool_runs::{RecordRuns, ToolRuns};
use futures_util::stream::Stream;
use futures_util::StreamExt;
use rig_agent::agent::MultiTurnStreamItem;
use rig_agent::completion::PromptError;
use rig_agent::tool::server::ToolServerHandle;
use rig_agent::{Agent, AgentBuilder, AgentRunner};
use rig_core::completion::{CompletionRequest, Document, Usage};
use rig_core::message::{AssistantContent, Message, ToolChoice};
use rig_core::operation::Completion;
use rig_core::providers::{
    anthropic, cohere, gemini, groq, huggingface, mistral, openai, openrouter, perplexity,
    together, xai,
};
use rig_core::streaming::{CompletionStream, Item, StreamEvent};
use rig_core::tool::context::ToolContext;
use rig_core::{DynModel, ProviderError};
use schemars::Schema;
use serde_json::Value;
use std::pin::Pin;
use std::sync::Arc;

/// Streaming chunk from completion.
#[derive(Debug, Clone)]
pub enum CompletionChunk {
    /// Text chunk from the stream.
    Text(String),
    /// Reasoning/thinking text fragment.
    Reasoning(String),
    /// Final response with the complete text and provider-reported
    /// token usage. Counters the provider omits are `None`.
    Final { text: String, usage: Usage },
    /// A tool the agent invoked mid-turn (name + arguments). rig runs the tool
    /// internally and doesn't stream its result, so this marks the call only.
    ToolCall {
        name: String,
        arguments: serde_json::Value,
        run: String,
    },
    /// Error during streaming, and whether retrying may succeed.
    Error { message: String, retryable: bool },
}

/// Errors that can occur during client creation and completion.
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum Error {
    #[error("Missing credentials for provider")]
    MissingCredentials,
    #[error("Custom provider requires endpoint configuration")]
    MissingEndpoint,
    #[error("Invalid header name '{name}': {source}")]
    InvalidHeaderName {
        name: String,
        #[source]
        source: http::header::InvalidHeaderName,
    },
    #[error("Invalid value for header '{name}': {source}")]
    InvalidHeaderValue {
        name: String,
        #[source]
        source: http::header::InvalidHeaderValue,
    },
    #[error("OpenAI client creation failed: {source}")]
    OpenAIClient {
        #[source]
        source: reqwest::Error,
    },
    #[error("Vertex AI client creation failed: {source}")]
    VertexAiClient {
        #[source]
        source: rig_vertexai::client::VertexAiClientError,
    },
    #[error("Completion request failed: {source}")]
    CompletionRequest {
        #[source]
        source: Box<PromptError>,
    },
    #[error("Completion passthrough failed: {source}")]
    CompletionPassthrough {
        #[source]
        source: Box<ProviderError>,
    },
}

impl Error {
    /// Whether sending the request again may succeed: rig's verdict for
    /// provider failures, while other failures are retried.
    pub fn is_retryable(&self) -> bool {
        match self {
            Error::CompletionRequest { source } => is_retryable(source),
            Error::CompletionPassthrough { source } => source.is_retryable(),
            _ => true,
        }
    }
}

/// rig's verdict for a provider failure; failures of the run itself carry no
/// provider reply and are retried.
fn is_retryable(error: &PromptError) -> bool {
    match error {
        PromptError::Provider(error) => error.is_retryable(),
        PromptError::Report(report) => report.is_retryable(),
        _ => true,
    }
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
    /// Optional HTTP headers sent with every request (`provider: custom` /
    /// Ollama only). Used, for example, to send `X-Flowgen-Client` when the
    /// downstream endpoint is another flowgen `llm_proxy` scoped by `headers`.
    headers: Option<std::collections::HashMap<String, String>>,
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
    /// Optional tool server handle for providing tools to the agent.
    tool_server_handle: Option<ToolServerHandle>,
    /// Optional tool choice strategy (auto, required, specific tool).
    tool_choice: Option<ToolChoice>,
    /// Optional maximum number of model calls per prompt (prevents infinite loops).
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
            ..Self::default()
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

    /// Sets extra HTTP headers sent with every request (`provider: custom` /
    /// Ollama only).
    pub fn headers(mut self, headers: std::collections::HashMap<String, String>) -> Self {
        self.headers = Some(headers);
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

    /// Sets the maximum number of model calls per prompt (prevents infinite loops in tool calling).
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
    pub fn build(mut self) -> Result<AgentClient, Error> {
        let completion_model = self.completion_model()?;

        let mut builder = AgentBuilder::new(completion_model.clone());
        if let Some(name) = self.name.take() {
            builder = builder.name(name);
        }
        if let Some(description) = self.description.take() {
            builder = builder.description(description);
        }
        if let Some(temperature) = self.temperature {
            builder = builder.temperature(temperature as f64);
        }
        if let Some(tokens) = self.max_tokens {
            builder = builder.max_tokens(tokens as u64);
        }
        if let Some(choice) = self.tool_choice.take() {
            builder = builder.tool_choice(choice);
        }
        if let Some(turns) = self.max_turns {
            builder = builder.default_max_turns(turns);
        }
        if let Some(schema) = self.output_schema.take() {
            builder = builder.output_schema_raw(schema);
        }
        if let Some(params) = self.additional_params.take() {
            builder = builder.additional_params(params);
        }
        if let Some(handle) = self.tool_server_handle.take() {
            builder = builder.tool_server_handle(handle);
        }

        Ok(AgentClient {
            provider: self.provider,
            model: self.model,
            static_context: self.static_context,
            agent: builder.build(),
            completion_model,
        })
    }

    /// The provider's completion model, erased to its operation.
    fn completion_model(&mut self) -> Result<DynModel<Completion>, Error> {
        let model = self.model.clone();
        match self.provider {
            Provider::OpenAi => Ok(openai::OpenAI::new(self.api_key()?)
                .completion(model)
                .into()),
            Provider::Anthropic => Ok(anthropic::Anthropic::new(self.api_key()?)
                .completion(model)
                .into()),
            Provider::Cohere => {
                // The native `/v2/chat` API, as Cohere was reached before.
                let mut cohere_model = cohere::Cohere::new(self.api_key()?).completion(model);
                cohere_model.wire = cohere_model.wire.with_route(cohere::ChatRoute::Native);
                Ok(cohere_model.into())
            }
            Provider::Google => Ok(gemini::Gemini::new(self.api_key()?)
                .completion(model)
                .into()),
            Provider::Mistral => Ok(mistral::new(self.api_key()?).completion(model).into()),
            Provider::Groq => Ok(groq::new(self.api_key()?).completion(model).into()),
            Provider::Together => Ok(together::new(self.api_key()?).completion(model).into()),
            Provider::Xai => Ok(xai::new(self.api_key()?).completion(model).into()),
            Provider::OpenRouter => Ok(openrouter::new(self.api_key()?).completion(model).into()),
            Provider::Perplexity => Ok(perplexity::new(self.api_key()?).completion(model).into()),
            Provider::HuggingFace => Ok(huggingface::new(self.api_key()?).completion(model).into()),
            Provider::VertexAi => {
                // Vertex AI uses Google Cloud Application Default Credentials (ADC).
                // Project and location can be set via credentials fields or environment
                // variables (GOOGLE_CLOUD_PROJECT, GOOGLE_CLOUD_LOCATION).
                let mut builder = rig_vertexai::VertexAi::builder();
                if let Some(ref creds) = self.credentials {
                    if let Some(ref project_id) = creds.project_id {
                        builder = builder.with_project(project_id);
                    }
                    if let Some(ref region) = creds.region {
                        builder = builder.with_location(region);
                    }
                }
                let client = builder
                    .build()
                    .map_err(|source| Error::VertexAiClient { source })?;
                Ok(client.completion(model).into())
            }
            Provider::Ollama | Provider::Custom => self.openai_compatible_model(),
        }
    }

    fn api_key(&self) -> Result<String, Error> {
        match &self.credentials {
            Some(credentials) => Ok(credentials.api_key.clone()),
            None => Err(Error::MissingCredentials),
        }
    }

    /// Custom and Ollama providers use OpenAI-compatible endpoints (requires
    /// explicit endpoint). Third-party servers implement classic chat
    /// completions, not the Responses API. Credentials are optional for
    /// local providers without auth.
    fn openai_compatible_model(&mut self) -> Result<DynModel<Completion>, Error> {
        let endpoint = self.endpoint.take().ok_or(Error::MissingEndpoint)?;
        let api_key = match &self.credentials {
            Some(credentials) => credentials.api_key.clone(),
            None => String::new(),
        };

        let mut header_map = http::HeaderMap::new();
        for (name, value) in self.headers.iter().flatten() {
            let header_name = http::HeaderName::try_from(name.as_str()).map_err(|source| {
                Error::InvalidHeaderName {
                    name: name.clone(),
                    source,
                }
            })?;
            let header_value =
                http::HeaderValue::from_str(value).map_err(|source| Error::InvalidHeaderValue {
                    name: name.clone(),
                    source,
                })?;
            header_map.insert(header_name, header_value);
        }

        let config = openai::OpenAIConfig::new(api_key).with_base_url(endpoint);
        let client = match header_map.is_empty() {
            true => config.client(),
            false => {
                let http = reqwest::Client::builder()
                    .default_headers(header_map)
                    .build()
                    .map_err(|source| Error::OpenAIClient { source })?;
                config.connect(rig_reqwest::ReqwestClient::from(http))
            }
        };
        Ok(client.chat(self.model.clone()).into())
    }
}

/// Non-passthrough completion output carrying rig-reported token usage
/// alongside the text. Reserving a struct here lets the processor and
/// the gateway forward `Usage` end-to-end without any per-call type
/// gymnastics or reaching into rig-native types.
pub struct CompletionOutput {
    pub text: String,
    pub usage: Usage,
}

/// Provider-agnostic response returned by tool-use passthrough.
pub struct PassthroughResponse {
    pub choice: Vec<AssistantContent>,
    pub usage: Usage,
}

/// Provider-agnostic streaming event carried out of tool-use
/// passthrough. Mirrors the subset of rig's stream events the AI gateway
/// cares about — text deltas and complete tool calls; reasoning and
/// provider-native items are dropped. `Done` carries the provider-reported
/// token usage of the final response, or `None` if the provider omitted it.
pub enum PassthroughStreamEvent {
    Text(String),
    ToolCall(rig_core::message::ToolCall),
    Done(Option<Usage>),
}

/// Stream of passthrough events.
pub type PassthroughStream =
    Pin<Box<dyn Stream<Item = Result<PassthroughStreamEvent, ProviderError>> + Send>>;

/// Maps one provider stream item to a passthrough event. Reasoning and
/// partial fragments become empty text, which the gateway skips.
fn passthrough_event(item: Item<StreamEvent>) -> PassthroughStreamEvent {
    match item {
        Item::Event(StreamEvent::Text { text, .. }) => PassthroughStreamEvent::Text(text),
        Item::Event(StreamEvent::End {
            content: AssistantContent::ToolCall(call),
            ..
        }) => PassthroughStreamEvent::ToolCall(call),
        _ => PassthroughStreamEvent::Text(String::new()),
    }
}

/// The stream's events, ending with `Done` once the provider's reply is read.
fn passthrough_events(stream: CompletionStream) -> PassthroughStream {
    Box::pin(futures_util::stream::unfold(
        Some(stream),
        |state| async move {
            let mut stream = state?;
            match stream.next().await {
                Some(Ok(item)) => Some((Ok(passthrough_event(item)), Some(stream))),
                Some(Err(error)) => Some((Err(error), None)),
                None => {
                    let done = stream.finish().await.map(|response| {
                        let usage = response.usage;
                        PassthroughStreamEvent::Done(usage.is_reported().then_some(usage))
                    });
                    Some((done, None))
                }
            }
        },
    ))
}

/// Maps one agent stream item to a completion chunk; `None` for items the
/// processor does not surface.
fn completion_chunk(
    item: Result<MultiTurnStreamItem, PromptError>,
    runs: &ToolRuns,
) -> Option<CompletionChunk> {
    match item {
        Ok(MultiTurnStreamItem::StreamAssistantItem(Item::Event(StreamEvent::Text {
            text,
            ..
        }))) => Some(CompletionChunk::Text(text)),
        Ok(MultiTurnStreamItem::StreamAssistantItem(Item::Event(StreamEvent::Reasoning {
            text,
            ..
        }))) => Some(CompletionChunk::Reasoning(text)),
        Ok(MultiTurnStreamItem::ToolCall { tool_call }) => Some(CompletionChunk::ToolCall {
            name: tool_call.function.name.as_str().to_string(),
            arguments: tool_call.function.arguments_value(),
            run: runs.streamed(&tool_call.id),
        }),
        Ok(MultiTurnStreamItem::FinalResponse(resp)) => Some(CompletionChunk::Final {
            text: resp.output(),
            usage: resp.usage,
        }),
        Err(e) => Some(CompletionChunk::Error {
            message: e.to_string(),
            retryable: is_retryable(&e),
        }),
        Ok(_) => None,
    }
}

/// Agent client wrapper.
///
/// Holds one Rig agent built over the provider's model, plus the model
/// itself for passthrough. The system prompt is applied per request.
pub struct AgentClient {
    /// AI provider being used.
    provider: Provider,
    /// Model identifier.
    model: String,
    /// Static context documents (RAG).
    static_context: Vec<Document>,
    /// Agent over the provider's model and the configured tools.
    agent: Agent,
    /// The provider's completion model (built once, reused for all requests).
    completion_model: DynModel<Completion>,
}

impl AgentClient {
    /// A run of `prompt` with the per-request system prompt and the static
    /// context. `runs` gives each tool call the flow run it starts.
    fn runner(
        &self,
        prompt: Message,
        system_prompt: Option<&str>,
        runs: &Arc<ToolRuns>,
    ) -> AgentRunner {
        let runner = self
            .agent
            .prompt(prompt)
            .documents(self.static_context.iter().cloned())
            .add_hook(RecordRuns(Arc::clone(runs)))
            .tool_context(ToolContext::default().with_scope(Arc::clone(runs) as _));
        match system_prompt {
            Some(preamble) => runner.preamble(preamble),
            None => runner,
        }
    }

    /// Generates a completion from a prompt with optional system prompt,
    /// together with rig-reported token usage.
    pub async fn complete(
        &self,
        prompt: Message,
        system_prompt: Option<&str>,
    ) -> Result<CompletionOutput, Error> {
        let resp = self
            .runner(prompt, system_prompt, &ToolRuns::new())
            .await
            .map_err(|source| Error::CompletionRequest {
                source: Box::new(source),
            })?;
        Ok(CompletionOutput {
            text: resp.output(),
            usage: resp.usage,
        })
    }

    /// Generates a streaming completion from a prompt with optional system prompt.
    ///
    /// Returns a stream of completion chunks (text or final response).
    pub async fn complete_stream(
        &self,
        prompt: Message,
        system_prompt: Option<&str>,
    ) -> Pin<Box<dyn Stream<Item = CompletionChunk> + Send>> {
        let runs = ToolRuns::new();
        let stream = self.runner(prompt, system_prompt, &runs).stream();
        Box::pin(stream.filter_map(move |item| {
            let chunk = completion_chunk(item, &runs);
            async move { chunk }
        }))
    }

    /// Returns the AI provider being used.
    pub fn provider(&self) -> &Provider {
        &self.provider
    }

    /// Returns the model identifier.
    pub fn model(&self) -> &str {
        &self.model
    }

    /// One-shot completion that skips rig's agent loop. Used by the AI
    /// gateway to forward client-supplied `tools` verbatim: the model
    /// sees the tools, returns `tool_calls`, and we hand them back
    /// without executing anything ourselves.
    pub async fn completion_passthrough(
        &self,
        request: CompletionRequest,
    ) -> Result<PassthroughResponse, Error> {
        let resp = self
            .completion_model
            .call(request)
            .await
            .map_err(|source| Error::CompletionPassthrough {
                source: Box::new(source),
            })?;
        Ok(PassthroughResponse {
            choice: resp.choice,
            usage: resp.usage,
        })
    }

    /// Streaming counterpart to `completion_passthrough`. Yields a
    /// provider-agnostic stream that the gateway maps to the OpenAI
    /// SSE format on the way out.
    pub async fn completion_stream_passthrough(
        &self,
        request: CompletionRequest,
    ) -> Result<PassthroughStream, Error> {
        let stream = self.completion_model.stream(request).map_err(|source| {
            Error::CompletionPassthrough {
                source: Box::new(source),
            }
        })?;
        Ok(passthrough_events(stream))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::completion::CompletionUsage;

    fn make_usage(input: u64, output: u64) -> Usage {
        Usage::new()
            .input_tokens(input)
            .output_tokens(output)
            .total_tokens(input + output)
    }

    #[test]
    fn completion_chunk_final_carries_text_and_usage() {
        let chunk = CompletionChunk::Final {
            text: "hello".to_string(),
            usage: make_usage(10, 5),
        };
        match chunk {
            CompletionChunk::Final { text, usage } => {
                assert_eq!(text, "hello");
                assert_eq!(usage.input_tokens, Some(10));
                assert_eq!(usage.output_tokens, Some(5));
                assert_eq!(usage.total_tokens, Some(15));
            }
            other => panic!("expected Final variant, got {other:?}"),
        }
    }

    #[test]
    fn passthrough_stream_event_done_carries_some_usage() {
        let event = PassthroughStreamEvent::Done(Some(make_usage(100, 200)));
        match event {
            PassthroughStreamEvent::Done(Some(u)) => {
                assert_eq!(u.input_tokens, Some(100));
                assert_eq!(u.output_tokens, Some(200));
                assert_eq!(u.total_tokens, Some(300));
            }
            _ => panic!("expected Done(Some(_))"),
        }
    }

    #[test]
    fn passthrough_stream_event_done_carries_none_when_provider_omits_usage() {
        // Providers that never yield a `Final` frame with usage produce
        // `Done(None)`, which the completion processor forwards as
        // `final_chunk.usage = None`. That branch must stay a valid
        // pattern so the compiler catches accidental removal.
        let event = PassthroughStreamEvent::Done(None);
        match event {
            PassthroughStreamEvent::Done(None) => {}
            _ => panic!("expected Done(None)"),
        }
    }

    #[test]
    fn rig_usage_converts_to_completion_usage_with_matching_totals() {
        // The completion processor's Fix 2 (final chunk carries usage)
        // relies on this conversion: `usage.map(Into::into)`. Assert the
        // three OpenAI wire fields survive the mapping unchanged.
        let rig_usage = make_usage(42, 7);
        let converted: CompletionUsage = rig_usage.into();
        assert_eq!(converted.prompt_tokens, 42);
        assert_eq!(converted.completion_tokens, 7);
        assert_eq!(converted.total_tokens, 49);
    }

    #[test]
    fn rig_usage_without_total_sums_input_and_output() {
        let rig_usage = Usage::new().input_tokens(42).output_tokens(7);
        let converted: CompletionUsage = rig_usage.into();
        assert_eq!(converted.total_tokens, 49);
    }

    #[test]
    fn a_streamed_rate_limit_is_retried_and_a_bad_request_is_not() {
        let retryable = |status: http::StatusCode| {
            let error = PromptError::from(ProviderError::from_http_response(status, ""));
            match completion_chunk(Err(error), &ToolRuns::new()) {
                Some(CompletionChunk::Error { retryable, .. }) => retryable,
                other => panic!("expected an error chunk, got {other:?}"),
            }
        };

        assert!(retryable(http::StatusCode::TOO_MANY_REQUESTS));
        assert!(retryable(http::StatusCode::BAD_GATEWAY));
        assert!(!retryable(http::StatusCode::BAD_REQUEST));
    }

    #[test]
    fn unreported_usage_converts_to_zeroes() {
        let converted: CompletionUsage = Usage::new().into();
        assert_eq!(converted.prompt_tokens, 0);
        assert_eq!(converted.completion_tokens, 0);
        assert_eq!(converted.total_tokens, 0);
    }
}

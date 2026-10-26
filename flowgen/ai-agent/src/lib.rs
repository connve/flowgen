//! AI processing capabilities for flowgen workers.
//!
//! Provides AI integration using the Rig framework with support for
//! completions, agents, RAG (retrieval-augmented generation), and
//! multi-modal capabilities (text, images, speech, etc.).

use flowgen_core::task::context::{Services, TaskContext};
use flowgen_core::task::meta::{Connection, Connector, Icon};
use flowgen_core::task::runner::Runner;
use flowgen_core::task::{Task, TaskError, TaskIo, TaskMeta};
use std::sync::Arc;

pub mod agent;
pub mod meta;
pub mod tool_runs;

/// AI completion task for generating responses using LLMs.
pub mod completion;
/// LLM proxy task registering a flow as an AI gateway backend.
pub mod llm_proxy;

/// AI gateway server hosting LLM proxy backends.
pub mod ai_gateway {
    pub mod config;
    pub mod protocol;
    pub mod server;
}

pub use agent::{AgentClient, ClientBuilder, CompletionChunk};
pub use completion::Processor;
pub use completion::Provider;

/// The connector of this crate's task types.
pub const CONNECTOR: Connector = Connector {
    name: "ai",
    label: "AI",
    icon: Icon::Svg(include_str!("../icons/ai.svg")),
    describe_credentials: None,
};

impl TaskMeta for completion::Completion {
    const CONNECTOR: Connector = CONNECTOR;

    fn connections(&self) -> Vec<Connection> {
        let provider = match (&self.provider, &self.endpoint) {
            (Provider::Custom | Provider::Ollama, Some(endpoint)) => endpoint.as_str(),
            (provider, _) => provider.as_str(),
        };
        let mut connections = vec![Connection::new(CONNECTOR, self.credentials_path.as_deref())
            .target(&[provider, &self.model])];
        connections.extend(self.mcp_servers.iter().map(|server| {
            Connection {
                on_behalf_of_user: server.on_behalf_of_user,
                ..Connection::new(flowgen_mcp::CONNECTOR, server.credentials_path.as_deref())
                    .target(&[&server.url])
            }
        }));
        connections
    }

    fn validate(&self) -> Result<(), TaskError> {
        Ok(completion::Completion::validate(self)?)
    }
}

impl TaskMeta for llm_proxy::LlmProxy {
    const CONNECTOR: Connector = CONNECTOR;
    const REGISTERS: bool = true;
}

impl Task for completion::Completion {
    async fn run(self, io: TaskIo, context: Arc<TaskContext>) -> Result<(), TaskError> {
        let mut builder = completion::ProcessorBuilder::new()
            .config(Arc::new(self))
            .task_id(io.task_id)
            .task_type(io.task_type)
            .task_context(context);
        if let Some(rx) = io.rx {
            builder = builder.receiver(rx);
        }
        if let Some(tx) = io.tx {
            builder = builder.sender(tx);
        }
        builder.build().await?.run().await?;
        Ok(())
    }
}

fn ai_gateway_server(
    services: &Services,
) -> Result<Arc<ai_gateway::server::AiGatewayServer>, llm_proxy::Error> {
    services
        .get::<ai_gateway::server::AiGatewayServer>()
        .ok_or(llm_proxy::Error::AiGatewayNotEnabled)
}

impl Task for llm_proxy::LlmProxy {
    fn check(&self, services: &Services) -> Result<(), TaskError> {
        ai_gateway_server(services)?;
        Ok(())
    }

    async fn run(self, io: TaskIo, context: Arc<TaskContext>) -> Result<(), TaskError> {
        let ai_gateway_server = ai_gateway_server(&context.services)?;
        let mut builder = llm_proxy::ProcessorBuilder::new()
            .config(Arc::new(self))
            .task_id(io.task_id)
            .task_type(io.task_type)
            .task_context(context)
            .ai_gateway_server(ai_gateway_server);
        if let Some(tx) = io.tx {
            builder = builder.sender(tx);
        }
        builder.build().await?.run().await?;
        Ok(())
    }
}

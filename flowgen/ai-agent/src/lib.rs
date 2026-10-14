//! AI processing capabilities for flowgen workers.
//!
//! Provides AI integration using the Rig framework with support for
//! completions, agents, RAG (retrieval-augmented generation), and
//! multi-modal capabilities (text, images, speech, etc.).

use flowgen_core::task::meta::{Connection, Connector, Icon, TaskMeta};

pub mod agent;
pub mod meta;

/// AI completion processor for generating responses using LLMs.
pub mod completion {
    pub mod config;
    pub mod passthrough;
    pub mod processor;
}

/// AI gateway server hosting LLM proxy backends.
pub mod ai_gateway {
    pub mod config;
    pub mod llm_proxy;
    pub mod protocol;
    pub mod server;
}

pub use agent::{AgentClient, ClientBuilder, CompletionChunk};
pub use completion::config::Provider;
pub use completion::processor::Processor;
pub use flowgen_core::nsjail;

/// The connector of this crate's task types.
pub const CONNECTOR: Connector = Connector {
    name: "ai",
    label: "AI",
    icon: Icon::Svg(include_str!("../icons/ai.svg")),
    describe_credentials: None,
};

impl TaskMeta for completion::config::Processor {
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
}

impl TaskMeta for ai_gateway::config::Processor {
    const CONNECTOR: Connector = CONNECTOR;
}

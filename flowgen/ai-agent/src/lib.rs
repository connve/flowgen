//! AI processing capabilities for flowgen workers.
//!
//! Provides AI integration using the Rig framework with support for
//! completions, agents, RAG (retrieval-augmented generation), and
//! multi-modal capabilities (text, images, speech, etc.).

pub mod agent;

/// AI completion processor for generating responses using LLMs.
pub mod completion {
    pub mod config;
    pub mod processor;
}

/// OpenAI-compatible AI gateway endpoint.
pub mod ai_gateway {
    pub mod config;
    pub mod processor;
}

pub use agent::{AgentClient, ClientBuilder, CompletionChunk};
pub use completion::config::Provider;
pub use completion::processor::Processor;
pub use flowgen_core::nsjail;

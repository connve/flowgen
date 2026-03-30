//! AI processing capabilities for flowgen workers.
//!
//! Provides AI integration using the Rig framework with support for
//! completions, agents, RAG (retrieval-augmented generation), and
//! multi-modal capabilities (text, images, speech, etc.).

pub mod agent;
pub mod completion;

pub use agent::{AgentClient, ClientBuilder, CompletionChunk};
pub use completion::config::Provider;
pub use completion::processor::Processor;
pub use flowgen_core::nsjail;

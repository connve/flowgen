//! AI completion processor module.
//!
//! Provides completion-based text generation using LLMs with support for
//! streaming responses and optional tool integration.

pub mod config;
pub mod processor;

pub use config::Processor as Config;
pub use processor::Processor;

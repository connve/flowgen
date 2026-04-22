//! LLM proxy — OpenAI-compatible chat completions endpoint.
//!
//! Routes requests to any AI provider via Rig with optional MCP tool access.

pub mod config;
pub mod processor;
pub mod types;

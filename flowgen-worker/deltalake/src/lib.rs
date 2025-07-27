/// Handles client-side logic for interacting with a remote service or API.
pub mod client;

/// Defines configuration structures (like `Reader`, `Writer`, etc.)
/// used throughout the crate.
pub mod config;

/// Provides functionality for writing data, likely using configurations
/// defined in the `config` module.
pub mod writer;

// Provides type extensions for flowgen_core::stream::Event.
pub mod event;

// Provides type extensions for apache_arrow::schema.
pub mod schema;

//! Log processor configuration.

use serde::{Deserialize, Serialize};

/// Log processor that outputs event data to logs.
#[derive(PartialEq, Clone, Debug, Deserialize, Serialize, Default)]
pub struct Processor {
    /// Task name identifier.
    pub name: String,
    /// Log level for output.
    #[serde(default)]
    pub level: LogLevel,
}

/// Log level options.
#[derive(PartialEq, Eq, Clone, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum LogLevel {
    /// Trace level logging.
    Trace,
    /// Debug level logging.
    Debug,
    /// Info level logging (default).
    #[default]
    Info,
    /// Warn level logging.
    Warn,
    /// Error level logging.
    Error,
}

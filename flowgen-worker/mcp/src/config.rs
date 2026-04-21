//! MCP tool task configuration.
//!
//! Defines the YAML configuration for `mcp_tool` tasks that expose
//! flows as MCP tools callable by LLMs.

use flowgen_core::config::ConfigExt;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// MCP tool task configuration.
///
/// # Example YAML
/// ```yaml
/// - mcp_tool:
///     name: send_welcome
///     description: "Send personalized welcome campaign with CRM sync"
///     credentials_path: /etc/mcp/api-keys.json  # optional, overrides global
///     input_schema:
///       type: object
///       properties:
///         user_id:
///           type: string
///           description: "User ID to send welcome to"
///       required: [user_id]
/// ```
#[derive(PartialEq, Clone, Debug, Default, Deserialize, Serialize)]
pub struct Processor {
    /// Unique tool name. Combined with flow name for the full tool identifier:
    /// `{flow_name}.{name}` (e.g., `crm_welcome_flow.send_welcome`).
    pub name: String,
    /// Human-readable description shown to LLMs for tool selection.
    pub description: String,
    /// JSON Schema describing the tool's input parameters.
    /// Follows the standard JSON Schema format used by MCP.
    #[serde(default)]
    pub input_schema: serde_json::Value,
    /// Optional path to credentials file for this specific tool.
    /// Overrides the global `worker.mcp_server.credentials_path` if set.
    pub credentials_path: Option<PathBuf>,
    /// Timeout for waiting on flow completion before responding to the MCP client.
    /// If not specified, waits indefinitely for flow completion.
    /// Accepts human-readable durations (e.g., "30s", "5m", "1h").
    #[serde(default, with = "humantime_serde")]
    pub ack_timeout: Option<std::time::Duration>,
    /// Optional list of upstream task names this task depends on.
    /// When set, this task only receives events from the named tasks.
    /// When not set, the task receives from the previous task in the list (linear chain).
    #[serde(default)]
    pub depends_on: Option<Vec<String>>,
    /// Optional retry configuration (overrides app-level retry config).
    #[serde(default)]
    pub retry: Option<flowgen_core::retry::RetryConfig>,
}

impl ConfigExt for Processor {}

/// Authentication credentials for MCP API key validation.
#[derive(PartialEq, Clone, Debug, Default, Deserialize, Serialize)]
pub struct Credentials {
    /// List of valid API keys for Bearer token authentication.
    pub api_keys: Vec<String>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_processor_default() {
        let processor = Processor::default();
        assert_eq!(processor.name, String::new());
        assert_eq!(processor.description, String::new());
        assert_eq!(processor.input_schema, serde_json::Value::default());
        assert!(processor.credentials_path.is_none());
        assert!(processor.retry.is_none());
    }

    #[test]
    fn test_processor_serialization() {
        let processor = Processor {
            name: "send_welcome".to_string(),
            description: "Send welcome campaign".to_string(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "user_id": { "type": "string" }
                },
                "required": ["user_id"]
            }),
            credentials_path: Some(std::path::PathBuf::from("/etc/mcp/keys.json")),
            ack_timeout: None,
            depends_on: None,
            retry: None,
        };

        let json = serde_json::to_string(&processor).unwrap();
        let deserialized: Processor = serde_json::from_str(&json).unwrap();
        assert_eq!(processor, deserialized);
    }

    #[test]
    fn test_credentials_serialization() {
        let creds = Credentials {
            api_keys: vec!["key1".to_string(), "key2".to_string()],
        };

        let json = serde_json::to_string(&creds).unwrap();
        let deserialized: Credentials = serde_json::from_str(&json).unwrap();
        assert_eq!(creds, deserialized);
    }
}

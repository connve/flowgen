//! Configuration shared by the MCP server and the `mcp_tool` task.

use serde::{Deserialize, Serialize};

/// Authentication credentials for MCP API key validation.
#[derive(PartialEq, Clone, Debug, Default, Deserialize, Serialize)]
pub struct Credentials {
    /// List of valid API keys for Bearer token authentication.
    pub api_keys: Vec<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

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

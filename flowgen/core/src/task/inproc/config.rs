//! Configuration for in-process flow calls.

use serde::{Deserialize, Serialize};
use std::time::Duration;

/// Source that makes its flow callable in-process at the flow's identity.
#[derive(PartialEq, Clone, Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Endpoint {
    /// The unique name of the task.
    pub name: String,
    /// How long a call may take, from handing its event to the flow until the
    /// flow finishes. Unbounded when unset.
    #[serde(default, with = "humantime_serde")]
    pub ack_timeout: Option<Duration>,
    /// Flows allowed to call, e.g. `["user/", "platform/"]`: each entry admits the
    /// flow of that identity and every flow in it as a folder, and `""` admits
    /// every flow. Defaults to the flow's own top-level folder; flowgen itself may
    /// always call.
    #[serde(default)]
    pub callers: Option<Vec<String>>,
}

/// Processor that calls a flow with an `inproc_endpoint` and emits its result.
#[derive(PartialEq, Clone, Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Request {
    /// The unique name of the task.
    pub name: String,
    /// Identity of the flow to call, e.g. `system/publish_workspace`.
    pub flow: String,
    /// Optional list of upstream task names this task depends on.
    #[serde(default)]
    pub depends_on: Option<Vec<String>>,
    /// Optional retry configuration.
    #[serde(default)]
    pub retry: Option<crate::retry::RetryConfig>,
}

impl crate::config::ConfigExt for Request {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn endpoint_ack_timeout_is_a_duration() {
        let endpoint: Endpoint =
            serde_json::from_str(r#"{"name": "on_call", "ack_timeout": "2m"}"#).unwrap();
        assert_eq!(endpoint.ack_timeout, Some(Duration::from_secs(120)));
    }
}

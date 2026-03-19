use flowgen_core::config::ConfigExt;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// Tooling API operations.
#[derive(PartialEq, Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolingOperation {
    /// Create a managed event subscription.
    CreateManagedEventSubscription,
}

/// Replay preset for managed event subscriptions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ReplayPreset {
    /// Start from the latest event.
    Latest,
    /// Start from the earliest retained event.
    Earliest,
}

/// State of a managed event subscription.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SubscriptionState {
    /// Subscription is actively running.
    Run,
    /// Subscription is stopped.
    Stop,
}

/// Configuration for Salesforce Tooling API operations.
///
/// # Examples
///
/// Create a managed event subscription:
/// ```yaml
/// salesforce_tooling:
///   name: create_subscription
///   operation: create_managed_event_subscription
///   credentials_path: /path/to/salesforce_creds.json
///   full_name: Managed_Sub_OpportunityChangeEvent
///   metadata:
///     label: "Managed Sub OpportunityChangeEvent"
///     topic_name: "/data/OpportunityChangeEvent"
///     default_replay: latest
///     state: run
///     error_recovery_replay: latest
/// ```
#[derive(PartialEq, Clone, Debug, Deserialize, Serialize)]
pub struct Tooling {
    /// Unique task identifier.
    pub name: String,
    /// Tooling API operation type.
    pub operation: ToolingOperation,
    /// Path to Salesforce authentication credentials.
    pub credentials_path: PathBuf,

    // Fields for create_managed_event_subscription operation.
    /// Full API name of the managed event subscription.
    #[serde(default)]
    pub full_name: Option<String>,
    /// Metadata configuration for the subscription.
    #[serde(default)]
    pub metadata: Option<ManagedEventSubscriptionMetadata>,

    /// Optional retry configuration (overrides app-level retry config).
    #[serde(default)]
    pub retry: Option<flowgen_core::retry::RetryConfig>,
}

/// Metadata for a managed event subscription.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManagedEventSubscriptionMetadata {
    /// Human-readable label for the subscription.
    pub label: String,
    /// Topic name to subscribe to (e.g., "/data/OpportunityChangeEvent").
    pub topic_name: String,
    /// Default replay preset for the subscription.
    pub default_replay: ReplayPreset,
    /// Current state of the subscription.
    pub state: SubscriptionState,
    /// Replay preset to use for error recovery.
    pub error_recovery_replay: ReplayPreset,
}

impl ConfigExt for Tooling {}

// Conversion functions to SDK types.
pub(crate) fn to_sdk_replay_preset(rp: &ReplayPreset) -> salesforce_core::tooling::ReplayPreset {
    match rp {
        ReplayPreset::Latest => salesforce_core::tooling::ReplayPreset::Latest,
        ReplayPreset::Earliest => salesforce_core::tooling::ReplayPreset::Earliest,
    }
}

pub(crate) fn to_sdk_subscription_state(
    state: &SubscriptionState,
) -> salesforce_core::tooling::SubscriptionState {
    match state {
        SubscriptionState::Run => salesforce_core::tooling::SubscriptionState::Run,
        SubscriptionState::Stop => salesforce_core::tooling::SubscriptionState::Stop,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tooling_operation_serialization() {
        assert_eq!(
            serde_json::to_string(&ToolingOperation::CreateManagedEventSubscription).unwrap(),
            "\"create_managed_event_subscription\""
        );
    }

    #[test]
    fn test_tooling_operation_deserialization() {
        assert_eq!(
            serde_json::from_str::<ToolingOperation>("\"create_managed_event_subscription\"")
                .unwrap(),
            ToolingOperation::CreateManagedEventSubscription
        );
    }

    #[test]
    fn test_replay_preset_serialization() {
        assert_eq!(
            serde_json::to_string(&ReplayPreset::Latest).unwrap(),
            "\"latest\""
        );
        assert_eq!(
            serde_json::to_string(&ReplayPreset::Earliest).unwrap(),
            "\"earliest\""
        );
    }

    #[test]
    fn test_subscription_state_serialization() {
        assert_eq!(
            serde_json::to_string(&SubscriptionState::Run).unwrap(),
            "\"run\""
        );
        assert_eq!(
            serde_json::to_string(&SubscriptionState::Stop).unwrap(),
            "\"stop\""
        );
    }
}

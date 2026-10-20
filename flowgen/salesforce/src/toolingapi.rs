//! The `salesforce_toolingapi` task: Tooling API operations, such as
//! creating managed event subscriptions.

use crate::processor::{required, to_json, Call, Emitter, Error, Operation, Output};
use flowgen_core::config::ConfigExt;
use flowgen_core::task::TaskConfig;
use salesforce_core::toolingapi as sdk;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

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

impl From<ReplayPreset> for sdk::ReplayPreset {
    fn from(preset: ReplayPreset) -> Self {
        match preset {
            ReplayPreset::Latest => sdk::ReplayPreset::Latest,
            ReplayPreset::Earliest => sdk::ReplayPreset::Earliest,
        }
    }
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

impl From<SubscriptionState> for sdk::SubscriptionState {
    fn from(state: SubscriptionState) -> Self {
        match state {
            SubscriptionState::Run => sdk::SubscriptionState::Run,
            SubscriptionState::Stop => sdk::SubscriptionState::Stop,
        }
    }
}

/// Configuration for Salesforce Tooling API operations.
///
/// # Examples
///
/// Create a managed event subscription:
/// ```yaml
/// salesforce_toolingapi:
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
#[derive(PartialEq, Clone, Debug, Deserialize, Serialize, TaskConfig)]
#[serde(deny_unknown_fields)]
pub struct Tooling {
    /// Unique task identifier.
    pub name: String,
    /// Tooling API operation type.
    pub operation: ToolingOperation,
    /// Path to Salesforce authentication credentials.
    pub credentials_path: PathBuf,
    /// Full API name of the managed event subscription.
    #[serde(default)]
    pub full_name: Option<String>,
    /// Metadata configuration for the subscription.
    #[serde(default)]
    pub metadata: Option<ManagedEventSubscriptionMetadata>,
    /// Optional list of upstream task names this task depends on.
    /// When set, this task only receives events from the named tasks.
    /// When not set, the task receives from the previous task in the list (linear chain).
    #[serde(default)]
    pub depends_on: Option<Vec<String>>,
    /// Optional retry configuration (overrides app-level retry config).
    #[serde(default)]
    pub retry: Option<flowgen_core::retry::RetryConfig>,
    /// Events handled at once; defaults to 64.
    #[serde(default)]
    pub max_in_flight: Option<std::num::NonZeroUsize>,
}

/// Metadata for a managed event subscription.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
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

#[async_trait::async_trait]
impl Operation for Tooling {
    type Client = sdk::Client;

    fn client(auth: &salesforce_core::client::Client) -> Result<Self::Client, Error> {
        sdk::ClientBuilder::new(auth.clone())
            .build()
            .map_err(|source| Error::Tooling {
                source: Box::new(source),
            })
    }

    fn credentials_path(&self) -> &Path {
        &self.credentials_path
    }

    fn retries_safely(&self) -> bool {
        !matches!(
            self.operation,
            ToolingOperation::CreateManagedEventSubscription
        )
    }

    async fn call(
        &self,
        client: &Self::Client,
        _call: &Call<'_>,
        emitter: &mut Emitter<'_>,
    ) -> Result<(), Error> {
        match self.operation {
            ToolingOperation::CreateManagedEventSubscription => {
                let full_name = required(self.full_name.as_ref(), "full_name")?;
                let metadata = required(self.metadata.as_ref(), "metadata")?;
                let request = sdk::CreateManagedEventSubscriptionRequest {
                    full_name: full_name.clone(),
                    metadata: sdk::ManagedEventSubscriptionMetadata {
                        label: metadata.label.clone(),
                        topic_name: metadata.topic_name.clone(),
                        default_replay: metadata.default_replay.into(),
                        state: metadata.state.into(),
                        error_recovery_replay: metadata.error_recovery_replay.into(),
                    },
                };
                let response = client
                    .create_managed_event_subscription(request)
                    .await
                    .map_err(|source| Error::Tooling {
                        source: Box::new(source),
                    })?;
                emitter
                    .emit(Output::json(to_json(&response)?).id(response.id))
                    .await
            }
        }
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
        assert_eq!(
            serde_json::from_str::<ToolingOperation>("\"create_managed_event_subscription\"")
                .unwrap(),
            ToolingOperation::CreateManagedEventSubscription
        );
    }

    #[test]
    fn test_replay_preset_and_state_serialization() {
        assert_eq!(
            serde_json::to_string(&ReplayPreset::Latest).unwrap(),
            "\"latest\""
        );
        assert_eq!(
            serde_json::to_string(&ReplayPreset::Earliest).unwrap(),
            "\"earliest\""
        );
        assert_eq!(
            serde_json::to_string(&SubscriptionState::Run).unwrap(),
            "\"run\""
        );
        assert_eq!(
            serde_json::to_string(&SubscriptionState::Stop).unwrap(),
            "\"stop\""
        );
    }

    #[test]
    fn creating_a_subscription_does_not_retry_safely() {
        let config: Tooling = serde_json::from_str(
            r#"{"name": "a", "operation": "create_managed_event_subscription", "credentials_path": "/a"}"#,
        )
        .unwrap();

        assert!(!config.retries_safely());
    }
}

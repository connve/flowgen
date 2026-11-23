//! The `braze_messaging` task.

use crate::processor::{present, request, required, Emitter, Error, Operation, Payload};
use flowgen_core::config::ConfigExt;
use flowgen_core::event::Event;
use flowgen_core::task::TaskConfig;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

/// What the task sends.
#[derive(PartialEq, Clone, Debug, Deserialize, Serialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum MessagingOperation {
    /// Send messages defined in `payload.messages` right away.
    SendMessages,
    /// Send an API-triggered campaign.
    TriggerCampaign,
    /// Send an API-triggered Canvas.
    TriggerCanvas,
    /// Create a send ID for tracking messages sent through the API.
    CreateSendIds,
    /// Send the transactional email campaign `campaign_id` to one user.
    SendTransactional,
}

/// Sends messages, campaigns, and Canvases through the Braze API.
///
/// ```yaml
/// - braze_messaging:
///     name: send_receipt
///     operation: send_transactional
///     credentials_path: /etc/braze/credentials.json
///     base_url: https://rest.iad-01.braze.com
///     campaign_id: "{{env.BRAZE_RECEIPT_CAMPAIGN_ID}}"
///     payload:
///       from_event: true
/// ```
#[derive(PartialEq, Clone, Debug, Deserialize, Serialize, schemars::JsonSchema, TaskConfig)]
#[serde(deny_unknown_fields)]
pub struct Messaging {
    /// Task name.
    pub name: String,
    /// What to send.
    pub operation: MessagingOperation,
    /// JSON file with the REST API key: `{"api_key": "..."}`.
    pub credentials_path: PathBuf,
    /// REST endpoint of the Braze instance, e.g. `https://rest.iad-01.braze.com`.
    pub base_url: String,
    /// Campaign ID for `send_transactional` only, whose campaigns send email;
    /// the other operations take `campaign_id` or `canvas_id` in `payload`.
    #[serde(default)]
    pub campaign_id: Option<String>,
    /// The request body, or `from_event: true`.
    #[serde(default)]
    pub payload: Option<Payload>,
    /// Upstream task names, when they differ from the previous task.
    #[serde(default)]
    pub depends_on: Option<Vec<String>>,
    /// Retry configuration, overriding the app-level one.
    #[serde(default)]
    pub retry: Option<flowgen_core::retry::RetryConfig>,
    /// Events handled at once; defaults to 64.
    #[serde(default)]
    pub max_in_flight: Option<std::num::NonZeroUsize>,
}

impl ConfigExt for Messaging {}

#[async_trait::async_trait]
impl Operation for Messaging {
    fn credentials_path(&self) -> &Path {
        &self.credentials_path
    }

    fn base_url(&self) -> &str {
        &self.base_url
    }

    fn validate(&self) -> Result<(), Error> {
        present(self.payload.as_ref(), "payload")?;
        match self.operation {
            MessagingOperation::SendTransactional => {
                present(self.campaign_id.as_ref(), "campaign_id")
            }
            _ => Ok(()),
        }
    }

    /// A send repeated after a timeout can deliver the message twice.
    fn retries_safely(&self) -> bool {
        matches!(self.operation, MessagingOperation::CreateSendIds)
    }

    async fn call(
        &self,
        client: &braze::Client,
        event: &Event,
        emitter: &mut Emitter<'_>,
    ) -> Result<(), Error> {
        let messaging = client.messaging();
        let payload = self.payload.as_ref();
        match self.operation {
            MessagingOperation::SendMessages => {
                let request: braze::messaging::SendMessagesRequest = request(payload, event)?;
                emitter
                    .emit(&messaging.send_messages(&request).send().await?)
                    .await
            }
            MessagingOperation::TriggerCampaign => {
                let request: braze::messaging::SendTriggeredCampaignsRequest =
                    request(payload, event)?;
                emitter
                    .emit(&messaging.send_triggered_campaigns(&request).send().await?)
                    .await
            }
            MessagingOperation::TriggerCanvas => {
                let request: braze::messaging::SendTriggeredCanvasesRequest =
                    request(payload, event)?;
                emitter
                    .emit(&messaging.send_triggered_canvases(&request).send().await?)
                    .await
            }
            MessagingOperation::CreateSendIds => {
                let request: braze::messaging::CreateSendIdsRequest = request(payload, event)?;
                emitter
                    .emit(&messaging.create_send_ids(&request).send().await?)
                    .await
            }
            MessagingOperation::SendTransactional => {
                let campaign_id = required(self.campaign_id.as_deref(), "campaign_id")?;
                let request: braze::messaging::SendTransactionalMessageRequest =
                    request(payload, event)?;
                let sent = messaging
                    .send_transactional_message(campaign_id, &request)
                    .send()
                    .await?;
                emitter.emit(&sent).await
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use flowgen_core::task::TaskMeta;

    fn messaging(config: serde_json::Value) -> Messaging {
        let mut fields = serde_json::json!({
            "name": "send",
            "credentials_path": "/etc/braze.json",
            "base_url": "https://rest.example.com"
        });
        if let (Some(fields), serde_json::Value::Object(config)) = (fields.as_object_mut(), config)
        {
            fields.extend(config);
        }
        serde_json::from_value(fields).unwrap()
    }

    #[test]
    fn a_config_missing_what_its_operation_needs_fails_validation() {
        let without_payload = messaging(serde_json::json!({"operation": "trigger_campaign"}));
        let without_campaign = messaging(serde_json::json!({
            "operation": "send_transactional",
            "payload": {"from_event": true}
        }));
        let complete = messaging(serde_json::json!({
            "operation": "trigger_campaign",
            "payload": {"from_event": true}
        }));

        assert_eq!(
            TaskMeta::validate(&without_payload)
                .unwrap_err()
                .to_string(),
            "This operation needs `payload`"
        );
        assert_eq!(
            TaskMeta::validate(&without_campaign)
                .unwrap_err()
                .to_string(),
            "This operation needs `campaign_id`"
        );
        assert!(TaskMeta::validate(&complete).is_ok());
    }
}

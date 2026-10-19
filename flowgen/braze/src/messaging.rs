//! The `braze_messaging` task.

use crate::processor::{present, request, required, Emitter, Error, Operation, Payload};
use flowgen_core::config::ConfigExt;
use flowgen_core::event::Event;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

/// What the task sends.
#[derive(PartialEq, Clone, Debug, Deserialize, Serialize)]
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
#[derive(PartialEq, Clone, Debug, Deserialize, Serialize)]
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
    /// Transactional email campaign ID (send_transactional).
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
    fn name(&self) -> &str {
        &self.name
    }

    fn credentials_path(&self) -> &Path {
        &self.credentials_path
    }

    fn base_url(&self) -> &str {
        &self.base_url
    }

    fn retry(&self) -> Option<&flowgen_core::retry::RetryConfig> {
        self.retry.as_ref()
    }

    fn max_in_flight(&self) -> Option<std::num::NonZeroUsize> {
        self.max_in_flight
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

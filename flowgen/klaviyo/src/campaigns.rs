//! The `klaviyo_campaigns` task.

use crate::processor::{
    attributes, present, request, required, Done, Emitter, Error, Operation, Payload,
};
use flowgen_core::config::ConfigExt;
use flowgen_core::event::Event;
use flowgen_core::task::TaskConfig;
use klaviyo::types;
use serde::{Deserialize, Serialize};
use serde_json::Map;
use std::path::{Path, PathBuf};

/// What the task does with a campaign.
#[derive(PartialEq, Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CampaignOperation {
    /// Get a campaign by `campaign_id`.
    Get,
    /// List campaigns, narrowed by `filter` if set.
    List,
    /// Create a draft campaign from `payload`.
    Create,
    /// Update the campaign `campaign_id` with `payload`.
    Update,
    /// Delete the campaign `campaign_id`.
    Delete,
    /// Copy the campaign `campaign_id`, named `payload.name` if set.
    Clone,
    /// List the messages of the campaign `campaign_id`.
    Messages,
    /// List the variations of the message `message_id`.
    Variations,
    /// Update the variation `variation_id` with `payload`.
    UpdateVariation,
    /// Send or schedule the campaign `campaign_id` as its send settings say.
    Send,
}

/// Reads and writes Klaviyo campaigns.
///
/// ```yaml
/// - klaviyo_campaigns:
///     name: find_campaign
///     operation: list
///     credentials_path: /etc/klaviyo/credentials.json
///     filter: "contains(name,'{{event.data.name}}')"
/// ```
#[derive(PartialEq, Clone, Debug, Deserialize, Serialize, TaskConfig)]
#[serde(deny_unknown_fields)]
pub struct Campaigns {
    /// Task name.
    pub name: String,
    /// What to do with the campaign.
    pub operation: CampaignOperation,
    /// JSON file with the account's private API key: `{"api_key": "pk_..."}`.
    pub credentials_path: PathBuf,
    /// Klaviyo API address, e.g. a proxy; Klaviyo's own when unset.
    #[serde(default)]
    pub base_url: Option<String>,
    /// Campaign ID (get, update, delete, clone, messages, send).
    #[serde(default)]
    pub campaign_id: Option<String>,
    /// Campaign message ID (variations).
    #[serde(default)]
    pub message_id: Option<String>,
    /// Campaign variation ID (update_variation).
    #[serde(default)]
    pub variation_id: Option<String>,
    /// Klaviyo filter expression (list), e.g. `equals(archived,false)`.
    #[serde(default)]
    pub filter: Option<String>,
    /// The attributes of the campaign (create, update, clone) or variation (update_variation).
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

impl ConfigExt for Campaigns {}

#[async_trait::async_trait]
impl Operation for Campaigns {
    fn credentials_path(&self) -> &Path {
        &self.credentials_path
    }

    fn base_url(&self) -> Option<&str> {
        self.base_url.as_deref()
    }

    fn validate(&self) -> Result<(), Error> {
        match self.operation {
            CampaignOperation::Get
            | CampaignOperation::Delete
            | CampaignOperation::Clone
            | CampaignOperation::Messages
            | CampaignOperation::Send => present(self.campaign_id.as_ref(), "campaign_id"),
            CampaignOperation::Update => {
                present(self.campaign_id.as_ref(), "campaign_id")?;
                present(self.payload.as_ref(), "payload")
            }
            CampaignOperation::List => Ok(()),
            CampaignOperation::Create => present(self.payload.as_ref(), "payload"),
            CampaignOperation::Variations => present(self.message_id.as_ref(), "message_id"),
            CampaignOperation::UpdateVariation => {
                present(self.variation_id.as_ref(), "variation_id")?;
                present(self.payload.as_ref(), "payload")
            }
        }
    }

    /// A send repeated after a timeout can deliver the campaign twice.
    fn retries_safely(&self) -> bool {
        !matches!(self.operation, CampaignOperation::Send)
    }

    async fn call(
        &self,
        client: &klaviyo::Client,
        event: &Event,
        emitter: &mut Emitter<'_>,
    ) -> Result<(), Error> {
        let campaigns = client.campaigns();
        match self.operation {
            CampaignOperation::Get => {
                let id = required(self.campaign_id.as_deref(), "campaign_id")?;
                emitter.emit(&campaigns.get(id).send().await?).await
            }
            CampaignOperation::List => {
                let mut pages = match &self.filter {
                    Some(filter) => campaigns.list().filter(filter).pages(),
                    None => campaigns.list().pages(),
                };
                while let Some(page) = pages.next_page().await {
                    emitter.emit(&page?).await?;
                }
                Ok(())
            }
            CampaignOperation::Create => {
                let request: types::CampaignCreateQuery =
                    request(None, attributes(self.payload.as_ref(), event)?)?;
                emitter
                    .emit(&campaigns.create(&request).send().await?)
                    .await
            }
            CampaignOperation::Update => {
                let id = required(self.campaign_id.as_deref(), "campaign_id")?;
                let request: types::CampaignPartialUpdateQuery =
                    request(Some(id), attributes(self.payload.as_ref(), event)?)?;
                emitter
                    .emit(&campaigns.update(id, &request).send().await?)
                    .await
            }
            CampaignOperation::Delete => {
                let id = required(self.campaign_id.as_deref(), "campaign_id")?;
                campaigns.delete(id).send().await?;
                emitter.emit(&Done { id, done: true }).await
            }
            CampaignOperation::Clone => {
                let id = required(self.campaign_id.as_deref(), "campaign_id")?;
                let attributes = match self.payload.as_ref() {
                    Some(payload) => attributes(Some(payload), event)?,
                    None => Map::new(),
                };
                let request: types::CampaignCloneQuery = request(Some(id), attributes)?;
                emitter
                    .emit(&campaigns.clone_campaign(id, &request).send().await?)
                    .await
            }
            CampaignOperation::Messages => {
                let id = required(self.campaign_id.as_deref(), "campaign_id")?;
                emitter.emit(&campaigns.messages(id).send().await?).await
            }
            CampaignOperation::Variations => {
                let id = required(self.message_id.as_deref(), "message_id")?;
                emitter.emit(&campaigns.variations(id).send().await?).await
            }
            CampaignOperation::UpdateVariation => {
                let id = required(self.variation_id.as_deref(), "variation_id")?;
                let request: types::VariationPartialUpdateQuery =
                    request(Some(id), attributes(self.payload.as_ref(), event)?)?;
                emitter
                    .emit(&campaigns.update_variation(id, &request).send().await?)
                    .await
            }
            CampaignOperation::Send => {
                let id = required(self.campaign_id.as_deref(), "campaign_id")?;
                emitter.emit(&campaigns.send(id).send().await?).await
            }
        }
    }
}

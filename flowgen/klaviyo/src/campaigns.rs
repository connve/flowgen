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

/// JSON:API type of a campaign.
const RESOURCE: &str = "campaign";

/// What the task does with a campaign.
#[derive(PartialEq, Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CampaignOperation {
    /// Get a campaign by `campaign_id`.
    Get,
    /// List campaigns matching `filter`, which must name a channel.
    List,
    /// Create a draft campaign from `payload`.
    Create,
    /// Update the campaign `campaign_id` with `payload`.
    Update,
    /// Delete the campaign `campaign_id`.
    Delete,
    /// Copy the campaign `campaign_id`, named `payload.new_name` if set.
    Clone,
    /// List the messages of the campaign `campaign_id`.
    Messages,
    /// Use `template_id` for the message `message_id`, or for every message
    /// of the campaign `campaign_id`.
    AssignTemplate,
    /// Send or schedule the campaign `campaign_id` as its send strategy says.
    Send,
}

/// Reads and writes Klaviyo campaigns.
///
/// ```yaml
/// - klaviyo_campaigns:
///     name: assign_template
///     operation: assign_template
///     credentials_path: /etc/klaviyo/credentials.json
///     campaign_id: "{{event.data.data.id}}"
///     template_id: "{{event.meta.template_id}}"
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
    /// Campaign ID (get, update, delete, clone, messages, assign_template, send).
    #[serde(default)]
    pub campaign_id: Option<String>,
    /// Campaign message ID (assign_template).
    #[serde(default)]
    pub message_id: Option<String>,
    /// Template ID (assign_template).
    #[serde(default)]
    pub template_id: Option<String>,
    /// Klaviyo filter expression (list), e.g. `equals(messages.channel,'email')`.
    #[serde(default)]
    pub filter: Option<String>,
    /// The campaign's attributes (create, update, clone).
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
            CampaignOperation::List => present(self.filter.as_ref(), "filter"),
            CampaignOperation::Create => present(self.payload.as_ref(), "payload"),
            CampaignOperation::AssignTemplate => {
                present(self.template_id.as_ref(), "template_id")?;
                match (&self.message_id, &self.campaign_id) {
                    (None, None) => Err(Error::MissingField("message_id or campaign_id")),
                    _ => Ok(()),
                }
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
                let filter = required(self.filter.as_deref(), "filter")?;
                let mut pages = campaigns.list(filter).pages();
                while let Some(page) = pages.next_page().await {
                    emitter.emit(&page?).await?;
                }
                Ok(())
            }
            CampaignOperation::Create => {
                let request: types::CampaignCreateQuery =
                    request(RESOURCE, None, attributes(self.payload.as_ref(), event)?)?;
                emitter
                    .emit(&campaigns.create(&request).send().await?)
                    .await
            }
            CampaignOperation::Update => {
                let id = required(self.campaign_id.as_deref(), "campaign_id")?;
                let request: types::CampaignPartialUpdateQuery = request(
                    RESOURCE,
                    Some(id),
                    attributes(self.payload.as_ref(), event)?,
                )?;
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
                let request: types::CampaignCloneQuery = request(RESOURCE, Some(id), attributes)?;
                emitter
                    .emit(&campaigns.clone_campaign(id, &request).send().await?)
                    .await
            }
            CampaignOperation::Messages => {
                let id = required(self.campaign_id.as_deref(), "campaign_id")?;
                emitter.emit(&campaigns.messages(id).send().await?).await
            }
            CampaignOperation::AssignTemplate => {
                let template_id = required(self.template_id.as_deref(), "template_id")?;
                match (&self.message_id, &self.campaign_id) {
                    (Some(message_id), _) => {
                        let assigned = campaigns
                            .assign_template(message_id, template_id)
                            .send()
                            .await?;
                        emitter.emit(&assigned).await
                    }
                    (None, Some(campaign_id)) => {
                        let messages = campaigns.messages(campaign_id).send().await?;
                        for message in &messages.data {
                            let assigned = campaigns
                                .assign_template(&message.id, template_id)
                                .send()
                                .await?;
                            emitter.emit(&assigned).await?;
                        }
                        Ok(())
                    }
                    (None, None) => Err(Error::MissingField("message_id or campaign_id")),
                }
            }
            CampaignOperation::Send => {
                let id = required(self.campaign_id.as_deref(), "campaign_id")?;
                emitter.emit(&campaigns.send(id).send().await?).await
            }
        }
    }
}

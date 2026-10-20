//! The `klaviyo_profiles` task.

use crate::processor::{
    attributes, present, request, required, Emitter, Error, Operation, Payload,
};
use flowgen_core::config::ConfigExt;
use flowgen_core::event::Event;
use flowgen_core::task::TaskConfig;
use klaviyo::types;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

/// JSON:API type of a profile.
const RESOURCE: &str = "profile";

/// What the task does with a profile.
#[derive(PartialEq, Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ProfileOperation {
    /// Get a profile by `profile_id`.
    Get,
    /// List profiles, optionally narrowed by `filter`.
    List,
    /// Create a profile from `payload`.
    Create,
    /// Update the profile `profile_id` with `payload`.
    Update,
    /// Update the profile matching the identifiers in `payload`, or create it.
    Upsert,
}

/// Reads and writes Klaviyo profiles.
///
/// ```yaml
/// - klaviyo_profiles:
///     name: save_profile
///     operation: upsert
///     credentials_path: /etc/klaviyo/credentials.json
///     payload:
///       email: "{{event.data.email}}"
///       first_name: "{{event.data.first_name}}"
/// ```
#[derive(PartialEq, Clone, Debug, Deserialize, Serialize, TaskConfig)]
#[serde(deny_unknown_fields)]
pub struct Profiles {
    /// Task name.
    pub name: String,
    /// What to do with the profile.
    pub operation: ProfileOperation,
    /// JSON file with the account's private API key: `{"api_key": "pk_..."}`.
    pub credentials_path: PathBuf,
    /// Klaviyo API address, e.g. a proxy; Klaviyo's own when unset.
    #[serde(default)]
    pub base_url: Option<String>,
    /// Profile ID (get, update).
    #[serde(default)]
    pub profile_id: Option<String>,
    /// Klaviyo filter expression (list), e.g. `equals(email,'ada@example.com')`.
    #[serde(default)]
    pub filter: Option<String>,
    /// The profile's attributes (create, update, upsert).
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

impl ConfigExt for Profiles {}

#[async_trait::async_trait]
impl Operation for Profiles {
    fn credentials_path(&self) -> &Path {
        &self.credentials_path
    }

    fn base_url(&self) -> Option<&str> {
        self.base_url.as_deref()
    }

    fn validate(&self) -> Result<(), Error> {
        match self.operation {
            ProfileOperation::Get => present(self.profile_id.as_ref(), "profile_id"),
            ProfileOperation::Update => {
                present(self.profile_id.as_ref(), "profile_id")?;
                present(self.payload.as_ref(), "payload")
            }
            ProfileOperation::Create | ProfileOperation::Upsert => {
                present(self.payload.as_ref(), "payload")
            }
            ProfileOperation::List => Ok(()),
        }
    }

    async fn call(
        &self,
        client: &klaviyo::Client,
        event: &Event,
        emitter: &mut Emitter<'_>,
    ) -> Result<(), Error> {
        let profiles = client.profiles();
        match self.operation {
            ProfileOperation::Get => {
                let id = required(self.profile_id.as_deref(), "profile_id")?;
                emitter.emit(&profiles.get(id).send().await?).await
            }
            ProfileOperation::List => {
                let mut pages = match &self.filter {
                    Some(filter) => profiles.list().filter(filter).pages(),
                    None => profiles.list().pages(),
                };
                while let Some(page) = pages.next_page().await {
                    emitter.emit(&page?).await?;
                }
                Ok(())
            }
            ProfileOperation::Create => {
                let request: types::ProfileCreateQuery =
                    request(RESOURCE, None, attributes(self.payload.as_ref(), event)?)?;
                emitter.emit(&profiles.create(&request).send().await?).await
            }
            ProfileOperation::Update => {
                let id = required(self.profile_id.as_deref(), "profile_id")?;
                let request: types::ProfilePartialUpdateQuery = request(
                    RESOURCE,
                    Some(id),
                    attributes(self.payload.as_ref(), event)?,
                )?;
                emitter
                    .emit(&profiles.update(id, &request).send().await?)
                    .await
            }
            ProfileOperation::Upsert => {
                let request: types::ProfileUpsertQuery =
                    request(RESOURCE, None, attributes(self.payload.as_ref(), event)?)?;
                emitter.emit(&profiles.upsert(&request).send().await?).await
            }
        }
    }
}

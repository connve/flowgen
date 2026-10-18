//! The `klaviyo_lists` task.

use crate::processor::{present, required, Emitter, Error, Operation};
use flowgen_core::config::ConfigExt;
use flowgen_core::event::Event;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

/// What the task does with a list.
#[derive(PartialEq, Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ListOperation {
    /// Get a list by `list_id`.
    Get,
    /// List lists, optionally narrowed by `filter`.
    List,
}

/// Reads Klaviyo lists, for example to find a list's ID by its name.
///
/// ```yaml
/// - klaviyo_lists:
///     name: find_list
///     operation: list
///     credentials_path: /etc/klaviyo/credentials.json
///     filter: "equals(name,'Newsletter')"
/// ```
#[derive(PartialEq, Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Lists {
    /// Task name.
    pub name: String,
    /// What to do.
    pub operation: ListOperation,
    /// JSON file with the account's private API key: `{"api_key": "pk_..."}`.
    pub credentials_path: PathBuf,
    /// Klaviyo API address, e.g. a proxy; Klaviyo's own when unset.
    #[serde(default)]
    pub base_url: Option<String>,
    /// List ID (get).
    #[serde(default)]
    pub list_id: Option<String>,
    /// Klaviyo filter expression (list), e.g. `equals(name,'Newsletter')`.
    #[serde(default)]
    pub filter: Option<String>,
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

impl ConfigExt for Lists {}

#[async_trait::async_trait]
impl Operation for Lists {
    fn name(&self) -> &str {
        &self.name
    }

    fn credentials_path(&self) -> &Path {
        &self.credentials_path
    }

    fn base_url(&self) -> Option<&str> {
        self.base_url.as_deref()
    }

    fn retry(&self) -> Option<&flowgen_core::retry::RetryConfig> {
        self.retry.as_ref()
    }

    fn max_in_flight(&self) -> Option<std::num::NonZeroUsize> {
        self.max_in_flight
    }

    fn validate(&self) -> Result<(), Error> {
        match self.operation {
            ListOperation::Get => present(self.list_id.as_ref(), "list_id"),
            ListOperation::List => Ok(()),
        }
    }

    async fn call(
        &self,
        client: &klaviyo::Client,
        _event: &Event,
        emitter: &mut Emitter<'_>,
    ) -> Result<(), Error> {
        let lists = client.lists();
        match self.operation {
            ListOperation::Get => {
                let id = required(self.list_id.as_deref(), "list_id")?;
                emitter.emit(&lists.get(id).send().await?).await
            }
            ListOperation::List => {
                let mut pages = match &self.filter {
                    Some(filter) => lists.list().filter(filter).pages(),
                    None => lists.list().pages(),
                };
                while let Some(page) = pages.next_page().await {
                    emitter.emit(&page?).await?;
                }
                Ok(())
            }
        }
    }
}

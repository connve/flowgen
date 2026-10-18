//! The `klaviyo_events` task.

use crate::processor::{attributes, present, request, Emitter, Error, Operation, Payload};
use flowgen_core::config::ConfigExt;
use flowgen_core::event::Event;
use klaviyo::types;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

/// JSON:API type of an event.
const RESOURCE: &str = "event";

/// What the task does with an event.
#[derive(PartialEq, Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum EventOperation {
    /// Track an event, creating its profile and metric if needed.
    Create,
}

/// Sends events to Klaviyo, where they can trigger flows and build segments.
///
/// ```yaml
/// - klaviyo_events:
///     name: track_order
///     operation: create
///     credentials_path: /etc/klaviyo/credentials.json
///     payload:
///       from_event: true
/// ```
#[derive(PartialEq, Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Events {
    /// Task name.
    pub name: String,
    /// What to do.
    pub operation: EventOperation,
    /// JSON file with the account's private API key: `{"api_key": "pk_..."}`.
    pub credentials_path: PathBuf,
    /// Klaviyo API address, e.g. a proxy; Klaviyo's own when unset.
    #[serde(default)]
    pub base_url: Option<String>,
    /// The event's attributes.
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

impl ConfigExt for Events {}

/// What `create` emits, since Klaviyo accepts the event without returning it.
#[derive(Serialize)]
struct Accepted {
    accepted: bool,
}

#[async_trait::async_trait]
impl Operation for Events {
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
        present(self.payload.as_ref(), "payload")
    }

    async fn call(
        &self,
        client: &klaviyo::Client,
        event: &Event,
        emitter: &mut Emitter<'_>,
    ) -> Result<(), Error> {
        match self.operation {
            EventOperation::Create => {
                let request: types::EventCreateQueryV2 =
                    request(RESOURCE, None, attributes(self.payload.as_ref(), event)?)?;
                client.events().create(&request).send().await?;
                emitter.emit(&Accepted { accepted: true }).await
            }
        }
    }
}

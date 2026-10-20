//! The `braze_export_user_data` task.

use crate::processor::{present, request, Emitter, Error, Operation, Payload};
use flowgen_core::config::ConfigExt;
use flowgen_core::event::Event;
use flowgen_core::task::TaskConfig;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

/// Which users the task exports.
#[derive(PartialEq, Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ExportOperation {
    /// Users named by identifier in `payload`, returned in the response.
    UsersByIds,
    /// Every user in the segment `payload.segment_id`, written to files Braze links to.
    UsersBySegment,
    /// Every user in the global control group, written to files Braze links to.
    UsersByGlobalControlGroup,
}

/// Exports Braze user profiles.
///
/// ```yaml
/// - braze_export_user_data:
///     name: export_users
///     operation: users_by_ids
///     credentials_path: /etc/braze/credentials.json
///     base_url: https://rest.iad-01.braze.com
///     payload:
///       external_ids: ["{{event.data.user_id}}"]
///       fields_to_export: [external_id, email]
/// ```
#[derive(PartialEq, Clone, Debug, Deserialize, Serialize, TaskConfig)]
#[serde(deny_unknown_fields)]
pub struct ExportUserData {
    /// Task name.
    pub name: String,
    /// Which users to export.
    pub operation: ExportOperation,
    /// JSON file with the REST API key: `{"api_key": "..."}`.
    pub credentials_path: PathBuf,
    /// REST endpoint of the Braze instance, e.g. `https://rest.iad-01.braze.com`.
    pub base_url: String,
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

impl ConfigExt for ExportUserData {}

#[async_trait::async_trait]
impl Operation for ExportUserData {
    fn credentials_path(&self) -> &Path {
        &self.credentials_path
    }

    fn base_url(&self) -> &str {
        &self.base_url
    }

    fn validate(&self) -> Result<(), Error> {
        present(self.payload.as_ref(), "payload")
    }

    async fn call(
        &self,
        client: &braze::Client,
        event: &Event,
        emitter: &mut Emitter<'_>,
    ) -> Result<(), Error> {
        let export = client.export();
        let payload = self.payload.as_ref();
        match self.operation {
            ExportOperation::UsersByIds => {
                let request: braze::export::ExportUsersByIdsRequest = request(payload, event)?;
                emitter
                    .emit(&export.users_by_ids(&request).send().await?)
                    .await
            }
            ExportOperation::UsersBySegment => {
                let request: braze::export::ExportUsersBySegmentRequest = request(payload, event)?;
                emitter
                    .emit(&export.users_by_segment(&request).send().await?)
                    .await
            }
            ExportOperation::UsersByGlobalControlGroup => {
                let request: braze::export::ExportUsersByGlobalControlGroupRequest =
                    request(payload, event)?;
                emitter
                    .emit(
                        &export
                            .users_by_global_control_group(&request)
                            .send()
                            .await?,
                    )
                    .await
            }
        }
    }
}

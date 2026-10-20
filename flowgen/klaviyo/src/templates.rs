//! The `klaviyo_templates` task.

use crate::processor::{
    attributes, equals, present, request, required, Done, Emitter, Error, Operation, Payload,
};
use flowgen_core::config::ConfigExt;
use flowgen_core::event::Event;
use flowgen_core::task::TaskConfig;
use klaviyo::types;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::path::{Path, PathBuf};

/// JSON:API type of a template.
const RESOURCE: &str = "template";

/// What the task does with a template.
#[derive(PartialEq, Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TemplateOperation {
    /// Get a template by `template_id`.
    Get,
    /// List templates, optionally narrowed by `filter`.
    List,
    /// Create a template from `payload`.
    Create,
    /// Update the template `template_id` with `payload`.
    Update,
    /// Update the template named `payload.name`, or create it.
    Upsert,
    /// Delete the template `template_id`.
    Delete,
    /// Render the template `template_id` with `payload.context`.
    Render,
    /// Copy the template `template_id`, named `payload.name` if set.
    Clone,
}

/// Reads and writes Klaviyo email templates.
///
/// ```yaml
/// - klaviyo_templates:
///     name: save_template
///     operation: upsert
///     credentials_path: /etc/klaviyo/credentials.json
///     payload:
///       from_event: true
/// ```
#[derive(PartialEq, Clone, Debug, Deserialize, Serialize, TaskConfig)]
#[serde(deny_unknown_fields)]
pub struct Templates {
    /// Task name.
    pub name: String,
    /// What to do with the template.
    pub operation: TemplateOperation,
    /// JSON file with the account's private API key: `{"api_key": "pk_..."}`.
    pub credentials_path: PathBuf,
    /// Klaviyo API address, e.g. a proxy; Klaviyo's own when unset.
    #[serde(default)]
    pub base_url: Option<String>,
    /// Template ID (get, update, delete, render, clone).
    #[serde(default)]
    pub template_id: Option<String>,
    /// Klaviyo filter expression (list), e.g. `equals(name,'Welcome')`.
    #[serde(default)]
    pub filter: Option<String>,
    /// The template's attributes (create, update, upsert, render, clone).
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

impl ConfigExt for Templates {}

#[async_trait::async_trait]
impl Operation for Templates {
    fn credentials_path(&self) -> &Path {
        &self.credentials_path
    }

    fn base_url(&self) -> Option<&str> {
        self.base_url.as_deref()
    }

    fn validate(&self) -> Result<(), Error> {
        match self.operation {
            TemplateOperation::Get | TemplateOperation::Delete | TemplateOperation::Clone => {
                present(self.template_id.as_ref(), "template_id")
            }
            TemplateOperation::Update | TemplateOperation::Render => {
                present(self.template_id.as_ref(), "template_id")?;
                present(self.payload.as_ref(), "payload")
            }
            TemplateOperation::Create | TemplateOperation::Upsert => {
                present(self.payload.as_ref(), "payload")
            }
            TemplateOperation::List => Ok(()),
        }
    }

    async fn call(
        &self,
        client: &klaviyo::Client,
        event: &Event,
        emitter: &mut Emitter<'_>,
    ) -> Result<(), Error> {
        let templates = client.templates();
        match self.operation {
            TemplateOperation::Get => {
                let id = required(self.template_id.as_deref(), "template_id")?;
                emitter.emit(&templates.get(id).send().await?).await
            }
            TemplateOperation::List => {
                let mut pages = match &self.filter {
                    Some(filter) => templates.list().filter(filter).pages(),
                    None => templates.list().pages(),
                };
                while let Some(page) = pages.next_page().await {
                    emitter.emit(&page?).await?;
                }
                Ok(())
            }
            TemplateOperation::Create => {
                let request: types::TemplateCreateHtmlOrDndQuery =
                    request(RESOURCE, None, attributes(self.payload.as_ref(), event)?)?;
                emitter
                    .emit(&templates.create(&request).send().await?)
                    .await
            }
            TemplateOperation::Update => {
                let id = required(self.template_id.as_deref(), "template_id")?;
                let request: types::TemplateUpdateHtmlOrDndQuery = request(
                    RESOURCE,
                    Some(id),
                    attributes(self.payload.as_ref(), event)?,
                )?;
                emitter
                    .emit(&templates.update(id, &request).send().await?)
                    .await
            }
            TemplateOperation::Upsert => {
                let attributes = attributes(self.payload.as_ref(), event)?;
                let name = match attributes.get("name") {
                    Some(Value::String(name)) => name.clone(),
                    _ => return Err(Error::MissingField("payload.name")),
                };
                let existing = templates
                    .list()
                    .filter(&equals("name", &name)?)
                    .send()
                    .await?;
                match existing.data.first() {
                    Some(template) => {
                        let request: types::TemplateUpdateHtmlOrDndQuery =
                            request(RESOURCE, Some(&template.id), attributes)?;
                        let updated = templates.update(&template.id, &request).send().await?;
                        emitter.emit(&updated).await
                    }
                    None => {
                        let request: types::TemplateCreateHtmlOrDndQuery =
                            request(RESOURCE, None, attributes)?;
                        emitter
                            .emit(&templates.create(&request).send().await?)
                            .await
                    }
                }
            }
            TemplateOperation::Delete => {
                let id = required(self.template_id.as_deref(), "template_id")?;
                templates.delete(id).send().await?;
                emitter.emit(&Done { id, done: true }).await
            }
            TemplateOperation::Render => {
                let id = required(self.template_id.as_deref(), "template_id")?;
                let request: types::TemplateRenderQuery = request(
                    RESOURCE,
                    Some(id),
                    attributes(self.payload.as_ref(), event)?,
                )?;
                emitter
                    .emit(&templates.render(id, &request).send().await?)
                    .await
            }
            TemplateOperation::Clone => {
                let id = required(self.template_id.as_deref(), "template_id")?;
                let attributes = match self.payload.as_ref() {
                    Some(payload) => attributes(Some(payload), event)?,
                    None => Map::new(),
                };
                let request: types::TemplateCloneQuery = request(RESOURCE, Some(id), attributes)?;
                emitter
                    .emit(&templates.clone_template(id, &request).send().await?)
                    .await
            }
        }
    }
}

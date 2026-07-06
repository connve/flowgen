//! Configuration for the Braze export user IDs task.

use flowgen_core::config::ConfigExt;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// User alias used to look up profiles when external identifiers are not available.
///
/// This is a thin wrapper around [`braze::export::UserAlias`] so the config struct
/// can derive `PartialEq` while the upstream type does not.
#[derive(PartialEq, Clone, Debug, Deserialize, Serialize)]
pub struct UserAlias {
    /// Alias value.
    pub alias_name: String,
    /// Namespace the alias belongs to.
    pub alias_label: String,
}

impl From<UserAlias> for braze::export::UserAlias {
    fn from(alias: UserAlias) -> Self {
        Self {
            alias_name: alias.alias_name,
            alias_label: alias.alias_label,
        }
    }
}

/// Configuration for exporting Braze user profiles by identifier.
///
/// This task calls `POST /users/export/ids` for every incoming event. The
/// caller provides identifiers either as static configuration or as templates
/// rendered against the event. At least one identifier type must resolve to a
/// non-empty value at runtime, otherwise the task fails with a validation
/// error.
///
/// # Example
///
/// ```yaml
/// export_user_ids:
///   name: export_users
///   credentials_path: /etc/braze/credentials.json
///   external_ids:
///     - "{{event.data.user_id}}"
///   fields_to_export:
///     - external_id
///     - email
///     - first_name
///     - last_name
/// ```
#[derive(PartialEq, Clone, Debug, Default, Deserialize, Serialize)]
pub struct Processor {
    /// Unique name / identifier of the task.
    pub name: String,
    /// Path to a JSON credentials file containing `api_key` and `rest_endpoint`.
    pub credentials_path: PathBuf,
    /// Optional list of external identifiers to export (up to 50 per call).
    /// Template expressions are rendered against the incoming event.
    #[serde(default)]
    pub external_ids: Option<Vec<String>>,
    /// Optional list of user aliases to export.
    #[serde(default)]
    pub user_aliases: Option<Vec<UserAlias>>,
    /// Optional device identifier to export (only one allowed by Braze).
    #[serde(default)]
    pub device_id: Option<String>,
    /// Optional Braze internal identifier to export (only one allowed by Braze).
    #[serde(default)]
    pub braze_id: Option<String>,
    /// Optional email address to export (only one allowed by Braze).
    #[serde(default)]
    pub email_address: Option<String>,
    /// Optional phone number to export (only one allowed by Braze).
    #[serde(default)]
    pub phone: Option<String>,
    /// Optional subset of fields to return on each user.
    /// Required for accounts onboarded after 2024-08-22.
    #[serde(default)]
    pub fields_to_export: Option<Vec<String>>,
    /// Optional list of upstream task names this task depends on.
    /// When set, this task only receives events from the named tasks.
    /// When not set, the task receives from the previous task in the list (linear chain).
    #[serde(default)]
    pub depends_on: Option<Vec<String>>,
    /// Optional retry configuration (overrides app-level retry config).
    #[serde(default)]
    pub retry: Option<flowgen_core::retry::RetryConfig>,
}

impl ConfigExt for Processor {}

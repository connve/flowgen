//! The `salesforce_soapapi_merge` task: merges duplicate records (Account,
//! Contact, Lead, Individual) into a master record through the SOAP API.
//! Related records of the merged ones are reparented to the master.

use crate::processor::{to_json, Call, Emitter, Error, Operation, Output};
use flowgen_core::config::ConfigExt;
use flowgen_core::task::TaskConfig;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::path::{Path, PathBuf};

/// Configuration for Salesforce SOAP API record merge operations.
///
/// Merges up to three duplicate SObject records (Account, Contact, Lead, or
/// Individual) into a single master record, reparenting related records to the
/// master and optionally overriding field values on the surviving record.
///
/// # Examples
///
/// Merge two duplicate accounts into a master:
/// ```yaml
/// salesforce_soapapi_merge:
///   name: merge_accounts
///   credentials_path: /etc/salesforce/credentials.json
///   sobject_type: Account
///   master_record_id: "{{event.data.master_id}}"
///   record_ids_to_merge:
///     - "{{event.data.duplicate_id}}"
/// ```
///
/// Merge with field overrides and duplicate detection bypass:
/// ```yaml
/// salesforce_soapapi_merge:
///   name: merge_contacts
///   credentials_path: /etc/salesforce/credentials.json
///   sobject_type: Contact
///   master_record_id: "{{event.data.master_id}}"
///   record_ids_to_merge:
///     - "{{event.data.dup_1}}"
///     - "{{event.data.dup_2}}"
///   master_field_overrides:
///     Email: "{{event.data.preferred_email}}"
///     Phone: "{{event.data.preferred_phone}}"
///   allow_duplicate_save: true
/// ```
#[derive(PartialEq, Clone, Debug, Deserialize, Serialize, TaskConfig)]
#[serde(deny_unknown_fields)]
pub struct Merge {
    /// Unique task identifier.
    pub name: String,
    /// Path to Salesforce authentication credentials.
    pub credentials_path: PathBuf,
    /// SObject type to merge (Account, Contact, Lead, or Individual).
    pub sobject_type: String,
    /// Salesforce ID of the master (winning) record.
    pub master_record_id: String,
    /// IDs of records to merge into the master (one or two).
    pub record_ids_to_merge: Vec<String>,
    /// Optional field values to override on the master record after merge.
    #[serde(default)]
    pub master_field_overrides: Option<Map<String, Value>>,
    /// Bypass duplicate detection rules during merge.
    #[serde(default)]
    pub allow_duplicate_save: bool,
    /// Optional list of upstream task names this task depends on.
    #[serde(default)]
    pub depends_on: Option<Vec<String>>,
    /// Optional retry configuration (overrides app-level retry config).
    #[serde(default)]
    pub retry: Option<flowgen_core::retry::RetryConfig>,
    /// Events handled at once; defaults to 64.
    #[serde(default)]
    pub max_in_flight: Option<std::num::NonZeroUsize>,
}

impl ConfigExt for Merge {}

/// The result of a merge.
#[derive(Serialize)]
struct Merged<'a> {
    success: bool,
    merged_record_ids: &'a [String],
    updated_related_ids: &'a [String],
}

#[async_trait::async_trait]
impl Operation for Merge {
    type Client = salesforce_core::soapapi::Client;

    fn client(auth: &salesforce_core::client::Client) -> Result<Self::Client, Error> {
        salesforce_core::soapapi::ClientBuilder::new(auth.clone())
            .build()
            .map_err(|source| Error::SoapClient { source })
    }

    fn credentials_path(&self) -> &Path {
        &self.credentials_path
    }

    fn retries_safely(&self) -> bool {
        false
    }

    async fn call(
        &self,
        client: &Self::Client,
        _call: &Call<'_>,
        emitter: &mut Emitter<'_>,
    ) -> Result<(), Error> {
        let count = self.record_ids_to_merge.len();
        if count == 0 || count > 2 {
            return Err(Error::InvalidMergeCount { count });
        }
        let response = client
            .merge(
                &self.sobject_type,
                &self.master_record_id,
                &self.record_ids_to_merge,
                self.master_field_overrides.as_ref(),
                self.allow_duplicate_save,
            )
            .await
            .map_err(|source| Error::Merge { source })?;
        let merged = Merged {
            success: response.success,
            merged_record_ids: &response.merged_record_ids,
            updated_related_ids: &response.updated_related_ids,
        };
        emitter
            .emit(Output::json(to_json(&merged)?).id(self.master_record_id.clone()))
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_merge_config_deserialization() {
        let json = r#"{
            "name": "merge_accounts",
            "credentials_path": "/etc/salesforce/credentials.json",
            "sobject_type": "Account",
            "master_record_id": "001000000000001",
            "record_ids_to_merge": ["001000000000002"],
            "allow_duplicate_save": false
        }"#;
        let config: Merge = serde_json::from_str(json).unwrap();
        assert_eq!(config.name, "merge_accounts");
        assert_eq!(config.sobject_type, "Account");
        assert_eq!(config.master_record_id, "001000000000001");
        assert_eq!(config.record_ids_to_merge.len(), 1);
        assert!(!config.allow_duplicate_save);
        assert!(config.master_field_overrides.is_none());
    }

    #[test]
    fn test_merge_config_with_overrides() {
        let json = r#"{
            "name": "merge_contacts",
            "credentials_path": "/etc/salesforce/credentials.json",
            "sobject_type": "Contact",
            "master_record_id": "003000000000001",
            "record_ids_to_merge": ["003000000000002", "003000000000003"],
            "master_field_overrides": {
                "Email": "preferred@example.com",
                "Phone": "+1234567890"
            },
            "allow_duplicate_save": true
        }"#;
        let config: Merge = serde_json::from_str(json).unwrap();
        assert_eq!(config.record_ids_to_merge.len(), 2);
        assert!(config.allow_duplicate_save);
        let overrides = config.master_field_overrides.unwrap();
        assert_eq!(overrides.get("Email").unwrap(), "preferred@example.com");
    }

    #[test]
    fn the_result_keeps_its_field_names() {
        let ids = vec!["001".to_string()];
        let merged = Merged {
            success: true,
            merged_record_ids: &ids,
            updated_related_ids: &[],
        };

        let expected: Value = serde_json::from_str(
            r#"{"success":true,"merged_record_ids":["001"],"updated_related_ids":[]}"#,
        )
        .unwrap();
        assert_eq!(to_json(&merged).unwrap(), expected);
    }
}

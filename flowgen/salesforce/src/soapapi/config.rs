//! Configuration for Salesforce SOAP API merge operations.

use flowgen_core::config::ConfigExt;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::path::PathBuf;

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
#[derive(PartialEq, Clone, Debug, Deserialize, Serialize)]
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
}

impl ConfigExt for Merge {}

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
}

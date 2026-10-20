//! The `salesforce_restapi_composite` task: create, get, update, upsert and
//! delete up to 200 records in one call, and create record trees.

use crate::processor::{
    required, to_json, Call, Emitter, Error, Operation, Output, ALLOW_SAVE_VALUE,
    DUPLICATE_RULE_HEADER,
};
use flowgen_core::config::ConfigExt;
use flowgen_core::task::TaskConfig;
use salesforce_core::restapi::{
    CompositeCollectionCreateRequest, CompositeCollectionRetrieveRequest,
    CompositeCollectionUpdateRequest, CompositeCollectionUpsertRequest, CompositeTreeRequest,
};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::path::{Path, PathBuf};
use tracing::warn;

/// Composite operations.
#[derive(PartialEq, Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CompositeOperation {
    /// Create records.
    Create,
    /// Get records by ID.
    Get,
    /// Update records by ID.
    Update,
    /// Upsert records by external ID.
    Upsert,
    /// Delete records by ID.
    Delete,
    /// Create a tree of related records.
    Tree,
}

/// The records of a composite call: written in the config, or the event data.
#[derive(PartialEq, Clone, Debug, Deserialize, Serialize)]
#[serde(untagged)]
pub enum CompositePayload {
    /// Use the incoming event data, an array of records.
    FromEvent { from_event: bool },
    /// Records written in the config.
    Records(Vec<Map<String, Value>>),
}

/// Configuration for Salesforce REST API Composite operations.
///
/// # Examples
///
/// ```yaml
/// salesforce_restapi_composite:
///   name: upsert_contacts
///   operation: upsert
///   credentials_path: /path/to/salesforce_creds.json
///   sobject_type: Contact
///   external_id_field: ExternalId__c
///   payload:
///     from_event: true
/// ```
#[derive(PartialEq, Clone, Debug, Deserialize, Serialize, TaskConfig)]
#[serde(deny_unknown_fields)]
pub struct Composite {
    /// Unique task identifier.
    pub name: String,
    /// Composite operation type.
    pub operation: CompositeOperation,
    /// Path to Salesforce authentication credentials.
    pub credentials_path: PathBuf,
    /// SObject type; fills in records' missing `attributes` (get, upsert, tree).
    #[serde(default)]
    pub sobject_type: Option<String>,
    /// The records (create, update, upsert, tree).
    #[serde(default)]
    pub payload: Option<CompositePayload>,
    /// Record IDs (get, delete).
    #[serde(default)]
    pub ids: Option<Vec<String>>,
    /// Fields to retrieve (get).
    #[serde(default)]
    pub fields: Option<Vec<String>>,
    /// External ID field name (upsert).
    #[serde(default)]
    pub external_id_field: Option<String>,
    /// Roll back every record when one fails.
    #[serde(default)]
    pub all_or_none: Option<bool>,
    /// Send `Sforce-Duplicate-Rule-Header: allowSave=true` so Salesforce
    /// accepts a record even when a duplicate detection rule would
    /// normally block it. Default duplicate rules ship as "Block" on
    /// Account/Lead/Contact, so without this flag composite create /
    /// update / upsert silently no-op on duplicates. Applies to
    /// create, update, upsert, and tree.
    #[serde(default)]
    pub allow_duplicate_save: bool,
    /// Optional list of upstream task names this task depends on.
    /// When set, this task only receives events from the named tasks.
    /// When not set, the task receives from the previous task in the list (linear chain).
    #[serde(default)]
    pub depends_on: Option<Vec<String>>,
    /// Optional retry configuration (overrides app-level retry config).
    #[serde(default)]
    pub retry: Option<flowgen_core::retry::RetryConfig>,
    /// Events handled at once; defaults to 64.
    #[serde(default)]
    pub max_in_flight: Option<std::num::NonZeroUsize>,
}

impl ConfigExt for Composite {}

#[async_trait::async_trait]
impl Operation for Composite {
    type Client = salesforce_core::restapi::Client;

    fn client(auth: &salesforce_core::client::Client) -> Result<Self::Client, Error> {
        crate::processor::rest_client(auth)
    }

    fn credentials_path(&self) -> &Path {
        &self.credentials_path
    }

    fn retries_safely(&self) -> bool {
        !matches!(
            self.operation,
            CompositeOperation::Create | CompositeOperation::Tree
        )
    }

    async fn call(
        &self,
        client: &Self::Client,
        call: &Call<'_>,
        emitter: &mut Emitter<'_>,
    ) -> Result<(), Error> {
        let composite = client.composite();
        let all_or_none = self.all_or_none.unwrap_or(false);
        let (response, id) = match self.operation {
            CompositeOperation::Create => {
                let request = CompositeCollectionCreateRequest {
                    all_or_none,
                    records: self.records(call)?,
                };
                let mut builder = composite.create_records(&request);
                if self.allow_duplicate_save {
                    builder = builder.header(DUPLICATE_RULE_HEADER, ALLOW_SAVE_VALUE);
                }
                let response = builder.send().await.map_err(composite_error)?;
                (
                    to_json(&*response)?,
                    format!("composite_create_{}", response.len()),
                )
            }
            CompositeOperation::Get => {
                let sobject_type = required(self.sobject_type.as_deref(), "sobject_type")?;
                let request = CompositeCollectionRetrieveRequest {
                    ids: required(self.ids.clone(), "ids")?,
                    fields: self.fields.clone().unwrap_or_default(),
                };
                let response = composite
                    .get_records(sobject_type, &request)
                    .send()
                    .await
                    .map_err(composite_error)?;
                (
                    to_json(&response)?,
                    format!("composite_get_{}", response.len()),
                )
            }
            CompositeOperation::Update => {
                let request = CompositeCollectionUpdateRequest {
                    all_or_none,
                    records: self.records(call)?,
                };
                let mut builder = composite.update_records(&request);
                if self.allow_duplicate_save {
                    builder = builder.header(DUPLICATE_RULE_HEADER, ALLOW_SAVE_VALUE);
                }
                let response = builder.send().await.map_err(composite_error)?;
                (
                    to_json(&*response)?,
                    format!("composite_update_{}", response.len()),
                )
            }
            CompositeOperation::Upsert => {
                let sobject_type = required(self.sobject_type.as_deref(), "sobject_type")?;
                let external_id_field =
                    required(self.external_id_field.as_deref(), "external_id_field")?;
                let request = CompositeCollectionUpsertRequest {
                    all_or_none,
                    records: self.records(call)?,
                };
                let mut builder =
                    composite.upsert_records(sobject_type, external_id_field, &request);
                if self.allow_duplicate_save {
                    builder = builder.header(DUPLICATE_RULE_HEADER, ALLOW_SAVE_VALUE);
                }
                let response = builder.send().await.map_err(composite_error)?;
                (
                    to_json(&*response)?,
                    format!("composite_upsert_{}", response.len()),
                )
            }
            CompositeOperation::Delete => {
                let ids = required(self.ids.as_ref(), "ids")?;
                let mut builder = composite.delete_records(ids.join(","));
                if let Some(all_or_none) = self.all_or_none {
                    builder = builder.all_or_none(all_or_none);
                }
                let response = builder.send().await.map_err(composite_error)?;
                (
                    to_json(&*response)?,
                    format!("composite_delete_{}", response.len()),
                )
            }
            CompositeOperation::Tree => {
                let sobject_type = required(self.sobject_type.as_deref(), "sobject_type")?;
                let request = CompositeTreeRequest {
                    records: self.records(call)?,
                };
                let mut builder = composite.create_record_tree(sobject_type, &request);
                if self.allow_duplicate_save {
                    builder = builder.header(DUPLICATE_RULE_HEADER, ALLOW_SAVE_VALUE);
                }
                let response = builder.send().await.map_err(composite_error)?;
                (
                    to_json(&response)?,
                    format!("composite_tree_{}", response.results.len()),
                )
            }
        };
        log_failures(&response);
        emitter.emit(Output::json(response).id(id)).await
    }
}

impl Composite {
    /// The records of `payload` for this event, as the request's record type.
    fn records<T: DeserializeOwned>(&self, call: &Call<'_>) -> Result<Vec<T>, Error> {
        let mut records = match self.payload.as_ref() {
            Some(CompositePayload::FromEvent { from_event: true }) => {
                match call.event.data_as_json()? {
                    Value::Array(records) => records
                        .into_iter()
                        .enumerate()
                        .map(|(index, record)| match record {
                            Value::Object(record) => Ok(record),
                            _ => Err(Error::RecordNotObject { index }),
                        })
                        .collect::<Result<Vec<_>, _>>()?,
                    _ => return Err(Error::EventDataNotArray),
                }
            }
            Some(CompositePayload::Records(records)) => records.clone(),
            Some(CompositePayload::FromEvent { from_event: false }) | None => {
                return Err(Error::MissingField("payload"))
            }
        };
        ensure_attributes(&mut records, self.sobject_type.as_deref())?;
        records
            .into_iter()
            .map(|record| {
                serde_json::from_value(Value::Object(record))
                    .map_err(|source| Error::InvalidRecords { source })
            })
            .collect()
    }
}

/// Gives each record without `attributes` the type `sobject_type`; without
/// a type, every record must carry its own.
fn ensure_attributes(
    records: &mut [Map<String, Value>],
    sobject_type: Option<&str>,
) -> Result<(), Error> {
    for (index, record) in records.iter_mut().enumerate() {
        if record.contains_key("attributes") {
            continue;
        }
        match sobject_type {
            Some(sobject_type) => {
                let attributes =
                    Map::from_iter([("type".to_string(), Value::String(sobject_type.to_string()))]);
                record.insert("attributes".to_string(), Value::Object(attributes));
            }
            None => return Err(Error::MissingAttributes { index }),
        }
    }
    Ok(())
}

fn composite_error(source: salesforce_core::restapi::CompositeError) -> Error {
    Error::Composite {
        source: Box::new(source),
    }
}

/// Logs the records that failed; the full response still goes downstream,
/// so later tasks can handle the records that succeeded.
fn log_failures(response: &Value) {
    let Some(results) = response.as_array() else {
        return;
    };
    for (index, result) in results.iter().enumerate() {
        if result.get("success").and_then(Value::as_bool) == Some(false) {
            let errors = result.get("errors").cloned().unwrap_or(Value::Null);
            warn!(
                index,
                errors = %errors,
                "Composite operation completed with one or more failures"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(fields: &[(&str, Value)]) -> Map<String, Value> {
        fields
            .iter()
            .map(|(name, value)| (name.to_string(), value.clone()))
            .collect()
    }

    fn typed(sobject_type: &str) -> Value {
        Value::Object(record(&[("type", Value::from(sobject_type))]))
    }

    #[test]
    fn create_and_tree_do_not_retry_safely() {
        let config = |operation| Composite {
            operation,
            ..serde_json::from_str(r#"{"name": "a", "operation": "get", "credentials_path": "/a"}"#)
                .unwrap()
        };

        assert!(config(CompositeOperation::Update).retries_safely());
        assert!(!config(CompositeOperation::Create).retries_safely());
        assert!(!config(CompositeOperation::Tree).retries_safely());
    }

    #[test]
    fn test_config_create_with_records() {
        let json = r#"{
            "name": "batch_create",
            "operation": "create",
            "credentials_path": "/etc/sfdc/credentials.json",
            "payload": [
                {"attributes": {"type": "Account"}, "Name": "Acme"},
                {"attributes": {"type": "Account"}, "Name": "Globex"}
            ]
        }"#;
        let config: Composite = serde_json::from_str(json).unwrap();
        assert_eq!(config.operation, CompositeOperation::Create);
        match config.payload {
            Some(CompositePayload::Records(ref records)) => assert_eq!(records.len(), 2),
            _ => panic!("expected Records payload"),
        }
    }

    #[test]
    fn test_config_create_from_event() {
        let json = r#"{
            "name": "batch_create",
            "operation": "create",
            "credentials_path": "/etc/sfdc/credentials.json",
            "payload": { "from_event": true }
        }"#;
        let config: Composite = serde_json::from_str(json).unwrap();
        match config.payload {
            Some(CompositePayload::FromEvent { from_event }) => assert!(from_event),
            _ => panic!("expected FromEvent payload"),
        }
    }

    #[test]
    fn test_config_get() {
        let json = r#"{
            "name": "batch_get",
            "operation": "get",
            "credentials_path": "/etc/sfdc/credentials.json",
            "sobject_type": "Account",
            "ids": ["001abc", "001def"],
            "fields": ["Id", "Name"]
        }"#;
        let config: Composite = serde_json::from_str(json).unwrap();
        assert_eq!(config.operation, CompositeOperation::Get);
        assert_eq!(config.sobject_type.as_deref(), Some("Account"));
        assert_eq!(config.ids.as_ref().unwrap().len(), 2);
        assert_eq!(config.fields.as_ref().unwrap().len(), 2);
    }

    #[test]
    fn test_config_all_or_none() {
        let json = r#"{
            "name": "batch_create",
            "operation": "create",
            "credentials_path": "/etc/sfdc/credentials.json",
            "all_or_none": true,
            "payload": { "from_event": true }
        }"#;
        let config: Composite = serde_json::from_str(json).unwrap();
        assert_eq!(config.all_or_none, Some(true));
    }

    #[test]
    fn test_ensure_attributes_injects_when_sobject_type_set() {
        let mut records = vec![
            record(&[("Name", Value::from("Acme"))]),
            record(&[("Name", Value::from("Globex"))]),
        ];
        ensure_attributes(&mut records, Some("Account")).unwrap();
        assert_eq!(records[0].get("attributes"), Some(&typed("Account")));
        assert_eq!(records[1].get("attributes"), Some(&typed("Account")));
    }

    #[test]
    fn test_ensure_attributes_preserves_existing() {
        let mut records = vec![record(&[
            ("attributes", typed("Contact")),
            ("Name", Value::from("Alice")),
        ])];
        ensure_attributes(&mut records, Some("Account")).unwrap();
        assert_eq!(records[0].get("attributes"), Some(&typed("Contact")));
    }

    #[test]
    fn test_ensure_attributes_errors_without_sobject_type() {
        let mut records = vec![
            record(&[
                ("attributes", typed("Account")),
                ("Name", Value::from("Acme")),
            ]),
            record(&[("Name", Value::from("Globex"))]),
        ];
        assert!(matches!(
            ensure_attributes(&mut records, None),
            Err(Error::MissingAttributes { index: 1 })
        ));
    }

    #[test]
    fn test_ensure_attributes_empty_records() {
        let mut records: Vec<Map<String, Value>> = Vec::new();
        ensure_attributes(&mut records, Some("Account")).unwrap();
        ensure_attributes(&mut records, None).unwrap();
    }
}

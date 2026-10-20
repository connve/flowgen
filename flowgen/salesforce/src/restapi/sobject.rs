//! The `salesforce_restapi_sobject` task: SObject create, get,
//! get_by_external_id, update, upsert, delete and get_deleted.

use crate::processor::{
    required, to_json, Call, Emitter, Error, Operation, Output, Payload, ALLOW_SAVE_VALUE,
    DUPLICATE_RULE_HEADER,
};
use flowgen_core::config::ConfigExt;
use flowgen_core::task::TaskConfig;
use salesforce_core::chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::path::{Path, PathBuf};
use tracing::warn;

/// SObject CRUD operations.
#[derive(PartialEq, Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SObjectOperation {
    /// Create a new record.
    Create,
    /// Get a record by ID.
    Get,
    /// Get a record by external ID field.
    GetByExternalId,
    /// Update an existing record by ID.
    Update,
    /// Upsert a record by external ID (update if exists, create if not).
    Upsert,
    /// Delete a record by ID.
    Delete,
    /// Retrieve IDs and deletion datetimes of records deleted within a time window.
    GetDeleted,
}

/// Configuration for Salesforce REST API CRUD operations.
///
/// # Examples
///
/// Create a new record with explicit payload:
/// ```yaml
/// salesforce_restapi_sobject:
///   name: create_account
///   operation: create
///   credentials_path: /path/to/salesforce_creds.json
///   sobject_type: Account
///   payload:
///     Name: "{{event.data.company_name}}"
///     Industry: "Technology"
///     BillingCity: "San Francisco"
/// ```
///
/// Create using event data as payload:
/// ```yaml
/// salesforce_restapi_sobject:
///   name: create_account
///   operation: create
///   credentials_path: /path/to/salesforce_creds.json
///   sobject_type: Account
///   payload:
///     from_event: true
/// ```
///
/// Update a record:
/// ```yaml
/// salesforce_restapi_sobject:
///   name: update_account
///   operation: update
///   credentials_path: /path/to/salesforce_creds.json
///   sobject_type: Account
///   record_id: "{{event.data.salesforce_id}}"
///   payload:
///     Name: "{{event.data.new_name}}"
///     Industry: "Manufacturing"
/// ```
///
/// Upsert by external ID:
/// ```yaml
/// salesforce_restapi_sobject:
///   name: upsert_contact
///   operation: upsert
///   credentials_path: /path/to/salesforce_creds.json
///   sobject_type: Contact
///   external_id_field: ExternalId__c
///   external_id_value: "{{event.data.external_id}}"
///   payload:
///     FirstName: "{{event.data.first_name}}"
///     LastName: "{{event.data.last_name}}"
///     Email: "{{event.data.email}}"
/// ```
///
/// Get records deleted within a time window:
/// ```yaml
/// salesforce_restapi_sobject:
///   name: get_deleted_accounts
///   operation: get_deleted
///   credentials_path: /path/to/salesforce_creds.json
///   sobject_type: Account
///   start: "{{event.data.start_iso}}"
///   end: "{{event.data.end_iso}}"
/// ```
#[derive(PartialEq, Clone, Debug, Deserialize, Serialize, TaskConfig)]
#[serde(deny_unknown_fields)]
pub struct SObject {
    /// Unique task identifier.
    pub name: String,
    /// SObject operation type.
    pub operation: SObjectOperation,
    /// Path to Salesforce authentication credentials.
    pub credentials_path: PathBuf,
    /// SObject type (e.g., "Account", "Contact", "CustomObject__c").
    pub sobject_type: String,
    /// Payload configuration for record data (create, update, upsert).
    #[serde(default)]
    pub payload: Option<Payload>,
    /// Salesforce record ID (get, update, delete).
    #[serde(default)]
    pub record_id: Option<String>,
    /// Comma-separated list of fields to retrieve (get, get_by_external_id).
    #[serde(default)]
    pub fields: Option<String>,
    /// External ID field name (get_by_external_id, upsert).
    #[serde(default)]
    pub external_id_field: Option<String>,
    /// External ID value (get_by_external_id, upsert).
    #[serde(default)]
    pub external_id_value: Option<String>,
    /// Start datetime of the deletion window (ISO 8601) for get_deleted.
    #[serde(default)]
    pub start: Option<String>,
    /// End datetime of the deletion window (ISO 8601) for get_deleted.
    #[serde(default)]
    pub end: Option<String>,
    /// Send `Sforce-Duplicate-Rule-Header: allowSave=true` so Salesforce
    /// accepts a record even when a duplicate detection rule would
    /// normally block it. Default duplicate rules ship as "Block" on
    /// Account/Lead/Contact, so without this flag create calls silently
    /// no-op on duplicates. Applies to create, update, and upsert.
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

impl ConfigExt for SObject {}

/// A record deleted within the requested window.
#[derive(Serialize)]
struct Deleted<'a> {
    id: &'a str,
    deleted_date: &'a DateTime<Utc>,
}

#[async_trait::async_trait]
impl Operation for SObject {
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
            SObjectOperation::Create | SObjectOperation::Upsert
        )
    }

    async fn call(
        &self,
        client: &Self::Client,
        call: &Call<'_>,
        emitter: &mut Emitter<'_>,
    ) -> Result<(), Error> {
        match self.operation {
            SObjectOperation::Create => {
                let fields = self.fields(call)?;
                emitter.emit(self.create(client, fields).await?).await
            }
            SObjectOperation::Get => {
                let record_id = required(self.record_id.as_deref(), "record_id")?;
                let mut builder = client.get(&self.sobject_type, record_id);
                if let Some(fields) = self.fields.as_deref() {
                    builder = builder.fields(fields);
                }
                let record = builder.send().await.map_err(sobject_error)?;
                emitter.emit(Output::json(record).id(record_id)).await
            }
            SObjectOperation::GetByExternalId => {
                let field = required(self.external_id_field.as_deref(), "external_id_field")?;
                let value = required(self.external_id_value.as_deref(), "external_id_value")?;
                let mut builder = client.get_by_external_id(&self.sobject_type, field, value);
                if let Some(fields) = self.fields.as_deref() {
                    builder = builder.fields(fields);
                }
                let record = builder.send().await.map_err(sobject_error)?;
                let record_id = match record.get("Id").and_then(Value::as_str) {
                    Some(id) => id.to_string(),
                    None => value.to_string(),
                };
                emitter.emit(Output::json(record).id(record_id)).await
            }
            SObjectOperation::Update => {
                let record_id = required(self.record_id.as_deref(), "record_id")?;
                let fields = self.fields(call)?;
                emitter
                    .emit(self.update(client, record_id, fields).await?)
                    .await
            }
            SObjectOperation::Upsert => {
                let field = required(self.external_id_field.as_deref(), "external_id_field")?;
                let value = required(self.external_id_value.as_deref(), "external_id_value")?;
                let fields = self.fields(call)?;
                let existing = client
                    .get_by_external_id(&self.sobject_type, field, value)
                    .fields("Id")
                    .send()
                    .await;
                let output = match existing {
                    Ok(record) => {
                        let record_id = record
                            .get("Id")
                            .and_then(Value::as_str)
                            .ok_or(Error::MissingRecordId)?;
                        self.update(client, record_id, fields).await?
                    }
                    Err(_) => self.create(client, fields).await?,
                };
                emitter.emit(output).await
            }
            SObjectOperation::Delete => {
                let record_id = required(self.record_id.as_deref(), "record_id")?;
                client
                    .delete(&self.sobject_type, record_id)
                    .send()
                    .await
                    .map_err(sobject_error)?;
                emitter
                    .emit(Output::json(Value::Object(Map::new())).id(record_id))
                    .await
            }
            SObjectOperation::GetDeleted => {
                let start = datetime(self.start.as_deref(), "start")?;
                let end = datetime(self.end.as_deref(), "end")?;
                let deleted = client
                    .get_deleted(&self.sobject_type, start..end)
                    .send()
                    .await
                    .map_err(sobject_error)?;
                let records: Vec<Deleted<'_>> = deleted
                    .deleted_records
                    .iter()
                    .map(|record| Deleted {
                        id: &record.id,
                        deleted_date: &record.deleted_date,
                    })
                    .collect();
                emitter.emit(Output::json(to_json(&records)?)).await
            }
        }
    }
}

impl SObject {
    /// The record fields of `payload` for this event.
    fn fields(&self, call: &Call<'_>) -> Result<Value, Error> {
        let payload = self
            .payload
            .as_ref()
            .ok_or(Error::MissingField("payload"))?;
        Ok(Value::Object(payload.fields(call.event)?))
    }

    /// Creates a record and returns the response to emit.
    async fn create(
        &self,
        client: &salesforce_core::restapi::Client,
        fields: Value,
    ) -> Result<Output, Error> {
        let mut builder = client.create(&self.sobject_type, fields);
        if self.allow_duplicate_save {
            builder = builder.header(DUPLICATE_RULE_HEADER, ALLOW_SAVE_VALUE);
        }
        let response = builder.send().await.map_err(sobject_error)?;
        let data = to_json(&response)?;
        log_failure(&data);
        Ok(Output::json(data).id(response.id))
    }

    /// Updates a record; Salesforce answers with no body, so the event is empty.
    async fn update(
        &self,
        client: &salesforce_core::restapi::Client,
        record_id: &str,
        fields: Value,
    ) -> Result<Output, Error> {
        let mut builder = client.update(&self.sobject_type, record_id, fields);
        if self.allow_duplicate_save {
            builder = builder.header(DUPLICATE_RULE_HEADER, ALLOW_SAVE_VALUE);
        }
        builder.send().await.map_err(sobject_error)?;
        Ok(Output::json(Value::Object(Map::new())).id(record_id))
    }
}

fn sobject_error(source: salesforce_core::restapi::SObjectError) -> Error {
    Error::SObject {
        source: Box::new(source),
    }
}

/// `value` of `field` as a UTC datetime.
fn datetime(value: Option<&str>, field: &'static str) -> Result<DateTime<Utc>, Error> {
    let value = required(value, field)?;
    DateTime::parse_from_rfc3339(value)
        .map(|datetime| datetime.with_timezone(&Utc))
        .map_err(|source| Error::InvalidDateTime { field, source })
}

/// Logs a failed save; the full response still goes downstream.
fn log_failure(response: &Value) {
    if response.get("success").and_then(Value::as_bool) == Some(false) {
        let errors = response.get("errors").cloned().unwrap_or(Value::Null);
        warn!(errors = %errors, "SObject operation completed with failure");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn get_and_delete_retry_safely_but_create_and_upsert_do_not() {
        let config = |operation| {
            SObject {
            operation,
            ..serde_json::from_str(
                r#"{"name": "a", "operation": "get", "credentials_path": "/a", "sobject_type": "Account"}"#,
            )
            .unwrap()
        }
        };

        assert!(config(SObjectOperation::Get).retries_safely());
        assert!(config(SObjectOperation::Delete).retries_safely());
        assert!(!config(SObjectOperation::Create).retries_safely());
        assert!(!config(SObjectOperation::Upsert).retries_safely());
    }

    #[test]
    fn deletion_window_must_be_rfc3339() {
        assert!(datetime(Some("2024-01-01T00:00:00+02:00"), "start").is_ok());
        assert!(matches!(
            datetime(Some("yesterday"), "start"),
            Err(Error::InvalidDateTime { field: "start", .. })
        ));
        assert!(matches!(
            datetime(None, "end"),
            Err(Error::MissingField("end"))
        ));
    }

    #[test]
    fn test_config_create_with_fields() {
        let json = r#"{
            "name": "create_account",
            "operation": "create",
            "credentials_path": "/etc/sfdc/credentials.json",
            "sobject_type": "Account",
            "payload": {
                "Name": "Acme Corp",
                "Industry": "Technology"
            }
        }"#;
        let config: SObject = serde_json::from_str(json).unwrap();
        assert_eq!(config.operation, SObjectOperation::Create);
        match config.payload {
            Some(Payload::Fields(ref fields)) => {
                assert_eq!(fields.get("Name").unwrap(), "Acme Corp");
                assert_eq!(fields.get("Industry").unwrap(), "Technology");
            }
            _ => panic!("expected Fields payload"),
        }
    }

    #[test]
    fn test_config_create_from_event() {
        let json = r#"{
            "name": "create_account",
            "operation": "create",
            "credentials_path": "/etc/sfdc/credentials.json",
            "sobject_type": "Account",
            "payload": { "from_event": true }
        }"#;
        let config: SObject = serde_json::from_str(json).unwrap();
        match config.payload {
            Some(Payload::FromEvent { from_event }) => assert!(from_event),
            _ => panic!("expected FromEvent payload"),
        }
    }

    #[test]
    fn test_config_get_with_fields() {
        let json = r#"{
            "name": "get_account",
            "operation": "get",
            "credentials_path": "/etc/sfdc/credentials.json",
            "sobject_type": "Account",
            "record_id": "001000000000001",
            "fields": "Id,Name,Industry"
        }"#;
        let config: SObject = serde_json::from_str(json).unwrap();
        assert_eq!(config.operation, SObjectOperation::Get);
        assert_eq!(config.record_id.as_deref(), Some("001000000000001"));
        assert_eq!(config.fields.as_deref(), Some("Id,Name,Industry"));
    }

    #[test]
    fn test_config_get_by_external_id() {
        let json = r#"{
            "name": "get_contact",
            "operation": "get_by_external_id",
            "credentials_path": "/etc/sfdc/credentials.json",
            "sobject_type": "Contact",
            "external_id_field": "ExternalId__c",
            "external_id_value": "EXT-001"
        }"#;
        let config: SObject = serde_json::from_str(json).unwrap();
        assert_eq!(config.operation, SObjectOperation::GetByExternalId);
        assert_eq!(config.external_id_field.as_deref(), Some("ExternalId__c"));
        assert_eq!(config.external_id_value.as_deref(), Some("EXT-001"));
    }

    #[test]
    fn test_config_get_deleted() {
        let json = r#"{
            "name": "get_deleted_accounts",
            "operation": "get_deleted",
            "credentials_path": "/etc/sfdc/credentials.json",
            "sobject_type": "Account",
            "start": "2024-01-01T00:00:00+00:00",
            "end": "2024-01-02T00:00:00+00:00"
        }"#;
        let config: SObject = serde_json::from_str(json).unwrap();
        assert_eq!(config.operation, SObjectOperation::GetDeleted);
        assert_eq!(config.start.as_deref(), Some("2024-01-01T00:00:00+00:00"));
        assert_eq!(config.end.as_deref(), Some("2024-01-02T00:00:00+00:00"));
    }

    #[test]
    fn test_config_defaults() {
        let json = r#"{
            "name": "minimal",
            "operation": "get",
            "credentials_path": "/etc/sfdc/credentials.json",
            "sobject_type": "Account"
        }"#;
        let config: SObject = serde_json::from_str(json).unwrap();
        assert!(config.payload.is_none());
        assert!(config.record_id.is_none());
        assert!(config.fields.is_none());
        assert!(config.external_id_field.is_none());
        assert!(config.external_id_value.is_none());
        assert!(config.depends_on.is_none());
        assert!(config.retry.is_none());
    }

    #[test]
    fn test_sobject_operation_serialization() {
        for (operation, name) in [
            (SObjectOperation::Create, "\"create\""),
            (SObjectOperation::Get, "\"get\""),
            (SObjectOperation::GetByExternalId, "\"get_by_external_id\""),
            (SObjectOperation::Update, "\"update\""),
            (SObjectOperation::Upsert, "\"upsert\""),
            (SObjectOperation::Delete, "\"delete\""),
            (SObjectOperation::GetDeleted, "\"get_deleted\""),
        ] {
            assert_eq!(serde_json::to_string(&operation).unwrap(), name);
            assert_eq!(
                serde_json::from_str::<SObjectOperation>(name).unwrap(),
                operation
            );
        }
    }
}

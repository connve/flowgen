# Salesforce REST API

CRUD operations on Salesforce SObjects and composite batch operations.

## SObject operations

Single-record operations: create, get, get_by_external_id, update, upsert, delete.

```yaml
- salesforce_restapi_sobject:
    name: create_account
    operation: create
    credentials_path: /etc/salesforce/credentials.json
    sobject_type: Account
    payload:
      Name: "{{event.data.company_name}}"
      Industry: "Technology"
```

### Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `name` | string | required | Task name. |
| `operation` | string | required | `create`, `get`, `get_by_external_id`, `update`, `upsert`, `delete`. |
| `credentials_path` | string | required | Path to Salesforce credentials. |
| `sobject_type` | string | required | SObject type (e.g., `Account`, `Contact`). |
| `payload` | object | | Record fields — explicit values or `from_event: true`. |
| `record_id` | string | | Salesforce record ID (for get, update, delete). Supports templating. |
| `fields` | string | | Comma-separated field list (for get). |
| `external_id_field` | string | | External ID field name (for upsert, get_by_external_id). |
| `external_id_value` | string | | External ID value. Supports templating. |
| `depends_on` | list | | Upstream task names. |
| `retry` | object | | Retry configuration. |

### Examples

**Upsert by external ID:**

```yaml
- salesforce_restapi_sobject:
    name: upsert_contact
    operation: upsert
    credentials_path: /etc/salesforce/credentials.json
    sobject_type: Contact
    external_id_field: External_Id__c
    external_id_value: "{{event.data.external_id}}"
    payload:
      FirstName: "{{event.data.first_name}}"
      LastName: "{{event.data.last_name}}"
```

**Get a record:**

```yaml
- salesforce_restapi_sobject:
    name: get_account
    operation: get
    credentials_path: /etc/salesforce/credentials.json
    sobject_type: Account
    record_id: "{{event.data.account_id}}"
    fields: "Id,Name,Industry"
```

## Composite operations

Batch operations on multiple records: create, get, update, upsert, delete, tree.

```yaml
- salesforce_restapi_composite:
    name: bulk_create
    operation: create
    credentials_path: /etc/salesforce/credentials.json
    sobject_type: Account
    payload:
      from_event: true
```

### Composite fields

| Field | Type | Default | Description |
|---|---|---|---|
| `name` | string | required | Task name. |
| `operation` | string | required | `create`, `get`, `update`, `upsert`, `delete`, `tree`. |
| `credentials_path` | string | required | Path to Salesforce credentials. |
| `sobject_type` | string | | SObject type. |
| `payload` | object | | Records — explicit list or `from_event: true`. |
| `ids` | list | | Record IDs (for get, delete). |
| `fields` | list | | Field list (for get). |
| `external_id_field` | string | | External ID field (for upsert). |
| `all_or_none` | bool | | Atomic transaction — all succeed or all fail. |
| `depends_on` | list | | Upstream task names. |
| `retry` | object | | Retry configuration. |

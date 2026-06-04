# Salesforce REST API

CRUD operations on Salesforce SObjects, composite batch operations, and SOSL search.

## Output

### SObject CRUD

| Format | Crate | Description |
|---|---|---|
| [JSON](https://docs.rs/serde_json/latest/serde_json/enum.Value.html) | [salesforce_core](https://crates.io/crates/salesforce_core) | Salesforce API response. For `create`: includes `id`, `success`, `errors`. For `get` / `get_by_external_id`: the full record object. Record ID is set as `event.id`. |

### Composite

| Format | Crate | Description |
|---|---|---|
| [JSON](https://docs.rs/serde_json/latest/serde_json/enum.Value.html) | [salesforce_core](https://crates.io/crates/salesforce_core) | Array of sub-request results, each with `statusCode`, `result`, and `errors`. |

### Search / Query

| Format | Crate | Description |
|---|---|---|
| [JSON](https://docs.rs/serde_json/latest/serde_json/enum.Value.html) | [salesforce_core](https://crates.io/crates/salesforce_core) | Salesforce SOSL/SOQL response with `searchRecords` or `records` array. |

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
| `retry` | object | | [Retry configuration](/docs/flowgen/concepts/retry). |

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
| `retry` | object | | [Retry configuration](/docs/flowgen/concepts/retry). |

## SOSL Search

Execute [SOSL](https://developer.salesforce.com/docs/atlas.en-us.soql_sosl.meta/soql_sosl/sforce_api_calls_sosl.htm) queries to search across multiple objects simultaneously. Returns matching records as a JSON event.

```yaml
- salesforce_restapi_search:
    name: find_accounts
    credentials_path: /etc/salesforce/credentials.json
    query: "FIND {Acme} IN ALL FIELDS RETURNING Account(Id, Name, Industry)"
```

### Search fields

| Field | Type | Default | Description |
|---|---|---|---|
| `name` | string | required | Task name. |
| `credentials_path` | string | required | Path to Salesforce credentials. |
| `query` | string | required | SOSL query string. Supports templating. |
| `depends_on` | list | | Upstream task names. |
| `retry` | object | | [Retry configuration](/docs/flowgen/concepts/retry). |

### Templating the search term

SOSL wraps the search term in curly braces (`FIND {term} ...`). Since Handlebars only treats double `{{` as special, a single `{` is a literal character. This means `{{{event.data.term}}}` is parsed as literal `{` + `{{event.data.term}}` + literal `}`.

```yaml
- salesforce_restapi_search:
    name: dynamic_search
    credentials_path: /etc/salesforce/credentials.json
    query: "FIND {{{event.data.search_term}}} IN ALL FIELDS RETURNING Account(Id, Name), Contact(Id, FirstName, LastName, Email)"
```

### Examples

**Search with specific scope and field limits:**

```yaml
- salesforce_restapi_search:
    name: search_name_fields
    credentials_path: /etc/salesforce/credentials.json
    query: "FIND {Acme} IN NAME FIELDS RETURNING Account(Id, Name, Industry LIMIT 10), Contact(Id, FirstName, LastName LIMIT 5)"
```

**Webhook-triggered search:**

```yaml
flow:
  name: salesforce_search
  tasks:
    - http_webhook:
        name: trigger
        method: POST
        endpoint: /search

    - salesforce_restapi_search:
        name: sosl_search
        credentials_path: $SALESFORCE_CREDENTIALS_PATH
        query: "FIND {{{event.data.term}}} IN ALL FIELDS RETURNING Account(Id, Name), Contact(Id, Email)"

    - log:
        name: log_results
        level: info
        structured: true
```


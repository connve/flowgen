# Salesforce Bulk API

Bulk query jobs for extracting large datasets from Salesforce.

## Operations

| Operation | Description |
|---|---|
| `create` | Create a bulk query job. |
| `get` | Get job status. |
| `get_results` | Download query results as Arrow RecordBatch. |
| `abort` | Abort a running job. |
| `delete` | Delete a job. |

## Configuration

```yaml
- salesforce_bulkapi_query_job:
    name: export_accounts
    operation: create
    credentials_path: /etc/salesforce/credentials.json
    query: "SELECT Id, Name, Industry FROM Account"
```

### Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `name` | string | required | Task name. |
| `operation` | string | required | `create`, `get`, `get_results`, `abort`, `delete`. |
| `credentials_path` | string | required | Path to Salesforce credentials. |
| `query` | string/resource | | SOQL query (for `create`). |
| `query_operation` | string | `query` | `query` or `query_all` (includes deleted/archived). |
| `content_type` | string | `csv` | Output format. |
| `column_delimiter` | string | `comma` | CSV delimiter: `comma`, `tab`, `semicolon`, `pipe`. |
| `line_ending` | string | `lf` | Line ending: `lf` or `crlf`. |
| `job_id` | string | | Job ID (for get, get_results, abort, delete). Supports templating. |
| `batch_size` | int | 10000 | Rows per Arrow RecordBatch. |
| `has_header` | bool | true | First row is header. |
| `depends_on` | list | | Upstream task names. |
| `retry` | object | | Retry configuration. |

## Example: Create job and get results

```yaml
flow:
  name: salesforce_export
  tasks:
    - generate:
        name: trigger
        cron: "0 2 * * *"

    - salesforce_bulkapi_query_job:
        name: create_job
        operation: create
        credentials_path: /etc/salesforce/credentials.json
        query: "SELECT Id, Name, Industry FROM Account WHERE LastModifiedDate = TODAY"

    - salesforce_bulkapi_query_job:
        name: get_results
        operation: get_results
        credentials_path: /etc/salesforce/credentials.json
        job_id: "{{event.data.id}}"
```

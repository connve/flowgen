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
| `parameters` | map | | Values bound to `:name` in the query (for `create`). See [Query parameters](/docs/flowgen/salesforce/rest#query-parameters). |
| `query_operation` | string | `query` | `query` or `queryAll` (includes deleted/archived). |
| `content_type` | string | `csv` | Output format. |
| `column_delimiter` | string | `comma` | CSV delimiter: `comma`, `tab`, `semicolon`, `pipe`, `caret`, `backquote`. |
| `line_ending` | string | `lf` | Line ending: `lf` or `crlf`. |
| `job_id` | string | | Job ID (for get, get_results, abort, delete). Supports templating. |
| `batch_size` | int | 10000 | Records per result page and per Arrow RecordBatch (for `get_results`). |
| `has_header` | bool | true | First row is header. |
| `depends_on` | list | | Upstream task names. |
| `retry` | object | | [Retry configuration](/docs/flowgen/concepts/retry). |

## Output

Format: [JSON](https://docs.rs/serde_json/latest/serde_json/enum.Value.html) / [Arrow RecordBatch](https://docs.rs/arrow/latest/arrow/record_batch/struct.RecordBatch.html) (for `get_results`)

What `event.data` contains depends on the operation:

- **`create`** / **`get`** / **`abort`**: Salesforce `QueryJobInfo` object (includes `id`, `state`, `operation`, `query`).
- **`get_results`**: Arrow RecordBatch — each row from the query results becomes a record. Reads every result page, `batch_size` records at a time, retrying a failed page on its own so no page is emitted twice.

### Result types

`get_results` types each column from the Salesforce describe of the job's object, so types match the fields rather than the values in them:

| Salesforce field type | Arrow type |
|---|---|
| `boolean` | Boolean |
| `int`, `long` | Int64 |
| `double`, `currency`, `percent` | Float64 |
| `date` | Date32 |
| `datetime` | Timestamp (milliseconds, UTC) |
| anything else | Utf8 |

A relationship column such as `Owner.Name` is typed from the related object. Columns through a polymorphic relationship, fields the describe doesn't list, and all columns when `has_header` is `false`, are Utf8. The column delimiter is read from the job.
- **`delete`**: `{job_id, deleted: true}`.

## Example: Create job and get results

```yaml
flow:
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

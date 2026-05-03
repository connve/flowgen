# BigQuery Storage API

Read from and write to BigQuery tables using the Storage API. Higher throughput than the query API for large datasets.

## Storage Read

Reads table data directly via the BigQuery Storage Read API. Returns Arrow RecordBatch.

```yaml
- gcp_bigquery_storage_read:
    name: read_accounts
    credentials_path: /etc/gcp/service-account.json
    project_id: my-project
    dataset_id: salesforce
    table_id: accounts
    selected_fields:
      - id
      - name
      - industry
    row_restriction: "industry = 'Technology'"
```

### Read fields

| Field | Type | Default | Description |
|---|---|---|---|
| `name` | string | required | Task name. |
| `credentials_path` | string | required | GCP service account credentials. |
| `project_id` | string | required | GCP project ID. |
| `dataset_id` | string | required | BigQuery dataset. |
| `table_id` | string | required | BigQuery table. |
| `selected_fields` | list | | Columns to read (all if omitted). |
| `row_restriction` | string | | WHERE clause for filtering rows. |
| `sample_percentage` | float | | Random sampling percentage. |
| `snapshot_time` | string | | Time-travel query timestamp (RFC 3339). |
| `max_stream_count` | int | | Max parallel read streams. |
| `data_format` | string | `arrow` | Result format: `arrow` or `avro`. |
| `depends_on` | list | | Upstream task names. |
| `retry` | object | | Retry configuration. |

## Storage Write

Streams data into BigQuery tables via the Storage Write API. Accepts Arrow RecordBatch input.

```yaml
- gcp_bigquery_storage_write:
    name: write_accounts
    credentials_path: /etc/gcp/service-account.json
    project_id: my-project
    dataset_id: salesforce
    table_id: accounts
```

### Write fields

| Field | Type | Default | Description |
|---|---|---|---|
| `name` | string | required | Task name. |
| `credentials_path` | string | required | GCP service account credentials. |
| `project_id` | string | required | GCP project ID. |
| `dataset_id` | string | required | BigQuery dataset. |
| `table_id` | string | required | BigQuery table. |
| `change_type` | string | | CDC change type: `upsert` or `delete`. |
| `depends_on` | list | | Upstream task names. |
| `retry` | object | | Retry configuration. |

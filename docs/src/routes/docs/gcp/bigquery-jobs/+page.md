# BigQuery Jobs

Create, monitor, and manage BigQuery jobs. Used for load jobs from GCS.

## Operations

| Operation | Description |
|---|---|
| `create` | Create a load job. |
| `get` | Get job status. |
| `cancel` | Cancel a running job. |
| `delete` | Delete a job. |

## Configuration

```yaml
- gcp_bigquery_job:
    name: load_from_gcs
    operation: create
    credentials_path: /etc/gcp/service-account.json
    project_id: my-project
    source_uris:
      - "gs://my-bucket/data/*.parquet"
    destination_table:
      project_id: my-project
      dataset_id: raw
      table_id: events
    source_format: parquet
    write_disposition: write_append
    autodetect: true
```

### Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `name` | string | required | Task name. |
| `operation` | string | required | `create`, `get`, `cancel`, `delete`. |
| `credentials_path` | string | required | GCP service account credentials. |
| `project_id` | string | required | GCP project ID. |
| `location` | string | | BigQuery location. |
| `source_uris` | list/template | | GCS source URIs (for create). |
| `destination_table` | object | | Target table (`project_id`, `dataset_id`, `table_id`). |
| `source_format` | string | `newline_delimited_json` | `parquet`, `csv`, `newline_delimited_json`, `avro`. |
| `write_disposition` | string | `write_append` | `write_append`, `write_truncate`, `write_empty`. |
| `create_disposition` | string | `create_if_needed` | `create_if_needed`, `create_never`. |
| `autodetect` | bool | | Auto-detect schema from source. |
| `schema` | list | | Explicit schema (list of field definitions). |
| `max_bad_records` | int | | Max bad records before job fails. |
| `job_id` | string | | Job ID (for get, cancel, delete). Supports templating. |
| `poll_interval` | duration | `5s` | Status check interval. |
| `max_poll_duration` | duration | `30m` | Max time to wait for completion. |
| `labels` | map | | Job labels. |
| `depends_on` | list | | Upstream task names. |
| `retry` | object | | Retry configuration. |

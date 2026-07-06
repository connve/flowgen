# BigQuery Query

Runs SQL queries against Google BigQuery. Returns results as Arrow RecordBatch.

## Configuration

```yaml
- gcp_bigquery_query:
    name: get_orders
    credentials_path: /etc/gcp/service-account.json
    project_id: my-project
    query: "SELECT * FROM `my-project.dataset.orders` WHERE date = @date"
    parameters:
      date: "{{event.data.date}}"
```

### Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `name` | string | required | Task name. |
| `credentials_path` | string | | GCP service account credentials. Falls back to Application Default Credentials when omitted. |
| `project_id` | string | required | GCP project ID (data project). |
| `job_project_id` | string | | GCP project ID for billing (if different). |
| `query` | string/resource | required | SQL query. Supports templating and resource files. |
| `parameters` | map | | Named query parameters. |
| `location` | string | | BigQuery location (e.g., `US`, `EU`). |
| `max_results` | int | | Max rows per page. |
| `timeout` | duration | `10s` | Query timeout. |
| `use_query_cache` | bool | true | Use BigQuery query cache. |
| `use_legacy_sql` | bool | false | Use legacy SQL syntax. |
| `default_dataset` | string | | Default dataset for unqualified table names. |
| `labels` | map | | Job labels. |
| `use_storage_read` | bool | false | Route the result through the BigQuery Storage Read API instead of paginated REST results. Recommended for large result sets (over one million rows or one hundred megabytes). Adds temporary-table overhead for smaller queries and is not compatible with data-definition or data-manipulation statements such as `INSERT` or `CREATE TABLE`. |
| `depends_on` | list | | Upstream task names. |
| `retry` | object | | [Retry configuration](/docs/flowgen/concepts/retry). |

## Output

The task streams the result and emits one downstream event per wire batch (`getQueryResults` page for the REST backend, Arrow batch for the Storage Read backend). The final event carries `completion_tx` so downstream buffers observe end-of-batch even on empty result sets. An empty result set still produces one event containing an empty `RecordBatch`.

| Format | Crate | Description |
|---|---|---|
| [Arrow RecordBatch](https://docs.rs/arrow/latest/arrow/record_batch/struct.RecordBatch.html) | [google-cloud-bigquery](https://github.com/googleapis/google-cloud-rust) | Query results with columns and types matching the BigQuery result set. On the REST backend, the job ID is set as `event.id` on the first emitted event. The Storage Read backend does not surface the parent job ID. |

## Example: Query with resource file

```yaml
- gcp_bigquery_query:
    name: daily_report
    credentials_path: /etc/gcp/service-account.json
    project_id: my-project
    query:
      resource: queries/daily_report.sql
    parameters:
      start_date: "{{event.data.start}}"
      end_date: "{{event.data.end}}"
```

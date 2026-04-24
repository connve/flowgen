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
| `credentials_path` | string | required | GCP service account credentials. |
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
| `depends_on` | list | | Upstream task names. |
| `retry` | object | | Retry configuration. |

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

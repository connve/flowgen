# MSSQL Query

Runs SQL queries against Microsoft SQL Server. Returns results as Arrow RecordBatch.

## Configuration

```yaml
- mssql_query:
    name: get_orders
    credentials_path: /etc/mssql/credentials.json
    query: "SELECT * FROM orders WHERE date = @p1"
    parameters:
      - "{{event.data.date}}"
    batch_size: 5000
```

### Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `name` | string | required | Task name. |
| `credentials_path` | string | required | Path to SQL Server credentials file. |
| `query` | string/resource | required | SQL query. Supports templating and resource files. |
| `parameters` | list | | Query parameters (`@p1`, `@p2`, etc.). |
| `batch_size` | int | 10000 | Rows per Arrow RecordBatch. |
| `max_connections` | int | 10 | Connection pool size. |
| `connection_timeout` | duration | `30s` | Connection timeout. |
| `query_timeout` | duration | `2m` | Query execution timeout. |
| `depends_on` | list | | Upstream task names. |
| `retry` | object | | Retry configuration. |

### Credentials file

```json
{
  "host": "sql-server.example.com",
  "port": 1433,
  "database": "mydb",
  "username": "user",
  "password": "pass",
  "trust_server_certificate": false,
  "encrypt": true
}
```

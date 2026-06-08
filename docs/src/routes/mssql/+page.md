# MSSQL

Flowgen connects to Microsoft SQL Server for query execution.

- [Query](/docs/flowgen/mssql/query) — runs SQL queries and returns results as Arrow RecordBatch.

## Credentials

The `credentials_path` field points to a JSON file with SQL Server connection details:

```json
{
  "host": "sqlserver.example.com",
  "port": 1433,
  "database": "mydb",
  "username": "user",
  "password": "pass",
  "trust_server_certificate": false,
  "encrypt": true
}
```

| Field | Type | Default | Description |
|---|---|---|---|
| `host` | string | required | SQL Server hostname. |
| `port` | number | `1433` | SQL Server port. |
| `database` | string | required | Database name. |
| `username` | string | required | SQL auth username. |
| `password` | string | required | SQL auth password. |
| `trust_server_certificate` | boolean | `false` | Set `true` for self-signed certificates. |
| `encrypt` | boolean | `true` | TLS encryption. Set `false` only for legacy servers. |

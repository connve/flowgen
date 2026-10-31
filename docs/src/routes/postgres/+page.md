# PostgreSQL

Flowgen connects to PostgreSQL for query execution, bulk writes and change data capture.

- [Query](/docs/flowgen/postgres/query) — runs SQL statements and returns results as Arrow RecordBatch.
- [Write](/docs/flowgen/postgres/write) — appends, upserts or deletes rows with `COPY`.
- [CDC](/docs/flowgen/postgres/cdc) — emits each row change of the tables in a publication.

## Credentials

The `credentials_path` field points to a JSON file with the connection details:

```json
{
  "host": "postgres.example.com",
  "port": 5432,
  "database": "mydb",
  "username": "user",
  "password": "pass",
  "ssl_mode": "require"
}
```

| Field | Type | Default | Description |
|---|---|---|---|
| `host` | string | required | Server hostname. |
| `port` | number | `5432` | Server port. |
| `database` | string | required | Database name. |
| `username` | string | required | Username. |
| `password` | string | required | Password. |
| `ssl_mode` | string | `prefer` | `disable`, `prefer` (TLS when the server offers it, plain when it doesn't) or `require`. |
| `ca_certificate_path` | string | | PEM file with the CA that signs the server certificate. System roots when unset. |

With TLS, the server certificate is always verified, also with `prefer`: a self-signed certificate needs `ca_certificate_path`.

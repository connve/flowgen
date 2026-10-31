# MSSQL Query

Runs a SQL statement against Microsoft SQL Server. Returns the rows as Arrow RecordBatch.

## Configuration

```yaml
- mssql_query:
    name: get_orders
    credentials_path: /etc/mssql/credentials.json
    query: "SELECT * FROM orders WHERE date = @P1"
    parameters:
      - "{{event.data.date}}"
    batch_size: 5000
```

### Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `name` | string | required | Task name. |
| `credentials_path` | string | required | Path to the [credentials file](#credentials-file). |
| `query` | string/resource | required | SQL statement. Supports templating and resource files. |
| `parameters` | list | | Values for `@P1`, `@P2`, etc. Sent as text; the server converts them to the types the statement needs. |
| `batch_size` | int | 10000 | Rows per Arrow RecordBatch. |
| `max_connections` | int | 10 | Connection pool size. |
| `connection_timeout` | duration | `30s` | Timeout to open a connection and log in. |
| `query_timeout` | duration | `2m` | Timeout to get a connection and the first result, and for each later answer of the server while the rows are read. |
| `depends_on` | list | | Upstream task names. |
| `retry` | object | | [Retry configuration](/docs/flowgen/concepts/retry). |

Connection errors, deadlocks, lock timeouts, and the server errors Microsoft lists as transient (such as a busy or unavailable Azure SQL database) are retried. SQL errors such as a syntax error or a constraint violation are not, nor is a statement that ran past `query_timeout`, nor a query that failed after it sent batches.

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

| Field | Default | Description |
|---|---|---|
| `port` | `1433` | Server port. |
| `trust_server_certificate` | `false` | Accept the server certificate without validating it, for self-signed certificates. |
| `encrypt` | `true` | Encrypt the whole connection. With `false`, only the login is encrypted. |

## Output

Rows are streamed and emitted as one event per `batch_size` rows, from the statement's first result. A query without rows emits one empty batch with the result columns. A statement without a result set, such as an `INSERT`, emits one empty batch.

| SQL Server | Arrow |
|---|---|
| `bit` | `Boolean` |
| `tinyint` | `UInt8` |
| `smallint`, `int` | `Int16`, `Int32` |
| `bigint`, and a nullable integer column of any width | `Int64` |
| `real` | `Float32` |
| `float`, `money`, `smallmoney` | `Float64` |
| `decimal`, `numeric` | `Utf8`, exact decimal text |
| `char`, `varchar`, `nchar`, `nvarchar`, `text`, `ntext`, `xml`, `uniqueidentifier` | `Utf8` |
| `date` | `Date32` |
| `time` | `Time64(Microsecond)` |
| `datetime`, `smalldatetime`, `datetime2` | `Timestamp(Microsecond)` |
| `datetimeoffset` | `Timestamp(Microsecond, UTC)` |
| `binary`, `varbinary`, `image` | `Binary` |

Other types, such as `sql_variant` and user-defined types, fail the event with the column name. Convert them in the query, for example `CAST(value AS nvarchar(max))`. For exact `money` values, cast them to `decimal(19, 4)`.

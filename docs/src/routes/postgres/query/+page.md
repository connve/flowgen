# PostgreSQL Query

Runs a SQL statement against PostgreSQL. Returns the rows as Arrow RecordBatch.

## Configuration

```yaml
- postgres_query:
    name: get_orders
    credentials_path: /etc/postgres/credentials.json
    query: "SELECT * FROM orders WHERE status = $1 AND placed_at > $2"
    parameters:
      - "{{event.data.status}}"
      - "{{event.data.since}}"
    batch_size: 5000
```

### Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `name` | string | required | Task name. |
| `credentials_path` | string | required | Path to the [credentials file](/docs/flowgen/postgres). |
| `query` | string/resource | required | SQL statement. Supports templating and resource files. |
| `parameters` | list | | Values for `$1`, `$2`, etc. Sent as text; the server converts them to the parameter types. |
| `batch_size` | int | 10000 | Rows per Arrow RecordBatch. |
| `max_connections` | int | 10 | Connection pool size. |
| `connection_timeout` | duration | `30s` | Timeout to open a connection. |
| `query_timeout` | duration | `2m` | Longest a statement runs on the server (`statement_timeout`), and timeout to get a connection and the first rows. |
| `depends_on` | list | | Upstream task names. |
| `retry` | object | | [Retry configuration](/docs/flowgen/concepts/retry). |

Connection errors, serialization failures and deadlocks are retried. SQL errors such as a syntax error or a constraint violation are not, nor is a statement that ran past `query_timeout`, nor a query that failed after it sent batches.

## Output

Rows are streamed and emitted as one event per `batch_size` rows. A query without rows emits one empty batch with the result columns. A statement without a result set, such as an `INSERT` without `RETURNING`, emits one empty batch.

| PostgreSQL | Arrow |
|---|---|
| `boolean` | `Boolean` |
| `smallint`, `integer`, `bigint` | `Int16`, `Int32`, `Int64` |
| `oid` | `UInt32` |
| `real`, `double precision` | `Float32`, `Float64` |
| `numeric` | `Utf8`, exact decimal text |
| `text`, `varchar`, `char`, `"char"`, `name`, `citext`, `xml`, enums | `Utf8` |
| `json`, `jsonb` | `Utf8`, JSON text |
| `uuid` | `Utf8` |
| `date` | `Date32` |
| `time` | `Time64(Microsecond)` |
| `timestamp` | `Timestamp(Microsecond)` |
| `timestamptz` | `Timestamp(Microsecond, UTC)` |
| `bytea` | `Binary` |

`infinity` and `-infinity` dates and timestamps are null.

Other types, such as arrays, `interval` and `inet`, fail the event with the column name. Cast them in the query, for example `tags::text` or `to_json(tags)`.

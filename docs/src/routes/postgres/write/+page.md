# PostgreSQL Write

Writes the rows of each event into a PostgreSQL table with `COPY`: appended, or upserted or deleted by the table's primary key.

## Configuration

```yaml
- postgres_write:
    name: save_orders
    credentials_path: /etc/postgres/credentials.json
    table: public.orders
    change_type: upsert
```

### Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `name` | string | required | Task name. |
| `credentials_path` | string | required | Path to the [credentials file](/docs/flowgen/postgres). |
| `table` | string | required | Table to write to, as `table` or `schema.table`. Supports templating. |
| `change_type` | string | | `upsert` or `delete`, matched by the table's primary key. Rows are appended when omitted. |
| `max_connections` | int | 10 | Connection pool size. |
| `connection_timeout` | duration | `30s` | Timeout to open a connection. |
| `query_timeout` | duration | `2m` | Longest a statement runs on the server (`statement_timeout`), and timeout to get a connection. |
| `depends_on` | list | | Upstream task names. |
| `retry` | object | | [Retry configuration](/docs/flowgen/concepts/retry). |

## Input

| Format | Rows |
|---|---|
| JSON object | One row. |
| JSON array of objects | One row per object. |
| Arrow RecordBatch | One row per record, such as the output of `postgres_query`. |

Keys are column names. A record batch writes all its columns, also one that is `null` in every row. JSON rows write the keys found in any of the event's rows; a row without one of them writes `null` to it. A column the rows don't write takes its default in a new row and keeps its value in an upserted one. A key that is not a column fails the event.

Values are sent as text and the server parses them into the column types: strings, numbers and booleans as they are, objects and arrays as JSON text for `json` and `jsonb` columns, and `null` as null. Timestamps, dates, UUIDs and numerics are written as strings in a format PostgreSQL accepts, such as `2026-10-30T10:00:00Z`. Binary columns of a record batch are written to `bytea`.

`table` and the column names are quoted, so they match exactly, including case.

## Change types

- **Append** (no `change_type`): the rows are copied into the table.
- **`upsert`**: the rows are copied into a temporary table, then inserted, and a row whose primary key exists updates the columns the rows set.
- **`delete`**: the rows' primary keys are copied into a temporary table, and the rows of the table with those keys are deleted. Other keys of the rows are ignored.

`upsert` and `delete` need a primary key on the table and its columns in the rows. When an event holds several rows with the same key, the last one is written.

## Output

| Event field | Value |
|---|---|
| `event.data.table` | The table, as configured. |
| `event.data.rows_written` | Rows copied, upserted or deleted. |

## Batches and single rows

`postgres_write` is built for batches: each event is one transaction with a `COPY`, and an upsert or delete also creates a temporary table and looks up the primary key. For thousands of rows per event that is the fastest way into PostgreSQL. For one row per event, such as the changes of a CDC task, either group the events with `buffer` first, or write each row with `postgres_query`:

```yaml
- postgres_query:
    name: save_order
    credentials_path: /etc/postgres/credentials.json
    query: |
      INSERT INTO orders (id, status, total) VALUES ($1, $2, $3)
      ON CONFLICT (id) DO UPDATE SET status = EXCLUDED.status, total = EXCLUDED.total
    parameters:
      - "{{event.data.id}}"
      - "{{event.data.status}}"
      - "{{event.data.total}}"
```

To write the rows of a `buffer`, return its `event.data.batch` from a `script` first. Changes from a CDC task can be written by routing on `event.meta.operation_type`: inserts, updates and the batches of an initial copy to a task with `change_type: upsert`, deletes to one with `change_type: delete`. A truncate carries no rows and fails the event, so leave it out of both.

## Behaviour

Each event's rows are written in one transaction, so an event is written completely or not at all. Connection errors, serialization failures and deadlocks are retried; SQL errors such as a type mismatch or a constraint violation fail the event. When the connection breaks while the transaction commits, the rows may already be written, and the retry appends them again; an upsert or delete gives the same result when repeated.

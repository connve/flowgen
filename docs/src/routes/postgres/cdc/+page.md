# PostgreSQL CDC

Emits each row change of the tables in a publication, read through logical replication (`pgoutput`).

## Configuration

```yaml
- postgres_cdc:
    name: watch_orders
    credentials_path: /etc/postgres/credentials.json
    publication: orders_pub
    slot: flowgen_orders
```

### Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `name` | string | required | Task name. |
| `credentials_path` | string | required | Path to the [credentials file](/docs/flowgen/postgres). |
| `publication` | string | required | Publication whose tables are captured. |
| `slot` | string | required | Logical replication slot that keeps the position. Created when missing. |
| `initial_copy` | bool | `false` | Emit every existing row of the publication's tables before the changes. See [Initial copy](#initial-copy). |
| `copy_batch_size` | int | `1000` | Rows per event of the initial copy. |
| `ack_timeout` | duration | | How long to wait for the flow to complete a change. Waits indefinitely when omitted. |
| `max_deliver` | int | | How many times a change is sent through the flow before it is skipped. When omitted, a change is delivered until the flow completes it. |
| `connection_timeout` | duration | `30s` | Timeout to open the replication connection and log in. |
| `status_interval` | duration | `10s` | Longest time between two reports of the position to the server. Must stay below the server's `wal_sender_timeout`. |
| `depends_on` | list | | Upstream task names. |
| `retry` | object | | [Retry configuration](/docs/flowgen/concepts/retry) for connecting and for redelivery. |

`publication` and `slot` take lowercase letters, digits and `_`.

### Database setup

- `wal_level = logical` on the server.
- A user with the `REPLICATION` attribute, or the `rds_replication` / `cloudsqlsuperuser` role on managed services.
- The publication, for example `CREATE PUBLICATION orders_pub FOR TABLE orders, order_items;`.

## Output

| Format | Description |
|---|---|
| [JSON](https://docs.rs/serde_json/latest/serde_json/enum.Value.html) | The changed row as an object of column names to values. |

| Event field | Value |
|---|---|
| `event.subject` | Table the change happened in. |
| `event.id` | `<transaction LSN>-<position>`, unique per change. A batch of the initial copy uses `<copy LSN>-copy-<schema>.<table>-<batch number>`, where the copy LSN is the position the copy reads the tables at. |
| `event.meta.schema` | Schema of the table. |
| `event.meta.table` | Table name. |
| `event.meta.operation_type` | `insert`, `update`, `delete`, `truncate`, or `copy` for a batch of the initial copy. |
| `event.meta.lsn` | LSN of the transaction, or the copy LSN for a batch of the initial copy. |
| `event.meta.before` | The old row of an update: the full row with `REPLICA IDENTITY FULL`, otherwise the key when it changed. |

`event.data` is the new row for an insert or update, the old row or its key for a delete, and `null` for a truncate, one event per truncated table. Booleans, integers, floats and JSON columns keep their JSON type; `numeric` and other types are strings in PostgreSQL text format. A large value an update did not change (TOAST) is left out of the row.

## Behaviour

Changes go through the flow one at a time, in commit order. When the flow has completed every change of a transaction, its position is confirmed to the slot. After a reconnection or restart, reading continues after the last confirmed transaction, so a change the flow did not complete is emitted again. Delivery is at least once. Between transactions, the position also moves on with the server's keepalives, so writes to tables outside the publication don't hold WAL back.

An event the flow fails to complete, or does not complete within `ack_timeout`, is delivered again after the [redelivery delay](/docs/flowgen/concepts/retry#redelivery), and the next change waits for it. With `max_deliver`, the change is skipped after that many failed deliveries and logged with its event ID.

A slot created by the task starts at the current position; earlier changes are not read unless `initial_copy` is set. A slot holds back WAL on the server until its changes are confirmed, so a stopped or failing flow makes WAL grow; set `max_slot_wal_keep_size` on the server to cap it. A slot past that cap loses WAL it needs (PostgreSQL 13+), and the task fails to start with an error naming the slot; dropping the slot lets the task create it again at the current position. Drop the slot with `SELECT pg_drop_replication_slot('flowgen_orders');` when the flow is removed.

## Initial copy

With `initial_copy: true` and no slot yet, the task creates the slot and reads every row of the publication's tables as they were at the moment the slot was created, then streams the changes made after it. No change is missed and none is emitted twice between the copy and the stream. Writes made during the copy wait in the slot.

Each table is read as the publication sends it: with the publication's row filter and column list, without generated columns, and without the rows of tables that inherit from it, which the publication sends as their own tables.

The rows are emitted in batches of `copy_batch_size`: each batch is one event with `operation_type: copy` and its rows as an array in `event.data`, typed like the rows of changes. `postgres_write` writes such a batch in one transaction; add `iterate` after the task for one event per row. Up to 64 batches go through the flow at a time, table by table, without a defined order inside a table.

When the copy finishes, the task records it in the cache and does not copy again. The copy keeps a transaction open on the server while the flow works through the rows; the server's `idle_in_transaction_session_timeout` does not apply to it.

When the slot already exists but the cache holds no record of a finished copy, the task keeps the slot, copies the tables as they are at that moment, then streams the changes from the slot's position. This happens after a copy interrupted by a restart or failure, and when `initial_copy` is set on a flow whose slot already exists. Rows already emitted are emitted again, and rows changed since the slot's position arrive twice, in the copy and as changes; no change is lost.

The record of a finished copy lives in the [cache](/docs/flowgen/concepts/configuration#cache). With the in-memory cache, used when no `cache` is configured, it is lost on restart, and every start copies the tables again.

A slot serves one reader at a time; set [`require_leader_election: true`](/docs/flowgen/concepts/flows) on the flow when it runs on more than one replica.

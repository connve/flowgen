# MongoDB Change Stream

Watches a MongoDB database for real-time change events and emits each change document as a JSON event.

## Configuration

```yaml
- mongodb_change_stream:
    name: watch_orders
    credentials_path: /etc/mongodb/credentials.json
    db_name: my_database
```

### Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `name` | string | required | Task name. |
| `credentials_path` | string | | Path to MongoDB credentials file. Omit to connect to `localhost:27017` without authentication. See [Credentials](/docs/flowgen/mongodb#credentials). |
| `db_name` | string | required | Database name to watch for changes. |
| `depends_on` | list | | Upstream task names. |
| `retry` | object | | [Retry configuration](/docs/flowgen/concepts/retry). |

## Output

| Format | Crate | Description |
|---|---|---|
| [JSON](https://docs.rs/serde_json/latest/serde_json/enum.Value.html) | [mongodb](https://docs.rs/mongodb/latest/mongodb/) | The changed document, converted to JSON. See [Behaviour](#behaviour). |

| Event field | Value |
|---|---|
| `event.subject` | Collection the change happened in; the database name for a database-level change. |
| `event.id` | The change's resume token, unique per change. |
| `event.meta.document_key` | The changed document's key (`{"_id": ...}`), absent for a collection or database change. |
| `event.meta.database` | Database the change happened in. |
| `event.meta.operation_type` | Change type: `insert`, `update`, `replace`, `delete`, `drop`, `rename`, `dropDatabase`, or `invalidate`. |

## Behaviour

The change stream watches the entire database and emits every change. `event.data` is the changed document for an insert or a replacement, and the current version of the document for an update, looked up when the change is read; an update to a document deleted before the lookup carries `null`. For a delete it is the deleted document's key (`{"_id": ...}`), and for a collection or database change such as a drop it is `null`. Route on `event.meta.operation_type` to handle each kind.

The stream reconnects automatically on connection loss using an infinite retry loop with exponential backoff and jitter.

:::caution[No resume token]
The reader does not store MongoDB resume tokens. After a reconnection or restart it opens a new change stream, which starts at the current point in the oplog, so changes made while the reader was disconnected are not emitted.
:::

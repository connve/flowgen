# MongoDB CDC

Watches a MongoDB database through a change stream and emits each change document as a JSON event.

## Configuration

```yaml
- mongodb_cdc:
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
| `initial_copy` | bool | `false` | Emit every existing document of the database's collections before the changes. See [Initial copy](#initial-copy). |
| `copy_batch_size` | int | `1000` | Documents per event of the initial copy. |
| `ack_timeout` | duration | | How long to wait for the flow to complete a change. Must be greater than zero. Waits indefinitely when omitted. |
| `max_deliver` | int | | How many times a change is sent through the flow before it is skipped. When omitted, a change is delivered until the flow completes it. |
| `depends_on` | list | | Upstream task names. |
| `retry` | object | | [Retry configuration](/docs/flowgen/concepts/retry) for connecting and for redelivery. |

## Output

| Format | Crate | Description |
|---|---|---|
| [JSON](https://docs.rs/serde_json/latest/serde_json/enum.Value.html) | [mongodb](https://docs.rs/mongodb/latest/mongodb/) | The changed document, converted to JSON. See [Behaviour](#behaviour). |

| Event field | Value |
|---|---|
| `event.subject` | Collection the change happened in; the database name for a database-level change. |
| `event.id` | The change's resume token, unique per change. A batch of the initial copy uses `<start token>-copy-<collection>-<batch number>`. |
| `event.meta.document_key` | The changed document's key (`{"_id": ...}`), absent for a collection or database change and for a batch of the initial copy. |
| `event.meta.database` | Database the change happened in. |
| `event.meta.operation_type` | Change type: `insert`, `update`, `replace`, `delete`, `drop`, `rename`, `dropDatabase`, or `invalidate`, and `copy` for a batch of the initial copy. |

## Behaviour

The change stream watches the entire database and emits every change. `event.data` is the changed document for an insert or a replacement, and the current version of the document for an update, looked up when the change is read; an update to a document deleted before the lookup carries `null`. For a delete it is the deleted document's key (`{"_id": ...}`), and for a collection or database change such as a drop it is `null`. Route on `event.meta.operation_type` to handle each kind.

Changes go through the flow one at a time. When the flow completes a change, its resume token is stored in the cache. When no change arrives for 60 seconds, the stream's current position is stored as well, so the stored token stays within the oplog on a quiet database. After a reconnection or restart the stream continues after the stored token, so a change the flow did not complete is emitted again. Without a stored token the stream starts at the current point in the oplog, after the [initial copy](#initial-copy) when it is enabled. When the oplog no longer holds the stored position, the changes since then are lost: the task logs an error and starts the same way.

Dropping the watched database emits a `drop` change per collection, a `dropDatabase` change, and an `invalidate` change, which ends the server's stream. The task stores the `invalidate` change's resume token and opens a new stream after it, so changes to a database recreated under the same name are emitted.

A change the flow fails to complete, or does not complete within `ack_timeout`, is delivered again after the [redelivery delay](/docs/flowgen/concepts/retry#redelivery), and the next change waits for it. With `max_deliver`, the change is skipped after that many failed deliveries and logged with its event ID.

The stream reconnects automatically on connection loss using an infinite retry loop with exponential backoff and jitter.

## Initial copy

With `initial_copy: true`, the task notes the current position in the oplog, reads every document of the database's collections, and then streams the changes from the noted position. No change is missed. A change made during the copy can be emitted twice: its document already in the copy, and the change itself after it. Write downstream as an upsert by `_id` so the result is the same.

The documents are emitted in batches of `copy_batch_size`: each batch is one event with `operation_type: copy` and its documents as an array in `event.data`; add `iterate` after the task for one event per document. Up to 64 batches go through the flow at a time, collection by collection, without a defined order inside a collection. Views and `system.` collections are not copied.

The copy runs when the cache holds no resume token: on the first start, after a copy that did not finish, and when the oplog no longer holds the stored position. Documents already emitted by an unfinished copy are emitted again. With the in-memory cache, used when no [`cache`](/docs/flowgen/concepts/configuration#cache) is configured, the token is lost on restart, so every start copies again.

The copy reads each collection through one cursor. When the flow holds a document longer than the server keeps an idle session, the server closes the cursor and the copy starts over. When the copy takes longer than the oplog keeps changes, the stream cannot continue from the noted position and the copy runs again; size the oplog to hold the changes made during a copy.

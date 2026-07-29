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
| [JSON](https://docs.rs/serde_json/latest/serde_json/enum.Value.html) | [mongodb](https://docs.rs/mongodb/latest/mongodb/) | The `fullDocument` from each change stream event, converted to JSON. Event ID is set to the document's `_id`. |

## Behaviour

The change stream watches the entire database for document inserts, updates, and replacements. Only operations that include a `fullDocument` are emitted. If a change event lacks a full document (e.g., unset `fullDocument` for pre- and post-images), the operation is skipped.

The stream reconnects automatically on connection loss using an infinite retry loop with exponential backoff and jitter.

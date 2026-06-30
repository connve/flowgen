# MongoDB Reader

Reads documents from a MongoDB collection and emits each document as a JSON event.

## Configuration

```yaml
- mongo_reader:
    name: read_users
    credentials_path: /etc/mongo/credentials.json
    db_name: my_database
    collection_name: users
    filter:
      status: "active"
```

### Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `name` | string | required | Task name. |
| `credentials_path` | string | required | Path to MongoDB credentials file. |
| `db_name` | string | required | Database name. |
| `collection_name` | string | required | Collection name. |
| `filter` | map | | Key-value pairs to filter documents (e.g., `status: "active"`). |
| `depends_on` | list | | Upstream task names. |
| `retry` | object | | [Retry configuration](/docs/flowgen/concepts/retry). |

### Credentials file

```json
{
  "MONGODB_URI": "mongodb+srv://cluster0.mongodb.net"
}
```

Either `MONGODB_URI` or `MONGODB_USERNAME` + `MONGODB_PASSWORD` must be provided.

## Output

| Format | Crate | Description |
|---|---|---|
| [JSON](https://docs.rs/serde_json/latest/serde_json/enum.Value.html) | [mongodb](https://docs.rs/mongodb/latest/mongodb/) | Each document from the collection, converted to JSON. Event ID is set to the document's `_id`. |

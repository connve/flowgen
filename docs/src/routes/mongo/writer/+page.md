# MongoDB Writer

Writes incoming JSON events as documents to a MongoDB collection.

## Configuration

```yaml
- mongo_writer:
    name: write_orders
    credentials_path: /etc/mongo/credentials.json
    db_name: my_database
    collection_name: orders
```

### Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `name` | string | required | Task name. |
| `credentials_path` | string | required | Path to MongoDB credentials file. |
| `db_name` | string | required | Database name. |
| `collection_name` | string | required | Collection name. |
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
| [JSON](https://docs.rs/serde_json/latest/serde_json/enum.Value.html) | [mongodb](https://docs.rs/mongodb/latest/mongodb/) | Insert result with the generated `ObjectId`. `event.id` is set to the inserted document's `_id`. |

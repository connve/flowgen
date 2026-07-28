# MongoDB Collection

Reads or writes documents in a MongoDB collection, depending on `operation`.

## Configuration

```yaml
- mongo_collection:
    name: read_customers
    operation: read
    credentials_path: /etc/mongo/credentials.json
    db_name: sales
    collection_name: customers
    filter:
      status: "active"
```

### Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `name` | string | required | Task name. |
| `operation` | string | required | `read` or `write`. |
| `credentials_path` | string | | Path to MongoDB credentials file. Omit to connect to `localhost:27017` without authentication. See [Credentials](/docs/flowgen/mongo#credentials). |
| `db_name` | string | required | Database name. |
| `collection_name` | string | required | Collection name. |
| `filter` | map | | Key-value pairs to filter documents. Only used by `operation: read`. |
| `depends_on` | list | | Upstream task names. |
| `retry` | object | | [Retry configuration](/docs/flowgen/concepts/retry). |

### Examples

**Read with a filter:**

```yaml
- mongo_collection:
    name: read_customers
    operation: read
    credentials_path: /etc/mongo/credentials.json
    db_name: sales
    collection_name: customers
    filter:
      status: "active"
```

**Write the incoming event as a document:**

```yaml
- mongo_collection:
    name: write_customer
    operation: write
    credentials_path: /etc/mongo/credentials.json
    db_name: sales
    collection_name: customers
```

See [Credentials](/docs/flowgen/mongo#credentials) for the credentials file format.

## Output

| Format | Crate | Description |
|---|---|---|
| [JSON](https://docs.rs/serde_json/latest/serde_json/enum.Value.html) | [mongodb](https://docs.rs/mongodb/latest/mongodb/) | `read`: each matching document, converted to JSON, `event.id` set to the document's `_id`. `write`: the insert result with the generated `ObjectId`, `event.id` set to the inserted document's `_id`. |

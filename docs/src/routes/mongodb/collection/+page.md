# MongoDB Collection

Reads, writes, or upserts documents in a MongoDB collection, depending on `operation`.

## Configuration

```yaml
- mongodb_collection:
    name: read_customers
    operation: read
    credentials_path: "{{env.FLOWGEN_CREDENTIALS}}/mongodb.json"
    db_name: sales
    collection_name: customers
    filter:
      status: "active"
```

### Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `name` | string | required | Task name. |
| `operation` | string | required | `read`, `write`, or `upsert`. |
| `credentials_path` | string | | Path to MongoDB credentials file. Omit to connect to `localhost:27017` without authentication. See [Credentials](/docs/flowgen/mongodb#credentials). |
| `db_name` | string | required | Database name. |
| `collection_name` | string | required | Collection name. |
| `filter` | map | | MongoDB query document selecting which documents to act on. Used by `operation: read`, and required by `operation: upsert`. See [Filters](#filters). |
| `depends_on` | list | | Upstream task names. |
| `retry` | object | | [Retry configuration](/docs/flowgen/concepts/retry). |

### Examples

**Read with a filter:**

```yaml
- mongodb_collection:
    name: read_customers
    operation: read
    credentials_path: "{{env.FLOWGEN_CREDENTIALS}}/mongodb.json"
    db_name: sales
    collection_name: customers
    filter:
      status: "active"
```

**Write the incoming event as a document:**

```yaml
- mongodb_collection:
    name: write_customer
    operation: write
    credentials_path: "{{env.FLOWGEN_CREDENTIALS}}/mongodb.json"
    db_name: sales
    collection_name: customers
```

**Upsert the first document matching `filter`:**

```yaml
- mongodb_collection:
    name: upsert_customers
    operation: upsert
    credentials_path: "{{env.FLOWGEN_CREDENTIALS}}/mongodb.json"
    db_name: sales
    collection_name: customers
    filter:
      email: "ada@example.com"
```

The incoming event is the update. A plain document is wrapped in `$set`:

```json
{ "name": "Ada Lovelace", "status": "active" }
```

A document whose top-level keys are all [update operators](https://www.mongodb.com/docs/manual/reference/operator/update/) is applied as written:

```json
{ "$inc": { "visits": 1 } }
```

The two forms cannot be mixed, and an empty payload is rejected. `filter` is required — without one every document matches, and the update would hit an arbitrary one instead of inserting.

If nothing matches, the document is inserted. An `_id` in the payload applies only to that insert, never overwriting the `_id` of a matched document, and must be written as `{ "$oid": "..." }` — the same shape `write` accepts.

### Filters

`filter` is a MongoDB query document. Values keep their YAML types and any [query operator](https://www.mongodb.com/docs/manual/reference/operator/query/) works:

```yaml
filter:
  age: { "$gt": 30 }
  status: { "$in": ["active", "trial"] }
  archived: false
```

A bare value means equality, and `count: 5` matches the number `5` rather than the string `"5"`.

See [Credentials](/docs/flowgen/mongodb#credentials) for the credentials file format.

## Output

| Format | Crate | Description |
|---|---|---|
| [JSON](https://docs.rs/serde_json/latest/serde_json/enum.Value.html) | [mongodb](https://docs.rs/mongodb/latest/mongodb/) | `read`: each matching document, converted to JSON, `event.id` set to the document's `_id`. `write`: the insert result with the generated `ObjectId`, `event.id` set to the inserted document's `_id`. `upsert`: the resulting document after the update/insert (`return_document: After`), `event.id` set to the document's `_id`. |

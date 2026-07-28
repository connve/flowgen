# MongoDB

Flowgen reads and writes MongoDB documents, and watches change streams for real-time updates.

- [Collection](/docs/flowgen/mongo/collection) — reads or writes documents in a collection.
- [Change Stream](/docs/flowgen/mongo/change_stream) — watches a database for real-time document changes.

## Credentials

`credentials_path` is optional and points to a JSON file with structured connection details. Every field is optional: `scheme` defaults to `mongodb`, `host` to `localhost`, `port` to `27017`. Omitting `credentials_path` entirely connects to `localhost:27017` without authentication.

```json
{
  "host": "mongo.example.com",
  "port": 27017,
  "username": "user",
  "password": "pass",
  "options": { "authSource": "admin", "replicaSet": "rs0" }
}
```

MongoDB Atlas connection strings use the `mongodb+srv://` scheme, where DNS resolves the actual cluster hosts and port — set `scheme` and omit `port`:

```json
{
  "scheme": "mongodb+srv",
  "host": "cluster0.abcde.mongodb.net",
  "username": "user",
  "password": "pass"
}
```

`options` is a map of arbitrary connection string query parameters (`authSource`, `tls`, `replicaSet`, `retryWrites`, `w`, ...), passed through verbatim.

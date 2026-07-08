# Braze

Flowgen connects to the [Braze REST API](https://www.braze.com/docs/api/endpoints) for marketing and customer engagement data operations.

- [Export Users by IDs](/docs/flowgen/braze/export/users) — calls `POST /users/export/ids`.

## Credentials

The `credentials_path` field points to a JSON file containing the Braze REST API key and the per-instance REST endpoint. Find the endpoint in the Braze dashboard under *Settings → API Keys*.

```json
{
  "api_key": "your-password",
  "rest_endpoint": "https://rest.iad-01.braze.com"
}
```

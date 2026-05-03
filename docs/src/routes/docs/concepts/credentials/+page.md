# Credentials

Tasks that talk to external systems load credentials from a JSON file referenced by `credentials_path`. Different connectors use different formats — there is no single global schema — but the convention is uniform: a file path on disk, parsed at task initialisation, never logged.

## Where credentials_path appears

| Task family | Format | What it authenticates |
|---|---|---|
| `http_webhook`, `http_request`, `mcp_tool` | `HttpCredentials` (bearer / basic) | The HTTP request header. |
| `nats_jetstream_*`, `nats_kv_store` | NATS `.creds` file | NATS server connection. |
| `gcp_*` (BigQuery) | GCP service account JSON | Google Cloud APIs. |
| `salesforce_*` | Salesforce credentials JSON | Salesforce REST / Pub/Sub / Bulk APIs. |
| `mssql_query` | MSSQL credentials JSON | SQL Server connection. |
| `object_store` | Cloud-specific credentials JSON | S3, GCS, or Azure. |
| `git_sync` | Inline `auth` block (token or SSH key) | Git remote. |

The format details live on each task's documentation page. This page covers what is shared.

## Why files, not environment variables

Credentials live in JSON files for three reasons:

- **Kubernetes secrets** mount cleanly as files. `kubectl create secret generic ...` then mount at `/etc/flowgen/credentials/...` — no env-var indirection.
- **Multiple credentials per task** (e.g., bearer plus basic auth fallback) compose naturally as one JSON object.
- **No accidental logging.** Environment variables show up in process inspection, error reports, and log scrapes. File paths in config do not leak the secret content.

If you need to inject secrets at deploy time without writing them to disk, use a secrets-management sidecar (External Secrets, Vault Agent, Sealed Secrets) to materialise the JSON file at the configured path.

## HTTP credentials

The most common shared format is `HttpCredentials`, used by `http_webhook`, `http_request`, and similar tasks:

```json
{
  "bearer_auth": "your-token-here"
}
```

Or basic auth:

```json
{
  "basic_auth": {
    "username": "your-user",
    "password": "your-pass"
  }
}
```

Both fields are optional. If neither is set, no `Authorization` header is added.

## Example — webhook with bearer auth

`/etc/flowgen/credentials/webhook.json`:

```json
{
  "bearer_auth": "shh-my-secret"
}
```

`/etc/flowgen/flows/secure-webhook.yaml`:

```yaml
flow:
  name: secure_webhook
  tasks:
    - http_webhook:
        name: ingest
        endpoint: /events
        method: POST
        credentials_path: /etc/flowgen/credentials/webhook.json
```

Incoming requests must include `Authorization: Bearer shh-my-secret`. Anything else returns 401.

## Worker-level fallback for HTTP

The HTTP server has a worker-level credentials path that webhooks can inherit:

```yaml
worker:
  http_server:
    enabled: true
    credentials_path: /etc/flowgen/credentials/http.json
```

Individual `http_webhook` tasks override the worker default by setting their own `credentials_path`. This is useful when most webhooks share the same shared-secret token but a few endpoints have stricter requirements.

## User-level authentication is separate

`credentials_path` authenticates the task itself (e.g., the bearer token for incoming webhook requests). User-level authentication — JWT, OIDC, session tokens — happens via the worker's `auth` configuration. See [Authentication](/docs/concepts/auth).

The two compose: a webhook can require a worker-level shared secret (via `credentials_path`) **and** a user-level JWT (via `auth.required: true`). Both checks must pass.

## Operational notes

- **File permissions** should be `0600` (read-write for the flowgen user only). Flowgen does not enforce this — the operator is responsible.
- **Hot reload** is not supported. Credentials are read once at task initialisation. To rotate, restart the worker (or use connector-specific token refresh, e.g., GCP service account auto-rotation).
- **Errors at startup**: if the file is missing or malformed, the task fails to initialise and the standard retry circuit breaker fires. After ~15 minutes the task gives up. Watch worker logs for `Failed to read credentials` or `Failed to parse credentials` messages.
- **Never commit credential files to Git.** Pair with `git_sync` carefully: sync flow YAML, but mount credentials separately via a Kubernetes secret.

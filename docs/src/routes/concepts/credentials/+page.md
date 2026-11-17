# Credentials

Tasks that talk to external systems load credentials from a JSON file referenced by `credentials_path`. Different connectors use different formats — there is no single global schema — but the convention is uniform: a file path on disk, parsed at task initialisation, never logged.

## Where credentials_path appears

Every task that reaches an external system takes a `credentials_path`, and each connector's page documents its format. The formats shared across connectors:

| Used by | Format | What it authenticates |
|---|---|---|
| HTTP and MCP clients, such as `http_request` and `ai_completion` `mcp_servers` | `HttpCredentials` (bearer / basic / OAuth 2.0 client credentials) | The outgoing HTTP request header. |
| HTTP servers, such as `http_endpoint` and `llm_proxy` | `HttpCredentials` (bearer / basic) | Incoming requests, checked against the expected token. |
| `mcp_tool` | `Credentials` (`api_keys` list) | Incoming MCP calls, checked against the allowed keys. |
| `web.auth` | `client_secret`, `cookie_secret` | The web UI's OIDC login. |

The format details live on each task's documentation page. This page covers what is shared.

## Why files, not environment variables

Credentials live in JSON files for three reasons:

- **Kubernetes secrets** mount cleanly as files. `kubectl create secret generic ...` then mount at `/etc/flowgen/credentials/...` — no env-var indirection.
- **Multiple credentials per task** (e.g., bearer plus basic auth fallback) compose naturally as one JSON object.
- **No accidental logging.** Environment variables show up in process inspection, error reports, and log scrapes. File paths in config do not leak the secret content.

If you need to inject secrets at deploy time without writing them to disk, use a secrets-management sidecar (External Secrets, Vault Agent, Sealed Secrets) to materialise the JSON file at the configured path.

Any config key can also be set from the environment — `APP_` prefixes the path and `__` separates each level, so `APP_WEB__AUTH__CLIENT_SECRET` sets `web.auth.client_secret`. That covers the application's own config; task credentials stay in files, for the reasons above.

## HTTP credentials

The most common shared format is `HttpCredentials`, used by `http_request`, `http_endpoint`, and similar tasks:

```json
{
  "bearer_auth": "your-password"
}
```

Or basic auth:

```json
{
  "basic_auth": {
    "username": "your-username",
    "password": "your-password"
  }
}
```

Both fields are optional. If neither is set, no `Authorization` header is added.

### OAuth 2.0 client credentials

For APIs that require short-lived access tokens, such as many hosted MCP servers, use the `oauth2_client_credentials` field instead of a static bearer token. Flowgen fetches a token from the token endpoint using the [client credentials grant type](https://datatracker.ietf.org/doc/html/rfc6749#section-4.4), caches it, and auto-refreshes when it expires:

```json
{
  "oauth2_client_credentials": {
    "token_url": "https://auth.example.com/oauth2/token",
    "client_id": "my-client-id",
    "client_secret": "my-client-secret",
    "scope": "api"
  }
}
```

The `scope` field is optional. When `oauth2_client_credentials` is present, it takes precedence over `bearer_auth` and `basic_auth`.

This applies to tasks that *send* authenticated requests — `http_request` and the `mcp_servers` entries on `ai_completion`. Tasks that *validate* incoming requests (`http_endpoint`) only read `bearer_auth` and `basic_auth`; an `oauth2_client_credentials` block there has no effect.

If the token endpoint omits `expires_in`, flowgen assumes a 5-minute lifetime and refreshes accordingly.

## Example — webhook with bearer auth

`/etc/flowgen/credentials/webhook.json`:

```json
{
  "bearer_auth": "your-password"
}
```

`/etc/flowgen/flows/secure-webhook.yaml`:

```yaml
flow:
  tasks:
    - http_endpoint:
        name: ingest
        endpoint: /events
        method: POST
        credentials_path: /etc/flowgen/credentials/webhook.json
```

Incoming requests must include `Authorization: Bearer your-password`. Anything else returns 401.

## Server-level fallback for HTTP

The HTTP server has a shared credentials path that webhooks can inherit:

```yaml
http_server:
  enabled: true
  credentials_path: /etc/flowgen/credentials/http.json
```

Individual `http_endpoint` tasks override the shared default by setting their own `credentials_path`. This is useful when most webhooks share the same shared-secret token but a few endpoints have stricter requirements.

## User-level authentication is separate

`credentials_path` authenticates the task itself (e.g., the bearer token for incoming webhook requests). User-level authentication — JWT, OIDC, session tokens — happens via the server's `auth` configuration. See [Authentication](/docs/flowgen/concepts/auth).

The web UI's own OIDC secrets follow this same file convention: `web.auth.credentials_path` takes a JSON file with `client_secret` and `cookie_secret`.

The two compose: a webhook can require a shared secret (via `credentials_path`) **and** a user-level JWT (via `auth.required: true`). Both checks must pass.

## Connection pooling

Tasks that share the same `credentials_path` automatically share the same authenticated client. The runtime hashes credential fields to detect duplicates — no configuration needed.

For example, if three flows each call the same API with one credentials file, the first task to initialise performs the login. The other two wait and reuse the same session. This avoids hammering external services with redundant login calls and respects their rate limits.

Clients are shared by the credentials file and, where a task has them, the settings that shape the client: the server URL, or the pool size and timeouts of a database pool. Tasks with different settings get separate clients, so two database tasks with the same credentials but different `max_connections` do not share a pool.

## Connections in the web UI

The **Connections** tab lists the external systems the loaded flows connect to, including `ai_completion` MCP servers. Each row is one connector and credentials file, so tasks of one connector that share a credentials file are one connection.

- Next to the name, a row shows what the credentials file points to, read from its non-secret fields, such as an instance host, a service account email, or a database server and database.
- It also shows where its tasks connect, from their configs: hosts, brokers, repositories, buckets, or an AI provider and model.
- Values set per event show as `per event`. An MCP server called with `on_behalf_of_user: true` shows as `on behalf of user`.
- A `{{env.…}}` value shows what it renders to, with the same parts left out as a value written in the config. When the variable is unset, it shows as written.
- The status comes from the latest log record of each task using the connection. It is `error` or `warn` when any of them is one, `ok` otherwise, and `idle` without records.
- The flow count links to the **Flows** tab filtered to that connection.

Tasks that receive calls, such as `http_endpoint` and `mcp_tool`, are not listed. `GET /api/connections` and `GET /api/connections/{id}` return the same data, and `GET /api/connectors` lists each connector with its task types.

## Encrypted credentials in the workspace

Credentials are named by their file in the `FLOWGEN_CREDENTIALS` folder: `{{env.FLOWGEN_CREDENTIALS}}/orders-api.json` is `orders-api`.

In the Connections tab, a connection whose file flowgen does not find shows **No credentials**. For a file in the credentials folder, its details offer **Add credentials**, or **Replace credentials** once they are there, with a form for that connector: its fields, such as an API key or a host and password, or a file to upload, such as a service account file. The file is checked the way the connector's tasks read it before it is saved. A connector without a form takes the file as JSON.

flowgen encrypts the credentials with [SOPS](https://github.com/getsops/sops) and age and proposes `credentials/<name>.yaml`, a Kubernetes Secret holding the one file `<name>.json`. It is committed like any [change](/docs/flowgen/concepts/authoring) once approved. The plaintext is never stored or shown again, and it never passes through a flow, an event, a log or a change.

A task reads its credentials in this order:

1. the file at `credentials_path`;
2. the encrypted Secret `<name>.yaml` beside it;
3. the system keychain, in a run without `--config`.

Anything SOPS-encrypted is decrypted in memory, with the age key `credentials.identity_path` names, or `SOPS_AGE_KEY_FILE` without one. A path in another folder, or one that reads the event, such as `{{event.data.account}}.json`, has no credentials to add in the web UI.

A change under `credentials/` is checked before it can be approved. Each file must be a Secret named after the file, encrypted with SOPS, with every value encrypted and exactly one file, `<name>.json`. A change that adds or removes credentials also rewrites `credentials/flowgen-credentials.yaml`, a ConfigMap listing them.

```yaml
# Encrypt for one age key and decrypt with it here.
credentials:
  recipients: ["age1…"]
  identity_path: .flowgen/age.key
  path: credentials
```

| Field | Default | Description |
|---|---|---|
| `recipients` | `[]` | age public keys each credential is encrypted for. |
| `identity_path` | | age key file this instance decrypts with; its public key is a recipient too. |
| `path` | required | The folder flows read credentials from. |

**On this machine**, a workspace started by `flowgen` without `--config` creates its age key in `.flowgen/age.key`, outside git, and reads credentials from the workspace's `credentials/` folder. Its `.sops.yaml` encrypts for the same key, so `sops credentials/<name>.yaml` edits a credential too.

**In a cluster**, Flux decrypts the Secrets (`decryption.provider: sops`) and flowgen holds only the public keys, so it can add credentials but not read them from git. The chart mounts the listed Secrets together, read-only, at `credentialsPath` (default `/etc/flowgen/credentials`), and sets `FLOWGEN_CREDENTIALS` to it:

```yaml
flowgen:
  credentials: [orders-api, warehouse]
```

A HelmRelease can read that list from the ConfigMap flowgen keeps, which is labeled `reconcile.fluxcd.io/watch: Enabled` so a new list upgrades the release:

```yaml
valuesFrom:
  - kind: ConfigMap
    name: flowgen-credentials
    valuesKey: values.yaml
    optional: true
```

Adding or removing credentials changes the list, so the pods roll. A listed Secret that is not there yet is skipped. A new value for existing credentials reaches the mounted file without a restart, and the tasks read it the next time they start. Argo CD needs a SOPS plugin, such as KSOPS, to decrypt the Secrets.

## Operational notes

- **File permissions** should be `0600` (read-write for the flowgen user only). Flowgen does not enforce this — the operator is responsible.
- **Hot reload** is not supported. Credentials are read once at task initialisation. To rotate, restart the worker. OAuth 2.0 access tokens are an exception — they auto-refresh transparently when they expire.
- **Errors at startup**: if the file is missing or malformed, the task retries its startup up to every 5 minutes until the file is fixed. Watch worker logs for `Failed to read credentials` or `Failed to parse credentials` messages.
- **Never commit unencrypted credentials to Git.** Commit them encrypted under `credentials/`, or mount them separately via a Kubernetes secret.

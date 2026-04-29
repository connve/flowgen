# Authentication

Flowgen supports user-level authentication for HTTP-facing tasks (webhooks, AI gateway, MCP server). Auth is configured once at the worker level and shared across every HTTP-facing task on that worker. When enabled, the resolved user identity is injected into the event metadata as `event.meta.auth`, where downstream tasks can read it for routing or audit.

User-level auth is **separate** from `credentials_path` — see [Credentials](/docs/concepts/credentials) for the distinction.

## Providers

Three provider types, configured under `worker.http_server.auth`:

| Provider | What it validates | When to use |
|---|---|---|
| `jwt` | JWT signed with HMAC (HS256) or asymmetric keys (RS256/ES256). | You issue tokens yourself, or you have a JWKS endpoint. |
| `oidc` | JWT issued by an OIDC provider, validated via discovery. | Auth0, Okta, Keycloak, Google, anyone with `.well-known/openid-configuration`. |
| `session` | Opaque session token validated by an external HTTP endpoint. | You already have a session API and want flowgen to delegate validation. |

## JWT provider

```yaml
worker:
  http_server:
    enabled: true
    auth:
      type: jwt
      secret: "your-hmac-secret"        # for HS256 (mutually exclusive with jwks_url)
      # jwks_url: "https://idp.example.com/.well-known/jwks.json"   # for RS256/ES256
      audience: "flowgen-prod"          # optional: reject tokens with a different aud
      issuer: "https://auth.example.com" # optional: reject tokens with a different iss
      user_id_claim: "sub"              # optional: defaults to "sub"
```

Supply either `secret` (symmetric) or `jwks_url` (asymmetric), not both. With `jwks_url`, flowgen fetches the JWKS at startup and matches incoming tokens by their `kid` header.

## OIDC provider

```yaml
worker:
  http_server:
    enabled: true
    auth:
      type: oidc
      issuer_url: "https://auth.example.com/realms/myapp"
      audience: "flowgen-prod"
      user_id_claim: "sub"
```

Flowgen reads `{issuer_url}/.well-known/openid-configuration` at startup, extracts the JWKS endpoint, and validates incoming tokens against those keys. The provider rejects tokens whose `iss` claim does not match the discovered issuer.

## Session provider

```yaml
worker:
  http_server:
    enabled: true
    auth:
      type: session
      validation_url: "https://auth.example.com/api/session/validate"
      user_id_field: "user_id"
```

For each incoming request, flowgen sends `Authorization: Bearer <token>` to `validation_url`. A 2xx response with a JSON body is treated as valid; the `user_id_field` is extracted from the response and becomes `UserContext.user_id`. Any other status returns 401 to the caller.

## Per-task opt-in

Setting `auth` on the worker enables the provider but does not force every task to require a token. Each HTTP-facing task opts in individually:

```yaml
- http_webhook:
    name: secure_endpoint
    endpoint: /admin/events
    method: POST
    auth:
      required: true
```

When `auth.required: true`:

- Requests without an `Authorization` header are rejected with 401.
- Requests with an invalid token are rejected with 401.
- Requests with a valid token proceed; `UserContext` is injected into `event.meta.auth`.

When `auth.required: false` (or `auth` is omitted on the task):

- Tokens are validated **if present** and injected into `event.meta`.
- Requests without a token still proceed (anonymous).
- This is useful for endpoints that personalise responses when the caller is logged in but stay public otherwise.

## Reading user identity in scripts

Once `event.meta.auth` is set, downstream tasks can read it:

```yaml
- script:
    name: route_by_user
    code: |
      let user_id = event.meta.auth.user_id;
      let role = event.meta.auth.claims.role;
      if role == "admin" {
          event.meta.lane = "priority";
      }
      event.data
```

The shape of `event.meta.auth`:

```json
{
  "user_id": "alice@example.com",
  "claims": {
    "sub": "alice@example.com",
    "email": "alice@example.com",
    "role": "admin",
    "iss": "https://auth.example.com",
    "exp": 1893456000
  }
}
```

`user_id` is the claim selected by `user_id_claim` (defaults to `sub`). `claims` is the full set of claims from the token, useful for fine-grained authorisation in scripts.

## Composing with credentials_path

A webhook can require both a worker-shared bearer secret (`credentials_path`) **and** a user-level JWT (`auth.required: true`). Both checks must pass. The shared secret is for service-to-service authentication (the caller proves they are an authorised service); the JWT identifies the end user.

```yaml
- http_webhook:
    name: secure_endpoint
    endpoint: /events
    method: POST
    credentials_path: /etc/flowgen/credentials/service-token.json
    auth:
      required: true
```

The `Authorization` header in the incoming request carries the JWT. Service-to-service auth uses a separate header — typically `X-Internal-Token` — that the webhook compares against the credentials file. (Implementation details vary by deployment; check the webhook docs.)

## Errors

| Status | Cause |
|---|---|
| 401 No token | Task has `auth.required: true` and no `Authorization` header was provided. |
| 401 Invalid token | Token signature, expiration, audience, or issuer check failed. |
| 401 Unknown key | JWKS-based JWT references a `kid` not present in the keyset. Refresh JWKS or verify the issuer. |
| 401 Session rejected | Session validation endpoint returned non-2xx. |
| 502 / 503 | Upstream JWKS endpoint or session validation service is unreachable. Retry by the caller. |

Worker logs include the specific reason at `error` level so operators can diagnose without leaking the token to the caller.

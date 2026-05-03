# HTTP Request

Makes outbound HTTP requests. URL, headers, and payload support templating.

## Configuration

```yaml
- http_request:
    name: call_api
    endpoint: "https://api.example.com/orders/{{event.data.id}}"
    method: GET
    credentials_path: /etc/http/api-credentials.json
```

### Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `name` | string | required | Task name. |
| `endpoint` | string | required | Target URL. Supports templating. |
| `method` | string | `GET` | `GET`, `POST`, `PUT`, `DELETE`, `PATCH`, `HEAD`. |
| `credentials_path` | string | | Path to credentials file for request authentication. |
| `payload` | object | | Request body (see below). |
| `headers` | map | | HTTP headers. Values support templating. |
| `timeout` | duration | `30s` | Total request timeout from start to response body received. Set explicitly to override or omit-with-`null` to disable. |
| `connect_timeout` | duration | `10s` | TCP/TLS connect timeout. |
| `depends_on` | list | | Upstream task names. |
| `retry` | object | | Retry configuration. |

### Payload options

| Field | Type | Description |
|---|---|---|
| `object` | map | JSON payload with explicit fields. Supports templating. |
| `input` | string | Raw JSON string. |
| `from_event` | bool | Use incoming event data as the request body. |
| `send_as` | string | Encoding: `json` (default), `urlencoded`, `queryparams`. |

### What `send_as` controls

| Value | Where the payload goes | `Content-Type` set by client |
|---|---|---|
| `json` (default) | Request body | `application/json` |
| `urlencoded` | Request body | `application/x-www-form-urlencoded` |
| `queryparams` | URL query string (`?a=1&b=2`) | not set; no body sent |

`send_as` is the only knob that changes how the payload is serialized.
Setting `Content-Type` in `headers` does not change serialization — it
just overrides the header value the client would otherwise send. Other
content types (XML, multipart form-data, raw binary) are not currently
supported.

### `urlencoded` and `queryparams` require flat scalar payloads

`urlencoded` and `queryparams` go through `serde_urlencoded`, which only
encodes flat objects of scalar values (strings, numbers, booleans). Nested
objects or arrays produce a request build error.

For nested data, either:
- use `send_as: json` (POST body), or
- flatten the payload in a `script` task before this one and choose a
  per-API key convention (bracket notation, dotted keys, comma-separated,
  etc.).

## Examples

**POST with payload:**

```yaml
- http_request:
    name: create_order
    endpoint: "https://api.example.com/orders"
    method: POST
    credentials_path: /etc/http/credentials.json
    payload:
      object:
        customer_id: "{{event.data.customer_id}}"
        amount: "{{event.data.total}}"
```

**Forward event data:**

```yaml
- http_request:
    name: forward
    endpoint: "https://api.example.com/ingest"
    method: POST
    credentials_path: /etc/http/credentials.json
    payload:
      from_event: true
```

**GET with query parameters:**

```yaml
- http_request:
    name: search_orders
    endpoint: "https://api.example.com/orders"
    method: GET
    credentials_path: /etc/http/credentials.json
    payload:
      send_as: queryparams
      object:
        status: "open"
        customer_id: "{{event.data.customer_id}}"
        limit: 50
```

Resolves to `GET https://api.example.com/orders?status=open&customer_id=…&limit=50`.

**Form-urlencoded body (typical for OAuth token endpoints):**

```yaml
- http_request:
    name: exchange_code
    endpoint: "https://login.example.com/oauth/token"
    method: POST
    payload:
      send_as: urlencoded
      object:
        grant_type: "authorization_code"
        code: "{{event.data.code}}"
        client_id: "{{env.OAUTH_CLIENT_ID}}"
```

## Response handling

Response bodies are decoded into the next event's `event.data` after the
HTTP client applies a few transparent transforms:

- **Compression**: `Content-Encoding: gzip`, `br` (Brotli), and `deflate`
  responses are decompressed automatically. The `Accept-Encoding` request
  header is set on every outbound call.
- **Character encoding**: when `Content-Type` declares a charset
  (`text/html; charset=windows-1250`, `iso-8859-2`, etc.), the body is
  decoded to UTF-8 before parsing. No special configuration needed.

If the server returns 4xx/5xx, the task fails with the status code and
body. 4xx (other than 429) is permanent and skips retries; 429, 5xx, and
network errors are retriable per the task's [retry config](/docs/concepts/retry).

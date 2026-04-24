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
| `depends_on` | list | | Upstream task names. |
| `retry` | object | | Retry configuration. |

### Payload options

| Field | Type | Description |
|---|---|---|
| `object` | map | JSON payload with explicit fields. Supports templating. |
| `input` | string | Raw JSON string. |
| `from_event` | bool | Use incoming event data as the request body. |
| `send_as` | string | Encoding: `json` (default), `url_encoded`, `query_params`. |

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

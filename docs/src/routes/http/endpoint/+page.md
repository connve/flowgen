# HTTP Endpoint

Registers an HTTP route on the worker's HTTP server. Each incoming request is injected into the flow as an event and the flow's response is returned to the caller. Supports `GET`, `POST`, `PUT`, `DELETE`, and `PATCH`. Requires `worker.http_server.enabled: true`.

## Configuration

```yaml
worker:
  http_server:
    enabled: true
    port: 3000
```

```yaml
- http_endpoint:
    name: receive_order
    endpoint: /webhooks/orders
    method: POST
    credentials_path: /etc/webhook/credentials.json
```

### Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `name` | string | required | Task name. |
| `endpoint` | string | required | Route path (e.g., `/webhooks/orders`). |
| `method` | string | `GET` | HTTP method: `GET`, `POST`, `PUT`, `DELETE`, `PATCH`. |
| `headers` | map | | Expected headers. |
| `credentials_path` | string | | Path to credentials for request authentication. |
| `ack_timeout` | duration | wait indefinitely | Max time to wait for flow completion before responding. |
| `max_body_bytes` | int | `10485760` | Maximum accepted request body size in bytes (10 MiB default). Larger requests are rejected with HTTP 413 before being read into memory. |
| `stream` | bool | false | Stream responses as Server-Sent Events. |
| `auth` | object | | User authentication configuration. |
| `depends_on` | list | | Upstream task names. |
| `retry` | object | | [Retry configuration](/docs/flowgen/concepts/retry). |

## Example: SSE streaming response

```yaml
flow:
  name: streaming_endpoint
  tasks:
    - http_endpoint:
        name: receive
        endpoint: /api/process
        method: POST
        stream: true
        ack_timeout: "30s"

    - script:
        name: transform
        code: |
          let data = event.data;
          #{result: data.input.to_upper()}

    - log:
        name: output
```

With `stream: true`, the client receives intermediate results as SSE events while the flow processes.

## Output

Format: [JSON](https://docs.rs/serde_json/latest/serde_json/enum.Value.html). Each received request produces an event with `event.data` containing:

| Field | Type | Description |
|---|---|---|
| `headers` | object | Selected HTTP headers from the request. |
| `payload` | value | Parsed request body as JSON. |

# HTTP Webhook

Listens for incoming HTTP requests and injects them into the flow. Requires the HTTP server enabled in worker config.

## Configuration

```yaml
worker:
  http_server:
    enabled: true
    port: 3000
```

```yaml
- http_webhook:
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
| `ack_timeout` | duration | | Max time to wait for flow completion before responding. |
| `stream` | bool | false | Stream responses as Server-Sent Events. |
| `auth` | object | | User authentication configuration. |
| `depends_on` | list | | Upstream task names. |
| `retry` | object | | Retry configuration. |

## Example: Webhook with SSE streaming

```yaml
flow:
  name: webhook_flow
  tasks:
    - http_webhook:
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

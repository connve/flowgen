# AI Gateway

OpenAI-compatible chat completions endpoint. Proxies requests through a flowgen flow, enabling pre/post processing, logging, and tool execution.

Exposes a `/v1/chat/completions` endpoint that accepts the OpenAI request format and streams responses as Server-Sent Events.

## Configuration

```yaml
- ai_gateway:
    name: gateway
    credentials_path: /path/to/credentials.json
```

The AI gateway task requires the HTTP server to be enabled in the worker config:

```yaml
worker:
  http_server:
    enabled: true
    port: 3000
```

Requests are sent to `POST /v1/chat/completions` with the standard OpenAI chat format.

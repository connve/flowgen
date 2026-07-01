# AI Gateway

OpenAI-compatible chat completions endpoint. Proxies requests through a flowgen flow, enabling pre/post processing, logging, and tool execution.

Exposes a `/v1/chat/completions` endpoint that accepts the OpenAI request format and streams responses as Server-Sent Events. Routes requests to registered `llm_proxy` backends based on the `model` field (`model: "<proxy-name>/<downstream-model>"`).

## Server configuration

The AI gateway runs as its own HTTP server, independent of the webhook HTTP server and MCP server:

```yaml
worker:
  ai_gateway:
    enabled: true
    port: 3002
    path: /v1
    # credentials_path: /etc/flowgen/credentials/ai.json
```

## Task configuration

```yaml
- llm_proxy:
    name: gateway
    protocol: openai
    credentials_path: /path/to/credentials.json
```

| Field | Type | Default | Description |
|---|---|---|---|
| `name` | string | required | Unique proxy name used in model routing (`<name>/<model>`). |
| `protocol` | string | `openai` | Wire protocol for the proxy endpoint. Currently only `openai`. |
| `credentials_path` | string | null | Path to credentials file for endpoint authentication. |

Requests are sent to `POST /v1/chat/completions` with the standard OpenAI chat format. Use `<proxy-name>/<downstream-model>` as the `model` value to route to a specific proxy backend.

## Output

Format: [JSON](https://docs.rs/serde_json/latest/serde_json/enum.Value.html)

| Field | Type | Description |
|---|---|---|
| `prompt` | string | User prompt merged from messages. |
| `system_prompt` | string / null | System message if present. |
| `model` | string | Requested model name. |
| `temperature` | number / null | Sampling temperature if specified. |
| `max_tokens` | int / null | Token limit if specified. |
| `stream` | bool | Whether streaming was requested. |

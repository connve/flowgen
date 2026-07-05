# AI Gateway

OpenAI-compatible chat completions and Anthropic-compatible messages endpoint. Proxies requests through a flowgen flow, enabling pre/post processing, logging, and tool execution.

Exposes two wire protocols on the same server, both routed by the request body's `model` field (`model: "<proxy-name>/<downstream-model>"`):

- `POST <path>/chat/completions` — OpenAI-compatible chat completions.
- `POST <path>/messages` — Anthropic Messages API (targets `ANTHROPIC_BASE_URL`).
- `GET <path>/models` — OpenAI-shape list of registered proxy names.

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

### Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `enabled` | bool | required | Must be `true` for any `llm_proxy` task to register. |
| `port` | int | `3002` | Port the AI gateway listens on. |
| `path` | string | `/v1` | Path prefix. Chat completions mount at `<path>/chat/completions`, messages at `<path>/messages`. |
| `credentials_path` | string | | Global credentials JSON. Individual `llm_proxy` tasks can override with their own `credentials_path`. |
| `auth` | object | | Auth provider config for user identity resolution (JWT / OIDC / session). |
| `max_body_bytes` | int | `134217728` | Maximum inbound request body size. Sized for 1 M-token prompts + tool schemas; requests over this cap return `413`. |

## `llm_proxy`

Registers a flow as a backend on the shared AI gateway server. Inbound chat completion (or Anthropic messages) requests translate into a pipeline event; the leaf task returns the completion which the gateway translates back to the wire shape.

```yaml
- llm_proxy:
    name: gateway
    protocol: openai
    credentials_path: /path/to/credentials.json
```

### Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `name` | string | required | Proxy name. Combined with the downstream model as `<name>/<downstream-model>` in the client's `model` field. |
| `protocol` | string | `openai` | Wire protocol exposed for this proxy. One of `openai` or `anthropic`. |
| `credentials_path` | string | | Overrides `worker.ai_gateway.credentials_path` for this proxy. |
| `auth` | object | | Per-proxy authentication. When `auth.required` is true, requests must include a valid bearer token validated by the worker auth provider. |
| `ack_timeout` | duration | wait indefinitely | Max time to wait for the pipeline to complete before returning a timeout error to the client. |
| `depends_on` | list | | Upstream task names. |
| `retry` | object | | [Retry configuration](/docs/flowgen/concepts/retry). |

### Output

Format: [JSON](https://docs.rs/serde_json/latest/serde_json/enum.Value.html)

The gateway writes a shared `EventPayload` into `event.data` regardless of which wire protocol the client used, so a leaf `ai_completion` task never learns whether it was invoked by an OpenAI or Anthropic client.

| Field | Type | Description |
|---|---|---|
| `prompt` | string | User prompt merged from messages. |
| `system_prompt` | string / null | System message if present. |
| `model` | string | Downstream model requested (the portion after the `/` in the client's `model` field). |
| `temperature` | number / null | Sampling temperature if specified. |
| `max_tokens` | int / null | Token limit if specified. |
| `stream` | bool | Whether streaming was requested. |
| `messages` | list / null | Full OpenAI-shape message list. Present only when the client sent `tools`, so tool-passthrough flows can rebuild the upstream request without loss. |
| `tools` | list / null | Client-supplied tool schemas when present. |
| `tool_choice` | any / null | Client-supplied tool selection strategy when present. |

## Protocols

### OpenAI (`protocol: openai`, default)

Standard OpenAI chat completions on `POST <path>/chat/completions`. Routing is driven by the request body's `model` field: the gateway splits on the first `/`, resolves the left side to a registered `llm_proxy`, and forwards the right side to the pipeline as `event.data.model`.

Example client call:

```json
{
  "model": "flowgen_openai/gpt-4",
  "messages": [{"role": "user", "content": "Hello"}],
  "stream": true,
  "stream_options": {"include_usage": true}
}
```

### Anthropic (`protocol: anthropic`)

Anthropic Messages API on `POST <path>/messages`. Added so clients honouring `ANTHROPIC_BASE_URL` (Claude Code, official Anthropic SDKs) can reach downstream OpenAI-compatible providers through the gateway. Routing uses the same `<proxy-name>/<downstream-model>` convention as OpenAI.

Anthropic's `x-api-key` header is folded into `Authorization: Bearer` before endpoint-level auth runs, so existing credentials files work unchanged.

Example client call:

```bash
ANTHROPIC_BASE_URL=http://<host>:3002 \
ANTHROPIC_API_KEY=<matches worker credentials> \
ANTHROPIC_MODEL=claude/kimi-k2 \
claude ...
```

## Token usage

When the leaf task's response carries `usage` (prompt / completion / total tokens), the gateway forwards it to the client in the wire-appropriate shape. Malformed usage shapes are logged and dropped rather than propagated, because token accounting is best-effort and must not fail an otherwise successful completion.

### OpenAI non-streaming

The `ChatCompletionResponse` carries `usage: {prompt_tokens, completion_tokens, total_tokens}` when the leaf populated it, omitted otherwise.

### OpenAI streaming

Per OpenAI convention, streaming responses only include a usage frame when the client opts in with `stream_options.include_usage: true`. When set, the gateway emits an extra SSE frame with `choices: []` and `usage: {...}` immediately before the terminating `data: [DONE]`. Without the opt-in the stream is byte-for-byte compatible with clients that never expect the extra frame.

### Anthropic

The Messages API always attaches usage to the `message_delta` event on streaming responses and to the `usage` field on non-streaming responses — no opt-in required, matching Anthropic's own behaviour.

## Model listing

`GET <path>/models` returns the OpenAI-shape model list of currently-registered proxy names. Hot-reload updates the list automatically as flows come and go.

## Hot reload

When a flow with `llm_proxy` tasks is redeployed via configmap or git-sync reload, the server bulk-deregisters every proxy the flow owned before re-registering under the new config. Inbound requests hitting an unregistered proxy return `-32000 Unknown proxy` until the reload completes.

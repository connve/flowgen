# AI Gateway

OpenAI-compatible chat completions endpoint. Each `llm_proxy` task registers a flow as a backend; clients reach it through the worker's AI gateway server using the standard OpenAI request shape, enabling pre/post processing, logging, and tool execution inside the flow.

The server exposes:

- `POST <ai_gateway.path>/chat/completions` — streams responses as Server-Sent Events when `stream: true`.
- `GET <ai_gateway.path>/models` — lists every registered `llm_proxy` task by name.

The default path is `/v1`, matching the convention used by vLLM, Ollama, LiteLLM, OpenRouter, and the OpenAI SDKs. Clients connect with `base_url=http://host:3002/v1` and reach the standard OpenAI URLs.

## Worker configuration

Enable the AI gateway server under `worker.ai_gateway`. It runs on its own port, independent of the webhook HTTP server.

```yaml
worker:
  ai_gateway:
    enabled: true
    port: 3002
    path: "/v1"
    # Optional credentials file (bearer/basic) shared across proxies.
    # credentials_path: "/etc/ai-gateway/credentials.json"
    # Optional auth provider — same shape as http_server.auth.
    # auth:
    #   type: jwt
    #   secret: "your-hmac-secret"
```

## Flow configuration

```yaml
tasks:
  - llm_proxy:
      name: gemini
      # protocol: openai   # default; mounts /v1/chat/completions

  - ai_completion:
      name: complete
      provider: google_vertex
      model: "{{event.data.model}}"
      stream: true
      prompt: "{{event.data.prompt}}"
```

## Routing by `model` field

Clients select which `llm_proxy` to hit by setting the request body's `model` field to `<task-name>/<downstream-model>`. The gateway splits on the first `/`, looks up the task by name, and forwards the rest as the downstream model.

For the flow above, a client sends:

```json
{
  "model": "gemini/gemini-2.0-flash",
  "messages": [{ "role": "user", "content": "..." }],
  "stream": true
}
```

The gateway routes to the `gemini` task and the downstream `ai_completion` receives `event.data.model = "gemini-2.0-flash"`.

Multiple `llm_proxy` tasks across flows coexist on the same `/v1/chat/completions` URL, each picked by its `name` prefix.

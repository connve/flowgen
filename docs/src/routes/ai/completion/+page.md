# AI Completion

Run LLM completions from multiple providers within a flow. Supports custom OpenAI-compatible endpoints, tool calling via MCP servers, streaming, and RAG via static context.

## Configuration

```yaml
- ai_completion:
    name: analyze
    provider: google
    model: gemini-2.5-flash-lite
    credentials_path: /path/to/credentials.json
    prompt: "Analyze this data: {{event.data}}"
```

### Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `name` | string | required | Task name. |
| `provider` | string | required | LLM provider — see [Providers](#providers) below. |
| `model` | string | required | Model identifier (e.g. `gpt-4`, `claude-3-5-sonnet-20241022`, `gemini-2.5-flash-lite`). |
| `prompt` | string/resource | required | User prompt template. Supports handlebars templating and resource files. |
| `credentials_path` | string | | Path to provider credentials JSON. Required for hosted providers, optional for `custom`/`ollama` (local providers without auth). |
| `endpoint` | string | | Base URL of a custom OpenAI-compatible endpoint. Required when `provider: custom` or `provider: ollama`. Example: `https://llm-gateway.internal.example.com/v1`. |
| `headers` | map of string to string | | HTTP headers sent with every request (`provider: custom`/`ollama` only). Use this to identify the agent to a downstream `llm_proxy` scoped by its own `headers` field — see [AI Gateway](/docs/flowgen/ai/gateway#llm_proxy). |
| `system_prompt` | string/resource | | System prompt. |
| `max_tokens` | int | | Maximum tokens in response. |
| `temperature` | float | | Sampling temperature (0.0–1.0). |
| `stream` | bool | `false` | Stream response as separate events instead of one final event. |
| `static_context` | list | | RAG documents (inline strings or `resource:` references) always available to the agent. |
| `max_turns` | int | `10` | Most model calls one event may make, tool calls included. At least 1, and at least 2 with `mcp_servers`: one call to use a tool and one to answer. |
| `mcp_servers` | list | | MCP server URLs for tool discovery. The agent connects to each, discovers tools, and makes them callable during completion. |
| `tool_passthrough` | bool | `false` | Skip rig's agent loop and forward caller-supplied `tools` verbatim to the upstream provider. See [Tool-use passthrough](#tool-use-passthrough). |
| `output_schema` | object | | JSON Schema the answer must match. The answer stays JSON text in `text`; parse it with `parse_json` in a `script`. |
| `images` | list of string | | Images sent with the prompt, for models that read images: http(s) URLs, downloaded and sent as base64, or base64 `data:` URLs. Each must be a PNG, JPEG, GIF, WebP, HEIC, HEIF or SVG image of at most `image_max_bytes`. A URL must reach a public address: loopback, private, link-local and other non-public addresses are refused, also after a redirect, and proxy settings are not used. A refused, empty or malformed URL fails the event without a retry. Supports handlebars templating. |
| `image_timeout` | duration | `30s` | How long fetching one image may take. |
| `image_max_bytes` | integer | `20971520` | Largest image sent to the model, in bytes. |
| `depends_on` | list | | Upstream task names. |
| `retry` | object | | [Retry configuration](/docs/flowgen/concepts/retry). A provider error is retried on 408, 425, 429, a 5xx or a failed connection; other 4xx fail at once. |

## Providers

| Provider | Auth | Required credentials fields |
|---|---|---|
| `openai` | API key | `api_key` |
| `anthropic` | API key | `api_key` |
| `google` | API key (Gemini API) | `api_key` |
| `cohere` | API key | `api_key` |
| `mistral` | API key | `api_key` |
| `groq` | API key | `api_key` |
| `together` | API key | `api_key` |
| `xai` | API key | `api_key` |
| `openrouter` | API key | `api_key` |
| `perplexity` | API key | `api_key` |
| `huggingface` | API key | `api_key` |
| `vertexai` | Google ADC | `project_id`, `region` (optional, can come from env) |
| `ollama` | Optional API key | optional `api_key` |
| `custom` | Optional API key | optional `api_key` |

For providers not listed above (cloud-native services with non-`api_key` auth, on-prem model servers, multi-tenant routing), set `provider: custom` and point `endpoint` at any service speaking the OpenAI chat-completions wire format. The flowgen [AI gateway](/docs/flowgen/ai/gateway) (`llm_proxy` task) can itself act as that endpoint, letting you route, log, and rate-limit across upstream models from one place.

### Credentials file format

For API-key providers, credentials file is a simple JSON:

```json
{ "api_key": "sk-..." }
```

For `vertexai`, the JSON sets the project and region; **authentication itself goes through Google Application Default Credentials (ADC)** — the pod must have access to a service account via the `GOOGLE_APPLICATION_CREDENTIALS` env var or workload identity:

```json
{ "project_id": "my-gcp-project", "region": "us-central1" }
```

## Example: custom OpenAI-compatible endpoint

Point at any service speaking the OpenAI chat-completions wire format — an internal model server, a self-hosted gateway, or the flowgen [AI gateway](/docs/flowgen/ai/gateway) itself routing to one or more upstream models.

```yaml
- ai_completion:
    name: internal_llm
    provider: custom
    model: my-internal-model
    endpoint: "https://llm-gateway.internal.example.com/v1"
    credentials_path: /etc/flowgen/credentials/internal_llm.json
    prompt: "{{event.data.question}}"
```

For endpoints without auth, omit `credentials_path`:

```yaml
- ai_completion:
    name: unauth_endpoint
    provider: custom
    model: my-model
    endpoint: "http://llm.cluster.local/v1"
    prompt: "{{event.data}}"
```

## Example: RAG with static context

`static_context` documents are injected into every completion. Mix inline strings and resource files.

```yaml
- ai_completion:
    name: support_bot
    provider: openai
    model: gpt-4-turbo
    credentials_path: /etc/flowgen/credentials/openai.json
    system_prompt: "Answer using the provided context."
    prompt: "{{event.data.question}}"
    static_context:
      - resource: "docs/product_manual.md"
      - resource: "docs/pricing.md"
      - "Support contact: support@example.com"
    max_turns: 3
```

## Example: structured answer with an image

The model answers JSON matching `output_schema`.

```yaml
- ai_completion:
    name: describe_image
    provider: openai
    model: gpt-4.1
    credentials_path: /etc/flowgen/credentials/openai.json
    prompt: "Describe the image."
    images: ["{{event.data.image_url}}"]
    output_schema:
      type: object
      properties:
        summary: { type: string }
        tags: { type: array, items: { type: string } }
      required: [summary, tags]

- script:
    name: read_answer
    code: parse_json(event.data.text)
```

## Example: MCP tool calling

The agent connects to MCP servers, discovers tools, and can call them during completion. Use `flowgen`'s own MCP server (`mcp_tool` task) or any external MCP-compatible server.

```yaml
- ai_completion:
    name: agent
    provider: anthropic
    model: claude-3-5-sonnet-20241022
    credentials_path: /etc/flowgen/credentials/anthropic.json
    prompt: "{{event.data.task}}"
    max_turns: 5
    mcp_servers:
      - url: "http://localhost:3001/mcp"
      - url: "http://external-tools:8080/mcp"
```

Optional per-server auth uses the same credentials JSON format as `http_request` (`bearer_auth`, `basic_auth`, or `oauth2_client_credentials`):

```yaml
    mcp_servers:
      - url: "https://tools.example.com/mcp"
        credentials_path: /etc/flowgen/credentials/mcp_tools.json
```

For MCP servers that require OAuth 2.0, use the `oauth2_client_credentials` format in the credentials file — flowgen fetches and refreshes the access token automatically:

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

Each entry also accepts `headers`, sent with every request to that MCP server. Use it to identify this agent to an `mcp_tool` scoped by its own `headers` field, so the agent only sees and can call the tools meant for it:

```yaml
    mcp_servers:
      - url: "http://localhost:3001/mcp"
        headers:
          X-Flowgen-Client: your-agent-name
```

### Calling tools as the signed-in user

With `on_behalf_of_user: true`, the agent calls the server as the user who sent the request, with their token as the bearer:

```yaml
    mcp_servers:
      - url: "http://localhost:3001/mcp"
        on_behalf_of_user: true
```

- The token is the bearer that an `llm_proxy`, `http_endpoint`, or `mcp_tool` verified with the worker's `auth` provider. This includes Agents chat requests when [`web.agents.on_behalf_of_user`](/docs/flowgen/concepts/configuration) is set. A `buffer` keeps it when every event in a batch comes from the same user.
- Set it only for servers that trust your identity provider, such as flowgen's own. For web UI tokens, the server's `auth` is `oidc` with the issuer of `web.auth` and its `client_id` as the audience.
- Setting it together with `credentials_path` fails the task at startup.
- A request without a signed-in user, or whose token the server answers with 401 or 403, fails without a retry.
- Each user gets their own connections and agent client, closed after 30 minutes without a request. A new token opens new connections. A request already running keeps its own until it finishes.

## Tool-use passthrough

When `tool_passthrough: true`, the task bypasses rig's agent loop and forwards caller-supplied `tools` and `tool_choice` (from the request body) directly to the upstream provider. Tool invocations returned by the model surface as `tool_calls` on the response instead of being executed in-process. The client — typically opencode, Claude Desktop, or any OpenAI-tool-use SDK — runs the tools locally and posts the results back as `role: "tool"` follow-up messages.

Use this mode when flowgen is the AI gateway for a tool-executing client. Leave it `false` for agent-mode flows that execute tools themselves via [`mcp_servers`](#example-mcp-tool-calling) or inline definitions.

```yaml
- llm_proxy:
    name: proxy

- ai_completion:
    name: forward
    provider: custom
    endpoint: "{{event.data.endpoint}}"
    model: "{{event.data.routed_model}}"
    credentials_path: "{{event.data.credentials_path}}"
    prompt: "{{event.data.prompt}}"
    system_prompt: "{{event.data.system_prompt}}"
    tool_passthrough: true
    stream: true
```

Passthrough is a no-op when the request omits `tools`: the flow follows the normal completion path so a single `ai_completion` task can serve both tool-using and plain chat clients without a second flow.

Works across every provider flowgen supports (OpenAI, Anthropic, Cohere, Gemini, Mistral, Groq, Together, xAI, OpenRouter, Perplexity, HuggingFace, VertexAI, and any OpenAI-compatible endpoint via `provider: custom`). Provider-native quirks in the tool schema — Anthropic's `tool_use` block, Google's parallel-call ordering — are handled by the underlying rig client without any per-provider config here.

## Output

Format: [JSON](https://docs.rs/serde_json/latest/serde_json/enum.Value.html)

| Field | Type | Description |
|---|---|---|
| `text` | string | Generated completion text. |
| `model` | string | Model used. |
| `provider` | string | Provider name. |
| `usage` | object | Token counts (`prompt_tokens`, `completion_tokens`, `total_tokens`). |

In `stream: true` mode, each chunk is emitted as a separate event with partial `text` content.

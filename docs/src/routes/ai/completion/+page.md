# AI Completion

Run LLM completions from multiple providers within a flow. Supports tool calling, streaming, and sandboxed execution.

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
| `provider` | string | required | LLM provider (`google`, `anthropic`, `openai`). |
| `model` | string | required | Model identifier. |
| `credentials_path` | string | required | Path to provider credentials. |
| `prompt` | string/resource | required | Prompt template. Supports templating and resource files. |
| `system_prompt` | string/resource | | System prompt. |
| `max_tokens` | int | | Maximum tokens in response. |
| `temperature` | float | | Sampling temperature. |
| `tools` | list | | Tool definitions for function calling. |
| `sandbox` | object | | Sandbox configuration for tool execution. |
| `depends_on` | list | | Upstream task names. |
| `retry` | object | | Retry configuration. |

### Sandbox

Optional sandboxing for tool execution via nsjail. Rhai scripts do not need sandboxing (safe by design).

```yaml
- ai_completion:
    name: agent
    provider: google
    model: gemini-2.5-flash-lite
    prompt: "{{event.data}}"
    sandbox:
      memory_limit_mb: 512
      time_limit_seconds: 30
      max_pids: 10
      allow_network: false
```

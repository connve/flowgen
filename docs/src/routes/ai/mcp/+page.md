# MCP Tools

Expose flows as [Model Context Protocol](https://modelcontextprotocol.io) tools. LLM agents can discover and call flowgen flows as tools over the MCP transport.

## Configuration

The MCP server must be enabled in the worker config:

```yaml
worker:
  mcp_server:
    enabled: true
    port: 3001
    path: /mcp/v1
    credentials_path: /path/to/credentials.json
```

Individual flows register as MCP tools using the `mcp_tool` task:

```yaml
- mcp_tool:
    name: lookup_user
    description: "Look up a user by ID."
    input_schema:
      type: object
      properties:
        user_id:
          type: string
      required:
        - user_id
```

### Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `name` | string | required | Tool name. Combined with the flow name as `{flow_name}.{name}` to form the full tool identifier. |
| `description` | string | required | Human-readable description shown to LLMs for tool selection. |
| `input_schema` | object | | JSON Schema for the tool's input parameters. |
| `credentials_path` | string | | Path to credentials file for this tool. Overrides `worker.mcp_server.credentials_path`. |
| `ack_timeout` | duration | wait indefinitely | Max time to wait for flow completion before returning a timeout error to the MCP client. |
| `auth` | object | | Per-tool authentication config. When `auth.required` is true, requests must include a valid bearer token validated by the worker auth provider; the resolved user context is injected into `event.meta.auth`. |
| `depends_on` | list | | Upstream task names. |
| `retry` | object | | [Retry configuration](/docs/flowgen/concepts/retry). |

The MCP server listens on the configured port and exposes registered tools over the MCP Streamable HTTP transport.

## Output

Format: [JSON](https://docs.rs/serde_json/latest/serde_json/enum.Value.html)

| Field | Type | Description |
|---|---|---|
| `arguments` | object | Tool input parameters from the MCP `tools/call` request, structured per the configured `input_schema`. |

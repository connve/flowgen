# MCP Tools

Expose flows as [Model Context Protocol](https://modelcontextprotocol.io) tools. LLM agents can discover and call flowgen flows as tools over the MCP transport.

## Configuration

The MCP server must be enabled in the worker config:

```yaml
worker:
  mcp_server:
    enabled: true
    port: 3001
    path: /mcp
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

The MCP server listens on the configured port and exposes registered tools over the MCP Streamable HTTP transport.

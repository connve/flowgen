<script>
  import AgentClients from '$lib/components/AgentClients.svelte';
</script>

# Agent setup

Build flows with the coding agent you already use. Every change it proposes waits for your approval.

## Pick your agent

<AgentClients />

The examples use the address of a local run:

```
http://127.0.0.1:3001/mcp/v1/system/flowgen_agent
```

For flowgen on a server, use its address instead; see [Remote instance](#remote-instance). When the server asks for an API key, send it as `Authorization: Bearer <key>`.

## OpenCode

In `opencode.json` of the project:

```json
{
  "$schema": "https://opencode.ai/config.json",
  "mcp": {
    "servers": {
      "flowgen": {
        "type": "remote",
        "url": "http://127.0.0.1:3001/mcp/v1/system/flowgen_agent",
        "oauth": false
      }
    }
  }
}
```

With an API key, add `"headers": { "Authorization": "Bearer <key>" }` to `flowgen`.

## Goose

Run `goose configure`, choose Add Extension, then Remote Extension (Streamable HTTP), or add it to `~/.config/goose/config.yaml`:

```yaml
extensions:
  flowgen:
    name: flowgen
    type: streamable_http
    uri: http://127.0.0.1:3001/mcp/v1/system/flowgen_agent
    enabled: true
    timeout: 300
```

With an API key, add `headers: { Authorization: "Bearer <key>" }`.

## Claude Code

```sh
claude mcp add --transport http flowgen http://127.0.0.1:3001/mcp/v1/system/flowgen_agent
```

With an API key, add `--header "Authorization: Bearer <key>"`.

## Codex

```sh
codex mcp add flowgen --url http://127.0.0.1:3001/mcp/v1/system/flowgen_agent
```

With an API key, name the variable that holds it in `~/.codex/config.toml`:

```toml
[mcp_servers.flowgen]
url = "http://127.0.0.1:3001/mcp/v1/system/flowgen_agent"
bearer_token_env_var = "FLOWGEN_MCP_KEY"
```

## Cursor

In `.cursor/mcp.json` of the project, or `~/.cursor/mcp.json` for every project:

```json
{
  "mcpServers": {
    "flowgen": {
      "url": "http://127.0.0.1:3001/mcp/v1/system/flowgen_agent"
    }
  }
}
```

With an API key, add `"headers": { "Authorization": "Bearer <key>" }` to `flowgen`.

## GitHub Copilot

In `.vscode/mcp.json` of the project:

```json
{
  "servers": {
    "flowgen": {
      "type": "http",
      "url": "http://127.0.0.1:3001/mcp/v1/system/flowgen_agent"
    }
  }
}
```

With an API key, add `"headers": { "Authorization": "Bearer <key>" }` to `flowgen`.

## Gemini CLI

```sh
gemini mcp add --transport http flowgen http://127.0.0.1:3001/mcp/v1/system/flowgen_agent
```

With an API key, add `--header "Authorization: Bearer <key>"`.

## Remote instance

On Kubernetes, the Helm chart gives the MCP server a Service on port 3001 whose name ends in `-mcp-server`, such as `flowgen-mcp-server` for the release `flowgen`. Reach it in one of two ways:

- From your machine, forward the port and keep the local address:

  ```sh
  kubectl port-forward service/flowgen-mcp-server 3001
  ```

- Expose it with `flowgen.services.mcp_server.type: LoadBalancer` or through your ingress, and use `https://<host>/mcp/v1/system/flowgen_agent`.

An exposed server needs an API key, in `mcp_server.credentials_path` as `{"api_keys": ["<key>"]}` with at least 32 characters, or [sign-in](/docs/flowgen/ai/mcp#sign-in) with `auth`; behind an ingress, set `mcp_server.public_url`. Agents in a browser, such as web chat apps, connect from their provider's servers, so they need the exposed address.

## What the agent gets

The toolset's `initialize` response carries [instructions](/docs/flowgen/ai/mcp#instructions): start from a template, check each task's fields in the docs, validate, then propose. Agents that support MCP instructions add them to the model's context, and the main rules are in the tools' descriptions too.

Every folder of flows is a toolset of its own, such as `/mcp/v1/sales`. Connect an agent to the toolsets it needs, and write `resources/<folder>/instructions.md` to tell it how to use them. See [Toolsets](/docs/flowgen/ai/mcp#toolsets).

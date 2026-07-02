# MCP

Expose flows and curated content to [Model Context Protocol](https://modelcontextprotocol.io) clients — Claude Desktop, Cursor, and other MCP-aware LLM applications. Flowgen ships four MCP task types built on a shared server:

| Task | Client sees | Purpose |
|---|---|---|
| `mcp_tool` | Callable tool | LLM invokes a flow with typed arguments. |
| `mcp_resource` | Read-only reference | Curated context (schemas, glossaries, docs) the LLM can pull in or cite. |
| `mcp_prompt` | Slash-command template | Argument-driven message templates surfaced in the prompt palette. |
| completion | Argument autocomplete | Per-argument value suggestions served for prompts and templated resources. |

## Server configuration

The MCP server must be enabled in the worker config to register any of the tasks below:

```yaml
worker:
  mcp_server:
    enabled: true
    port: 3001
    path: /mcp/v1
    credentials_path: /path/to/credentials.json
    resource_uri_scheme: flowgen
```

### Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `enabled` | bool | required | Must be `true` for any MCP task to register. |
| `port` | int | `3001` | Port the MCP server listens on. |
| `path` | string | `/mcp/v1` | HTTP endpoint path. |
| `credentials_path` | string | | Global API-key credentials JSON. Individual `mcp_tool` tasks can override with their own `credentials_path`. |
| `auth` | object | | Auth provider config for user identity resolution (JWT/OIDC/session). |
| `resource_uri_scheme` | string | `flowgen` | Scheme used when auto-generating `mcp_resource` URIs (`<scheme>://<flow_name>/<name>`). White-label deployments override this so LLM-visible identifiers carry the deployment brand instead of `flowgen`. |

The server exposes a single endpoint at `path` that speaks the MCP Streamable HTTP transport: `POST <path>` accepts JSON-RPC requests, `GET <path>` opens a long-lived SSE stream for [notifications](#notifications).

## `mcp_tool`

Registers a flow as an LLM-callable tool. Inbound `tools/call` requests are injected as events at the head of the flow's pipeline; downstream tasks process the arguments and the final result is returned to the client.

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
| `credentials_path` | string | | Overrides `worker.mcp_server.credentials_path` for this tool. |
| `ack_timeout` | duration | wait indefinitely | Max time to wait for flow completion before returning a timeout error to the client. |
| `auth` | object | | Per-tool authentication. When `auth.required` is true, requests must include a valid bearer token validated by the worker auth provider; the resolved user context is injected into `event.meta.auth`. |
| `depends_on` | list | | Upstream task names. |
| `retry` | object | | [Retry configuration](/docs/flowgen/concepts/retry). |

### Output

Format: [JSON](https://docs.rs/serde_json/latest/serde_json/enum.Value.html)

| Field | Type | Description |
|---|---|---|
| `arguments` | object | Tool input parameters from the MCP `tools/call` request, structured per the configured `input_schema`. |

## `mcp_resource`

Publishes curated read-only content. The content is served by `resources/read` on request; concrete resources resolve once at task init, while templated URIs defer rendering to per-read so `{placeholder}` bindings drive distinct output.

### Concrete resource (fixed URI)

```yaml
- mcp_resource:
    name: sf_account_schema
    description: "Salesforce Account SObject field schema."
    mime_type: application/json
    content:
      resource: "schemas/sf-account.json"
```

Auto-generated URI: `flowgen://<flow_name>/sf_account_schema` (scheme controlled by `resource_uri_scheme`).

### Explicit URI override

```yaml
- mcp_resource:
    name: api_docs
    uri: "https://docs.example.com/api.md"
    description: "Internal API documentation."
    mime_type: text/markdown
    content:
      resource: "docs/api.md"
```

Use when the resource has a real URL the client can also reach directly.

### Templated URI

```yaml
- mcp_resource:
    name: account_summary
    uri_template: "flowgen://account/{id}"
    description: "Per-account summary; bind {id} to a Salesforce ID."
    mime_type: text/markdown
    content: |
      # Account {{id}}
      Loaded via templated resource.
    parameters:
      - name: id
        completion:
          resource: "completions/account_ids.txt"
```

Client calls `resources/read({uri: "flowgen://account/001Qy0abc"})`; the server matches the pattern, binds `id=001Qy0abc`, and renders `content` as a Handlebars template with `{{id}}` in scope. `parameters` attaches [completion](#completion) suggestions to `{placeholder}`s.

### Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `name` | string | required | Task name. Used as the trailing segment of the auto-generated URI when `uri` and `uri_template` are unset. |
| `uri` | string | | Explicit fixed URI. Mutually exclusive with `uri_template`. |
| `uri_template` | string | | RFC 6570 URI template with `{placeholder}` bindings. Registers as a resource template rather than a concrete resource. |
| `description` | string | required | Human-readable description shown to MCP clients. |
| `mime_type` | string | `text/plain` | Standard MIME type. |
| `content` | string/resource | required | Inline string or `resource:` reference. For templates, rendered as a Handlebars template against `{placeholder}` bindings on each read. |
| `parameters` | list | | URI-template parameter descriptors. Only valid when `uri_template` is set. Each entry may attach a `completion` source served by `completion/complete`. |
| `depends_on` | list | | Upstream task names. |
| `retry` | object | | [Retry configuration](/docs/flowgen/concepts/retry). |

## `mcp_prompt`

Publishes a slash-command template. MCP clients surface each registered prompt in their prompt palette; when the user invokes it, `prompts/get` renders the template against the collected argument values and returns the resulting messages as the start of the conversation.

### Single-message form

```yaml
- mcp_prompt:
    name: explain_query
    description: "Explain what a BigQuery SQL query does."
    arguments:
      - name: query
        description: "The SQL query text."
        required: true
    template: |
      Explain what this BigQuery query does, what tables it reads,
      and what the result shape will look like.

      ```sql
      {{arguments.query}}
      ```
```

`template` is rendered as a single `user` message.

### Multi-message form (few-shot)

```yaml
- mcp_prompt:
    name: classify_intent
    description: "Classify user intent as sales/support/other."
    arguments:
      - name: message
        description: "User message to classify."
        required: true
    messages:
      - role: user
        content: "How much does the enterprise plan cost?"
      - role: assistant
        content: "sales"
      - role: user
        content: "{{arguments.message}}"
```

`messages` is a list of role-tagged (`user` / `assistant`) messages, each rendered as a Handlebars template. Use this form when the prompt benefits from pre-filled turns.

### Arguments

```yaml
arguments:
  - name: tone
    description: "Tone: formal, casual, urgent."
    required: false
    default: "formal"
    completion:
      values: [formal, casual, urgent]
```

| Field | Type | Default | Description |
|---|---|---|---|
| `name` | string | required | Argument identifier. Referenced from the template as `{{arguments.<name>}}`. |
| `description` | string | required | Description shown when the client prompts the user for a value. |
| `required` | bool | `false` | If true, `prompts/get` returns an error when the value is missing. |
| `default` | string | | Value substituted when the client omits an optional argument. Empty string when unset. |
| `completion` | object | | [Completion](#completion) source served by `completion/complete`. |

### Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `name` | string | required | Prompt identifier. Becomes the slash command in MCP clients. |
| `description` | string | required | Description shown in the prompt palette. |
| `arguments` | list | | Argument definitions (see above). |
| `template` | string/resource | | Single-message shorthand. Mutually exclusive with `messages`. |
| `messages` | list | | Multi-message form. Each entry is `{role, content}` where `role` is `user` or `assistant` and `content` is a string or `resource:` reference rendered as a Handlebars template. Mutually exclusive with `template`. |
| `depends_on` | list | | Upstream task names. |
| `retry` | object | | [Retry configuration](/docs/flowgen/concepts/retry). |

## Completion

Argument autocomplete for prompts and templated resource parameters. When the user is typing an argument value in the client UI, the client sends `completion/complete` with the reference (`ref/prompt` or `ref/resource`), the argument name, and the current partial value; the server returns candidate matches ranked by relevance.

Completion sources are declared per-argument. Two forms are supported:

### Inline values

```yaml
completion:
  values: [python, pytorch, pyside]
```

### Loader-backed file

```yaml
completion:
  resource: "completions/account_ids.txt"
```

One value per line. Blank lines and lines beginning with `#` are ignored; leading and trailing whitespace on each line is trimmed. The file is loaded once at task init.

### Filtering

Matches are case-insensitive prefix matches against the partial value; empty value returns all candidates. Responses are capped at 100 values with `total` and `hasMore` fields set.

## Notifications

The MCP server hosts a long-lived SSE stream on `GET <path>` for server-to-client push notifications. Clients that open the stream receive an `Mcp-Session-Id` header and get pushed:

- `notifications/resources/list_changed` — a `mcp_resource` task was registered or a flow with resources was hot-reloaded away.
- `notifications/prompts/list_changed` — same for `mcp_prompt`.

Broadcast is non-blocking (`try_send`) so a slow client does not stall registration; sessions whose receiver has been dropped are evicted lazily on the next broadcast.

Not currently emitted:

- `resources/subscribe` and `notifications/resources/updated` (per-URI content-change subscriptions).
- Session resumability via `Last-Event-ID`.

## Hot reload

When a flow with MCP tasks is redeployed via configmap or git-sync reload, the server bulk-deregisters every entry the flow owned across tools, resources, resource templates, and prompts, then re-registers under the new config. `list_changed` notifications fire only for tables that actually lost or gained entries during the sweep.

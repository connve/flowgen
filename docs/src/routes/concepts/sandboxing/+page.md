# Sandboxing

LLM tool calls are code flowgen does not control. The model decides what to invoke, and a prompt injection can turn a benign tool into a destructive one. When sandboxing is enabled, flowgen runs tool execution under [nsjail](https://github.com/google/nsjail), isolating it from the host filesystem, network, and process tree.

## When to sandbox

| Task | Recommendation |
|---|---|
| `ai_completion` with tools | **Recommended.** LLMs are vulnerable to prompt injection, and tool calls are LLM-generated code. Sandbox protects the host if a model is tricked into running a destructive tool. |
| `script` (Rhai) | Not needed. Rhai is a sandboxed scripting language by design — no IO, no FFI, no host access except what flowgen explicitly exposes (`ctx.cache`, `ctx.resource`, built-in helpers). |
| `http_webhook`, `http_request`, SQL queries, connector tasks | Not applicable. They make network calls or run against external systems; there is no local code execution to isolate. |

## Configuring a sandbox

Sandboxing is opt-in. Add a `sandbox` block to the task to enable it; omit it to run unsandboxed.

```yaml
- ai_completion:
    name: agent
    provider: google
    model: "gemini-2.5-flash-lite"
    prompt: "{{event.data.question}}"
    tools:
      - name: lookup_user
        # ... tool definition ...
    sandbox:
      memory_limit_mb: 1024
      time_limit_seconds: 60
      allow_network: true
```

To run with all defaults, use an empty block:

```yaml
- ai_completion:
    name: agent
    provider: google
    model: "gemini-2.5-flash-lite"
    prompt: "{{event.data.question}}"
    tools:
      - name: lookup_user
        # ... tool definition ...
    sandbox: {}
```

## Defaults

| Field | Default | Description |
|---|---|---|
| `memory_limit_mb` | `512` | Maximum resident memory the sandboxed process may use. |
| `time_limit_seconds` | `30` | CPU time budget. The process is killed if it exceeds this. |
| `max_pids` | `10` | Maximum number of processes/threads inside the sandbox. |
| `allow_network` | `false` | When `false`, the sandbox cannot make network connections. |
| `nsjail_path` | `"nsjail"` | Path to the nsjail binary. Searches `PATH` by default. |
| `user_id` | `99999` | UID the sandboxed process runs as (usually `nobody`). |
| `group_id` | `99999` | GID the sandboxed process runs as (usually `nogroup`). |

## Threat model

What sandboxing protects against:

- **Filesystem access** — the sandboxed process cannot read or write the host filesystem outside its private working directory.
- **Network egress** — when `allow_network: false`, the process cannot make outbound connections (DNS, HTTP, anything).
- **Privilege escalation** — runs as an unprivileged user, with capabilities dropped.
- **Resource exhaustion** — memory, CPU time, and process count are bounded. A runaway script cannot starve the worker.

What sandboxing does **not** protect against:

- **Logical attacks** on data the sandboxed tool is given — if you pass untrusted input into a tool, the tool can still mishandle that input.
- **Side-channel attacks** — timing, cache-based attacks against co-resident processes.
- **Vulnerabilities in nsjail itself** — keep nsjail up to date.
- **Whatever the tool returns** — sandboxing isolates execution, but the tool's output becomes part of the next event payload. Validate downstream.

## Operational notes

- **nsjail must be installed** on the worker host or container image. The official flowgen Docker image ships with it; custom images need to install nsjail explicitly.
- **Linux only.** nsjail uses Linux kernel features (cgroups, namespaces, seccomp). Sandboxing is unavailable on macOS and Windows. Without nsjail, sandboxed tasks fail at init time with a clear error.
- **Tools that need network access** (e.g., a tool that calls an external API) require `allow_network: true`. Consider whether the tool needs full network or whether the call should be done by a non-sandboxed `http_request` task instead, with the result piped into the agent.
- **Time budget vs retry budget**: a sandboxed tool that exceeds `time_limit_seconds` is killed by nsjail. Flowgen's retry layer treats this as a retriable error and re-runs the task — so a slow tool can burn through the retry circuit-breaker budget. Tune both together.

## Example — AI agent with sandboxed tools

```yaml
- ai_completion:
    name: secure_agent
    provider: google
    model: "gemini-2.5-flash-lite"
    prompt: "{{event.data.question}}"
    tools:
      - name: lookup_user
        # ... tool definition ...
    sandbox:
      memory_limit_mb: 512
      time_limit_seconds: 30
      allow_network: true   # Tool needs to call the user service.
```

When the model decides to invoke `lookup_user`, the tool runs inside nsjail. A prompt injection that tricks the model into running `rm -rf /` cannot escape the sandbox.

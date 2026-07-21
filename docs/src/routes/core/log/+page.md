# Log

Logs event data to stdout.

## Configuration

```yaml
- log:
    name: output
```

### Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `name` | string | required | Task name. |
| `level` | string | `info` | Log level: `trace`, `debug`, `info`, `warn`, `error`. |
| `include_meta` | bool | false | Include `event.meta` in the log body alongside the payload. |
| `depends_on` | list | | Upstream task names. |
| `retry` | object | | [Retry configuration](/docs/flowgen/concepts/retry). |

## Output

| Format | Crate | Description |
|---|---|---|
| same as input | — | Pass-through — event data is forwarded unchanged. |

The log line body is the pretty-printed JSON of `event.data`. The event's
`id` and `subject` are emitted as structured tracing fields (`event.id`,
`event.subject`) so a log store keeps them as top-level attributes.

## Example

```yaml
- log:
    name: debug_output
    level: debug
    include_meta: true
```

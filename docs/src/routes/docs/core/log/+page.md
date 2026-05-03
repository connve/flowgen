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
| `structured` | bool | false | Output as structured JSON. |
| `depends_on` | list | | Upstream task names. |
| `retry` | object | | Retry configuration. |

## Example

```yaml
- log:
    name: debug_output
    level: debug
    structured: true
```

# Generate

Produces events on a schedule. Source task — typically first in a flow.

## Configuration

```yaml
- generate:
    name: ticker
    interval: "5s"
```

### Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `name` | string | required | Task name. |
| `payload` | object | | Structured data to include in each event. |
| `interval` | duration | | Interval schedule (e.g., `5s`, `1m`). Mutually exclusive with `cron`. |
| `cron` | string | | Cron expression. Mutually exclusive with `interval`. |
| `timezone` | string | `UTC` | Timezone for cron evaluation. |
| `count` | int | | Max events to generate. Runs indefinitely if omitted. |
| `allow_rerun` | bool | false | Reset the counter on restart. |
| `ack_timeout` | duration | | Flow completion timeout. |
| `depends_on` | list | | Upstream task names. |
| `retry` | object | | Retry configuration. |

## Examples

**Interval with payload:**

```yaml
- generate:
    name: heartbeat
    interval: "1m"
    payload:
      type: heartbeat
      source: flowgen
```

**Cron schedule:**

```yaml
- generate:
    name: daily_trigger
    cron: "0 2 * * *"
    timezone: "America/New_York"
```

**Run once:**

```yaml
- generate:
    name: init
    interval: "1s"
    count: 1
```

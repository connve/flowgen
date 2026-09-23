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
| `payload` | object | | Structured data to include in each event. `system_info` is added only when the payload is an object. |
| `interval` | duration | | Interval schedule (e.g., `500ms`, `5s`, `1m`). The first run fires one interval after start, or at most one interval after the last successful run when resuming. Maximum `100y`. Mutually exclusive with `cron`. |
| `cron` | string | | Cron expression, validated at startup. Mutually exclusive with `interval`. |
| `timezone` | string | `UTC` | IANA timezone for cron evaluation (e.g., `Europe/London`), validated at startup. |
| `count` | int | | Max events to generate. Runs indefinitely if omitted. |
| `allow_rerun` | bool | false | Reset the counter on restart. |
| `ack_timeout` | duration | wait indefinitely | Max time to wait for flow completion before the next scheduled run. A run that fails or times out is not recorded as completed: `interval` and `cron` schedules fire again at the next scheduled time, and run-once mode retries with the task's retry backoff. |
| `depends_on` | list | | Upstream task names. |
| `retry` | object | | [Retry configuration](/docs/flowgen/concepts/retry). |

## Output

Format: [JSON](https://docs.rs/serde_json/latest/serde_json/enum.Value.html). Includes user-defined payload fields (if configured) plus system info.

| Field | Type | Description |
|---|---|---|
| `system_info.last_run_time` | int | Unix timestamp (seconds) of the current run. |
| `system_info.next_run_time` | int / null | Unix timestamp (seconds) of the next scheduled run. |

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

# Retry

Every task in flowgen has retry built in. The same exponential-backoff-with-jitter strategy is applied uniformly across processors, subscribers, and webhooks — there is no per-processor retry code to write or maintain. Tune the defaults at the worker level, override per task when a specific operation needs different bounds.

## Defaults

| Field | Default | Description |
|---|---|---|
| `max_attempts` | `10` | Maximum number of attempts before the task fails. |
| `initial_backoff` | `"1s"` | Delay before the first retry. Each subsequent retry doubles, with jitter applied. |

With the defaults, a task retries for approximately **15 minutes** before giving up. The full sequence (with ±50% jitter on each delay):

```
attempt 1 fails → wait ~1s
attempt 2 fails → wait ~2s
attempt 3 fails → wait ~4s
attempt 4 fails → wait ~8s
attempt 5 fails → wait ~16s
attempt 6 fails → wait ~32s
attempt 7 fails → wait ~64s    (~1 min)
attempt 8 fails → wait ~128s   (~2 min)
attempt 9 fails → wait ~256s   (~4 min)
attempt 10 fails → give up
```

Total elapsed: about 15 minutes.

`initial_backoff` accepts human-readable durations: `"500ms"`, `"2s"`, `"1m"`, etc. Setting `max_attempts: null` enables infinite retries, which is rarely what you want for a processor — see the patterns below.

## Two patterns

Flowgen applies retry differently depending on the task's role.

### Circuit breaker (processors, publishers, webhooks)

Most tasks use the retry config as a circuit breaker. They retry up to `max_attempts` with exponential backoff and jitter; if every attempt fails, the task emits an error event downstream and the source learns the flow did not complete (acknowledgement is not delivered).

This applies to: `script`, `convert`, `iterate`, `buffer`, `log`, `http_request`, `http_webhook`, `ai_completion`, `ai_gateway`, and every connector processor (database queries, object store operations, message publishers, and so on).

Failed events are not silently dropped. The error event carries `event.error` with the failure message, which downstream tasks can inspect and route to a dead-letter destination, an audit log, or an alerting endpoint.

### Infinite reconnect (subscribers)

Subscribers — `nats_jetstream_subscriber`, `salesforce_pubsubapi_subscriber`, and other long-lived consumers — must maintain connectivity indefinitely. They use the retry config differently:

- **Initialization** is wrapped in the circuit-breaker strategy. If credentials are wrong or the broker is unreachable for the full retry window, the task fails fast so the operator notices.
- **Connection loss** during the consume loop reconnects forever, sleeping `initial_backoff` between attempts. A subscriber never gives up on a transient network blip.

If you set `max_attempts: 1` on a subscriber, only the initialisation circuit breaker is affected — the runtime reconnect loop still runs forever. This is intentional: the alternative is silently dropping a critical infrastructure component when the upstream broker has a hiccup.

## Worker-level configuration

Set defaults for every flow on the worker by configuring `worker.retry`:

```yaml
worker:
  retry:
    max_attempts: 10
    initial_backoff: "1s"
```

If `worker.retry` is omitted, flowgen uses `max_attempts: 10`, `initial_backoff: "1s"` as documented above.

## Per-task overrides

Any task can override the worker default by setting `retry` on the task itself:

```yaml
- http_request:
    name: forward
    endpoint: https://api.example.com/events
    method: POST
    retry:
      max_attempts: 5
      initial_backoff: "500ms"
```

Task-level retry **fully replaces** the worker-level config — there is no field-level merge. If you set only `max_attempts: 5` at the task level, `initial_backoff` falls back to the global default (`"1s"`), not to the worker-level value.

## When to tune

A few patterns where the defaults are wrong.

**External APIs with strict rate limits** — back off more aggressively so you do not burn through your quota on transient errors:

```yaml
- http_request:
    name: rate_limited
    endpoint: https://api.partner.com/orders
    method: POST
    retry:
      max_attempts: 6
      initial_backoff: "5s"   # ~5s, 10s, 20s, 40s, 80s — about 2.5 minutes total
```

**Fast-failing operations where retries are cheap** — drop the initial backoff to recover quickly from blips:

```yaml
- http_request:
    name: internal_lookup
    endpoint: http://service.internal/lookup
    method: GET
    retry:
      max_attempts: 5
      initial_backoff: "100ms"  # ~100ms, 200ms, 400ms, 800ms — under 2 seconds total
```

**Operations you do not want to retry at all** — set `max_attempts: 1` to fail immediately. Useful for idempotent operations protected by external deduplication, or for testing error paths in development:

```yaml
- script:
    name: validate_strict
    code: |
      if event.data.id == () { throw "missing id"; }
      event.data
    retry:
      max_attempts: 1
```

## What retry does **not** do

- It does not distinguish transient from permanent errors. Every error is retried up to `max_attempts`.
- It does not preserve state between retries. The handler restarts from scratch with the same input event each time.
- It does not coordinate across replicas. Each replica retries independently.
- It does not affect the message broker's delivery semantics. After `max_attempts` fail, the source's acknowledgement never fires; the broker handles redelivery according to its own configuration (`ack_wait`, `max_deliver`, dead-letter subjects, and so on).

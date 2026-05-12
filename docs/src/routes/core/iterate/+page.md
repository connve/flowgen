# Iterate

Splits an array into individual events. Each element becomes a separate downstream event.

## Configuration

```yaml
- iterate:
    name: split_records
```

### Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `name` | string | required | Task name. |
| `iterate_key` | string | | Key to extract the array from a JSON object. Without this, the entire event data is treated as an array. |
| `depends_on` | list | | Upstream task names. |
| `retry` | object | | Retry configuration. |

## Examples

**Split entire event (event data is an array):**

```yaml
- iterate:
    name: split
```

Input: `[{id: 1}, {id: 2}, {id: 3}]` produces three separate events.

**Extract and split a nested array:**

```yaml
- iterate:
    name: split_records
    iterate_key: records
```

Input: `{total: 3, records: [{id: 1}, {id: 2}]}` produces two events from the `records` array.

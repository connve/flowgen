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
| `retry` | object | | [Retry configuration](/docs/flowgen/concepts/retry). |

## Output

| Format | Crate | Description |
|---|---|---|
| [JSON](https://docs.rs/serde_json/latest/serde_json/enum.Value.html) | element type | One event per element from the iterated array. Each element becomes `event.data` for downstream tasks. |

Each element's meta has `iterate_count`, the number of elements in the array, and `iterate_id`, which is the same for every element of one array. A `buffer` removes both from the batch it sends on.

`http_endpoint` and `mcp_tool` respond with the result of the element that finishes last with one. To respond with all of them, merge them with a `buffer` with `flush_on_completion`.

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

**Answer with every element's result:**

```yaml
- iterate:
    name: split_records
    iterate_key: records

- script:
    name: process
    code: "#{ id: event.data.id, done: true }"

- buffer:
    name: collect
    size: 100
    timeout: 5s
    flush_on_completion: true

- script:
    name: answer
    code: "#{ records: event.data.batch }"
```

# Tasks

Tasks are the building blocks of a flow. Each task performs a specific operation — subscribing to a message stream, transforming data, querying a database, or writing results. All tasks receive events and emit events to the next task in the flow.

## Task categories

Flowgen has two categories of tasks: **subscribers** and **processors**.

### Subscribers

Subscribers are source tasks that ingest data into the flow. They connect to external systems and produce events: `http_endpoint` turns incoming requests into events, `generate` produces them on a schedule, and the subscribe and change-data-capture tasks of the messaging and database connectors read streams and table changes.

Subscribers appear as the first task in a flow. They manage acknowledgment — a message is only acked when the entire downstream flow completes successfully.

### Processors

Processors receive events, do something with them, and emit events to the next task. The core tasks — `script`, `convert`, `iterate`, `buffer` and `log` — transform, convert, split, batch and inspect events. Every other processor works with an external system: it calls an API, queries or writes a database, publishes a message, or reads and writes storage. Each connector's page lists its tasks.

## Task wiring

By default, tasks are wired sequentially — each task receives from the previous task and sends to the next. Linear chains, fan-out, fan-in, and end-to-end acknowledgement semantics are covered in [Flows](/docs/flowgen/concepts/flows).

## Common configuration

All tasks share these optional fields:

```yaml
- script:
    name: my_transform        # required: unique within the flow
    depends_on: [source]       # optional: receive from named tasks instead of previous
    retry:                     # optional: overrides app-level retry
      max_attempts: 5
      initial_backoff: "2s"
    max_in_flight: 4           # optional: events processed at a time (processors)
    code: |
      event.data
```

## Backpressure

Each processor works on at most `max_in_flight` events at a time and picks up the next one only when one finishes. Only one event can wait between two tasks, so when a task is slow, the tasks before it wait too, all the way back to the source. Memory depends on `max_in_flight` and how big each event is (for example the `batch_size` of an `object_store` read), not on how much data comes in.

`max_in_flight` defaults to 64 for tasks that mostly wait on an external service, and to the number of CPUs for tasks that parse, encode or batch data.

Set it to `1` when order matters, for example for updates to the same record: the task then handles one event at a time and passes them on in the order they arrived. Each task has its own limit, so every task that could reorder events needs it. After a fan-out, each branch gets events in the order the parent sent them. Where branches merge, each branch's events stay in order, but events from different branches can arrive in any order.

An event that is being retried keeps its slot until it succeeds or runs out of attempts. When an external service is down, its task fills up with retrying events and the flow stops taking new input until the service is back, instead of piling up events in memory.

`http_endpoint`, `llm_proxy`, and `mcp_tool` count the wait for the flow to take a request as part of `ack_timeout`. If the flow is too busy to take it in time, HTTP clients get `503` and MCP clients a "Server is too busy" error.

## Event data formats

Tasks can produce and consume three event data formats:

- **JSON** — Default for REST APIs, webhooks, and script output.
- **Arrow RecordBatch** — Columnar data, such as query results and Parquet. Zero-copy through the flow.
- **Avro** — Binary format of gRPC and pub/sub streams.

The `convert` task translates between formats when needed.

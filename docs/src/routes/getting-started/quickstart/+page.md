---
description: Run flowgen, add a first flow to the workspace and see it run.
---

# Quick Start

## Run flowgen

After [installing](/docs/flowgen/getting-started/installation) flowgen, run:

```bash
flowgen
```

The first run creates the workspace `~/flowgen`, a git repository, and opens the web UI at `http://127.0.0.1:8080/flowgen/`:

```
~/flowgen/
├── flows/        one flow per YAML file
│   └── system/   the commit, release and deploy flows and Flowgen Agent
└── resources/    scripts, SQL and templates the flows read
```

`flowgen <folder>` or `FLOWGEN_HOME` runs another workspace. `flowgen --help` lists the commands.

## Your first flow

Create `~/flowgen/flows/hello.yaml`:

```yaml
flow:
  tasks:
    - generate:
        name: ticker
        interval: 5s
        payload:
          message: hello from flowgen

    - script:
        name: transform
        code: |
          #{
            original: event.data.message,
            uppercased: event.data.message.to_upper(),
            processed_at: timestamp_to_iso(timestamp_now())
          }

    - log:
        name: output
```

Check it, then commit it:

```bash
cd ~/flowgen
flowgen validate
git add flows/hello.yaml
git commit -m "Add hello flow"
```

Flowgen runs what is committed: within a minute it releases the commit and starts the flow. Every 5 seconds `generate` emits an event, `script` reshapes it and `log` prints it in the terminal where flowgen runs. The flow appears on the Flows page of the web UI with its graph and activity. A file that is not committed does not run. A change made in the web UI runs once it is approved, which commits it.

## Next steps

- [Flows](/docs/flowgen/concepts/flows): how tasks connect.
- [Script (Rhai)](/docs/flowgen/core/script): the scripting API, and `flowgen test` to run a script on sample events.
- [Configuration](/docs/flowgen/concepts/configuration): running with `--config`, as in a cluster, with the full config reference.
- [Kubernetes](/docs/flowgen/getting-started/kubernetes).

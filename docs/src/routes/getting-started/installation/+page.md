---
description: Install Flowgen on macOS or Linux with Homebrew or a one-line script, then run it. One binary starts a local workspace with the web UI.
---

# Installation

## Install

```bash
brew install connve/tap/flowgen
```

or

```bash
curl -fsSL https://connve.com/flowgen/install.sh | sh
```

On Linux, flowgen needs glibc 2.36 or later and `libssl3`.

Then run `flowgen`.

## Commands

| Command | What it does |
|---|---|
| `flowgen [<folder>]` | Runs the workspace in `<folder>`, `FLOWGEN_HOME` or `~/flowgen`, and creates it when it is new. |
| `flowgen --config <file>` | Runs with an app config file, as in a cluster. `CONFIG_PATH` sets it too. |
| `flowgen validate [<folder>] [--json]` | [Checks the flows and resources](/docs/flowgen/concepts/configuration#validating-a-workspace) of a workspace without running them. Exits with 1 when there are issues. |
| `flowgen test <script> --data <samples>` | [Runs a Rhai script](/docs/flowgen/concepts/configuration#testing-a-script) on sample events and prints the events it emits. |
| `flowgen bootstrap [<folder>]` | Commits the system flows of this version to a workspace, after an upgrade. |
| `flowgen --version` | Prints the version. |
| `flowgen --help` | Lists the commands and options. |

## Deployment models

Flowgen runs in two shapes depending on what you need:

- **Single-node / local.** One process, cache in a local redb file, no leader
  election needed. Suits development, edge boxes, small batch jobs,
  and anywhere a single replica is enough. Installation paths below
  (binary, source, Docker) all work in this mode.
- **Multi-node / Kubernetes.** A `Deployment` with multiple replicas
  backed by a distributed cache (e.g. NATS) for the system bucket
  (leader-election leases) and the runtime bucket (per-flow state —
  replay IDs, counters, last-run timestamps). One pod runs each
  leader-elected flow at a time; others stand by and take over on
  failure.

The two modes share the same configuration shape — the only
difference is whether `cache.type` points at a distributed backend
or is omitted (a local redb file). See
[Configuration](https://connve.com/docs/flowgen/concepts/configuration)
for the field reference.

## Flow sources

Flowgen loads flow definitions from one or more of three sources. All
three can be combined in the same deployment; on name collisions the
filesystem source wins.

- **Filesystem.** Set `flows.path` to a directory or glob. Loaded
  once at startup; changes require a restart in single-node, or a
  rollout in Kubernetes. This is the simplest model and the right
  choice when flows live next to the binary or are mounted from a
  ConfigMap. See [Flow discovery](https://connve.com/docs/flowgen/concepts/flows#flow-discovery).
- **Git sync.** The deploy flow, with `git_sync` in place of `oci_sync`,
  pulls flow YAMLs and resource files from a Git repository at a fixed
  interval and writes them to the system cache. Hot-reload picks up
  changes without restarting the worker — push to the branch and
  watcher restarts only the affected flows. Works with any HTTPS Git
  host. See
  [Git Sync](https://connve.com/docs/flowgen/git/sync) and
  [Loading flows from Git](https://connve.com/docs/flowgen/git/guides/loading-flows).
- **OCI artifact sync.** Same hot-reload shape as Git, but the
  workspace is packaged as an OCI artifact and pulled from an OCI
  registry. OCI registries
  hold more than container images — any tarball can be pushed as a
  tagged artifact. If you already operate a registry, you can
  publish workspace bundles to it, version them with tags, and reuse
  the same authentication. Credentials auto-detect both the
  flowgen-native `{username, password}` shape and the standard
  Docker `config.json` payload, so the same `imagePullSecrets`
  Secret authenticates artifact pulls. See
  [OCI Sync](https://connve.com/docs/flowgen/oci/sync) and
  [Loading flows from an OCI registry](https://connve.com/docs/flowgen/oci/guides/loading-flows).

Hot-reload requires a system cache that supports key watches (the
filesystem source does not).

## Pre-built binaries

To install by hand, download the release for your platform from the [releases page](https://github.com/connve/flowgen/releases):

| Platform | Architecture | Archive |
|---|---|---|
| Linux | `x86_64` | `flowgen-linux-amd64-VERSION.tar.gz` |
| Linux | `ARM64` | `flowgen-linux-arm64-VERSION.tar.gz` |
| macOS | `Intel` | `flowgen-darwin-amd64-VERSION.tar.gz` |
| macOS | `Apple Silicon` | `flowgen-darwin-arm64-VERSION.tar.gz` |

```bash
# Example: Linux x86_64, adjust the archive name for your platform
VERSION=$(curl -s https://api.github.com/repos/connve/flowgen/releases/latest | grep '"tag_name"' | cut -d'"' -f4 | sed 's/^v//')
curl -LO "https://github.com/connve/flowgen/releases/download/v${VERSION}/flowgen-linux-amd64-${VERSION}.tar.gz"
tar -xzf "flowgen-linux-amd64-${VERSION}.tar.gz"
sudo mv flowgen /usr/local/bin/
```

## From source

Requires [Rust](https://rustup.rs) 1.97.1+, a C compiler, `protoc` (the Protocol Buffers compiler), and Node.js with npm for the embedded web UI. On Linux, `pkg-config` and the OpenSSL headers are needed as well.

```bash
# macOS
xcode-select --install
brew install protobuf node

# Ubuntu / Debian
sudo apt install -y build-essential pkg-config libssl-dev protobuf-compiler nodejs npm

# Verify
protoc --version
```

Set `FLOWGEN_SKIP_WEB_BUILD=1` to build without Node.js; the binary then serves no web UI.

Then build and install:

```bash
git clone https://github.com/connve/flowgen.git
cd flowgen
cargo install --path flowgen/app
```

This installs the `flowgen` binary to `~/.cargo/bin/`. Make sure it's in your `PATH`.

```bash
flowgen --config config.yaml
```

## Docker

```bash
docker run -v $(pwd)/config.yaml:/etc/app/config.yaml \
           -v $(pwd)/flows:/etc/app/flows \
           ghcr.io/connve/flowgen:latest \
           --config /etc/app/config.yaml
```

## Kubernetes

Add the Helm repository and install:

```bash
helm repo add connve https://helm.connve.com
helm install flowgen connve/flowgen
```

See the [Helm chart values](https://github.com/connve/flowgen/blob/main/charts/flowgen/values.yaml) for configuration options. `flowgen.config.settings` is the app config, with defaults for a cluster: a NATS cache, and flows and resources from the cache. Set the keys to change; each server it enables gets its port and a Service. `flowgen.systemFlows` runs the [lifecycle flows](/docs/flowgen/concepts/authoring#lifecycle-flows), Flowgen Agent and the hubs, with their repository, releases and credentials:

```yaml
flowgen:
  config:
    settings:
      cache:
        url: "nats://nats.nats:4222"
      web:
        enabled: true
        auth: { ... }
        authoring:
          enabled: true
  systemFlows:
    enabled: true
    repository: https://git.example.com/acme/workspace.git
    releases: registry.example.com/acme/workspace
    gitCredentials:
      secretName: workspace-git
    registryCredentials:
      secretName: workspace-registry
```

Credentials added in the web UI need `credentials` in the app config, with the age key the cluster decrypts with as a recipient, and `flowgen.credentials` listing the Secrets to mount. See [Credentials](/docs/flowgen/concepts/credentials#encrypted-credentials-in-the-workspace).

```yaml
flowgen:
  config:
    settings:
      credentials:
        recipients: ["age1…"]
        path: /etc/flowgen/credentials
  credentials: [orders-api]
```

`flowgen.autoscaling.enabled` adds a HorizontalPodAutoscaler, which needs metrics-server. More pods share the flows that run on every pod, such as endpoints and queue subscribers; a flow with `require_leader_election` runs on one pod at a time, and the others stand by.

# Installation

## Deployment models

Flowgen runs in two shapes depending on what you need:

- **Single-node / local.** One process, cache in a local redb file, no leader
  election needed. Suits development, edge boxes, small batch jobs,
  and anywhere a single replica is enough. Installation paths below
  (binary, source, Docker) all work in this mode out of the box.
- **Multi-node / Kubernetes.** A `Deployment` with multiple replicas
  backed by a distributed cache (e.g. NATS) for the system bucket
  (leader-election leases) and the runtime bucket (per-flow state —
  replay IDs, counters, last-run timestamps). One pod runs each
  leader-elected flow at a time; others stand by and take over on
  failure.

The two modes share the same configuration shape — the only
difference is whether `cache.type` points at a distributed backend
or is omitted (single-node in-memory). See
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
- **Git sync.** A bootstrap flow (`sync_workspace`) pulls
  flow YAMLs and resource files from a Git repository at a fixed
  interval and writes them to the system cache. Hot-reload picks up
  changes without restarting the worker — push to the branch and
  watcher restarts only the affected flows. Works with any HTTPS Git
  host (GitHub, GitLab, Bitbucket, Gitea, self-hosted). See
  [Git Sync](https://connve.com/docs/flowgen/git/sync) and
  [Loading flows from Git](https://connve.com/docs/flowgen/git/guides/loading-flows).
- **OCI artifact sync.** Same hot-reload shape as Git, but the
  workspace is packaged as an OCI artifact and pulled from an OCI
  registry (GHCR, ECR, GAR, Artifactory, Harbor). OCI registries
  hold more than container images — any tarball can be pushed as a
  tagged artifact. If you already operate a registry, you can
  publish workspace bundles to it, version them with tags, and reuse
  the same authentication. Credentials auto-detect both the
  flowgen-native `{username, password}` shape and the standard
  Docker `config.json` payload, so the same `imagePullSecrets`
  Secret authenticates artifact pulls. See
  [OCI Sync](https://connve.com/docs/flowgen/oci/sync) and
  [Loading flows from OCI](https://connve.com/docs/flowgen/oci/guides/loading-flows).

Hot-reload requires a system cache that supports key watches (the
filesystem source does not). In single-node mode the filesystem path
is the recommended source.

The Git and OCI bootstraps ship paired examples for syncing
[resources](/docs/flowgen/concepts/resources) (SQL queries, JSON
schemas, Rhai scripts, etc.) on the same mechanism but under a
different cache prefix, so flow YAML and resource changes can ship in
the same repository or registry but redeploy independently.

## Pre-built binaries

Download the latest release for your platform from [GitHub Releases](https://github.com/connve/flowgen/releases):

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
    repository: https://github.com/acme/workspace.git
    releases: ghcr.io/acme/workspace
    gitCredentials:
      secretName: workspace-git
    registryCredentials:
      secretName: workspace-registry
```

## Verify

```bash
flowgen --version
flowgen --help
```

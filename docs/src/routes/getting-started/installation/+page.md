# Installation

## Deployment models

Flowgen runs in two shapes depending on what you need:

- **Single-node / local.** One process, in-memory cache, no leader
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
- **Git sync.** A bootstrap flow (`system_sync_flows`) pulls flow
  YAMLs from a Git repository at a fixed interval and writes them to
  the system cache. Hot-reload picks up changes without restarting
  the worker — push to the branch and watcher restarts only the
  affected flows. Works with any HTTPS Git host (GitHub, GitLab,
  Bitbucket, Gitea, self-hosted). See
  [Git Sync](https://connve.com/docs/flowgen/git/sync) and the
  [`examples/git/system_sync_flows.yaml`](https://github.com/connve/flowgen/blob/main/examples/git/system_sync_flows.yaml)
  bootstrap.
- **OCI artifact sync.** Same hot-reload shape as Git, but flows are
  packaged as an OCI artifact and pulled from an OCI registry
  (GHCR, ECR, GAR, Artifactory, Harbor). OCI registries hold more
  than container images — any tarball can be pushed as a tagged
  artifact. If you already operate a registry, you can publish flow
  bundles to it, version them with tags, and reuse the same
  authentication. Credentials auto-detect both the flowgen-native
  `{username, password}` shape and the standard Docker `config.json`
  payload, so the same `imagePullSecrets` Secret authenticates
  artifact pulls. See [OCI Sync](https://connve.com/docs/flowgen/oci/sync)
  and the [`examples/oci/system_sync_flows.yaml`](https://github.com/connve/flowgen/blob/main/examples/oci/system_sync_flows.yaml)
  bootstrap.

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
# Example: Linux x86_64, replace version as needed
VERSION=0.121.0
curl -LO "https://github.com/connve/flowgen/releases/download/v${VERSION}/flowgen-linux-amd64-${VERSION}.tar.gz"
tar -xzf "flowgen-linux-amd64-${VERSION}.tar.gz"
sudo mv flowgen /usr/local/bin/
```

## From source

Requires [Rust](https://rustup.rs) 1.88+ and `protoc` (the Protocol Buffers compiler).

```bash
# macOS
brew install protobuf

# Ubuntu / Debian
sudo apt install -y protobuf-compiler

# Verify
protoc --version
```

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

See the [Helm chart values](https://github.com/connve/flowgen/blob/main/charts/flowgen/values.yaml) for configuration options.

## Verify

```bash
flowgen --version
flowgen --help
```

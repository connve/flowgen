# Flowgen

[![CI](https://github.com/connve/flowgen/actions/workflows/test.yml/badge.svg)](https://github.com/connve/flowgen/actions/workflows/test.yml) [![License: MPL 2.0](https://img.shields.io/badge/License-MPL%202.0-brightgreen.svg)](https://opensource.org/licenses/MPL-2.0) [![GitHub release](https://img.shields.io/github/v/release/connve/flowgen)](https://github.com/connve/flowgen/releases)

Flowgen is an open-source composable data activation platform. Turn your data into the next best action!

## Overview

Flowgen doesn't own your data or replace your stack. It sits between the systems you already have, your warehouse, your CRM, your marketing tools, and you compose flows from small tasks: read, transform, call, write. A flow can run on a schedule, react to events, serve an HTTP endpoint or become a tool for your agents, and one flow can call another.

One binary, no database, with a web UI, an MCP server and an AI gateway. See [Why Flowgen](https://connve.com/docs/flowgen/getting-started/why-flowgen) and the [docs](https://connve.com/docs/flowgen).

## Install

```bash
brew install connve/tap/flowgen
```

or

```bash
curl -fsSL https://connve.com/flowgen/install.sh | sh
```

Then run `flowgen`. The first run creates a workspace in `~/flowgen` and opens the web UI at `http://127.0.0.1:8080/flowgen/`.

## Deployment

### Kubernetes

Install using Helm:

```bash
helm repo add connve https://helm.connve.com
helm repo update
helm install flowgen connve/flowgen --namespace flowgen --create-namespace
```

Chart configuration options are documented in [charts/flowgen/values.yaml](charts/flowgen/values.yaml).

### Docker

```bash
docker pull ghcr.io/connve/flowgen:latest

docker run \
  -v $(pwd)/config.yaml:/etc/app/config.yaml \
  -v $(pwd)/flows:/etc/app/flows \
  ghcr.io/connve/flowgen:latest --config /etc/app/config.yaml
```

### Local Development

You need:

- Rust via [`rustup`](https://rustup.rs) (`rust-toolchain.toml` picks the version)
- a C compiler (macOS: `xcode-select --install`; Debian/Ubuntu: `build-essential`)
- on Linux, `pkg-config` and the OpenSSL headers (Debian/Ubuntu: `libssl-dev`)
- [`mise`](https://mise.jdx.dev): `curl https://mise.run | sh`, then add `eval "$(mise activate zsh)"` to your shell profile
- Docker, only for the integration tests

Then run:

```bash
mise install      # node, protoc, helm, kind, kubectl and cargo-audit, pinned in mise.toml
mise run setup    # pre-commit hook and the MSRV toolchain
```

`mise run msrv` checks that the workspace builds on the minimum supported Rust
version. To build without the web UI, set `FLOWGEN_SKIP_WEB_BUILD=1`.

1. Copy `config.example.yaml` to `config.yaml` and configure as needed
2. Create flow definitions in the flows directory
3. Update the flow path in `config.yaml`
4. Run with `cargo run`

The pre-commit hook ([`.githooks/pre-commit`](.githooks/pre-commit)) runs
fmt, clippy, the tests and `cargo audit`, so everyone runs the same checks.

To release, change `version` under `[workspace.package]` in `Cargo.toml`. The
hook copies it into `web/` and `docs/` (`package.json` and
`package-lock.json`) and the API spec's `info.version`, and a test fails if
they drift apart.

### Testing

```bash
cargo test                        # unit tests
cargo test -- --ignored           # integration tests (requires Docker)
cargo test -- --include-ignored   # both
```

Tests that spin up real containers (via
[`testcontainers`](https://docs.rs/testcontainers)) or depend on a
running Docker daemon are marked `#[ignore]` so a plain
`cargo test` stays fast and works on machines without Docker. CI
runs the ignored set in a dedicated `test-integration` job.

### Kubernetes

```bash
mise run kind:up      # a kind cluster, flowgen-dev, with an empty workspace repository
mise run kind:deploy  # builds the image, installs NATS and the chart with dev/kind/ values
mise run kind:ui      # the web UI at http://127.0.0.1:18080/flowgen/
mise run kind:down    # deletes the cluster
```

The cluster runs three pods with the system flows of the image. The
workspace repository and its releases sit on the node, so it needs no git
server, registry or credentials.

## Configuration

- `config.yaml`: Application configuration
- Flow files: YAML files defining workflow tasks

See `config.example.yaml` for configuration reference.

## Examples

Example flow configurations are available in the [`examples/`](examples/) directory.

## License

Licensed under the [Mozilla Public License 2.0](LICENSE).



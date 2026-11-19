---
description: Run flowgen on Kubernetes with a chart or manifests of your own, with the system flows that commit, release and deploy changes.
---

# Kubernetes

The [Helm chart](https://github.com/connve/flowgen/tree/main/charts/flowgen) sets up all of the below. With a chart or manifests of your own, a flowgen Deployment needs the following.

## Config

The config is a ConfigMap with a `config.yaml` key, mounted as a file and passed with `--config`, as in `/bin/flowgen --config /etc/app/config.yaml`:

```yaml
cache:
  type: nats
  url: "nats://nats:4222"
  credentials_path: /etc/nats/credentials.json
flows:
  path: /etc/app/flows
  system: true
  cache:
    enabled: true
resources:
  path: /etc/app/resources
  cache:
    enabled: true
health:
  enabled: true
web:
  enabled: true
  auth: { ... }
  authoring:
    enabled: true
```

`flows.system` runs the system flows of the running version: commit, release and deploy, Flowgen Agent and the hubs. `flows.cache` and `resources.cache` load what the deploy flow writes. A commit carries who approved it, so `web.auth` is required. Flowgen Agent also needs `mcp_server`, `ai_gateway` and `web.agents`.

Keep secrets out of the ConfigMap: set them from a Secret with `APP_` variables, such as `APP_WEB__AUTH__CLIENT_SECRET` for `web.auth.client_secret`. See [Configuration](/docs/flowgen/concepts/configuration).

## Environment

| Variable | Value |
|---|---|
| `FLOWGEN_REPOSITORY` | The workspace repository's URL. |
| `FLOWGEN_WORKSPACE_PATH` | The workspace folder in it, ending in `/`, or empty. |
| `FLOWGEN_RELEASES` | The registry repository releases go to, e.g. `registry.example.com/acme/workspace`. |
| `FLOWGEN_GIT_CREDENTIALS` | The mounted [git credentials](/docs/flowgen/git#credentials) file. |
| `FLOWGEN_REGISTRY_CREDENTIALS` | The mounted [registry credentials](/docs/flowgen/oci#credentials) file. |
| `FLOWGEN_CREDENTIALS` | The folder the connections' [credentials](/docs/flowgen/concepts/credentials) are mounted in. |
| `POD_NAME` | `metadata.name`, which names the pod's leases. |
| `POD_IP` | `status.podIP`, where the other pods read its logs and counters. |

The first five are the [lifecycle flows'](/docs/flowgen/concepts/authoring#lifecycle-flows) variables.

## Pod

- A writable `/tmp`, such as an `emptyDir`, when the root filesystem is read-only: `git_sync` reads a repository by its URL into a temporary folder, which the release flow does on the pod that leads it.
- Probes on the health port, `8081` by default: `/livez` for startup and liveness, `/readyz` for readiness.
- Port `8082` open between the flowgen pods.
- `terminationGracePeriodSeconds` longer than `shutdown_timeout` plus 5 seconds, so flows stop before the pod is killed. The chart uses 65.

The image carries a build provenance attestation from the workflow and commit that built it, so an admission policy can admit only images built by `connve/flowgen`.

## When CI releases the workspace

If the repository's CI pushes the releases to your registry, leave the release flow out and let flowgen only deploy them:

```yaml
flows:
  system: true
  system_exclude: ["system/release_workspace"]
```

The deploy flow then needs only to read the registry, and `FLOWGEN_REGISTRY_CREDENTIALS` can be empty when it reads without a login. With the Helm chart, set `systemFlows.release: false`.

## From system flows in your chart

If your chart carries its own commit, release or sync flows, remove them, set `flows.system: true` and set the variables above. The system flows then always match the running version, with no `flowgen bootstrap` in the cluster.

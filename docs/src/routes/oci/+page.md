# OCI

Flowgen can pull flow definitions and resources from an OCI artifact registry (GHCR, ECR, GAR, Harbor, Artifactory, Docker Hub).

- [OCI Sync](/docs/flowgen/oci/sync) — pulls an artifact from a registry and emits one event per layer.

## Why OCI

OCI registries store any artifact, not just container images. Publishing flow yamls as an OCI artifact gives you immutable digests, atomic deploys (one tag flip = one artifact), tag-based environment routing (`prod`, `staging`, `v1.0.0`), and pull-through caches. The pod auth path is the same `ImagePullSecrets` mechanism that already pulls your container images.

## Credentials

OCI tasks authenticate via a JSON credentials file referenced by `credentials_path`. Two formats are auto-detected:

**Flowgen-native** — minimal, one registry:

```json
{
  "username": "robot",
  "password": "ghp_xxxxxxxxxxxx"
}
```

**Docker `config.json`** — the standard `kubernetes.io/dockerconfigjson` Secret payload, supports multiple registries; the entry whose host matches the artifact's registry is picked automatically:

```json
{
  "auths": {
    "ghcr.io": { "auth": "<base64 of user:pass>" },
    "registry.gitlab.com": { "username": "alice", "password": "secret" }
  }
}
```

Mounting the same Secret used as the pod's `imagePullSecrets` and pointing `credentials_path` at its `config.json` lets one Secret cover both image pulls and OCI sync.

If `credentials_path` is omitted, anonymous auth is used — fine for public artifacts.

See [OCI Sync](/docs/flowgen/oci/sync) for the field reference.

//! OCI registry processing capabilities for flowgen workers.
//!
//! Provides the `oci_sync` task: a pull-based loader for flow artifacts
//! published to OCI registries (GHCR, ECR, GAR, Artifactory, Harbor, etc.).
//! Each tick probes the artifact's manifest digest; on a change, the layers
//! are pulled and emitted as one event per file, mirroring the shape of
//! `git_sync` so bootstrap pipelines can swap one for the other.

/// OCI registry artifact sync processor.
pub mod sync;

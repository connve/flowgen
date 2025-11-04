//! Kubernetes implementation of host coordination.
//!
//! Provides Kubernetes-based lease management using the coordination.k8s.io API.

use crate::client::Client as FlowgenClient;
use crate::host::Host;
use async_trait::async_trait;
use k8s_openapi::api::coordination::v1::{Lease, LeaseSpec};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{MicroTime, ObjectMeta};
use kube::{
    api::{Api, DeleteParams, Patch, PatchParams, PostParams},
    Client,
};
use std::sync::Arc;
use tracing::{debug, info};

/// Errors specific to Kubernetes host operations.
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum Error {
    #[error("Connection to Kubernetes API failed with error: {source}")]
    KubernetesClient {
        #[source]
        source: kube::Error,
    },
    #[error("Reading namespace from service account failed with error: {source}")]
    NamespaceRead {
        #[source]
        source: std::io::Error,
    },
    #[error("Kubernetes Client is not connect / setup properly")]
    KubernetesClientNotConnected,
    #[error("Lease creation failed with error: {source}")]
    CreateLease {
        #[source]
        source: kube::Error,
    },
    #[error("Lease get failed with error: {source}")]
    GetLease {
        #[source]
        source: kube::Error,
    },
    #[error("Lease renewal failed with error: {source}")]
    RenewLease {
        #[source]
        source: kube::Error,
    },
    #[error("Lease takeover failed with error: {source}")]
    TakeoverLease {
        #[source]
        source: kube::Error,
    },
    #[error("Lease deletion failed with error: {source}")]
    DeleteLease {
        #[source]
        source: kube::Error,
    },
    #[error("Lease pathing failed with error: {source}")]
    PatchLease {
        #[source]
        source: kube::Error,
    },
    #[error("Existing lease has no spec")]
    MissingLeaseSpec,
    #[error("Existing lease has no holder identity")]
    MissingHolderIdentity,
    #[error("Lease {name} is held by another instance: {holder}")]
    LeaseHeldByOther { name: String, holder: String },
    #[error("Missing required attribute: {0}")]
    MissingRequiredAttribute(String),
}

/// Default lease duration in seconds.
const DEFAULT_LEASE_DURATION_SECS: i32 = 60;

/// Kubernetes host coordinator for lease management.
#[derive(Clone)]
pub struct K8sHost {
    /// Kubernetes client.
    client: Option<Arc<Client>>,
    /// Namespace for Kubernetes resources (auto-detected from service account).
    namespace: String,
    /// Lease duration in seconds.
    lease_duration_secs: i32,
    /// Holder identity (typically pod name).
    holder_identity: String,
}

impl std::fmt::Debug for K8sHost {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("K8sHost")
            .field("client", &self.client.as_ref().map(|_| "Client"))
            .field("namespace", &self.namespace)
            .field("lease_duration_secs", &self.lease_duration_secs)
            .field("holder_identity", &self.holder_identity)
            .finish()
    }
}

impl FlowgenClient for K8sHost {
    type Error = Error;

    #[tracing::instrument(skip(self), name = "k8s.connect")]
    async fn connect(mut self) -> Result<Self, Self::Error> {
        let client = Client::try_default()
            .await
            .map_err(|source| Error::KubernetesClient { source })?;

        // Auto-detect namespace from pod's service account.
        let namespace =
            tokio::fs::read_to_string("/var/run/secrets/kubernetes.io/serviceaccount/namespace")
                .await
                .map_err(|source| Error::NamespaceRead { source })?
                .trim()
                .to_string();

        self.namespace = namespace.clone();
        self.client = Some(Arc::new(client));
        info!(
            "Successfully connected to Kubernetes cluster in namespace: {}",
            namespace
        );
        Ok(self)
    }
}

#[async_trait]
impl Host for K8sHost {
    #[tracing::instrument(skip(self), name = "k8s.create_lease", fields(lease_name = %name))]
    async fn create_lease(&self, name: &str) -> Result<(), crate::host::Error> {
        let namespace = &self.namespace;
        let client = self
            .client
            .as_ref()
            .ok_or_else(|| Box::new(Error::KubernetesClientNotConnected) as crate::host::Error)?;

        let api: Api<Lease> = Api::namespaced((**client).clone(), namespace);

        let now = MicroTime(chrono::Utc::now());
        let lease = Lease {
            metadata: ObjectMeta {
                name: Some(name.to_string()),
                namespace: Some(namespace.to_string()),
                ..Default::default()
            },
            spec: Some(LeaseSpec {
                holder_identity: Some(self.holder_identity.clone()),
                lease_duration_seconds: Some(self.lease_duration_secs),
                acquire_time: Some(now.clone()),
                renew_time: Some(now),
                ..Default::default()
            }),
        };

        match api.create(&PostParams::default(), &lease).await {
            Ok(_) => {
                info!("Created lease: {} in namespace: {}", name, namespace);
                Ok(())
            }
            Err(kube::Error::Api(api_err)) if api_err.code == 409 => {
                debug!(
                    "Lease already exists: {} in namespace: {}, checking holder",
                    name, namespace
                );

                // Lease already exists, check if we're the holder.
                let existing_lease = api
                    .get(name)
                    .await
                    .map_err(|source| Box::new(Error::GetLease { source }) as crate::host::Error)?;

                let spec = existing_lease
                    .spec
                    .as_ref()
                    .ok_or_else(|| Box::new(Error::MissingLeaseSpec) as crate::host::Error)?;

                let existing_holder = spec
                    .holder_identity
                    .as_ref()
                    .ok_or_else(|| Box::new(Error::MissingHolderIdentity) as crate::host::Error)?;

                // Check if lease is expired.
                let is_expired = if let (Some(renew_time), Some(duration)) =
                    (spec.renew_time.as_ref(), spec.lease_duration_seconds)
                {
                    let elapsed = chrono::Utc::now()
                        .signed_duration_since(renew_time.0)
                        .num_seconds();
                    elapsed > duration as i64
                } else {
                    false
                };

                if existing_holder == &self.holder_identity {
                    // We're the holder, renew the lease.
                    debug!(
                        "We hold the lease: {} in namespace: {}, renewing",
                        name, namespace
                    );

                    let renew_patch = serde_json::json!({
                        "spec": {
                            "renewTime": MicroTime(chrono::Utc::now()),
                        }
                    });

                    api.patch(name, &PatchParams::default(), &Patch::Merge(renew_patch))
                        .await
                        .map_err(|source| {
                            Box::new(Error::RenewLease { source }) as crate::host::Error
                        })?;

                    debug!("Renewed lease: {} in namespace: {}", name, namespace);
                    Ok(())
                } else if is_expired {
                    // Lease is expired, take it over.
                    debug!(
                        "Lease {} in namespace {} is expired, taking over from {}",
                        name, namespace, existing_holder
                    );

                    let takeover_patch = serde_json::json!({
                        "spec": {
                            "holderIdentity": self.holder_identity,
                            "acquireTime": MicroTime(chrono::Utc::now()),
                            "renewTime": MicroTime(chrono::Utc::now()),
                        }
                    });

                    api.patch(name, &PatchParams::default(), &Patch::Merge(takeover_patch))
                        .await
                        .map_err(|source| {
                            Box::new(Error::TakeoverLease { source }) as crate::host::Error
                        })?;

                    info!(
                        "Took over expired lease: {} in namespace: {}",
                        name, namespace
                    );
                    Ok(())
                } else {
                    // Someone else holds the lease and it's not expired.
                    Err(Box::new(Error::LeaseHeldByOther {
                        name: name.to_string(),
                        holder: existing_holder.clone(),
                    }) as crate::host::Error)
                }
            }
            Err(source) => Err(Box::new(Error::CreateLease { source }) as crate::host::Error),
        }
    }

    #[tracing::instrument(skip(self), name = "k8s.delete_lease", fields(lease_name = %name, namespace = ?namespace))]
    async fn delete_lease(
        &self,
        name: &str,
        namespace: Option<&str>,
    ) -> Result<(), crate::host::Error> {
        let namespace = namespace.unwrap_or(&self.namespace);
        let client = self
            .client
            .as_ref()
            .ok_or_else(|| Box::new(Error::KubernetesClientNotConnected) as crate::host::Error)?;
        let api: Api<Lease> = Api::namespaced((**client).clone(), namespace);

        api.delete(name, &DeleteParams::default())
            .await
            .map_err(|source| Box::new(Error::DeleteLease { source }) as crate::host::Error)?;

        info!("Deleted lease: {} in namespace: {}", name, namespace);
        Ok(())
    }

    #[tracing::instrument(skip(self), name = "k8s.renew_lease", fields(lease_name = %name))]
    async fn renew_lease(
        &self,
        name: &str,
        namespace: Option<&str>,
    ) -> Result<(), crate::host::Error> {
        let namespace = namespace.unwrap_or(&self.namespace);
        let client = self
            .client
            .as_ref()
            .ok_or_else(|| Box::new(Error::KubernetesClientNotConnected) as crate::host::Error)?;
        let api: Api<Lease> = Api::namespaced((**client).clone(), namespace);

        let patch = serde_json::json!({
            "spec": {
                "renewTime": MicroTime(chrono::Utc::now()),
            }
        });

        api.patch(name, &PatchParams::default(), &Patch::Merge(patch))
            .await
            .map_err(|source| Box::new(Error::PatchLease { source }) as crate::host::Error)?;

        debug!("Renewed lease: {} in namespace: {}", name, namespace);
        Ok(())
    }
}

/// Builder for K8sHost.
pub struct K8sHostBuilder {
    lease_duration_secs: i32,
    holder_identity: Option<String>,
}

impl Default for K8sHostBuilder {
    fn default() -> Self {
        Self {
            lease_duration_secs: DEFAULT_LEASE_DURATION_SECS,
            holder_identity: None,
        }
    }
}

impl K8sHostBuilder {
    /// Creates a new K8sHostBuilder.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the lease duration in seconds.
    pub fn lease_duration_secs(mut self, duration: i32) -> Self {
        self.lease_duration_secs = duration;
        self
    }

    /// Sets the holder identity (typically pod name).
    pub fn holder_identity(mut self, identity: String) -> Self {
        self.holder_identity = Some(identity);
        self
    }

    /// Builds the K8sHost instance without connecting.
    /// Namespace will be auto-detected from service account during connect().
    pub fn build(self) -> Result<K8sHost, Error> {
        Ok(K8sHost {
            client: None,
            namespace: String::new(), // Will be set during connect()
            lease_duration_secs: self.lease_duration_secs,
            holder_identity: self
                .holder_identity
                .ok_or_else(|| Error::MissingRequiredAttribute("holder_identity".to_string()))?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::host::Host;

    fn create_test_host(holder: &str) -> K8sHost {
        K8sHost {
            client: None,
            namespace: "test-namespace".to_string(),
            lease_duration_secs: DEFAULT_LEASE_DURATION_SECS,
            holder_identity: holder.to_string(),
        }
    }

    #[tokio::test]
    async fn test_create_lease_without_client_fails() {
        let host = create_test_host("test-pod");
        let result = host.create_lease("test-lease").await;

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.downcast_ref::<Error>().is_some());
        assert!(matches!(
            err.downcast_ref::<Error>().unwrap(),
            Error::KubernetesClientNotConnected
        ));
    }

    #[tokio::test]
    async fn test_delete_lease_without_client_fails() {
        let host = create_test_host("test-pod");
        let result = host.delete_lease("test-lease", None).await;

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(
            err.downcast_ref::<Error>().unwrap(),
            Error::KubernetesClientNotConnected
        ));
    }

    #[tokio::test]
    async fn test_delete_lease_uses_default_namespace() {
        let host = create_test_host("test-pod");
        let result = host.delete_lease("test-lease", None).await;

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(
            err.downcast_ref::<Error>().unwrap(),
            Error::KubernetesClientNotConnected
        ));
    }

    #[tokio::test]
    async fn test_delete_lease_uses_custom_namespace() {
        let host = create_test_host("test-pod");
        let result = host.delete_lease("test-lease", Some("custom-ns")).await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_renew_lease_without_client_fails() {
        let host = create_test_host("test-pod");
        let result = host.renew_lease("test-lease", None).await;

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(
            err.downcast_ref::<Error>().unwrap(),
            Error::KubernetesClientNotConnected
        ));
    }

    #[tokio::test]
    async fn test_renew_lease_uses_default_namespace() {
        let host = create_test_host("test-pod");
        let result = host.renew_lease("test-lease", None).await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_renew_lease_uses_custom_namespace() {
        let host = create_test_host("test-pod");
        let result = host.renew_lease("test-lease", Some("custom-ns")).await;

        assert!(result.is_err());
    }

    #[test]
    fn test_builder_default() {
        let builder = K8sHostBuilder::default();
        assert_eq!(builder.lease_duration_secs, DEFAULT_LEASE_DURATION_SECS);
        assert!(builder.holder_identity.is_none());
    }

    #[test]
    fn test_builder_new() {
        let builder = K8sHostBuilder::new();
        assert_eq!(builder.lease_duration_secs, DEFAULT_LEASE_DURATION_SECS);
        assert!(builder.holder_identity.is_none());
    }

    #[test]
    fn test_builder_set_lease_duration() {
        let builder = K8sHostBuilder::new().lease_duration_secs(120);
        assert_eq!(builder.lease_duration_secs, 120);
    }

    #[test]
    fn test_builder_set_holder_identity() {
        let builder = K8sHostBuilder::new().holder_identity("test-pod".to_string());
        assert_eq!(builder.holder_identity, Some("test-pod".to_string()));
    }

    #[test]
    fn test_builder_method_chaining() {
        let builder = K8sHostBuilder::new()
            .lease_duration_secs(90)
            .holder_identity("chain-pod".to_string());

        assert_eq!(builder.lease_duration_secs, 90);
        assert_eq!(builder.holder_identity, Some("chain-pod".to_string()));
    }

    #[test]
    fn test_builder_missing_holder_identity() {
        let result = K8sHostBuilder::new().build();

        assert!(result.is_err());
        match result.unwrap_err() {
            Error::MissingRequiredAttribute(attr) => {
                assert_eq!(attr, "holder_identity");
            }
            _ => panic!("Expected MissingRequiredAttribute error"),
        }
    }

    #[test]
    fn test_builder_build_success() {
        let result = K8sHostBuilder::new()
            .holder_identity("my-pod".to_string())
            .lease_duration_secs(120)
            .build();

        assert!(result.is_ok());
        let host = result.unwrap();
        assert_eq!(host.holder_identity, "my-pod");
        assert_eq!(host.lease_duration_secs, 120);
        assert_eq!(host.namespace, "");
        assert!(host.client.is_none());
    }

    #[test]
    fn test_builder_build_with_default_duration() {
        let result = K8sHostBuilder::new()
            .holder_identity("default-pod".to_string())
            .build();

        assert!(result.is_ok());
        let host = result.unwrap();
        assert_eq!(host.lease_duration_secs, DEFAULT_LEASE_DURATION_SECS);
    }

    #[test]
    fn test_k8s_host_clone() {
        let host = create_test_host("clone-pod");
        let cloned = host.clone();

        assert_eq!(host.holder_identity, cloned.holder_identity);
        assert_eq!(host.namespace, cloned.namespace);
        assert_eq!(host.lease_duration_secs, cloned.lease_duration_secs);
    }
}

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
    #[error("Kubernetes API connection error: {source}")]
    KubernetesClient {
        #[source]
        source: kube::Error,
    },
    #[error("Error reading namespace from service account: {source}")]
    NamespaceRead {
        #[source]
        source: std::io::Error,
    },
    #[error("Kubernetes client is not connected")]
    KubernetesClientNotConnected,
    #[error("Lease creation error: {source}")]
    CreateLease {
        #[source]
        source: kube::Error,
    },
    #[error("Lease get error: {source}")]
    GetLease {
        #[source]
        source: kube::Error,
    },
    #[error("Lease renewal error: {source}")]
    RenewLease {
        #[source]
        source: kube::Error,
    },
    #[error("Lease takeover error: {source}")]
    TakeoverLease {
        #[source]
        source: kube::Error,
    },
    #[error("Lease deletion error: {source}")]
    DeleteLease {
        #[source]
        source: kube::Error,
    },
    #[error("Lease patch error: {source}")]
    PatchLease {
        #[source]
        source: kube::Error,
    },
    #[error("Existing lease has no spec")]
    MissingLeaseSpec,
    #[error("Existing lease has no holder identity")]
    MissingHolderIdentity,
    #[error("Existing lease has no resource version")]
    MissingResourceVersion,
    #[error("Lease {name} is held by another instance: {holder}")]
    LeaseHeldByOther { name: String, holder: String },
    #[error("Failed to acquire lease {name}: lost race condition to another pod during takeover attempt")]
    LeaseTakeoverRaceCondition { name: String },
    #[error("Missing required attribute: {0}")]
    MissingBuilderAttribute(String),
    #[error("Not running in Kubernetes cluster (service account token not found at /var/run/secrets/kubernetes.io/serviceaccount/)")]
    NotInKubernetesCluster,
    #[error(
        "Could not detect pod identity: POD_NAME and HOSTNAME environment variables are not set"
    )]
    MissingPodIdentity,
}

/// Default lease duration in seconds.
/// Set to 60s to handle network hiccups while still allowing reasonable failover time.
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
        // Check if running in Kubernetes by detecting service account token
        let sa_token_path = "/var/run/secrets/kubernetes.io/serviceaccount/token";
        if !tokio::fs::try_exists(sa_token_path).await.unwrap_or(false) {
            return Err(Error::NotInKubernetesCluster);
        }

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
            namespace = %namespace,
            holder = %self.holder_identity,
            lease_duration_secs = self.lease_duration_secs,
            "K8s host coordinator connected"
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
                info!("Pod {} acquired lease {}", self.holder_identity, name);
                Ok(())
            }
            Err(kube::Error::Api(api_err)) if api_err.code == 409 => {
                debug!("Lease {} already exists, checking holder", name);

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
                    debug!("Pod {} renewing lease {}", self.holder_identity, name);

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

                    debug!("Pod {} renewed lease {}", self.holder_identity, name);
                    Ok(())
                } else if is_expired {
                    debug!(
                        "Pod {} attempting to take over expired lease {} from {}",
                        self.holder_identity, name, existing_holder
                    );

                    if existing_lease.metadata.resource_version.is_none() {
                        return Err(Box::new(Error::MissingResourceVersion) as crate::host::Error);
                    }

                    let mut updated_lease = existing_lease.clone();
                    if let Some(ref mut spec) = updated_lease.spec {
                        let now = MicroTime(chrono::Utc::now());
                        spec.holder_identity = Some(self.holder_identity.clone());
                        spec.acquire_time = Some(now.clone());
                        spec.renew_time = Some(now);
                    }

                    match api
                        .replace(name, &PostParams::default(), &updated_lease)
                        .await
                    {
                        Ok(_) => {
                            info!(
                                "Pod {} took over expired lease {} from {}",
                                self.holder_identity, name, existing_holder
                            );
                            Ok(())
                        }
                        Err(kube::Error::Api(api_err)) if api_err.code == 409 => {
                            debug!(
                                "Pod {} lost takeover race for lease {} to another pod",
                                self.holder_identity, name
                            );
                            Err(Box::new(Error::LeaseTakeoverRaceCondition {
                                name: name.to_string(),
                            }) as crate::host::Error)
                        }
                        Err(source) => {
                            Err(Box::new(Error::TakeoverLease { source }) as crate::host::Error)
                        }
                    }
                } else {
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

        info!("Pod {} deleted lease {}", self.holder_identity, name);
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

        debug!("Pod {} renewed lease {}", self.holder_identity, name);
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
    /// Holder identity will be auto-detected from POD_NAME or HOSTNAME environment variables.
    pub fn build(self) -> Result<K8sHost, Error> {
        // Auto-detect holder_identity if not provided
        let holder_identity = match self.holder_identity {
            Some(id) => id,
            None => {
                // Try POD_NAME (K8s standard), then HOSTNAME
                std::env::var("POD_NAME")
                    .or_else(|_| std::env::var("HOSTNAME"))
                    .map_err(|_| Error::MissingPodIdentity)?
            }
        };

        tracing::debug!(
            holder_identity = %holder_identity,
            "K8s host coordinator initialized"
        );

        Ok(K8sHost {
            client: None,
            namespace: String::new(), // Will be set during connect()
            lease_duration_secs: self.lease_duration_secs,
            holder_identity,
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
        // Temporarily unset POD_NAME and HOSTNAME to test error case
        let _pod_name = std::env::var("POD_NAME").ok();
        let _hostname = std::env::var("HOSTNAME").ok();
        std::env::remove_var("POD_NAME");
        std::env::remove_var("HOSTNAME");

        let result = K8sHostBuilder::new().build();

        assert!(matches!(result, Err(Error::MissingPodIdentity)));

        // Restore environment variables if they existed
        if let Some(pod_name) = _pod_name {
            std::env::set_var("POD_NAME", pod_name);
        }
        if let Some(hostname) = _hostname {
            std::env::set_var("HOSTNAME", hostname);
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

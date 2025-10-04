//! Kubernetes implementation of host coordination.
//!
//! Provides Kubernetes-based lease management using the coordination.k8s.io API.

use crate::client::Client as FlowgenClient;
use crate::host::{Error, Host};
use async_trait::async_trait;
use k8s_openapi::api::coordination::v1::{Lease, LeaseSpec};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{MicroTime, ObjectMeta};
use kube::{
    api::{Api, DeleteParams, Patch, PatchParams, PostParams},
    Client,
};
use std::sync::Arc;
use tracing::{debug, info};

/// Default namespace for Kubernetes resources.
const DEFAULT_NAMESPACE: &str = "default";

/// Default lease duration in seconds.
const DEFAULT_LEASE_DURATION_SECS: i32 = 60;

/// Kubernetes host coordinator for lease management.
#[derive(Clone)]
pub struct K8sHost {
    /// Kubernetes client.
    client: Option<Arc<Client>>,
    /// Namespace for Kubernetes resources.
    namespace: String,
    /// Lease duration in seconds.
    lease_duration_secs: i32,
    /// Holder identity (typically pod name).
    holder_identity: String,
}

impl FlowgenClient for K8sHost {
    type Error = Error;

    async fn connect(mut self) -> Result<Self, Self::Error> {
        let client = Client::try_default()
            .await
            .map_err(|e| Error::Connection(e.to_string()))?;
        self.client = Some(Arc::new(client));
        info!("Successfully connected to Kubernetes cluster");
        Ok(self)
    }
}

#[async_trait]
impl Host for K8sHost {
    async fn create_lease(&self, name: &str) -> Result<(), Error> {
        let namespace = &self.namespace;
        let client = self
            .client
            .as_ref()
            .ok_or_else(|| Error::Connection("Client not connected".to_string()))?;

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
                let existing_lease = api.get(name).await.map_err(|e| {
                    Error::CreateLease(format!("Failed to get existing lease: {e}"))
                })?;

                let existing_holder = existing_lease
                    .spec
                    .as_ref()
                    .and_then(|spec| spec.holder_identity.as_ref())
                    .ok_or_else(|| {
                        Error::CreateLease("Existing lease has no holder identity".to_string())
                    })?;

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
                        .map_err(|e| Error::CreateLease(format!("Failed to renew lease: {e}")))?;

                    debug!("Renewed lease: {} in namespace: {}", name, namespace);
                    Ok(())
                } else {
                    // Someone else holds the lease.
                    Err(Error::CreateLease(format!(
                        "Lease {name} is held by another instance: {existing_holder}"
                    )))
                }
            }
            Err(e) => Err(Error::CreateLease(e.to_string())),
        }
    }

    async fn delete_lease(&self, name: &str, namespace: Option<&str>) -> Result<(), Error> {
        let namespace = namespace.unwrap_or(&self.namespace);
        let client = self
            .client
            .as_ref()
            .ok_or_else(|| Error::Connection("Client not connected".to_string()))?;
        let api: Api<Lease> = Api::namespaced((**client).clone(), namespace);

        api.delete(name, &DeleteParams::default())
            .await
            .map_err(|e| Error::DeleteLease(e.to_string()))?;

        info!("Deleted lease: {} in namespace: {}", name, namespace);
        Ok(())
    }

    async fn renew_lease(&self, name: &str, namespace: Option<&str>) -> Result<(), Error> {
        let namespace = namespace.unwrap_or(&self.namespace);
        let client = self
            .client
            .as_ref()
            .ok_or_else(|| Error::Connection("Client not connected".to_string()))?;
        let api: Api<Lease> = Api::namespaced((**client).clone(), namespace);

        let patch = serde_json::json!({
            "spec": {
                "renewTime": MicroTime(chrono::Utc::now()),
            }
        });

        api.patch(name, &PatchParams::default(), &Patch::Merge(patch))
            .await
            .map_err(|e| Error::RenewLease(e.to_string()))?;

        debug!("Renewed lease: {} in namespace: {}", name, namespace);
        Ok(())
    }
}

/// Builder for K8sHost.
pub struct K8sHostBuilder {
    namespace: String,
    lease_duration_secs: i32,
    holder_identity: Option<String>,
}

impl Default for K8sHostBuilder {
    fn default() -> Self {
        Self {
            namespace: DEFAULT_NAMESPACE.to_string(),
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

    /// Sets the namespace for Kubernetes resources.
    pub fn namespace(mut self, namespace: String) -> Self {
        self.namespace = namespace;
        self
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
    pub fn build(self) -> Result<K8sHost, Error> {
        Ok(K8sHost {
            client: None,
            namespace: self.namespace,
            lease_duration_secs: self.lease_duration_secs,
            holder_identity: self
                .holder_identity
                .ok_or_else(|| Error::Connection("holder_identity must be set".to_string()))?,
        })
    }
}

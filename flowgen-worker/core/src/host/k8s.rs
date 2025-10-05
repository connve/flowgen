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
    /// Kubernetes client error.
    #[error("Kubernetes client error")]
    Kube(#[source] kube::Error),
    /// Failed to read namespace from service account.
    #[error("Failed to read namespace from service account")]
    NamespaceRead(#[source] std::io::Error),
    /// Client not connected.
    #[error("Client not connected")]
    ClientNotConnected,
    /// Failed to create lease.
    #[error("Failed to create lease")]
    CreateLease(#[source] kube::Error),
    /// Failed to get existing lease.
    #[error("Failed to get existing lease")]
    GetLease(#[source] kube::Error),
    /// Existing lease has no spec.
    #[error("Existing lease has no spec")]
    MissingLeaseSpec,
    /// Existing lease has no holder identity.
    #[error("Existing lease has no holder identity")]
    MissingHolderIdentity,
    /// Failed to renew lease.
    #[error("Failed to renew lease")]
    RenewLease(#[source] kube::Error),
    /// Failed to take over expired lease.
    #[error("Failed to take over expired lease")]
    TakeoverLease(#[source] kube::Error),
    /// Lease is held by another instance.
    #[error("Lease {name} is held by another instance: {holder}")]
    LeaseHeldByOther { name: String, holder: String },
    /// Failed to delete lease.
    #[error("Failed to delete lease")]
    DeleteLease(#[source] kube::Error),
    /// Failed to patch lease.
    #[error("Failed to patch lease")]
    PatchLease(#[source] kube::Error),
    /// Required builder attribute was not provided.
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

impl FlowgenClient for K8sHost {
    type Error = Error;

    async fn connect(mut self) -> Result<Self, Self::Error> {
        let client = Client::try_default().await.map_err(Error::Kube)?;

        // Auto-detect namespace from pod's service account.
        let namespace =
            tokio::fs::read_to_string("/var/run/secrets/kubernetes.io/serviceaccount/namespace")
                .await
                .map_err(Error::NamespaceRead)?
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
    async fn create_lease(&self, name: &str) -> Result<(), crate::host::Error> {
        let namespace = &self.namespace;
        let client = self
            .client
            .as_ref()
            .ok_or_else(|| Box::new(Error::ClientNotConnected) as crate::host::Error)?;

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
                    .map_err(|e| Box::new(Error::GetLease(e)) as crate::host::Error)?;

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
                        .map_err(|e| Box::new(Error::RenewLease(e)) as crate::host::Error)?;

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
                        .map_err(|e| Box::new(Error::TakeoverLease(e)) as crate::host::Error)?;

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
            Err(e) => Err(Box::new(Error::CreateLease(e)) as crate::host::Error),
        }
    }

    async fn delete_lease(&self, name: &str, namespace: Option<&str>) -> Result<(), crate::host::Error> {
        let namespace = namespace.unwrap_or(&self.namespace);
        let client = self
            .client
            .as_ref()
            .ok_or_else(|| Box::new(Error::ClientNotConnected) as crate::host::Error)?;
        let api: Api<Lease> = Api::namespaced((**client).clone(), namespace);

        api.delete(name, &DeleteParams::default())
            .await
            .map_err(|e| Box::new(Error::DeleteLease(e)) as crate::host::Error)?;

        info!("Deleted lease: {} in namespace: {}", name, namespace);
        Ok(())
    }

    async fn renew_lease(&self, name: &str, namespace: Option<&str>) -> Result<(), crate::host::Error> {
        let namespace = namespace.unwrap_or(&self.namespace);
        let client = self
            .client
            .as_ref()
            .ok_or_else(|| Box::new(Error::ClientNotConnected) as crate::host::Error)?;
        let api: Api<Lease> = Api::namespaced((**client).clone(), namespace);

        let patch = serde_json::json!({
            "spec": {
                "renewTime": MicroTime(chrono::Utc::now()),
            }
        });

        api.patch(name, &PatchParams::default(), &Patch::Merge(patch))
            .await
            .map_err(|e| Box::new(Error::PatchLease(e)) as crate::host::Error)?;

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

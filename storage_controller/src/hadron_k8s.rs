use crate::config_manager;
use crate::hadron_drain_and_fill::DrainAndFillManager;
use crate::hadron_pageserver_watcher::create_pageserver_pod_watcher;
use crate::hadron_sk_maintenance::SKMaintenanceManager;
use crate::metrics::{self, ConfigWatcherCompleteLabelGroup, ReconcileOutcome};
use crate::node::transform_pool_id;
use crate::persistence::Persistence;
use crate::service;
use anyhow::{anyhow, Context};
use async_trait::async_trait;
use core::fmt;
use itertools::Either;
use storage_scrubber::NodeKind;
use tokio::time::sleep;
use utils::env::is_chaos_testing;
use utils::env::is_dev_or_staging;

use reqwest::Url;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::fmt::Debug;
use std::fs::File;
use std::io::BufReader;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};
use tokio::io::AsyncReadExt;
use tokio::runtime::Handle;
use tracing::Instrument;

use uuid::Uuid;

use compute_api::spec::{DatabricksSettings, PgComputeTlsSettings};
use k8s_openapi::api::apps::v1::{
    DaemonSet, DaemonSetSpec, DaemonSetUpdateStrategy, Deployment, DeploymentSpec, ReplicaSet,
    ReplicaSetSpec, RollingUpdateDaemonSet,
};
use k8s_openapi::api::core::v1::{
    Affinity, ConfigMapVolumeSource, Container, ContainerPort, EmptyDirVolumeSource, EnvVar,
    EnvVarSource, HTTPGetAction, HostPathVolumeSource, LocalObjectReference, NodeAffinity,
    NodeSelector, NodeSelectorRequirement, NodeSelectorTerm, ObjectFieldSelector,
    PersistentVolumeClaim, PersistentVolumeClaimSpec, Pod, PodAffinityTerm, PodAntiAffinity,
    PodReadinessGate, PodSecurityContext, PodSpec, PodTemplateSpec, Probe, ResourceRequirements,
    SecretKeySelector, SecretVolumeSource, SecurityContext, Service, ServicePort, ServiceSpec,
    Toleration, Volume, VolumeMount, VolumeResourceRequirements,
};
use k8s_openapi::apimachinery::pkg::api::resource::Quantity;
use k8s_openapi::apimachinery::pkg::apis::meta;
use k8s_openapi::apimachinery::pkg::util::intstr::IntOrString;
use kube::api::{DeleteParams, ListParams, PostParams};
use kube::{Api, Client};
use mockall::automock;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use utils::ip_address::HADRON_NODE_IP_ADDRESS;

use crate::hadron_token::HadronTokenGeneratorImpl;
use hcc_api::models::{EndpointConfig, EndpointTShirtSize, PostgresConnectionInfo};
use openkruise::{
    StatefulSet, StatefulSetPersistentVolumeClaimRetentionPolicy, StatefulSetSpec,
    StatefulSetUpdateStrategy, StatefulSetUpdateStrategyRollingUpdate,
};

#[derive(PartialEq, Clone, Debug, Hash, Eq, Serialize, Deserialize)]
pub enum CloudProvider {
    AWS,
    Azure,
}

/// Represents the model to use when deploying/managing "compute".
/// - PrivatePreview: In the Private Preview mode, each "compute" is modeled as a Kubernetes Deployment object,
///   a ClusterIP admin Service object, and a LoadBalancer Service object used for direct ingress.
/// - PublicPreview: In the Public Preview mode, each "compute" is modeled as a Kubernetes ReplicaSet object
///   and a ClusterIP admin Service object.
#[derive(Serialize, Deserialize, Debug)]
pub enum ComputeModel {
    PrivatePreview,
    PublicPreview,
}

/// Enum representing the Kubernetes Service type.
pub enum K8sServiceType {
    ClusterIP,
    LoadBalancer,
}

// Struct representing the various parameters we can set when defining a readiness probe for a container.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ReadinessProbeParams {
    /// The endpoint on the server that we want to use to perform the readiness check. If pass
    /// the path of /healthz then Kubernetes will send a HTTP GET to /healthz and any code in the
    /// range [200, 400) is considered a success and any other code is considered a failure
    pub endpoint_path: String,
    /// The minimum number of consecutive failures for the probe to be considered failed after having
    /// succeeded
    pub failure_threshold: i32,
    /// The number of seconds after the container has started that the probe is run for the first time
    pub initial_delay_seconds: i32,
    /// How often, in seconds, does Kubernetes perform a probe
    pub period_seconds: i32,
    /// Minimum consecutive success for the probe to be considered success after a failure
    pub success_threshold: i32,
    /// Number of seconds after which the probe times out
    pub timeout_seconds: i32,
}

impl Default for ReadinessProbeParams {
    fn default() -> Self {
        Self {
            endpoint_path: "/status".to_string(),
            failure_threshold: 2,
            initial_delay_seconds: 10,
            period_seconds: 2,
            success_threshold: 2,
            timeout_seconds: 1,
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct PgCompute {
    // NB: Consider DNS name length limits when modifying the compute_name format.
    // Most cloud DNS providers will limit records to ~250 chars in length, which,
    // depending on the number of additional DNS labels, could impose restrictions
    // on compute name length specifically.
    pub name: String,
    pub compute_id: String,
    pub control_plane_token: String,
    pub workspace_id: Option<String>,
    pub workspace_url: Option<Url>,
    pub image_override: Option<String>,
    pub exporter_image_override: Option<String>,
    pub node_selector_override: Option<BTreeMap<String, String>>,
    pub resources: ResourceRequirements,
    pub tshirt_size: EndpointTShirtSize,
    pub model: ComputeModel,
    pub readiness_probe: Option<ReadinessProbeParams>,
    // Used in the PublicPreview model as the endpoint id for PG authentication
    pub instance_id: Option<String>,
}

pub const INSTANCE_ID_LABEL_KEY: &str = "instanceId";
pub const COMPUTE_SECONDARY_LABEL_KEY: &str = "isSecondary";
pub const COMPUTE_ID_LABEL_KEY: &str = "computeId";
pub const BRICKSTORE_POOL_TYPES_LABEL_KEY: &str = "brickstore-pool-types";
// The default instance ID to use when the instance ID is not provided in PrPr model.
const DEFAULT_INSTANCE_ID: &str = "00000000-0000-0000-0000-000000000000";

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum BrcDbletNodeGroup {
    Dblet2C,
    Dblet4C,
    Dblet8C,
    Dblet16C,
}

impl fmt::Display for BrcDbletNodeGroup {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BrcDbletNodeGroup::Dblet2C => write!(f, "dbletbrc2c"),
            BrcDbletNodeGroup::Dblet4C => write!(f, "dbletbrc4c"),
            BrcDbletNodeGroup::Dblet8C => write!(f, "dbletbrc8c"),
            BrcDbletNodeGroup::Dblet16C => write!(f, "dbletbrc16c"),
        }
    }
}

fn select_node_group_by_tshirt_size(tshirt_size: &EndpointTShirtSize) -> BrcDbletNodeGroup {
    match tshirt_size {
        EndpointTShirtSize::XSmall => BrcDbletNodeGroup::Dblet2C,
        EndpointTShirtSize::Small => BrcDbletNodeGroup::Dblet4C,
        EndpointTShirtSize::Medium => BrcDbletNodeGroup::Dblet8C,
        EndpointTShirtSize::Large => BrcDbletNodeGroup::Dblet16C,
        EndpointTShirtSize::Test => BrcDbletNodeGroup::Dblet4C,
    }
}

impl PgCompute {
    fn choose_dblet_node_group(&self) -> String {
        // If the user has provided a node selector override and it contains the pool type label key,
        // use that value. Otherwise, use the tshirt size to determine the node group.
        self.node_selector_override
            .as_ref()
            .and_then(|node_selector| node_selector.get(BRICKSTORE_POOL_TYPES_LABEL_KEY))
            .cloned()
            .unwrap_or_else(|| select_node_group_by_tshirt_size(&self.tshirt_size).to_string())
    }

    fn dblet_node_selector(node_group: String) -> BTreeMap<String, String> {
        vec![(BRICKSTORE_POOL_TYPES_LABEL_KEY.to_string(), node_group)]
            .into_iter()
            .collect()
    }

    fn k8s_label_compute_id(&self) -> String {
        // Replace slashes with dashes as k8s does not allow slashes in labels.
        self.compute_id.clone().replace('/', "-")
    }

    fn dblet_tolerations(&self, node_group: String) -> Vec<Toleration> {
        vec![
            Toleration {
                key: Some("databricks.com/node-type".to_string()),
                operator: Some("Equal".to_string()),
                value: Some(node_group),
                effect: Some("NoSchedule".to_string()),
                ..Default::default()
            },
            Toleration {
                key: Some("dblet.dev/appid".to_string()),
                operator: Some("Equal".to_string()),
                value: Some(self.k8s_label_compute_id()),
                effect: Some("NoSchedule".to_string()),
                ..Default::default()
            },
        ]
    }

    // Return the node selector to be used for this compute deployment. User overrides take precedence, followed by
    // resource-based selection.
    pub fn get_node_selector(&self, dblet_node_group: String) -> BTreeMap<String, String> {
        self.node_selector_override
            .clone()
            .unwrap_or(Self::dblet_node_selector(dblet_node_group))
    }

    pub fn get_tolerations(&self, dblet_node_group: String) -> Vec<Toleration> {
        self.dblet_tolerations(dblet_node_group)
    }
}

/// Represents a HadronCluster object in Kubernetes.
/// A JSON object representing this struct is stored in the cluster-config ConfigMap.
/// The ConfigMap is periodically polled by the storage controller to update the cluster.
#[derive(Serialize, Deserialize)]
pub struct HadronCluster {
    /// The metadata for the resource, like kind and API version.
    pub type_meta: Option<kube::core::TypeMeta>,
    /// The object metadata, like name and namespace.
    pub object_meta: Option<kube::core::ObjectMeta>,
    /// The specification of the HadronCluster.
    pub hadron_cluster_spec: Option<HadronClusterSpec>,
}

/// Represents the specification of a HadronCluster object in Kubernetes.
#[derive(Serialize, Deserialize)]
pub struct HadronClusterSpec {
    /// The name of the service account to access object storage.
    pub service_account_name: Option<String>,
    /// The configuration for object storage such as bucket name and region.
    pub object_storage_config: Option<HadronObjectStorageConfig>,
    /// The specification for the storage broker.
    pub storage_broker_spec: Option<HadronStorageBrokerSpec>,
    /// The specification for the safe keepers. It is a vector in case there are different version of safe keepers to be deployed.
    pub safe_keeper_specs: Option<Vec<HadronSafeKeeperSpec>>,
    /// The specification for the page servers. It is a vector in case there are different version of page servers to be deployed.
    pub page_server_specs: Option<Vec<HadronPageServerSpec>>,
}

#[derive(Serialize, Deserialize)]
pub struct ObjectStorageConfigTestParameters {
    pub endpoint: Option<String>,
    pub access_key_id: Option<String>,
    pub secret_access_key: Option<String>,
}

/// Represents the configuration for object storage such as bucket name and region.
/// TODO(steve.greene): Refactor this config to support at most one object storage provider at a time.
/// This would be considered a breaking change and would require careful coordination to migrate
/// existing config maps to the new config format.
#[derive(Serialize, Deserialize, Default)]
pub struct HadronObjectStorageConfig {
    /// AWS S3 config options.
    /// The name of the AWS S3 bucket in the object storage.
    bucket_name: Option<String>,
    /// The region of the AWS S3 bucket in the object storage.
    bucket_region: Option<String>,

    /// Azure storage account options.
    /// The (full) Azure storage account resource ID.
    storage_account_resource_id: Option<String>,
    /// The tenant ID for Azure object storage.
    azure_tenant_id: Option<String>,
    /// The Azure storage account container name.
    storage_container_name: Option<String>,
    /// The Azure storage container region.
    storage_container_region: Option<String>,

    /// [Test-only] Parameters used in testing to point to a mock object store.
    test_params: Option<ObjectStorageConfigTestParameters>,
}

/// Helper functions for deducing the object storage provider.
/// TODO(steve.greene): If we refactor HadronObjectStorageConfig to support at most one object storage,
/// we can most likely remove these functions.
impl HadronObjectStorageConfig {
    fn is_aws(&self) -> bool {
        self.bucket_name.is_some() && self.bucket_region.is_some()
    }

    pub fn is_azure(&self) -> bool {
        self.storage_account_resource_id.is_some()
            && self.azure_tenant_id.is_some()
            && self.storage_container_name.is_some()
            && self.storage_container_region.is_some()
    }
}

/// Represents the specification for the storage broker.
#[derive(Serialize, Deserialize)]
pub struct HadronStorageBrokerSpec {
    /// The hadron image.
    pub image: Option<String>,
    /// The image pull policy.
    pub image_pull_policy: Option<String>,
    /// The image pull secrets.
    pub image_pull_secrets: Option<Vec<LocalObjectReference>>,
    /// The node selector requirements.
    pub node_selector: Option<NodeSelectorRequirement>,
    /// The resources for the storage broker.
    pub resources: Option<ResourceRequirements>,
}

/// Represents the specification for the safe keepers.
#[derive(Serialize, Deserialize)]
pub struct HadronSafeKeeperSpec {
    /// The hadron image.
    pub image: Option<String>,
    /// The image pull policy.
    pub image_pull_policy: Option<String>,
    /// The image pull secrets.
    pub image_pull_secrets: Option<Vec<LocalObjectReference>>,
    /// The pool ID distinguishes between different StatefulSets if there are different version of safe keepers in one cluster.
    pub pool_id: Option<i32>,
    /// The number of replicas.
    pub replicas: Option<i32>,
    /// The node selector requirements.
    pub node_selector: Option<NodeSelectorRequirement>,
    /// Suffix of the availability zone for this pool of safekeepers. Note that this is just the suffix, not the full availability zone name.
    /// For example, if the full availability zone name is "us-west-2a", the suffix is "a". Region is assumed to be the same as where HCC runs.
    pub availability_zone_suffix: Option<String>,
    /// The storage class name.
    pub storage_class_name: Option<String>,
    /// The resources for the safe keeper.
    pub resources: Option<ResourceRequirements>,
    /// Whether to use low downtime maintenance to upgrade Safekeepers.
    pub enable_low_downtime_maintenance: Option<bool>,
    /// Whether to LDTM checks SK status.
    pub enable_ldtm_sk_status_check: Option<bool>,
}

/// Represents the specification for the page servers.
#[derive(Serialize, Deserialize)]
pub struct HadronPageServerSpec {
    /// The hadron image.
    pub image: Option<String>,
    /// The image pull policy.
    pub image_pull_policy: Option<String>,
    /// The image pull secrets.
    pub image_pull_secrets: Option<Vec<LocalObjectReference>>,
    /// The pool ID distinguishes between different StatefulSets if there are different version of page servers in one cluster.
    pub pool_id: Option<i32>,
    /// The number of replicas.
    pub replicas: Option<i32>,
    /// The node selector requirements.
    pub node_selector: Option<NodeSelectorRequirement>,
    /// Suffix of the availability zone for this pool of pageservers. Note that this is just the suffix, not the full availability zone name.
    /// For example, if the full availability zone name is "us-west-2a", the suffix is "a". Region is assumed to be the same as where HCC runs.
    pub availability_zone_suffix: Option<String>,
    /// The storage class name.
    pub storage_class_name: Option<String>,
    /// The resources for the page server.
    /// The storage must be specified with Gi as the suffix and only the limits value is read.
    pub resources: Option<ResourceRequirements>,
    /// Custom pageserver.toml configuration to use for this pool of pageservers. Intended to be used in dev/testing only.
    /// Any content specified here is inserted verbatim to the pageserver launch script, so do not use any user-generated
    /// content for this field.
    pub custom_pageserver_toml: Option<String>,
    /// Parallelism to pre-pull pageserver images before starting rolling updates.
    pub image_prepull_parallelism: Option<i32>,
    /// Timeout for pre-pulling pageserver images before starting rolling updates.
    pub image_prepull_timeout_seconds: Option<u64>,
    /// Whether to use drain_and_fill to upgrade PageServers.
    pub use_drain_and_fill: Option<bool>,
}

#[automock]
#[async_trait]
pub trait K8sManager: Send + Sync {
    fn get_client(&self) -> Arc<Client>;

    fn get_current_pg_params(&self) -> Result<PgParams, anyhow::Error>;

    fn set_pg_params(&self, params: PgParams) -> Result<(), anyhow::Error>;

    async fn deploy_compute(&self, pg_compute: PgCompute) -> kube::Result<()>;

    // Delete compute resources for a given compute name and model.
    // Returns Ok(true) if all the required resources are completely cleaned up i.e.
    // they are not found in k8s anymore.
    // Return Ok(false) if all the required resources are called for deletion but
    // the resources are still found in k8s.
    // The callers can retry the deletion if Ok(false) is returned with some backoff.
    async fn delete_compute(
        &self,
        pg_compute_name: &str,
        model: ComputeModel,
    ) -> kube::Result<bool>;

    async fn get_http_urls_for_compute_services(&self, service_names: Vec<String>) -> Vec<Url>;

    async fn get_databricks_compute_settings(
        &self,
        workspace_url: Option<Url>,
    ) -> DatabricksSettings;

    // Retrieve the k8s Service object handling the primary (read/write traffic) ingress to an instance.
    async fn get_instance_primary_ingress_service(
        &self,
        instance_id: Uuid,
    ) -> kube::Result<Service>;

    // Idempotently create or patch the k8s Service object handling primary (read/write traffic) ingress to an instance,
    // so that it routes traffic to the compute Pod with the specified ID.
    async fn create_or_patch_instance_primary_ingress_service(
        &self,
        instance_id: Uuid,
        compute_id: Uuid,
        service_type: K8sServiceType,
    ) -> kube::Result<Service>;

    async fn create_or_patch_readable_secondary_ingress_service(
        &self,
        instance_id: Uuid,
    ) -> kube::Result<Service>;

    // Idempotently delete the k8s Service object handling primary (read/write traffic) ingress to an instance.
    // Returns true if the service was found and deleted, false if the service was not found.
    async fn delete_instance_primary_ingress_service(
        &self,
        instance_id: Uuid,
    ) -> kube::Result<bool>;
}

/// Hadron K8sManager manages access to Kubernetes API. This object is not mutable and therefore
/// thread-safe. (Mutability and thread-safety is type-checked in Rust.)
pub struct K8sManagerImpl {
    // The k8s client to use for all operations by the `K8sManager`.
    pub client: Arc<Client>,
    // The k8s namespace where this HCC is running.
    namespace: String,
    // The region this HCC runs in.
    region: String,
    // The reachable DNS name of the Hadron Cluster Coordinator (HCC) that is advertised to other nodes.
    hcc_dns_name: String,
    // The port of the HCC service that is advertised to storage nodes (PS/SK).
    hcc_listening_port: u16,
    // The port of the HCC service that is advertised to PG compute nodes.
    hcc_compute_listening_port: u16,
    // The defaults for PG compute nodes.
    pg_params: RwLock<PgParams>,
    // The configured cloud provider (when not running in tests).
    cloud_provider: Option<CloudProvider>,
}

/// Stores the Deployment and Service objects for the StorageBroker.
pub struct StorageBrokerObjs {
    pub deployment: Deployment,
    pub service: Service,
}

/// Stores the StatefulSets and Service objects for SafeKeepers.
/// There may be multiple StatefulSets if there are different versions of SafeKeepers.
pub struct SafeKeeperObjs {
    pub stateful_sets: Vec<StatefulSet>,
    pub service: Service,
}

/// Struct describing an image-prepull operation to be performed using an image puller DaemonSet.
const IMAGE_PREPULL_DEFAULT_TIMEOUT: Duration = Duration::from_secs(60);
pub struct ImagePullerDaemonsetInfo {
    /// The DaemonSet manifest of the image puller.
    pub daemonset: DaemonSet,
    /// The timeout to wait for the prepull operation to complete before starting the rolling update.
    /// If None, defaults to IMAGE_PREPULL_DEFAULT_TIMEOUT.
    pub image_prepull_timeout: Option<Duration>,
}

/// Stores the StatefulSets and Service objects for PageServers.
/// There may be multiple StatefulSets if there are different versions of PageServers.
pub struct PageServerObjs {
    pub image_puller_daemonsets: Vec<ImagePullerDaemonsetInfo>,
    pub stateful_sets: Vec<StatefulSet>,
    pub service: Service,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MountType {
    ConfigMap,
    Secret,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct K8sMount {
    pub name: String,
    pub mount_type: MountType,
    pub mount_path: String,
    pub files: Vec<String>,
}

pub struct K8sSecretVolumesAndMounts {
    pub volumes: Vec<Volume>,
    pub volume_mounts: Vec<VolumeMount>,
}

/// Stores the parameter values for PG compute nodes.
/// TODO: Find a better way to do unwraps with default values.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct PgParams {
    /// The namespace for compute nodes. Required.
    pub compute_namespace: String,
    /// The image for compute nodes. Required.
    pub compute_image: String,
    /// The image for the prometheus exporter that runs in the same pod as the compute nodes. Required.
    pub prometheus_exporter_image: String,
    /// The compute node image pull secret name.
    #[serde(default = "pg_params_default_compute_image_pull_secret")]
    pub compute_image_pull_secret: Option<String>,
    /// The pg port for compute nodes.
    #[serde(default = "pg_params_default_compute_pg_port")]
    pub compute_pg_port: Option<u16>,
    /// The http port for compute nodes.
    #[serde(default = "pg_params_default_compute_http_port")]
    pub compute_http_port: Option<u16>,
    /// The kubernetes secret name, its mount path in the container and chmod mode.
    #[serde(default = "pg_params_default_compute_mounts")]
    pub compute_mounts: Option<Vec<K8sMount>>,
    #[serde(default = "pg_params_pg_compute_tls_settings")]
    pub pg_compute_tls_settings: Option<PgComputeTlsSettings>,
    #[serde(default = "pg_params_default_databricks_pg_hba")]
    pub databricks_pg_hba: Option<String>,
    #[serde(default = "pg_params_default_databricks_pg_ident")]
    pub databricks_pg_ident: Option<String>,
}

impl Default for PgParams {
    fn default() -> Self {
        PgParams {
            compute_namespace: String::new(),
            compute_image: String::new(),
            prometheus_exporter_image: String::new(),
            compute_image_pull_secret: pg_params_default_compute_image_pull_secret(),
            compute_pg_port: pg_params_default_compute_pg_port(),
            compute_http_port: pg_params_default_compute_http_port(),
            compute_mounts: pg_params_default_compute_mounts(),
            pg_compute_tls_settings: pg_params_pg_compute_tls_settings(),
            databricks_pg_hba: pg_params_default_databricks_pg_hba(),
            databricks_pg_ident: pg_params_default_databricks_pg_ident(),
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct PageServerBillingMetricsConfig {
    pub metric_collection_endpoint: Option<String>,
    pub metric_collection_interval: Option<String>,
    pub synthetic_size_calculation_interval: Option<String>,
}

impl PageServerBillingMetricsConfig {
    pub fn to_toml(&self) -> String {
        let mut toml_str = String::new();

        if let Some(ref endpoint) = self.metric_collection_endpoint {
            toml_str.push_str(&format!("metric_collection_endpoint = \"{}\"\n", endpoint));
        }
        if let Some(ref interval) = self.metric_collection_interval {
            toml_str.push_str(&format!("metric_collection_interval = \"{}\"\n", interval));
        }
        if let Some(ref synthetic_interval) = self.synthetic_size_calculation_interval {
            toml_str.push_str(&format!(
                "synthetic_size_calculation_interval = \"{}\"\n",
                synthetic_interval
            ));
        }

        toml_str
    }
}

/// The information in /etc/config/config.json.
#[derive(Serialize, Deserialize)]
pub struct ConfigData {
    pub hadron_cluster: Option<HadronCluster>,
    pub pg_params: Option<PgParams>,
    pub page_server_billing_metrics_config: Option<PageServerBillingMetricsConfig>,
}

// Implement default for PgParams optional fields.
fn pg_params_default_compute_image_pull_secret() -> Option<String> {
    Some("harbor-image-pull-secret".to_string())
}

fn pg_params_default_compute_pg_port() -> Option<u16> {
    Some(55432)
}

fn pg_params_default_compute_http_port() -> Option<u16> {
    Some(55433)
}

fn brickstore_internal_token_verification_key_mount_path() -> String {
    "/databricks/secrets/brickstore-internal-token-public-keys".to_string()
}

fn brickstore_internal_token_verification_key_secret_mount() -> K8sMount {
    K8sMount {
        name: "brickstore-internal-token-public-keys".to_string(),
        mount_type: MountType::Secret,
        mount_path: brickstore_internal_token_verification_key_mount_path(),
        files: vec!["key1.pem".to_string(), "key2.pem".to_string()],
    }
}

pub fn azure_storage_accont_service_principal_mount_path() -> String {
    "/databricks/secrets/azure-service-principal".to_string()
}

fn azure_storage_account_service_principal_secret_mount() -> K8sMount {
    K8sMount {
        name: "brickstore-hadron-storage-account-service-principal-secret".to_string(),
        mount_type: MountType::Secret,
        mount_path: azure_storage_accont_service_principal_mount_path(),
        files: vec!["client.pem".to_string()],
    }
}

fn pg_params_default_compute_mounts() -> Option<Vec<K8sMount>> {
    Some(vec![
        brickstore_internal_token_verification_key_secret_mount(),
        K8sMount {
            name: "brickstore-domain-certs".to_string(),
            mount_type: MountType::Secret,
            mount_path: "/databricks/secrets/brickstore-domain-certs".to_string(),
            files: vec!["server.key".to_string(), "server.crt".to_string()],
        },
        K8sMount {
            name: "trusted-ca-certificates".to_string(),
            mount_type: MountType::Secret,
            mount_path: "/databricks/secrets/trusted-ca".to_string(),
            files: vec!["data-plane-misc-root-ca-cert.pem".to_string()],
        },
        K8sMount {
            name: "pg-compute-config".to_string(),
            mount_type: MountType::ConfigMap,
            mount_path: "/databricks/pg_config".to_string(),
            files: vec![
                "databricks_pg_hba.conf".to_string(),
                "databricks_pg_ident.conf".to_string(),
            ],
        },
    ])
}

fn pg_params_pg_compute_tls_settings() -> Option<PgComputeTlsSettings> {
    Some(PgComputeTlsSettings {
        cert_file: "/databricks/secrets/brickstore-domain-certs/server.crt".to_string(),
        key_file: "/databricks/secrets/brickstore-domain-certs/server.key".to_string(),
        ca_file: "/databricks/secrets/trusted-ca/data-plane-misc-root-ca-cert.pem".to_string(),
    })
}

fn pg_params_default_databricks_pg_hba() -> Option<String> {
    Some("/databricks/pg_config/databricks_pg_hba.conf".to_string())
}

fn pg_params_default_databricks_pg_ident() -> Option<String> {
    Some("/databricks/pg_config/databricks_pg_ident.conf".to_string())
}

/// Gets the volume claim template for PS, SK.
fn get_volume_claim_template(
    resources: Option<ResourceRequirements>,
    storage_class_name: Option<String>,
) -> anyhow::Result<Option<Vec<PersistentVolumeClaim>>> {
    let volume_resource_requirements = VolumeResourceRequirements {
        requests: resources
            .as_ref()
            .and_then(|res| res.limits.as_ref())
            .and_then(|limits| limits.get("storage"))
            .map(|storage| {
                let mut map = BTreeMap::new();
                map.insert("storage".to_string(), storage.clone());
                map
            }),
        ..Default::default()
    };

    Ok(Some(vec![PersistentVolumeClaim {
        metadata: meta::v1::ObjectMeta {
            name: Some("local-data".to_string()),
            ..Default::default()
        },
        spec: Some(PersistentVolumeClaimSpec {
            access_modes: Some(vec!["ReadWriteOnce".to_string()]),
            storage_class_name: storage_class_name.clone(),
            resources: Some(volume_resource_requirements),
            ..Default::default()
        }),
        ..Default::default()
    }]))
}

/// Gets the pod metadata for PS, SK, SB.
fn get_pod_metadata(app_name: String, prometheus_port: u16) -> Option<meta::v1::ObjectMeta> {
    Some(meta::v1::ObjectMeta {
        labels: Some(
            vec![("app".to_string(), app_name.clone())]
                .into_iter()
                .collect(),
        ),
        annotations: {
            Some(
                vec![
                    ("prometheus.io/path".to_string(), "/metrics".to_string()),
                    (
                        "prometheus.io/port".to_string(),
                        prometheus_port.to_string(),
                    ),
                    ("prometheus.io/scrape".to_string(), "true".to_string()),
                    ("enableLogDaemon".to_string(), "true".to_string()),
                    (
                        "logDaemonDockerLoggingGroup".to_string(),
                        "docker-common-log-group".to_string(),
                    ),
                ]
                .into_iter()
                .collect(),
            )
        },
        ..Default::default()
    })
}

/// Gets the security context for PS, SK.
fn get_pod_security_context() -> Option<PodSecurityContext> {
    Some(PodSecurityContext {
        run_as_user: Some(1000),
        fs_group: Some(2000),
        run_as_non_root: Some(true),
        ..Default::default()
    })
}

/// Gets the volume mounts for PS, SK.
fn get_local_data_volume_mounts() -> Vec<VolumeMount> {
    vec![VolumeMount {
        mount_path: "/data/.neon/".to_string(),
        name: "local-data".to_string(),
        ..Default::default()
    }]
}

/// Gets the container ports for PS, SK, SB.
fn get_container_ports(ports: Vec<i32>) -> Option<Vec<ContainerPort>> {
    Some(
        ports
            .iter()
            .map(|port| ContainerPort {
                container_port: *port,
                ..Default::default()
            })
            .collect(),
    )
}

/// Gets the environment variables for PS, SK.
pub fn get_env_vars(
    object_storage_config: &HadronObjectStorageConfig,
    additional_env_vars: Vec<EnvVar>,
) -> Option<Vec<EnvVar>> {
    let mut env_vars = vec![EnvVar {
        name: "BROKER_ENDPOINT".to_string(),
        value: Some("http://storage-broker:50051".to_string()),
        ..Default::default()
    }];

    if object_storage_config.is_aws() {
        env_vars.push(EnvVar {
            name: "S3_BUCKET_URI".to_string(),
            value: object_storage_config.bucket_name.clone(),
            ..Default::default()
        });
        env_vars.push(EnvVar {
            name: "S3_REGION".to_string(),
            value: object_storage_config.bucket_region.clone(),
            ..Default::default()
        });
    }

    if object_storage_config.is_azure() {
        // The following azure env vars come from the object storage config directly.
        env_vars.push(EnvVar {
            // Need to fish out the account name for the full storage account resource ID.
            // Luckily its the last component when splitting on slashes by design.
            name: "AZURE_STORAGE_ACCOUNT_NAME".to_string(),
            value: object_storage_config
                .storage_account_resource_id
                .clone()
                .unwrap()
                .split('/')
                .last()
                .map(|s| s.to_string()),
            ..Default::default()
        });
        env_vars.push(EnvVar {
            name: "AZURE_TENANT_ID".to_string(),
            value: object_storage_config.azure_tenant_id.clone(),
            ..Default::default()
        });
        env_vars.push(EnvVar {
            name: "AZURE_STORAGE_CONTAINER_NAME".to_string(),
            value: object_storage_config.storage_container_name.clone(),
            ..Default::default()
        });
        env_vars.push(EnvVar {
            name: "AZURE_STORAGE_CONTAINER_REGION".to_string(),
            value: object_storage_config.storage_container_region.clone(),
            ..Default::default()
        });

        // The following azure env vars come from the mounted azure service principal secret.
        env_vars.push(EnvVar {
            name: "AZURE_CLIENT_ID".to_string(),
            // Extrat client_id from service principal secret.
            value_from: Some(EnvVarSource {
                secret_key_ref: Some(SecretKeySelector {
                    key: "client-id".to_string(),
                    name: azure_storage_account_service_principal_secret_mount().name,
                    optional: Some(false),
                }),
                ..Default::default()
            }),
            ..Default::default()
        });
        env_vars.push(EnvVar {
            name: "AZURE_CLIENT_CERTIFICATE_PATH".to_string(),
            value: Some(format!(
                "{}/client.pem",
                azure_storage_accont_service_principal_mount_path()
            )),
            ..Default::default()
        });
    }

    // Add EnvVars used in KIND tests if specified.
    if let Some(test_params) = &object_storage_config.test_params {
        // AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY are only used in tests to authenticate with
        // the mocked S3 service. We use service accounts and STS to authenticate with S3 in production.
        if let Some(key_id) = &test_params.access_key_id {
            env_vars.push(EnvVar {
                name: "AWS_ACCESS_KEY_ID".to_string(),
                value: Some(key_id.clone()),
                ..Default::default()
            });
        }
        if let Some(access_key) = &test_params.secret_access_key {
            env_vars.push(EnvVar {
                name: "AWS_SECRET_ACCESS_KEY".to_string(),
                value: Some(access_key.clone()),
                ..Default::default()
            });
        }
    }

    env_vars.extend(additional_env_vars);

    Some(env_vars)
}

/// Gets the headless service for PS, SK.
fn get_service(name: String, namespace: String, port: i32, admin_port: i32) -> Service {
    Service {
        metadata: meta::v1::ObjectMeta {
            name: Some(name.clone()),
            namespace: Some(namespace.clone()),
            ..Default::default()
        },
        spec: Some(ServiceSpec {
            selector: Some(BTreeMap::from([("app".to_string(), name.clone())])),
            ports: Some(vec![
                ServicePort {
                    port,
                    target_port: Some(IntOrString::Int(port)),
                    name: Some(name.clone()),
                    ..Default::default()
                },
                ServicePort {
                    port: admin_port,
                    target_port: Some(IntOrString::Int(admin_port)),
                    name: Some(format!("{}-{}", name.clone(), "admin")),
                    ..Default::default()
                },
            ]),
            cluster_ip: Some("None".to_string()),
            ..Default::default()
        }),
        ..Default::default()
    }
}

pub async fn hash_file_contents(file_path: &str) -> std::io::Result<String> {
    // Open and read file
    let mut file = tokio::fs::File::open(file_path).await?;
    let mut buffer = Vec::new();
    file.read_to_end(&mut buffer).await?;

    // Hash file
    let mut hasher = Sha256::new();
    hasher.update(&buffer);
    let hash_result = hasher.finalize();

    // Convert the hash result to a hex string
    let hash_hex = format!("{:x}", hash_result);
    Ok(hash_hex)
}

/// Helper function parse a workspace URL (if any) string to a proper `Url` object. Returns an error if a URL string is
/// present but it is invalid.
fn parse_to_url(url_str: Option<String>) -> anyhow::Result<Option<Url>> {
    match url_str {
        Some(url) => Url::parse(&url).map(Some).map_err(|e| anyhow::anyhow!(e)),
        None => Ok(None),
    }
}

pub fn endpoint_default_resources() -> ResourceRequirements {
    // Regardless T-shirt size, each PG pod requests 500m CPU and 4 GiB memory (1/4 of a 2-core node), and no limit.
    // This does not reflect the actual resource usage of the PG pod, but just a short-term solution to balance operation, perf and HA:
    // - perf: PG should be able to leverage all idle resources on the node. Therefore, we don't want to limit it.
    // - HA: PG should be able to schedule on a node, regardless of unregulated increases from daemonsets, sidecar containers, or node allocatable resources.
    //       Low requests makes sure scheduling works for all pods when unregulated increases are not crazily high.
    //       High priority makes sure PG are not preempted by other pods when their increases are unreasonably high.
    // - operation: We don't need to frequently adjust/backfill the resource requests for PG pods. Low requests make sure that.
    // Note that QoS limitation still stands - when the node is under memory pressure, PG pods will be OOM killed if other pods memory usage are under their requests.
    // TODO(yan): follow up with the Compute Lifecycle team about the long-term solution tracked in https://databricks.atlassian.net/browse/ES-1282825.
    ResourceRequirements {
        requests: Some(
            vec![
                ("cpu".to_string(), Quantity("500m".to_string())),
                ("memory".to_string(), Quantity("4Gi".to_string())),
            ]
            .into_iter()
            .collect(),
        ),
        ..Default::default()
    }
}

impl K8sManagerImpl {
    /// Creates a new K8sManager.
    /// - `region`: The region in which the Hadron Cluster Coordinator runs.
    /// - `namespace`: The namespace in which the Hadron Cluster Coordinator (this binary) service is running.
    /// - `advertised_hcc_host`: The reachable hostname of this HCC advertised to other nodes.
    /// - `advertised_hcc_port`: The reachable port of this HCC advertised to trusted storage nodes (PS/SK).
    /// - `advertised_hcc_compute_port`: The reachable port of this HCC advertised to PG compute nodes.
    /// - `pg_params`: Parameters used to launch Postgres compute nodes.
    /// - `cloud_provider`: The cloud provider where this K8sManager is running.
    #[allow(clippy::too_many_arguments)] // Clippy is too opinionated about this.
    pub async fn new(
        client: Arc<Client>,
        region: String,
        namespace: String,
        advertised_hcc_host: String,
        advertised_hcc_port: u16,
        advertised_hcc_compute_port: u16,
        pg_params: PgParams,
        cloud_provider: Option<CloudProvider>,
    ) -> anyhow::Result<Self> {
        Ok(Self {
            client,
            namespace,
            region,
            hcc_dns_name: advertised_hcc_host,
            hcc_listening_port: advertised_hcc_port,
            hcc_compute_listening_port: advertised_hcc_compute_port,
            pg_params: RwLock::new(pg_params),
            cloud_provider,
        })
    }

    #[cfg(test)]
    fn new_for_test(
        mock_client: Arc<Client>,
        region: String,
        cloud_provider: Option<CloudProvider>,
    ) -> Self {
        Self {
            client: mock_client,
            namespace: "test-namespace".to_string(),
            region,
            hcc_dns_name: "localhost".to_string(),
            hcc_listening_port: 1234,
            hcc_compute_listening_port: 1236,
            pg_params: RwLock::new(
                serde_json::from_str::<PgParams>(
                    r#"{
              "compute_namespace": "test-namespace",
              "compute_image": "test-image",
              "prometheus_exporter_image": "test-prometheus-exporter-image"
            }"#,
                )
                .unwrap(),
            ),
            cloud_provider,
        }
    }

    async fn k8s_create_or_replace<T>(api: Api<T>, name: &str, data: T) -> kube::Result<()>
    where
        T: Clone + Serialize + DeserializeOwned + Debug,
    {
        if (api.get_opt(name).await?).is_some() {
            api.replace(name, &PostParams::default(), &data).await?;
        } else {
            api.create(&PostParams::default(), &data).await?;
        }
        Ok(())
    }

    async fn k8s_get<T>(api: Api<T>, name: &str) -> kube::Result<Option<T>>
    where
        T: Clone + Serialize + DeserializeOwned + Debug,
    {
        api.get_opt(name).await
    }

    // Wait for a DaemonSet to reach the desired state.
    // Returns an error if we cannot confirm that the DaemonSet has reached the desired state by the specified deadline.
    async fn k8s_wait_for_daemonset(
        api: Api<DaemonSet>,
        name: &str,
        deadline: Instant,
    ) -> anyhow::Result<()> {
        let mut interval = tokio::time::interval(Duration::from_secs(10));
        let timeout = tokio::time::timeout_at(deadline.into(), async {
            // Poll the image puller daemonset pods every 10 seconds and inspect statuses, until timeout.
            loop {
                match api.get(name).await {
                    Ok(ds) => {
                        if let Some(status) = ds.status {
                            let generation_is_current = ds.metadata.generation.is_some()
                                && (ds.metadata.generation == status.observed_generation);
                            let all_pods_updated = status.desired_number_scheduled
                                == status.updated_number_scheduled.unwrap_or(
                                    if let Some(observed_generation) = status.observed_generation {
                                        if observed_generation == 1 {
                                            // If observed_generation is 1, the DaemonSet has just been created and the
                                            // `updated_number_scheduled` field is not populated. We simply use the
                                            // current number of scheduled pods.
                                            // Note that generation number starts from 1 in Kubernetes.
                                            status.current_number_scheduled
                                        } else {
                                            // For all other generations we expect `update_number_scheduled` to be
                                            // (eventually) present. If it's not present it means no pods have been
                                            // updated yet, so we return 0 here.
                                            0
                                        }
                                    } else {
                                        // We don't know anything if there isn't an `observed_generation`, so just bail
                                        // out with -1 here to force a retry.
                                        -1
                                    },
                                );
                            if generation_is_current && all_pods_updated {
                                tracing::info!(
                                    "DaemonSet {} is ready, successfully updated {} pods",
                                    name,
                                    status.desired_number_scheduled
                                );
                                break;
                            } else {
                                tracing::info!(
                                    "DaemonSet {} is not ready yet. Generation is current: {}, All pods updated: {}",
                                    name,
                                    generation_is_current,
                                    all_pods_updated
                                );
                            }
                        }
                    }
                    Err(e) => {
                        tracing::warn!("Error retriving status of DaemonSet {}: {:?}", name, e);
                    }
                }
                interval.tick().await;
            }
        });

        timeout.await.map_err(|_| {
            anyhow::anyhow!(
                "Cannot confirm DaemonSet {} reached the desired state by {:?}",
                name,
                deadline
            )
        })
    }

    pub(crate) async fn k8s_get_stateful_set(
        api: &Api<StatefulSet>,
        name: &str,
    ) -> Option<StatefulSet> {
        match api.get(name).await {
            Ok(stateful_set) => Some(stateful_set),
            Err(e) => {
                tracing::info!(
                    "Error retrieving StatefulSet {}, treating it as non-existent. Error: {:?}",
                    name,
                    e
                );
                None
            }
        }
    }

    pub(crate) fn k8s_is_update_paused(stateful_set: &StatefulSet) -> bool {
        stateful_set
            .spec
            .update_strategy
            .as_ref()
            .and_then(|update_strategy| update_strategy.rolling_update.as_ref())
            .and_then(|rolling_update| rolling_update.paused)
            .unwrap_or(false)
    }

    /**
     * Return all pods for a given stateful set. K8s does not have an API to do this directly, so here
     * we first fetch all pods based on the app_name, then filter by ownerReferences.
     */
    pub(crate) async fn k8s_get_stateful_set_pods(
        api: &Api<Pod>,
        name: &str,
        app_name: &str,
    ) -> anyhow::Result<Vec<Pod>> {
        let params = ListParams::default().labels(format!("app={}", app_name).as_str());
        let pods = api.list(&params).await?;
        let mut result = Vec::new();
        for pod in pods {
            if let Some(owner_references) = pod.metadata.owner_references.as_ref() {
                if owner_references
                    .iter()
                    .any(|owner| owner.kind == "StatefulSet" && owner.name == name)
                {
                    result.push(pod);
                }
            }
        }
        Ok(result)
    }

    pub(crate) fn k8s_set_stateful_set_paused(
        name: &str,
        stateful_set: &mut StatefulSet,
        paused: bool,
    ) -> anyhow::Result<()> {
        let update_strategy = stateful_set.spec.update_strategy.as_mut().ok_or_else(|| {
            anyhow::anyhow!("StatefulSet {} does not have an update strategy", name)
        })?;
        let rolling_update = update_strategy.rolling_update.as_mut().ok_or_else(|| {
            anyhow::anyhow!("StatefulSet {} does not have a rolling update", name)
        })?;
        rolling_update.paused = Some(paused);
        Ok(())
    }

    pub(crate) async fn k8s_replace_stateful_set(
        api: &Api<StatefulSet>,
        stateful_set: &StatefulSet,
        name: &str,
    ) -> anyhow::Result<()> {
        api.replace(name, &PostParams::default(), stateful_set)
            .await?;

        Ok(())
    }

    pub(crate) async fn k8s_create_or_replace_advanced_stateful_set(
        api: Api<StatefulSet>,
        name: &str,
        data: StatefulSet,
    ) -> anyhow::Result<()> {
        let existing_stateful_set: Option<StatefulSet> =
            Self::k8s_get_stateful_set(&api, name).await;

        // If the StatefulSet is marked as "paused" explicitly, we have a manual operation (likely node pool rotation)
        // going on and the HCC should not touch this StatefulSet until this marker is removed.
        if let Some(sts) = existing_stateful_set.as_ref() {
            if Self::k8s_is_update_paused(sts) {
                return Err(anyhow::anyhow!(
                    "StatefulSet {} is paused, not proceeding with the update",
                    name
                ));
            }
        }

        let mut data = data;
        data.metadata.resource_version =
            existing_stateful_set.and_then(|sts| sts.metadata.resource_version);

        // If current resource is None, then create otherwise replace
        if data.metadata.resource_version.is_none() {
            api.create(&PostParams::default(), &data).await?;
        } else {
            api.replace(name, &PostParams::default(), &data).await?;
        }

        Ok(())
    }

    // Get kubernetes volumes and mounts for hadron kubernete secrets
    // The secrets are deployed using HCC release pipeline in universe repostiory.
    fn get_hadron_volumes_and_mounts(&self, mounts: Vec<K8sMount>) -> K8sSecretVolumesAndMounts {
        let mut volumes = Vec::new();
        let mut volume_mounts = Vec::new();

        for mount in mounts {
            if mount.mount_type == MountType::ConfigMap {
                volumes.push(Volume {
                    name: mount.name.clone(),
                    config_map: Some(ConfigMapVolumeSource {
                        name: mount.name.clone(),
                        ..Default::default()
                    }),
                    ..Default::default()
                });
            } else {
                volumes.push(Volume {
                    name: mount.name.clone(),
                    secret: Some(SecretVolumeSource {
                        secret_name: Some(mount.name.clone()),
                        ..Default::default()
                    }),
                    ..Default::default()
                });
            }

            // Create and add the VolumeMount
            volume_mounts.push(VolumeMount {
                name: mount.name.clone(),
                read_only: Some(true),
                mount_path: mount.mount_path.to_string(),
                ..Default::default()
            });
        }

        K8sSecretVolumesAndMounts {
            volumes,
            volume_mounts,
        }
    }

    pub async fn deploy_compute(&self, pg_compute: PgCompute) -> kube::Result<()> {
        let client = Arc::clone(&self.client);
        let (
            pg_params_compute_namespace,
            pg_params_compute_http_port,
            pg_params_compute_pg_port,
            pg_params_compute_image,
            pg_params_compute_image_pull_secret,
            pg_params_secret_mounts,
            pg_params_prometheus_exporter_image,
        ) = {
            let pg_params = self.pg_params.read().expect("pg_params lock poisoned");
            (
                pg_params.compute_namespace.clone(),
                pg_params
                    .compute_http_port
                    .unwrap_or(pg_params_default_compute_http_port().unwrap()),
                pg_params
                    .compute_pg_port
                    .unwrap_or(pg_params_default_compute_pg_port().unwrap()),
                pg_params.compute_image.clone(),
                pg_params.compute_image_pull_secret.clone(),
                pg_params
                    .compute_mounts
                    .clone()
                    .unwrap_or(pg_params_default_compute_mounts().unwrap()),
                pg_params.prometheus_exporter_image.clone(),
            )
        };

        // echo 0x31 > /proc/self/coredump_filter is to disable dumping shared buffers.
        // Child processes (PG) inherit this setting.
        let launch_command = format!(
            r#"echo 0x31 > /proc/self/coredump_filter
if [ $? -ne 0 ]; then
    echo "Failed to set coredump filter"
    exit 1
fi

shutdown() {{
    echo "Shutting down compute_ctl running at pid $pid"
    kill -TERM $pid
    wait $pid
}}

trap shutdown TERM

/usr/local/bin/compute_ctl --http-port {http_port} \
                           --pgdata "/var/db/postgres/compute" \
                           --connstr "postgresql://cloud_admin@localhost:{pg_port}/postgres" \
                           --compute-id "{compute_id}" \
                           --control-plane-uri "http://{advertised_host}:{advertised_port}/hadron" \
                           --pgbin /usr/local/bin/postgres &

pid=$!

wait
"#,
            http_port = pg_params_compute_http_port,
            pg_port = pg_params_compute_pg_port,
            compute_id = pg_compute.compute_id.clone(),
            advertised_host = self.hcc_dns_name,
            advertised_port = self.hcc_compute_listening_port
        );

        // Choose a dblet node group to use for this compute node based on resource settings.
        let selected_node_group = pg_compute.choose_dblet_node_group();
        let secret_volumes_and_mounts = self.get_hadron_volumes_and_mounts(pg_params_secret_mounts);
        let node_selector = pg_compute.get_node_selector(selected_node_group.clone());
        let tolerations = pg_compute.get_tolerations(selected_node_group.clone());

        let pg_exporter_launch_command = format!(
            r#"#!/bin/sh
# queries.yaml is technically deprecated
# define extra metrics to collect
cat <<EOF > /tmp/queries.yaml
# metric name: pg_backpressure_throttling_time
pg_backpressure:
  query: "SELECT backpressure_throttling_time AS throttling_time FROM neon.backpressure_throttling_time()"
  metrics:
    - throttling_time:
        usage: "COUNTER"
        description: "Total time spent throttling since the system was started."
# metric name: pg_lfc_hits, etc.
# All the MAX(CASE WHEN...) lines are needed to transpose rows into columns.
pg_lfc:
  query: "
WITH lfc_stats AS (
   SELECT stat_name, count
   FROM neon.neon_get_lfc_stats() AS t(stat_name text, count bigint)
)
SELECT
	MAX(count) FILTER (WHERE stat_name = 'file_cache_misses') AS misses,
	MAX(count) FILTER (WHERE stat_name = 'file_cache_hits') AS hits,
	MAX(count) FILTER (WHERE stat_name = 'file_cache_used') AS used,
	MAX(count) FILTER (WHERE stat_name = 'file_cache_writes') AS writes,
	MAX(count) FILTER (WHERE stat_name = 'file_cache_writes_eviction') AS writes_eviction,
	MAX(count) FILTER (WHERE stat_name = 'file_cache_writes_ps_read') AS writes_ps_read,
	MAX(count) FILTER (WHERE stat_name = 'file_cache_writes_extend') AS writes_extend,
	MAX(count) FILTER (WHERE stat_name = 'file_cache_used_pages') AS pages,
	MAX(count) FILTER (WHERE stat_name = 'outstanding_reads') AS outstanding_reads,
	MAX(count) FILTER (WHERE stat_name = 'outstanding_writes') AS outstanding_writes,
	MAX(count) FILTER (WHERE stat_name = 'skipped_writes') AS skipped_writes,
	MAX(count) FILTER (WHERE stat_name = 'file_cache_evictions') AS evictions,
	MAX(count) FILTER (WHERE stat_name = 'cumulative_read_time') AS cumulative_read_time,
	MAX(count) FILTER (WHERE stat_name = 'cumulative_write_time') AS cumulative_write_time,
	MAX(count) FILTER (WHERE stat_name = 'blocks_per_chunk') AS blocks_per_chunk,
	pg_size_bytes(current_setting('neon.file_cache_size_limit')) as size_limit
FROM lfc_stats"
  metrics:
    - misses:
        usage: "COUNTER"
        description: "Number of GetPage@LSN requests not found in LFC. Same as Hadron Storage GetPage QPS."
    - hits:
        usage: "COUNTER"
        description: "Number of GetPage@LSN requests satisfied by the LFC."
    - used:
        usage: "GAUGE"
        description: "Number of chunks in LFC."
    - writes:
        usage: "COUNTER"
        description: "Total number of writes to LFC for any reason."
    - writes_eviction:
        usage: "COUNTER"
        description: "Number of writes to LFC due to buffer pool eviction."
    - writes_ps_read:
        usage: "COUNTER"
        description: "Number of writes to LFC due to page server read."
    - writes_extend:
        usage: "COUNTER"
        description: "Number of writes to LFC due to extending a relation/file."
    - pages:
        usage: "GAUGE"
        description: "Number of live pages in LFC."
    - outstanding_reads:
        usage: "GAUGE"
        description: "Number of outstanding reads IOs."
    - outstanding_writes:
        usage: "GAUGE"
        description: "Number of outstanding write IOs."
    - skipped_writes:
        usage: "COUNTER"
        description: "Number of LFC writes skipped to to too many outstanding IOs."
    - evictions:
        usage: "COUNTER"
        description: "Number of LFC evictions."
    - cumulative_read_time:
        usage: "COUNTER"
        description: "Cumulative time spent reading from LFC."
    - cumulative_write_time:
        usage: "COUNTER"
        description: "Cumulative time spent writing to LFC."
    - blocks_per_chunk:
        usage: "GAUGE"
        description: "Number of pages/blocks per chunk."
    - size_limit:
        usage: "GAUGE"
        description: "Currently set LFC max size in bytes (dynamically configurable)."
# metric name: pg_lfc_working_set_size{{duration=1m, 5m, 15m, 1h}}
pg_lfc_working_set:
  query: "
    select
	    x as duration,
	    neon.approximate_working_set_size_seconds(extract('epoch' from x::interval)::int)::bigint*8192 as size
    from (values ('1m'), ('5m'),('15m'),('1h')) as t (x)"
  metrics:
    - duration:
        usage: "LABEL"
        description: "Estimation window."
    - size:
        usage: "GAUGE"
        description: "Estimated working set size in bytes."
# metric name: pg_writable_bool
pg_writable:
  query: "SELECT health_check_write_succeeds AS bool FROM health_check_write_succeeds()"
  metrics:
    - bool:
        usage: "GAUGE"
        description: "Whether the last write probe succeeded (1 for yes, 0 for no)."
# metric name: pg_cluster_size_bytes
pg_cluster_size:
  query: "SELECT pg_cluster_size AS bytes FROM neon.pg_cluster_size()"
  metrics:
    - bytes:
        usage: "GAUGE"
        description: "Hadron logical size."
# metric name: pg_snapshot_files_count
pg_snapshot_files:
  query: "SELECT COUNT(*) AS count FROM pg_ls_dir('/var/db/postgres/compute/pg_logical/snapshots/') WHERE pg_ls_dir LIKE '%.snap'"
  cache_seconds: 120
  metrics:
    - count:
        usage: "GAUGE"
        description: "Number of .snap files currently accumulated on the Postgres compute node"
# metric name: getpage, data_corruptions, etc.
# All the MAX(CASE WHEN...) lines are needed to transpose rows into columns.
pg_metrics:
  query: "
WITH pg_perf_counters AS (
   SELECT metric, value
   FROM neon.neon_perf_counters
)
SELECT
	MAX(value) FILTER (WHERE metric = 'sql_index_corruption_count') AS sql_index_corruption_count,
    MAX(value) FILTER (WHERE metric = 'sql_data_corruption_count') AS sql_data_corruption_count,
    MAX(value) FILTER (WHERE metric = 'sql_internal_error_count') AS sql_internal_error_count,
    MAX(value) FILTER (WHERE metric = 'ps_corruption_detected') AS ps_corruption_detected,
    MAX(value) FILTER (WHERE metric = 'getpage_wait_seconds_count') AS getpage_wait_seconds_count,
    MAX(value) FILTER (WHERE metric = 'getpage_wait_seconds_sum') AS getpage_wait_seconds_sum,
    MAX(value) FILTER (WHERE metric = 'file_cache_read_wait_seconds_count') AS file_cache_read_wait_seconds_count,
    MAX(value) FILTER (WHERE metric = 'file_cache_read_wait_seconds_sum') AS file_cache_read_wait_seconds_sum,
    MAX(value) FILTER (WHERE metric = 'file_cache_write_wait_seconds_count') AS file_cache_write_wait_seconds_count,
    MAX(value) FILTER (WHERE metric = 'file_cache_write_wait_seconds_sum') AS file_cache_write_wait_seconds_sum,
    MAX(value) FILTER (WHERE metric = 'pageserver_disconnects_total') AS pageserver_disconnects_total,
    MAX(value) FILTER (WHERE metric = 'pageserver_open_requests') AS pageserver_open_requests,
    MAX(value) FILTER (WHERE metric = 'num_active_safekeepers') AS num_active_safekeepers,
    MAX(value) FILTER (WHERE metric = 'num_configured_safekeepers') AS num_configured_safekeepers,
    MAX(value) FILTER (WHERE metric = 'max_active_safekeeper_commit_lag') AS max_active_safekeeper_commit_lag
FROM pg_perf_counters"
  metrics:
    - sql_index_corruption_count:
        usage: "COUNTER"
        description: "Number of index corruption errors."
    - sql_data_corruption_count:
        usage: "COUNTER"
        description: "Number of data corruption errors."
    - sql_internal_error_count:
        usage: "COUNTER"
        description: "Number of internal errors."
    - ps_corruption_detected:
        usage: "COUNTER"
        description: "Number of page server corruption errors."
    - getpage_wait_seconds_count:
        usage: "COUNTER"
        description: "Number of GetPage@LSN waits."
    - getpage_wait_seconds_sum:
        usage: "COUNTER"
        description: "Number of GetPage@LSN wait seconds."
    - file_cache_read_wait_seconds_count:
        usage: "COUNTER"
        description: "Number of file cache read waits."
    - file_cache_read_wait_seconds_sum:
        usage: "COUNTER"
        description: "Number of file cache read wait seconds."
    - file_cache_write_wait_seconds_count:
        usage: "COUNTER"
        description: "Number of file cache write waits."
    - file_cache_write_wait_seconds_sum:
        usage: "COUNTER"
        description: "Number of file cache write wait seconds."
    - pageserver_disconnects_total:
        usage: "COUNTER"
        description: "Number of page server disconnects."
    - pageserver_open_requests:
        usage: "GAUGE"
        description: "Number of open page server requests."
    - num_active_safekeepers:
        usage: "GAUGE"
        description: "Number of active safekeepers."
    - num_configured_safekeepers:
        usage: "GAUGE"
        description: "Number of configured safekeepers."
    - max_active_safekeeper_commit_lag:
        usage: "GAUGE"
        description: "Maximum commit lag (in LSN bytes) among active safekeepers."
EOF

# constantLabels is technically deprecated
/bin/postgres_exporter --constantLabels=pg_endpoint={0},pg_compute_id={1},pg_instance_id={2} \
                        --extend.query-path="/tmp/queries.yaml" --no-collector.database"#,
            pg_compute.name.clone(),
            pg_compute.k8s_label_compute_id(),
            pg_compute
                .instance_id
                .clone()
                .unwrap_or(DEFAULT_INSTANCE_ID.to_string()),
        );

        // Optionally defines a readiness probe that can be used by Kubernetes to when the container
        // is ready to start serving traffic. Note that we use a readiness probe rather than startup
        // or liveness check as Kubernetes will kill/restart containers if it fails those checks but
        // if we use a readiness check it will just stop directing traffic away from it while keeping
        // the container running. We want to have this behaviour as we want our compute manager, rather
        // than Kubernetes, to be responsible for killing/restarting compute.
        let readiness_probe = pg_compute
            .readiness_probe
            .clone()
            .map(|readiness_probe_params| Probe {
                failure_threshold: Some(readiness_probe_params.failure_threshold),
                http_get: Some(HTTPGetAction {
                    path: Some(readiness_probe_params.endpoint_path.clone()),
                    port: IntOrString::Int(pg_params_compute_http_port as i32),
                    ..Default::default()
                }),
                initial_delay_seconds: Some(readiness_probe_params.initial_delay_seconds),
                period_seconds: Some(readiness_probe_params.period_seconds),
                success_threshold: Some(readiness_probe_params.success_threshold),
                timeout_seconds: Some(readiness_probe_params.timeout_seconds),
                ..Default::default()
            });

        let pod_template_spec = PodTemplateSpec {
                    metadata: Some(meta::v1::ObjectMeta {
                        annotations: Some(
                            vec![
                                (
                                    // This annotation is consumed by networking components to allow egress traffic
                                    // from this compute Pod to the workspace URL/control plane. The networking components
                                    // require that the "URL" to be just the hostname, without any schemes or trailing "/".
                                    "databricks.com/workspace-url".to_string(),
                                    pg_compute.workspace_url.clone()
                                      .map(|url| url.host_str().unwrap_or_default().to_string())
                                      .unwrap_or("".to_string()),
                                ),
                                ("enableLogDaemon".to_string(), "true".to_string()),
                            ]
                                .into_iter()
                                .filter(|(_, v)| !v.is_empty())
                                .collect(),
                        ),
                        labels: Some(
                            vec![
                                ("app".to_string(), pg_compute.name.clone()),
                                (
                                    "dblet.dev/appid".to_string(),
                                    pg_compute.k8s_label_compute_id(),
                                ),
                                ("dblet.dev/managed".to_string(), "true".to_string()),
                                ("orgId".to_string(), pg_compute.workspace_id.clone().unwrap_or_default()),
                                ("hadron-component".to_string(), "compute".to_string()),
                                (COMPUTE_ID_LABEL_KEY.to_string(), pg_compute.k8s_label_compute_id()),
                                (INSTANCE_ID_LABEL_KEY.to_string(), pg_compute.instance_id.clone().unwrap_or("".to_string())),
                            ]
                            .into_iter()
                            .filter(|(_, v)| !v.is_empty())
                            .collect(),
                        ),
                        ..Default::default()
                    }),
                    spec: Some(PodSpec {
                        // Pod Anti Affinity: Do not schedule to nodes already have compute pods running.
                        // dblet nodes are single-compute. They are single-tenant and use NodePort for
                        // Pod networking, so running multiple compute Pods on the same node is not
                        // going to work.
                        affinity: Some(Affinity {
                            pod_anti_affinity: Some(PodAntiAffinity {
                                required_during_scheduling_ignored_during_execution: Some(vec![
                                    PodAffinityTerm {
                                        label_selector: Some(meta::v1::LabelSelector {
                                            match_labels: Some(
                                                vec![(
                                                    "hadron-component".to_string(),
                                                    "compute".to_string(),
                                                )]
                                                .into_iter()
                                                .collect(),
                                            ),
                                            ..Default::default()
                                        }),
                                        topology_key: "kubernetes.io/hostname".to_string(),
                                        ..Default::default()
                                    },
                                ]),
                                ..Default::default()
                            }),
                            ..Default::default()
                        }),
                        node_selector: Some(node_selector),
                        tolerations: Some(tolerations),
                        image_pull_secrets: pg_params_compute_image_pull_secret.map(
                            |secret_name| {
                                vec![LocalObjectReference {
                                    name: secret_name,
                                }]
                            },
                        ),
                        priority_class_name: Some("pg-compute".to_string()),
                        init_containers: Some(vec![Container {
                            name: "compute-local-ssd-init".to_string(),
                            image: Some(
                                pg_compute
                                    .image_override
                                    .clone()
                                    .unwrap_or_else(|| pg_params_compute_image.clone()),
                            ),
                            // Copy the pgdata dir to the mounted local SSD to transparently
                            // initialize any existing PG data/metadata set in the docker
                            // image before the compute container starts. Use `mv` with the
                            // `-f` flag in place of `cp` to overwrite any existing data explicitly.
                            //
                            // While root, create brickstore dir for logs. dbctl runs as root group, so need to give
                            // at least 550 so that it can cd to serve dbctl app-logs. Need at least 007 so that
                            // PG can create new log files to write logs to in the directory, so 777 is most sensible.
                            //
                            // While we are running as root, also configure the *hosts* core dump settings.
                            // In vanilla neon, `compute_ctl` expects core dumps to be written to the PG data directory
                            // in a specific format as mentioned in https://github.com/neondatabase/autoscaling/issues/784.
                            // Since `compute_ctl` wipes the data dir across container restarts, we have patched it to
                            // read core dump files from a fixed path (`/databricks/logs/brickstore/core`) instead,
                            // since that will *always* persist container restarts.
                            command: Some(vec!["/bin/bash".to_string(), "-c".to_string()]),
                            args: Some(vec![
                               "rm -rf /local_ssd/compute && mv /var/db/postgres/compute /local_ssd/ && \
                                mkdir -p -m 777 /databricks/logs/brickstore && \
                                echo '/databricks/logs/brickstore/core' > /proc/sys/kernel/core_pattern && \
                                echo '1' > /proc/sys/kernel/core_uses_pid".to_string(),
                            ]),
                            volume_mounts: Some(vec![VolumeMount {
                                name: "local-ssd".to_string(),
                                mount_path: "/local_ssd".to_string(),
                                ..Default::default()
                            },VolumeMount {
                                name: "logs".to_string(),
                                mount_path: "/databricks/logs".to_string(),
                                ..Default::default()
                            }]),
                            security_context: Some(SecurityContext {
                                // The init container runs as root so that it can create the pgdata directory.
                                // The compute container runs as the postgres user (and should not
                                // run as the root user) afterwards using an explicit subdir mount
                                // on /local_ssd/compute for file isolation purposes (the dblet
                                // host may leverage this local disk for image layer storage, etc).
                                privileged: Some(true),
                                run_as_user: Some(0),
                                ..Default::default()
                            }),
                            ..Default::default()
                        }]),
                        containers: vec![Container {
                            name: "compute".to_string(),
                            image: Some(
                                pg_compute
                                    .image_override
                                    .clone()
                                    .unwrap_or_else(|| pg_params_compute_image.clone()),
                            ),
                            ports: Some(vec![
                                ContainerPort {
                                    container_port: pg_params_compute_pg_port as i32,
                                    ..Default::default()
                                },
                                ContainerPort {
                                    container_port: pg_params_compute_http_port as i32,
                                    ..Default::default()
                                },
                            ]),
                            env: Some({
                                let mut env = vec![EnvVar {
                                    name: "NEON_CONTROL_PLANE_TOKEN".to_string(),
                                    value: Some(pg_compute.control_plane_token.clone()),
                                    ..Default::default()
                                }];
                                if pg_compute.instance_id.is_some() {
                                    env.push(EnvVar {
                                        name: "INSTANCE_ID".to_string(),
                                        value: pg_compute.instance_id.clone(),
                                        ..Default::default()
                                    });
                                };
                                env
                            }),
                            resources: Some(
                                pg_compute
                                    .resources
                                    .clone()
                            ),
                            command: Some(vec!["/bin/sh".to_string(), "-c".to_string()]),
                            args: Some(vec![launch_command]),
                            volume_mounts: Some(
                                [
                                    &secret_volumes_and_mounts.volume_mounts[..],
                                    &[VolumeMount {
                                        name: "local-ssd".to_string(),
                                        // Explicitly mount over the PGDATA dir with the pre-initialized
                                        // local-SSD path.
                                        mount_path: "/var/db/postgres/compute".to_string(),
                                        // Important that we mount the compute subpath here to avoid giving
                                        // pg visibility to any other data that may exist on the local disk.
                                        sub_path: Some("compute".to_string()),
                                        ..Default::default()
                                    },VolumeMount {
                                        name: "logs".to_string(),
                                        mount_path: "/databricks/logs".to_string(),
                                        ..Default::default()
                                    },VolumeMount {
                                        // Default shared memory size is 64MB, which is not enough for PG.
                                        // See https://stackoverflow.com/questions/43373463/how-to-increase-shm-size-of-a-kubernetes-container-shm-size-equivalent-of-doc.
                                        name: "dshm".to_string(),
                                        mount_path: "/dev/shm".to_string(),
                                        ..Default::default()
                                    }],
                                ]
                                .concat(),
                            ),
                            readiness_probe,
                            ..Default::default()
                        }, Container {
                            name: "prometheus-exporter".to_string(),
                            image: Some(
                                pg_compute
                                    .exporter_image_override.clone()
                                    .unwrap_or_else(|| pg_params_prometheus_exporter_image.clone()),
                            ),
                            ports: Some(vec![
                                ContainerPort {
                                    container_port: 9187,
                                    name: Some("info-service".to_string()),
                                    ..Default::default()
                                }
                            ]),
                            env: Some(vec![EnvVar {
                                name: "DATA_SOURCE_NAME".to_string(),
                                value: Some("user=databricks_monitor host=127.0.0.1 port=55432 sslmode=disable database=databricks_system".to_string()),
                                ..Default::default()
                            }]),
                            resources: Some(ResourceRequirements {
                                    requests: Some(BTreeMap::from([
                                        ("cpu".to_string(), Quantity("50m".to_string())),
                                        ("memory".to_string(), Quantity("128Mi".to_string())),
                                    ])),
                                    limits: Some(BTreeMap::from([
                                        ("cpu".to_string(), Quantity("200m".to_string())),
                                        ("memory".to_string(), Quantity("128Mi".to_string())),
                                    ])),
                                    ..Default::default()
                                }),
                            command: Some(vec!["/bin/sh".to_string(), "-c".to_string()]),
                            args: Some(vec![pg_exporter_launch_command]),
                            volume_mounts: Some(secret_volumes_and_mounts.volume_mounts),
                            ..Default::default()
                        }, Container {
                            name: "pg-log-redactor".to_string(),
                            image: Some(
                                pg_compute
                                    .image_override
                                    .clone()
                                    .unwrap_or_else(|| pg_params_compute_image.clone()),
                            ),
                            resources: Some(ResourceRequirements {
                                    requests: Some(BTreeMap::from([
                                        ("cpu".to_string(), Quantity("50m".to_string())),
                                        ("memory".to_string(), Quantity("128Mi".to_string())),
                                    ])),
                                    limits: Some(BTreeMap::from([
                                        ("cpu".to_string(), Quantity("200m".to_string())),
                                        ("memory".to_string(), Quantity("128Mi".to_string())),
                                    ])),
                                    ..Default::default()
                                }),
                            command: Some(vec!["python3".to_string()]),
                            args: Some(vec!["/usr/local/bin/pg_log_redactor.py".to_string()]),
                            volume_mounts: Some(
                                vec![VolumeMount {
                                    name: "logs".to_string(),
                                    mount_path: "/databricks/logs".to_string(),
                                    ..Default::default()
                                }],
                            ),
                            ..Default::default()
                        }],
                        volumes: Some(
                            [
                                &secret_volumes_and_mounts.volumes[..],
                                &[Volume {
                                    name: "local-ssd".to_string(),
                                    host_path: Some(HostPathVolumeSource {
                                        path: "/local_disk0".to_string(),
                                        // Require the directory to exist beforehand. We could in
                                        // theory set this to `DirectoryOrCreate`, but that might require
                                        // the entire pod to run as root in some cases, which we want to avoid.
                                        type_: Some("Directory".to_string()),
                                    }),
                                    ..Default::default()
                                },Volume {
                                    name: "logs".to_string(),
                                    // Empty dir means we lose local logs on pod restart. This is ok because
                                    // log-daemon backs them up.
                                    empty_dir: Some(EmptyDirVolumeSource::default()),
                                    ..Default::default()
                                },Volume {
                                    // Default shared memory size is 64MB, which is not enough for PG.
                                    // See https://stackoverflow.com/questions/43373463/how-to-increase-shm-size-of-a-kubernetes-container-shm-size-equivalent-of-doc.
                                    name: "dshm".to_string(),
                                    empty_dir: Some(EmptyDirVolumeSource { medium: Some("Memory".to_string()), size_limit: None }),
                                    ..Default::default()
                                }],
                            ]
                            .concat(),
                        ),
                        ..Default::default()
                    }),
                };

        let admin_service = Service {
            metadata: meta::v1::ObjectMeta {
                name: Some(format!("{}-admin", &pg_compute.name)),
                ..Default::default()
            },
            spec: Some(ServiceSpec {
                ports: Some(vec![ServicePort {
                    port: 80,
                    target_port: Some(IntOrString::Int(pg_params_compute_http_port as i32)),
                    ..Default::default()
                }]),
                type_: Some("ClusterIP".to_string()),
                selector: Some(
                    vec![("app".to_string(), pg_compute.name.clone())]
                        .into_iter()
                        .collect(),
                ),
                ..Default::default()
            }),
            ..Default::default()
        };

        tracing::info!("Deploying compute node {:?}", pg_compute.name);
        tracing::debug!("Compute Admin Service: {:?}", admin_service);

        match pg_compute.model {
            ComputeModel::PrivatePreview => {
                let lb_service = self
                    .build_loadbalancer_service(pg_params_compute_pg_port, pg_compute.name.clone());
                tracing::debug!("PrPr LB Service: {:?}", lb_service);

                self.create_k8s_deployment(
                    &client,
                    &pg_compute,
                    pg_params_compute_namespace.clone(),
                    pod_template_spec,
                )
                .await?;
                Self::k8s_create_or_replace(
                    Api::namespaced((*client).clone(), pg_params_compute_namespace.as_str()),
                    &pg_compute.name,
                    lb_service,
                )
                .await?;
                Self::k8s_create_or_replace(
                    Api::namespaced((*client).clone(), pg_params_compute_namespace.as_str()),
                    &admin_service.metadata.name.clone().unwrap(),
                    admin_service,
                )
                .await
            }
            ComputeModel::PublicPreview => {
                self.create_k8s_replica_set(
                    &client,
                    &pg_compute,
                    pg_params_compute_namespace.clone(),
                    pod_template_spec,
                )
                .await?;
                // Note that in the Public Preview model, we don't create any k8s Service objects here to handle postgres protocol
                // ingress. The ingress Service objects are created via trait methods `create_or_patch_cluster_primary_ingress_service()`
                // and `create_or_patch_instance_ingress_service()`, invoked directly from reconcilers in `compute_manager/src/reconcilers`.
                Self::k8s_create_or_replace(
                    Api::namespaced((*client).clone(), pg_params_compute_namespace.as_str()),
                    &admin_service.metadata.name.clone().unwrap(),
                    admin_service,
                )
                .await
            }
        }
    }

    fn allow_not_found_error<K>(
        result: kube::Error,
    ) -> kube::Result<Either<K, kube::core::Status>> {
        match result {
            kube::Error::Api(e) if e.code == 404 => {
                Ok(Either::Right(kube::core::Status::success()))
            }
            e => Err(e),
        }
    }

    fn get_hadron_compute_spec_metadata(&self, name: String) -> meta::v1::ObjectMeta {
        meta::v1::ObjectMeta {
            name: Some(name),
            annotations: Some(
                vec![("hadron.dev/managed".to_string(), "true".to_string())]
                    .into_iter()
                    .collect(),
            ),
            ..Default::default()
        }
    }

    fn get_hadron_compute_label_selector(&self, name: String) -> meta::v1::LabelSelector {
        meta::v1::LabelSelector {
            match_labels: Some(vec![("app".to_string(), name)].into_iter().collect()),
            ..Default::default()
        }
    }

    async fn create_k8s_deployment(
        &self,
        client: &Client,
        pg_compute: &PgCompute,
        pg_params_compute_namespace: String,
        pod_template_spec: PodTemplateSpec,
    ) -> kube::Result<()> {
        let deployment = Deployment {
            metadata: self.get_hadron_compute_spec_metadata(pg_compute.name.clone()),
            spec: Some(DeploymentSpec {
                replicas: Some(1),
                selector: self.get_hadron_compute_label_selector(pg_compute.name.clone()),
                template: pod_template_spec,
                ..Default::default()
            }),
            ..Default::default()
        };

        tracing::debug!("K8s deployment: {:?}", deployment);

        Self::k8s_create_or_replace(
            Api::namespaced((*client).clone(), pg_params_compute_namespace.as_str()),
            &pg_compute.name,
            deployment,
        )
        .await
    }

    async fn create_k8s_replica_set(
        &self,
        client: &Client,
        pg_compute: &PgCompute,
        pg_params_compute_namespace: String,
        pod_template_spec: PodTemplateSpec,
    ) -> kube::Result<()> {
        let replica_set = ReplicaSet {
            metadata: self.get_hadron_compute_spec_metadata(pg_compute.name.clone()),
            spec: Some(ReplicaSetSpec {
                replicas: Some(1),
                selector: self.get_hadron_compute_label_selector(pg_compute.name.clone()),
                template: Some(pod_template_spec),
                ..Default::default()
            }),
            ..Default::default()
        };

        tracing::debug!("K8s replica set: {:?}", replica_set);

        Self::k8s_create_or_replace(
            Api::namespaced((*client).clone(), pg_params_compute_namespace.as_str()),
            &pg_compute.name,
            replica_set,
        )
        .await
    }

    fn get_loadbalancer_annotations(
        &self,
        dns_name_hint: &str,
    ) -> Option<BTreeMap<String, String>> {
        match self.cloud_provider {
            Some(CloudProvider::AWS) => {
                Some(
                    vec![
                        // AWS specific annotations.
                        // xref https://kubernetes-sigs.github.io/aws-load-balancer-controller/v2.2/guide/service/annotations/
                        (
                            "service.beta.kubernetes.io/aws-load-balancer-type".to_string(),
                            "external".to_string(),
                        ),
                        (
                            "service.beta.kubernetes.io/aws-load-balancer-nlb-target-type"
                                .to_string(),
                            "ip".to_string(),
                        ),
                        (
                            "service.beta.kubernetes.io/aws-load-balancer-scheme".to_string(),
                            "internet-facing".to_string(),
                        ),
                    ]
                    .into_iter()
                    .collect(),
                )
            }
            Some(CloudProvider::Azure) => {
                Some(
                    vec![
                        // Azure specific annotations.
                        // xref https://cloud-provider-azure.sigs.k8s.io/topics/loadbalancer/#loadbalancer-annotations
                        (
                            "service.beta.kubernetes.io/azure-dns-label-name".to_string(),
                            dns_name_hint.to_string(),
                        ),
                        (
                            "service.beta.kubernetes.io/azure-load-balancer-internal".to_string(),
                            "false".to_string(),
                        ),
                    ]
                    .into_iter()
                    .collect(),
                )
            }
            _ => None,
        }
    }

    fn build_loadbalancer_service(
        &self,
        pg_params_compute_pg_port: u16,
        pg_compute_name: String,
    ) -> Service {
        if std::env::var("HADRON_STRESS_TEST_MODE").is_ok() {
            // In stress test mode, we deploy lots of computes, so we don't want to create LoadBalancer Service.
            // Instead, we create ClusterIP Services and deploy benchmark Pods within the cluster.
            return Service {
                metadata: meta::v1::ObjectMeta {
                    name: Some(pg_compute_name.clone()),
                    ..Default::default()
                },
                spec: Some(ServiceSpec {
                    ports: Some(vec![ServicePort {
                        port: 5432,
                        target_port: Some(IntOrString::Int(pg_params_compute_pg_port as i32)),
                        name: Some("postgres".to_string()),
                        ..Default::default()
                    }]),
                    type_: Some("ClusterIP".to_string()),
                    selector: Some(
                        vec![("app".to_string(), pg_compute_name)]
                            .into_iter()
                            .collect(),
                    ),
                    ..Default::default()
                }),
                ..Default::default()
            };
        }

        Service {
            metadata: meta::v1::ObjectMeta {
                name: Some(pg_compute_name.clone()),
                annotations: self.get_loadbalancer_annotations(pg_compute_name.as_str()),
                ..Default::default()
            },
            spec: Some(ServiceSpec {
                ports: Some(vec![ServicePort {
                    port: 5432,
                    target_port: Some(IntOrString::Int(pg_params_compute_pg_port as i32)),
                    name: Some("postgres".to_string()),
                    ..Default::default()
                }]),
                type_: Some("LoadBalancer".to_string()),
                selector: Some(
                    vec![("app".to_string(), pg_compute_name)]
                        .into_iter()
                        .collect(),
                ),
                external_traffic_policy: match self.cloud_provider {
                    Some(CloudProvider::AWS) => Some("Local".to_string()),
                    Some(CloudProvider::Azure) => Some("Cluster".to_string()),
                    _ => Some("Local".to_string()),
                },
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    // A helper to check if a resource has been deleted.
    async fn is_resource_deleted<T>(api: &Api<T>, name: &str) -> kube::Result<bool>
    where
        T: kube::Resource + Clone + DeserializeOwned + Debug,
    {
        match api.get(name).await {
            Err(kube::Error::Api(e)) if e.code == 404 => Ok(true),
            Err(e) => Err(e),
            Ok(_) => Ok(false),
        }
    }

    pub async fn delete_compute(
        &self,
        compute_name: &str,
        model: ComputeModel,
    ) -> kube::Result<bool> {
        let pg_params_compute_namespace = self
            .pg_params
            .read()
            .expect("pg_param lock poisoned")
            .compute_namespace
            .clone();

        let client = Arc::clone(&self.client);
        let service_api: Api<Service> =
            Api::namespaced((*client).clone(), pg_params_compute_namespace.as_str());

        tracing::info!("Deleting resources for compute {compute_name}");
        match model {
            ComputeModel::PrivatePreview => {
                let deployment_api: Api<Deployment> =
                    Api::namespaced((*client).clone(), pg_params_compute_namespace.as_str());
                deployment_api
                    .delete(compute_name, &DeleteParams::default())
                    .await
                    .or_else(Self::allow_not_found_error)?;

                // Azure load balancer services require special attention when deleting since
                // we leverage the `azure-dns-label-name` annotation to generate the DNS name.
                // On Azure, we have to wipe out this annotation before deleting the service,
                // in order to ensure that the reserved DNS name (and its associated IP address)
                // is properly cleaned. Note that its OK to make this update right before deletion
                // (the AKS LB controller is designed to handle this kind of cleanup in one shot).
                if self.cloud_provider == Some(CloudProvider::Azure) {
                    let service = service_api.get(compute_name).await?;
                    let mut service = service.clone();
                    if let Some(annotations) = service.metadata.annotations.as_mut() {
                        annotations.insert(
                            "service.beta.kubernetes.io/azure-dns-label-name".to_string(),
                            "".to_string(),
                        );
                    }
                    service_api
                        .replace(compute_name, &PostParams::default(), &service)
                        .await?;
                }

                service_api
                    .delete(compute_name, &DeleteParams::default())
                    .await
                    .or_else(Self::allow_not_found_error)?;
            }
            ComputeModel::PublicPreview => {
                let replica_set_api: Api<ReplicaSet> =
                    Api::namespaced((*client).clone(), pg_params_compute_namespace.as_str());
                replica_set_api
                    .delete(compute_name, &DeleteParams::default())
                    .await
                    .or_else(Self::allow_not_found_error)?;
            }
        }

        // Delete the admin service resource.
        let admin_name = format!("{}-admin", compute_name);
        service_api
            .delete(&admin_name, &DeleteParams::default())
            .await
            .or_else(Self::allow_not_found_error)?;

        // Verification: Check that all resources have been removed.
        let mut all_deleted = true;

        match model {
            ComputeModel::PrivatePreview => {
                // Check the Deployment and Service for the compute_name.
                let deployment_api: Api<Deployment> =
                    Api::namespaced((*client).clone(), pg_params_compute_namespace.as_str());
                let deployment_deleted =
                    Self::is_resource_deleted(&deployment_api, compute_name).await?;
                all_deleted &= deployment_deleted;

                let service_deleted = Self::is_resource_deleted(&service_api, compute_name).await?;
                all_deleted &= service_deleted;
            }
            ComputeModel::PublicPreview => {
                // Check the ReplicaSet for the compute_name.
                let replica_set_api: Api<ReplicaSet> =
                    Api::namespaced((*client).clone(), pg_params_compute_namespace.as_str());
                let replicaset_deleted =
                    Self::is_resource_deleted(&replica_set_api, compute_name).await?;
                all_deleted &= replicaset_deleted;
            }
        }

        // Check the admin service deletion.
        let admin_deleted = Self::is_resource_deleted(&service_api, &admin_name).await?;
        all_deleted &= admin_deleted;

        Ok(all_deleted)
    }

    pub async fn get_compute_connection_info(
        &self,
        compute_name: &str,
    ) -> Option<PostgresConnectionInfo> {
        let pg_params_compute_namespace = self
            .pg_params
            .read()
            .expect("pg_param lock poisoned")
            .compute_namespace
            .clone();
        let service_api: Api<Service> =
            Api::namespaced((*self.client).clone(), pg_params_compute_namespace.as_str());
        match service_api.get(compute_name).await {
            Ok(svc) => svc
                .status
                .as_ref()
                .and_then(|status| status.load_balancer.as_ref())
                .and_then(|lb| lb.ingress.as_ref())
                .and_then(|ingress| ingress.first())
                .map(|ingress| PostgresConnectionInfo {
                    // On Azure, ingress.ip is set instead of ingress.hostname. In that case,
                    // return the DNS name generated by the `azure-dns-label-name` annotation,
                    // since the raw IP address isn't ideal for clients.
                    host: ingress.hostname.clone().map_or_else(
                        || {
                            if ingress.ip.is_some()
                                && self.cloud_provider == Some(CloudProvider::Azure)
                            {
                                // It is important that we ensure that ingress.ip is in fact set
                                // to avoid returning an incorrect/broken hostname during LB creation.
                                Some(format!(
                                    "{}.{}.cloudapp.azure.com",
                                    compute_name, self.region
                                ))
                            } else {
                                None
                            }
                        },
                        Some,
                    ),
                    port: svc.spec.as_ref().and_then(|spec| {
                        spec.ports.as_ref().and_then(|ports| {
                            ports.first().map(|port: &ServicePort| port.port as u16)
                        })
                    }),
                }),
            Err(e) => {
                // TODO: Figure out if there is a better way to match 404 errors. Ideally we still want to surface
                // other errors to the caller.
                tracing::error!("Failed to get service {:?}: {:?}", compute_name, e);
                None
            }
        }
    }

    /// This function returns additional settings that will be injected into ComputeSpec when deploying a compute node.
    pub async fn get_databricks_compute_settings(
        &self,
        workspace_url: Option<Url>,
    ) -> DatabricksSettings {
        let pg_params = self.pg_params.read().expect("pg_param lock poisoned");

        let pg_compute_tls_settings = pg_params
            .pg_compute_tls_settings
            .clone()
            .unwrap_or(pg_params_pg_compute_tls_settings().unwrap());

        let databricks_pg_hba = pg_params
            .databricks_pg_hba
            .clone()
            .unwrap_or(pg_params_default_databricks_pg_hba().unwrap());

        let databricks_pg_ident = pg_params
            .databricks_pg_ident
            .clone()
            .unwrap_or(pg_params_default_databricks_pg_ident().unwrap());

        DatabricksSettings {
            pg_compute_tls_settings,
            databricks_pg_hba,
            databricks_pg_ident,
            databricks_workspace_host: workspace_url
                .and_then(|url| url.host_str().map(|s| s.to_string()))
                .unwrap_or("".to_string()),
        }
    }

    // Functions manipulating k8s Service objects used for the intra-cluster last-leg DpApiProxy -> Postgres
    // routing (a.k.a. ingress services).

    // Constructs the name of the primary ingress k8s Service object of a Hadron database instance.
    // DpApiProxy selects this service as the "upstream" for `instance-$instance_id.database.$TLD` SNI matches.
    fn instance_primary_ingress_service_name(instance_id: Uuid) -> String {
        format!("instance-{}", instance_id)
    }

    fn instance_read_only_ingress_service_name(instance_id: Uuid) -> String {
        format!("instance-ro-{}", instance_id)
    }

    // Gets a k8s Service object by name, in the namespace where PG computes are deployed.
    async fn get_ingress_service(&self, service_name: &str) -> kube::Result<Service> {
        let pg_params_compute_namespace = self
            .pg_params
            .read()
            .expect("pg_params lock poisoned")
            .compute_namespace
            .clone();

        let api: Api<Service> =
            Api::namespaced((*self.client).clone(), pg_params_compute_namespace.as_str());
        api.get(service_name).await
    }

    // Idempotently create the readable secondary ingress services.
    async fn create_if_not_exists_readable_secondary_ingress(
        &self,
        service_name: String,
        instance_id: Uuid,
    ) -> kube::Result<Service> {
        let (pg_params_compute_namespace, pg_params_compute_pg_port) = {
            let pg_params = self.pg_params.read().expect("pg_param lock poisoned");
            (
                pg_params.compute_namespace.clone(),
                pg_params
                    .compute_pg_port
                    .unwrap_or(pg_params_default_compute_pg_port().unwrap()),
            )
        };

        // We match every compute in the compute_pool with a mode equal to secondary.
        let ingress_service_selector = Some(
            vec![
                (INSTANCE_ID_LABEL_KEY.to_string(), instance_id.to_string()),
                (COMPUTE_SECONDARY_LABEL_KEY.to_string(), true.to_string()),
            ]
            .into_iter()
            .collect(),
        );

        let ingress_service_type = Some("ClusterIP".to_string());

        let ingress_service = Service {
            metadata: meta::v1::ObjectMeta {
                name: Some(service_name.to_string()),
                annotations: None,
                ..Default::default()
            },
            spec: Some(ServiceSpec {
                selector: ingress_service_selector.clone(),
                ports: Some(vec![ServicePort {
                    port: 5432,
                    target_port: Some(IntOrString::Int(pg_params_compute_pg_port as i32)),
                    name: Some("postgres".to_string()),
                    protocol: Some("TCP".to_string()),
                    ..Default::default()
                }]),
                type_: ingress_service_type.clone(),
                external_traffic_policy: None,
                ..Default::default()
            }),
            ..Default::default()
        };

        let api: Api<Service> =
            Api::namespaced((*self.client).clone(), pg_params_compute_namespace.as_str());
        match api.get(&service_name).await {
            // If the service exists, no-op
            Ok(service) => Ok(service),

            // If the service does not exist, create it.
            Err(kube::Error::Api(e)) if e.code == 404 => {
                api.create(&PostParams::default(), &ingress_service).await
            }
            Err(e) => Err(e),
        }
    }

    // Idempotently create or patch a k8s Service object with the given name to route traffic to the specified compute.
    async fn create_or_patch_ingress_service(
        &self,
        service_name: &str,
        compute_id: Uuid,
        service_type: K8sServiceType,
    ) -> kube::Result<Service> {
        let (pg_params_compute_namespace, pg_params_compute_pg_port) = {
            let pg_params = self.pg_params.read().expect("pg_param lock poisoned");
            (
                pg_params.compute_namespace.clone(),
                pg_params
                    .compute_pg_port
                    .unwrap_or(pg_params_default_compute_pg_port().unwrap()),
            )
        };

        let ingress_service_selector = Some(
            vec![(COMPUTE_ID_LABEL_KEY.to_string(), compute_id.to_string())]
                .into_iter()
                .collect(),
        );
        let ingress_service_type = match service_type {
            K8sServiceType::ClusterIP => Some("ClusterIP".to_string()),
            K8sServiceType::LoadBalancer => Some("LoadBalancer".to_string()),
        };
        let annotations = if matches!(service_type, K8sServiceType::LoadBalancer) {
            self.get_loadbalancer_annotations(service_name)
        } else {
            None
        };
        let external_traffic_policy = if matches!(service_type, K8sServiceType::LoadBalancer) {
            // External traffic policy is only relevant for LoadBalancer services. Normally we are okay with default setting (Local), which
            // means ingress traffic from the public network is routed to the target Pod (as defined by the `Service` selector) directly by
            // the cloud provider's load balancer. However, in Azure, this does not work if there are security policy configured for the node
            // pool receiving the traffic (which is the case in our setup). Techncially in AWS these security policies are also in place, but
            // the Network Load Balancers are smart enough to punch holes in the security group to allow the traffic to flow. In Azure, even
            // traffic from the Network Load Balancers are always subject to the explicit configurations in the security policy. As a
            // workaround, we set the external traffic policy to "Cluster" in Azure to have the traffic routed through the cluster's internal
            // network. This results in some performance penalty, but since we are retiring this NLB-based ingress stack soon we are not going
            // to optimize this.
            match self.cloud_provider {
                Some(CloudProvider::AWS) => Some("Local".to_string()),
                Some(CloudProvider::Azure) => Some("Cluster".to_string()),
                _ => Some("Local".to_string()),
            }
        } else {
            None
        };

        let ingress_service = Service {
            metadata: meta::v1::ObjectMeta {
                name: Some(service_name.to_string()),
                annotations: annotations.clone(),
                ..Default::default()
            },
            spec: Some(ServiceSpec {
                selector: ingress_service_selector.clone(),
                ports: Some(vec![ServicePort {
                    port: 5432,
                    target_port: Some(IntOrString::Int(pg_params_compute_pg_port as i32)),
                    name: Some("postgres".to_string()),
                    protocol: Some("TCP".to_string()),
                    ..Default::default()
                }]),
                type_: ingress_service_type.clone(),
                external_traffic_policy: external_traffic_policy.clone(),
                ..Default::default()
            }),
            ..Default::default()
        };

        let api: Api<Service> =
            Api::namespaced((*self.client).clone(), pg_params_compute_namespace.as_str());
        match api.get(service_name).await {
            // If the service exists, patch it.
            Ok(mut service) => {
                // We only need to update select fields such as "selector" in the service spec. Everything else, including the resource version,
                // should be left untouched so that the "replace" API can perform the atomic update.
                service.metadata.annotations = annotations.clone();
                service.spec.iter_mut().for_each(|spec| {
                    spec.selector = ingress_service_selector.clone();
                    spec.type_ = ingress_service_type.clone();
                    spec.external_traffic_policy = external_traffic_policy.clone();
                });
                api.replace(service_name, &PostParams::default(), &service)
                    .await
            }
            // If the service does not exist, create it.
            Err(kube::Error::Api(e)) if e.code == 404 => {
                api.create(&PostParams::default(), &ingress_service).await
            }
            Err(e) => Err(e),
        }
    }

    async fn delete_ingress_service(&self, service_name: &str) -> kube::Result<bool> {
        // Retrieve the namespace from the internal pg_params configuration.
        let pg_params_compute_namespace = {
            let pg_params = self.pg_params.read().expect("pg_params lock poisoned");
            pg_params.compute_namespace.clone()
        };

        // Create a namespaced API for Service resources.
        let api: Api<Service> =
            Api::namespaced((*self.client).clone(), pg_params_compute_namespace.as_str());

        // Define the deletion parameters.
        let dp = DeleteParams::default();

        // Attempt to delete the service.
        match api.delete(service_name, &dp).await {
            // Service delete request accepted, but the service may not have been deleted yet.
            Ok(_) => {
                // Check if the service still exists.
                let service_opt = api.get_opt(service_name).await?;
                Ok(service_opt.is_none())
            }
            // Service doesn't exist, so it's already deleted.
            Err(kube::Error::Api(e)) if e.code == 404 => Ok(true),
            // Any other error is returned.
            Err(e) => Err(e),
        }
    }

    /// Watches the config file for changes. If the config file changes, the HadronCluster is deployed and PgParams are updated.
    /// Note: Currently even if only PgParams are changed, deploy_storage for the HadronCluster is called. This is not a correctness issue.
    /// This is because the hash of the entire config file is used to detect changes.
    /// TODO: Consider refactoring the k8s manager struct to take in the additional Arc params here.
    pub async fn config_watcher(
        &self,
        persistence: Arc<Persistence>,
        token_generator: Option<HadronTokenGeneratorImpl>,
        startup_delay: Duration,
        service: Arc<service::Service>,
    ) {
        let mut last_successfully_applied_hash = None;

        // Wait for the startup to be fully completed (e.g., startup reconcilations are scheduled)
        service.startup_complete.clone().wait().await;
        tracing::info!("Storage controller startup completed, waiting for all spawned reconciliation tasks to complete.");

        const MAX_WAIT_SECONDS: i32 = 600;
        let mut waits = 0;
        let mut active_tasks: usize;
        loop {
            active_tasks = service.get_active_reconcile_tasks();
            if active_tasks == 0 || waits >= MAX_WAIT_SECONDS {
                break;
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
            waits += 1;
        }

        tracing::info!("Finished waiting for startup reconciliation tasks ({} tasks remaining). Starting config watcher.", active_tasks);

        // Wait for startup delay before starting the reconcliation loop. The startup delay is useful to avoid the HCC updating PS/SK/PG nodes
        // (which casues restarts) when the HCC itself also just started and needs to contact these nodes to discover cluster state.
        tokio::time::sleep(startup_delay).await;

        // Instantiate the page server watchdog to monitor all page server pods in the given namespace for PVC breakages async.
        let client = self.client.clone();
        let namespace = self.namespace.clone();
        tokio::task::spawn_blocking(move || {
            Handle::current().block_on(async {
                let result =
                    create_pageserver_pod_watcher(client, namespace, "page-server".to_string())
                        .await;

                if let Err(e) = result {
                    tracing::error!("Error running page server pod watcher: {:?}", e);
                }
            })
        });

        loop {
            tokio::time::sleep(Duration::from_secs(1)).await;

            let result = self
                .watch_config(
                    &mut last_successfully_applied_hash,
                    Arc::clone(&persistence),
                    token_generator.clone(),
                    service.clone(),
                )
                .await;

            let outcome_label = if result.is_err() {
                tracing::error!("Error watching and applying config: {:?}", result);
                ReconcileOutcome::Error
            } else {
                ReconcileOutcome::Success
            };

            metrics::METRICS_REGISTRY
                .metrics_group
                .storage_controller_config_watcher_complete
                .inc(ConfigWatcherCompleteLabelGroup {
                    status: outcome_label,
                });
        }
    }

    // Read the cluster config file and validate that it contains the required fields.
    // Returns:
    // - The HadronCluster object defining the storage cluster.
    // - The PgParams object defining parameters used to launch Postgres compute nodes.
    // - The hash of the contents of the config file.
    // - Or an error if the config file is missing or otherwise invalid.
    pub async fn read_and_validate_cluster_config(
        config_file_path: &str,
    ) -> anyhow::Result<(
        HadronCluster,
        PgParams,
        String,
        Option<PageServerBillingMetricsConfig>,
    )> {
        let file = File::open(config_file_path)
            .context(format!("Failed to open config file {config_file_path}"))?;
        let reader = BufReader::new(file);
        let config: ConfigData =
            serde_json::from_reader(reader).context("Failed to parse config file")?;
        let file_hash = hash_file_contents(config_file_path)
            .await
            .context("Failed to hash file contents")?;

        let hadron_cluster = config
            .hadron_cluster
            .ok_or(anyhow::anyhow!("hadron_cluster is required in config.json"))?;
        let pg_params = config
            .pg_params
            .ok_or(anyhow::anyhow!("pg_params is required in config.json"))?;
        let billing_metrics_conf = config.page_server_billing_metrics_config;

        Ok((hadron_cluster, pg_params, file_hash, billing_metrics_conf))
    }

    /// Update the gauge metrics to publish the desired number of pageservers and safekeepers managed by us.
    fn publish_desired_ps_sk_counts(&self, cluster: &HadronCluster) {
        let num_pageservers = cluster
            .hadron_cluster_spec
            .as_ref()
            .and_then(|spec| spec.page_server_specs.as_ref())
            .map(|pools| pools.iter().map(|p| p.replicas.unwrap_or(0) as i64).sum())
            .unwrap_or(0);
        metrics::METRICS_REGISTRY
            .metrics_group
            .storage_controller_num_pageservers_desired
            .set(num_pageservers);

        let num_safekeepers = cluster
            .hadron_cluster_spec
            .as_ref()
            .and_then(|spec| spec.safe_keeper_specs.as_ref())
            .map(|pools| pools.iter().map(|p| p.replicas.unwrap_or(0) as i64).sum())
            .unwrap_or(0);
        metrics::METRICS_REGISTRY
            .metrics_group
            .storage_controller_num_safekeeper_desired
            .set(num_safekeepers);
    }

    async fn watch_config(
        &self,
        last_successfully_applied_hash: &mut Option<String>,
        persistence: Arc<Persistence>,
        token_generator: Option<HadronTokenGeneratorImpl>,
        service: Arc<service::Service>,
    ) -> anyhow::Result<()> {
        let cluster_config_file_path = config_manager::get_cluster_config_file_path();
        let (hadron_cluster, pg_params, hash, billing_metrics_conf) =
            Self::read_and_validate_cluster_config(&cluster_config_file_path).await?;

        // Report desired pageserver and safekeeper counts.
        self.publish_desired_ps_sk_counts(&hadron_cluster);

        // Check if the config file has changed since the last successful deployment.
        if Some(hash.as_str()) == last_successfully_applied_hash.as_deref() {
            return Ok(());
        }

        // Deploy storage
        if let Err(e) = self
            .deploy_storage(hadron_cluster, billing_metrics_conf, service)
            .await
        {
            return Err(anyhow::anyhow!("Failed to deploy HadronCluster: {:?}", e));
        } else {
            tracing::info!("Successfully deployed HadronCluster");
        }

        // Get the PG compute defaults from the config file and update the local in-memory defaults,
        // as well all available managed PG compute deployments.
        let compute_namespace = pg_params.compute_namespace.clone();
        *self.pg_params.write().expect("pg_params lock poisoned") = pg_params;
        tracing::info!("Updated PG params in K8sManager");

        // Iterate over all active PG endpoints and update their compute deployments.
        let active_endpoints = match persistence.get_active_endpoint_infos().await {
            Ok(endpoints) => endpoints,
            Err(e) => {
                return Err(anyhow::anyhow!("Failed to get active endpoints: {:?}", e));
            }
        };

        tracing::info!(
            "Successfully retrieved {} active endpoints",
            active_endpoints.len()
        );

        for endpoint in active_endpoints {
            // Re-generate the PG HCC auth token for the tenant endpoint / compute.
            let pg_hcc_auth_token: String = match token_generator
                .as_ref()
                .unwrap()
                .generate_tenant_endpoint_scope_token(endpoint.endpoint_id)
            {
                Ok(token) => token,
                Err(e) => {
                    return Err(anyhow::anyhow!(
                        "Failed to generate PG HCC auth token: {:?}",
                        e
                    ));
                }
            };

            for compute in endpoint.computes {
                // Deserialize the compute's config into the per-compute EndpointConfig.
                let endpoint_config: EndpointConfig =
                    match serde_json::from_str(&compute.compute_config) {
                        Ok(config) => config,
                        Err(e) => {
                            return Err(anyhow::anyhow!(
                                "Failed to deserialize endpoint config: {:?}",
                                e
                            ));
                        }
                    };

                let workspace_url = {
                    match parse_to_url(endpoint.workspace_url.clone()) {
                        Ok(url) => url,
                        Err(e) => {
                            tracing::error!("Failed to parse workspace URL: {:?}", e);
                            None
                        }
                    }
                };

                let pg_compute = PgCompute {
                    name: compute.compute_name.clone(),
                    compute_id: format!("{}/{}", endpoint.endpoint_id, compute.compute_index),
                    workspace_id: endpoint.workspace_id.clone(),
                    workspace_url,
                    image_override: endpoint_config.image,
                    node_selector_override: endpoint_config.node_selector,
                    control_plane_token: pg_hcc_auth_token.clone(),
                    resources: endpoint_config
                        .resources
                        .unwrap_or(endpoint_default_resources()),
                    // T-shirt size is guranteed to exist when creating the endpoint. The same value should exist in meta PG as well.
                    tshirt_size: endpoint_config.tshirt_size.unwrap_or_else(|| {
                        panic!("T-shirt size did not exist in the endpoint config from meta PG");
                    }),
                    exporter_image_override: endpoint_config.prometheus_exporter_image,
                    model: ComputeModel::PrivatePreview,
                    readiness_probe: None,
                    instance_id: None,
                };

                // Fetch the deployment and check if it has the skip reconciliation annotation.
                let deployments: Api<Deployment> =
                    Api::namespaced((*self.client).clone(), &compute_namespace);

                let deployment = Self::k8s_get(deployments, &compute.compute_name).await;

                // If the deployment exists, and has the hadorn.dev/managed annotation set to false, skip reconciling the deployment.
                if let Ok(Some(deployment)) = deployment {
                    if let Some(annotations) = deployment.metadata.annotations {
                        if let Some(managed) = annotations.get("hadron.dev/managed") {
                            if managed == "false" {
                                tracing::info!("Skipping syncing deployment for compute {} as it is not managed", compute.compute_name);
                                continue;
                            }
                        }
                    }
                }

                match self.deploy_compute(pg_compute).await {
                    Ok(_) => {
                        tracing::info!("Deployed compute {} successfully", compute.compute_name)
                    }
                    Err(e) => {
                        return Err(anyhow::anyhow!("Failed to deploy compute: {:?}", e));
                    }
                }
            }
        }

        tracing::info!("Successfully updated PG compute deployments");

        *last_successfully_applied_hash = Some(hash);
        Ok(())
    }

    /// Deploys the HadronCluster to the Kubernetes cluster.
    async fn deploy_storage(
        &self,
        storage: HadronCluster,
        billing_metrics_conf: Option<PageServerBillingMetricsConfig>,
        service: Arc<service::Service>,
    ) -> anyhow::Result<()> {
        let client = Arc::clone(&self.client);

        // Get objects
        let storage_broker_objs: StorageBrokerObjs = self.get_storage_broker_objs(&storage)?;
        tracing::info!("Got StorageBroker objects");
        let safe_keeper_objs: SafeKeeperObjs = self.get_safe_keeper_objs(&storage)?;
        tracing::info!("Got SafeKeeper objects");
        let page_server_objs: PageServerObjs =
            self.get_page_server_objs(&storage, billing_metrics_conf)?;
        tracing::info!("Got PageServer objects");

        // Create objects
        Self::k8s_create_or_replace(
            Api::namespaced((*client).clone(), &self.namespace),
            "storage-broker",
            storage_broker_objs.deployment,
        )
        .await?;
        tracing::info!("Deployed StorageBroker Deployment");
        Self::k8s_create_or_replace(
            Api::namespaced((*client).clone(), &self.namespace),
            "storage-broker",
            storage_broker_objs.service,
        )
        .await?;
        tracing::info!("Deployed StorageBroker Service");

        let hadron_cluster_spec = storage
            .hadron_cluster_spec
            .as_ref()
            .ok_or(anyhow::anyhow!("Expected HadronCluster spec"))?;
        let safe_keeper_specs = hadron_cluster_spec
            .safe_keeper_specs
            .as_ref()
            .ok_or(anyhow::anyhow!("Expected SafeKeeper spec"))?;

        for (i, spec) in safe_keeper_specs.iter().enumerate() {
            let span = tracing::info_span!("sk_maintenance_manager");
            SKMaintenanceManager::new(
                Api::namespaced((*client).clone(), &self.namespace),
                service.clone(),
                safe_keeper_objs.stateful_sets[i]
                    .metadata
                    .name
                    .clone()
                    .ok_or(anyhow::anyhow!("Expected name"))?,
                safe_keeper_objs.stateful_sets[i].clone(),
                spec.pool_id,
            )
            .run()
            .instrument(span)
            .await?;
        }

        tracing::info!("Deployed SafeKeeper Deployment");
        Self::k8s_create_or_replace(
            Api::namespaced((*client).clone(), &self.namespace),
            "safe-keeper",
            safe_keeper_objs.service,
        )
        .await?;
        tracing::info!("Deployed SafeKeeper Service");

        // Handle PS updates
        // 1. Create ot update the image puller DaemonSet, which will pre-download hadron images to pageserver nodes.
        // 2. Wait (best-effort, with timeout) for all image puller DaemonSet Pods be updated.
        // 3. Create or update the pageserver StatefulSet and Service objects.
        let mut image_puller_names_and_deadlines: Vec<(String, Instant)> = Vec::new();
        for ImagePullerDaemonsetInfo {
            daemonset: image_puller,
            image_prepull_timeout: timeout,
        } in page_server_objs.image_puller_daemonsets
        {
            let image_pull_job_name = &image_puller
                .metadata
                .name
                .clone()
                .ok_or(anyhow::anyhow!("Expected name"))?;
            if let Err(e) = Self::k8s_create_or_replace(
                Api::namespaced((*client).clone(), &self.namespace),
                image_pull_job_name,
                image_puller,
            )
            .await
            {
                tracing::warn!(
                    "Failed to create or update image puller Daemonset {}: {:?}",
                    image_pull_job_name,
                    e
                );
                // Skip instead of failing the whole call. Image puller is just an optimization, we still want to deploy
                // the PS/SK/SB if there are issues with the image puller.
                continue;
            }
            // We successfully created/updated the DaemonSet. Add it to the list of DaemonSets we need to wait for.
            // Note that the wait deadline is calculated from each DaemonSet's creation/update time + timeout.
            image_puller_names_and_deadlines.push((
                image_pull_job_name.to_owned(),
                Instant::now() + timeout.unwrap_or(IMAGE_PREPULL_DEFAULT_TIMEOUT),
            ));
        }

        for (ds_name, deadline) in image_puller_names_and_deadlines {
            match Self::k8s_wait_for_daemonset(
                Api::namespaced((*client).clone(), &self.namespace),
                &ds_name,
                deadline,
            )
            .await
            {
                Ok(_) => {
                    tracing::info!(
                        "Image puller Daemonset {} preloaded images successfully",
                        &ds_name
                    )
                }
                Err(e) => {
                    // The image puller DeamonSet is a best-effort performance optimization and is not strictly
                    // required for the system to function. If it fails for any reason, just log a warning and
                    // proceed with updating the pageserver StatefulSets.
                    tracing::warn!(
                        "Image puller Daemonset {} did not preload images successfully on all nodes, proceeding. Error: {:?}",
                        &ds_name,
                        e
                    )
                }
            }
        }

        if self.cloud_provider.is_some() {
            // during release, we may first update the config map and then restart the storage controller. Add a sleep here
            // to avoid the old storage controller from starting the drain-and-fill process, which is currently not resumable.
            tracing::info!("Waiting for 60 seconds before deploying page servers");
            sleep(Duration::from_secs(60)).await;
        }

        for stateful_set in page_server_objs.stateful_sets {
            let span = tracing::info_span!("drain_and_fill_manager");
            DrainAndFillManager::new(
                Api::namespaced((*client).clone(), &self.namespace),
                Api::namespaced((*client).clone(), &self.namespace),
                service.clone(),
                stateful_set
                    .metadata
                    .name
                    .clone()
                    .ok_or(anyhow::anyhow!("Expected name"))?,
                stateful_set,
            )
            .run()
            .instrument(span)
            .await?;
        }
        tracing::info!("Deployed PageServer Deployment");
        Self::k8s_create_or_replace(
            Api::namespaced((*client).clone(), &self.namespace),
            "page-server",
            page_server_objs.service,
        )
        .await?;
        tracing::info!("Deployed PageServer Service");

        kube::Result::Ok(())
    }

    fn extract_cpu_memory_resources(
        &self,
        resources: ResourceRequirements,
    ) -> anyhow::Result<ResourceRequirements> {
        // Note that we only extra resource requests, not limits. Hadron storage components (PS/SK) should not have CPU or memory limits.
        // Other containers running on the node should be evicted/killed first if the node is oversubscribed.
        let requests = resources
            .requests
            .clone()
            .ok_or(anyhow::anyhow!("Expected resource requests"))?;
        Ok(ResourceRequirements {
            requests: Some(BTreeMap::from([
                ("cpu".to_string(), requests["cpu"].clone()),
                ("memory".to_string(), requests["memory"].clone()),
            ])),
            ..Default::default()
        })
    }

    /// Calculate the node affinity setting to use for a PS/SK/SB component.
    /// - `node_group_requirement`: Node selector requirement specifying which node group (usually identified by "bickstore-pool-types" node label)
    ///   we should use to deploy the component.
    /// - `availability_zone_suffix`: The availability zone suffix to use for the component. This is used to schedule different pools to different
    ///   AZs.
    fn compute_node_affinity(
        &self,
        node_group_requirement: Option<&NodeSelectorRequirement>,
        availability_zone_suffix: Option<&String>,
    ) -> Option<Affinity> {
        // There are two node selector requirements we potentially need:
        // 1. The node group node selector requirement, which specifies which node label value we require for the "brickstore-pool-types" label key we require. This is
        //    needed if we want to schedule a component to a specific type of VM instances/node groups.
        // 2. The availability zone node selector requirement, which specifies which node label value we require for the "topology.kubernetes.io/zone" label key.
        //    This is needed if we want to schedule a component to a specific availability zone.
        let mut requirements: Vec<NodeSelectorRequirement> =
            node_group_requirement.into_iter().cloned().collect();

        let az_requirement = availability_zone_suffix.map(|az_suffix| NodeSelectorRequirement {
            key: "topology.kubernetes.io/zone".to_string(),
            operator: "In".to_string(),
            values: Some(vec![self.region.clone() + az_suffix]),
        });

        requirements.extend(az_requirement);

        if requirements.is_empty() {
            None
        } else {
            // From official documentation: https://kubernetes.io/docs/concepts/scheduling-eviction/assign-pod-node/#node-affinity
            // There are 2 ways to specify affinity rules: NodeSelectorTerms and MatchExpressions. They work as follows:
            //
            // - nodeSelectorTerms:
            //   - matchExpressions:
            //     - cond1
            //     - cond2
            // - nodeSelectorTerms:
            //   - matchExpressions:
            //     - cond3
            //
            // A node is considered a match if (cond1 AND cond2) OR (cond3).
            //
            // In other words, the nodeSelectorTerms are OR-ed together, and the matchExpressions within a nodeSelectorTerm are AND-ed together.
            // In our use case we want the node selector requirements to be AND-ed together, so we put them under a MatchExpressions within a
            // single NodeSelectorTerms.
            Some(Affinity {
                node_affinity: Some(NodeAffinity {
                    required_during_scheduling_ignored_during_execution: Some(NodeSelector {
                        node_selector_terms: vec![NodeSelectorTerm {
                            match_expressions: Some(requirements),
                            ..Default::default()
                        }],
                    }),
                    ..Default::default()
                }),
                ..Default::default()
            })
        }
    }

    fn node_selector_requirement_to_tolerations(
        &self,
        node_selector: Option<&NodeSelectorRequirement>,
    ) -> Option<Vec<Toleration>> {
        node_selector.and_then(|node_selector| {
            node_selector.values.as_ref().map(|values| {
                values
                    .iter()
                    .map(|label_value| Toleration {
                        key: Some("databricks.com/node-type".to_string()),
                        operator: Some("Equal".to_string()),
                        value: Some(label_value.clone()),
                        effect: match self.cloud_provider {
                            Some(CloudProvider::AWS) => Some("NoSchedule".to_string()),
                            // AKS node pools support PreferNoSchedule but not NoSchedule.
                            Some(CloudProvider::Azure) => Some("PreferNoSchedule".to_string()),
                            _ => Some("NoSchedule".to_string()),
                        },
                        ..Default::default()
                    })
                    .collect()
            })
        })
    }

    /// Generate an image puller DaemonSet manifest. An image puller DaemonSet downloads the specified image to
    /// the specified nodes by running a dummy container (runs a simple `sleep infinity` command).
    /// - `name`: The name of the ImagePullJob.
    /// - `image`: The image ref of the image to download.
    /// - `image_pull_secrets`: Any image pull secrets to use when downloading the image.
    /// - `node_selector_requirement`: Node selector requirement specifying which nodes to download the image to.
    /// - `availability_zone_suffix`: Specify which available zone to download the image to.
    ///    The `node_selector_requirement` and `availability_zone_suffix` parameters together determine the nodes
    ///    to download the image to. If none are specified, the image will be downloaded to all nodes.
    /// - `image_pull_parallelism`: The max number of nodes that can pre-download the image in parallel.
    fn generate_image_puller_daemonset(
        &self,
        name: &str,
        image: &Option<String>,
        image_pull_secrets: &Option<Vec<LocalObjectReference>>,
        node_selector_requirement: &Option<NodeSelectorRequirement>,
        availability_zone_suffix: &Option<String>,
        image_pull_parallelism: &Option<i32>,
    ) -> anyhow::Result<DaemonSet> {
        let image_puller_ds = DaemonSet {
            metadata: meta::v1::ObjectMeta {
                name: Some(name.to_string()),
                namespace: Some(self.namespace.clone()),
                ..Default::default()
            },
            spec: Some(DaemonSetSpec {
                selector: meta::v1::LabelSelector {
                    match_labels: Some(
                        vec![("app".to_string(), name.to_string())]
                            .into_iter()
                            .collect(),
                    ),
                    ..Default::default()
                },
                update_strategy: Some(DaemonSetUpdateStrategy {
                    type_: Some("RollingUpdate".to_string()),
                    rolling_update: Some(RollingUpdateDaemonSet {
                        // We use max_unavailable to control the parallelism of the image pre-pull operation, as
                        // Kubernetes will allow up to max_unavailable pods to start updating at the same time.
                        max_unavailable: image_pull_parallelism.map(IntOrString::Int),
                        ..Default::default()
                    }),
                }),
                template: PodTemplateSpec {
                    metadata: Some(meta::v1::ObjectMeta {
                        labels: Some(
                            vec![("app".to_string(), name.to_string())]
                                .into_iter()
                                .collect(),
                        ),
                        ..Default::default()
                    }),
                    spec: Some(PodSpec {
                        image_pull_secrets: image_pull_secrets.clone(),
                        containers: vec![Container {
                            name: "sleep".to_string(),
                            image: image.clone(),
                            image_pull_policy: Some("IfNotPresent".to_string()),
                            command: Some(vec!["/bin/sleep".to_string(), "infinity".to_string()]),
                            resources: Some(ResourceRequirements {
                                // Set tiny requests/limits as this container doesn't really do anything.
                                requests: Some(
                                    vec![
                                        ("cpu".to_string(), Quantity("10m".to_string())),
                                        ("memory".to_string(), Quantity("10Mi".to_string())),
                                    ]
                                    .into_iter()
                                    .collect(),
                                ),
                                limits: Some(
                                    vec![
                                        ("cpu".to_string(), Quantity("10m".to_string())),
                                        ("memory".to_string(), Quantity("20Mi".to_string())),
                                    ]
                                    .into_iter()
                                    .collect(),
                                ),
                                ..Default::default()
                            }),
                            ..Default::default()
                        }],
                        affinity: self.compute_node_affinity(
                            node_selector_requirement.as_ref(),
                            availability_zone_suffix.as_ref(),
                        ),
                        // The image puller pod can be terminated immediately because as all it does is `sleep`.
                        termination_grace_period_seconds: Some(0),
                        tolerations: self.node_selector_requirement_to_tolerations(
                            node_selector_requirement.as_ref(),
                        ),
                        priority_class_name: Some("databricks-daemonset".to_string()),
                        ..Default::default()
                    }),
                },
                ..Default::default()
            }),
            ..Default::default()
        };

        Ok(image_puller_ds)
    }

    /// Returns the Deployment and Service objects for the StorageBroker.
    fn get_storage_broker_objs(
        &self,
        storage: &HadronCluster,
    ) -> anyhow::Result<StorageBrokerObjs> {
        let hadron_cluster_spec = storage
            .hadron_cluster_spec
            .as_ref()
            .ok_or(anyhow::anyhow!("Expected HadronCluster spec"))?;
        let storage_broker_spec = hadron_cluster_spec
            .storage_broker_spec
            .as_ref()
            .ok_or(anyhow::anyhow!("Expected StorageBroker spec"))?;
        let launch_command = r#"#
            /usr/local/bin/storage_broker --listen-addr=0.0.0.0:50051 \
                                          --timeline-chan-size=1024   \
                                          --all-keys-chan-size=524288
            "#
        .to_string();

        let deployment = Deployment {
            metadata: meta::v1::ObjectMeta {
                name: Some("storage-broker".to_string()),
                namespace: Some(self.namespace.clone()),
                ..Default::default()
            },
            spec: Some(DeploymentSpec {
                replicas: Some(1),
                selector: meta::v1::LabelSelector {
                    match_labels: Some(
                        vec![("app".to_string(), "storage-broker".to_string())]
                            .into_iter()
                            .collect(),
                    ),
                    ..Default::default()
                },
                template: PodTemplateSpec {
                    metadata: get_pod_metadata("storage-broker".to_string(), 50051),
                    spec: Some(PodSpec {
                        affinity: self.compute_node_affinity(
                            storage_broker_spec.node_selector.as_ref(),
                            // StorageBroker is a single Pod, so we allow it to be in any AZ.
                            None,
                        ),
                        tolerations: self.node_selector_requirement_to_tolerations(
                            storage_broker_spec.node_selector.as_ref(),
                        ),
                        image_pull_secrets: storage_broker_spec.image_pull_secrets.clone(),
                        containers: vec![Container {
                            name: "storage-broker".to_string(),
                            image: storage_broker_spec.image.clone(),
                            image_pull_policy: storage_broker_spec.image_pull_policy.clone(),
                            ports: get_container_ports(vec![50051]),
                            resources: storage_broker_spec.resources.clone(),
                            command: Some(vec!["/bin/sh".to_string(), "-c".to_string()]),
                            args: Some(vec![launch_command]),
                            ..Default::default()
                        }],
                        ..Default::default()
                    }),
                },
                ..Default::default()
            }),
            ..Default::default()
        };

        let service = Service {
            metadata: meta::v1::ObjectMeta {
                name: Some("storage-broker".to_string()),
                namespace: Some(self.namespace.clone()),
                ..Default::default()
            },
            spec: Some(ServiceSpec {
                selector: Some(BTreeMap::from([(
                    "app".to_string(),
                    "storage-broker".to_string(),
                )])),
                ports: Some(vec![ServicePort {
                    port: 50051,
                    target_port: Some(IntOrString::Int(50051)),
                    ..Default::default()
                }]),
                type_: Some("ClusterIP".to_string()),
                ..Default::default()
            }),
            ..Default::default()
        };

        Ok(StorageBrokerObjs {
            deployment,
            service,
        })
    }

    pub fn get_remote_storage_args(
        node_kind: NodeKind,
        object_storage_config: &HadronObjectStorageConfig,
    ) -> String {
        let test_endpoint_opt = object_storage_config
            .test_params
            .as_ref()
            .and_then(|test_params| test_params.endpoint.as_ref())
            .map(|endpoint| format!("endpoint='{endpoint}', "))
            .unwrap_or_default();

        let mut arg_string = "".to_string();
        // This key is different between AWS and Azure, which is very subtle.
        let mut prefix_in_bucket_arg = "".to_string();

        if object_storage_config.is_aws() {
            arg_string = format!(
                "bucket_name='{}', bucket_region='{}'",
                object_storage_config
                    .bucket_name
                    .clone()
                    .unwrap_or_default(),
                object_storage_config
                    .bucket_region
                    .clone()
                    .unwrap_or_default()
            );
            prefix_in_bucket_arg = "prefix_in_bucket".to_string();
        } else if object_storage_config.is_azure() {
            arg_string = format!(
                "storage_account='{}', container_name='{}', container_region='{}'",
                object_storage_config
                    .storage_account_resource_id
                    .clone()
                    .unwrap_or_default()
                    .split('/')
                    .last()
                    .map(|s| s.to_string())
                    .unwrap_or_default(),
                object_storage_config
                    .storage_container_name
                    .clone()
                    .unwrap_or_default(),
                object_storage_config
                    .storage_container_region
                    .clone()
                    .unwrap_or_default(),
            );
            prefix_in_bucket_arg = "prefix_in_container".to_string();
        }

        let mut prefix_in_bucket = match node_kind {
            NodeKind::Pageserver => "pageserver/",
            NodeKind::Safekeeper => "safekeeper/",
        };

        // Tests use empty prefix.
        if !test_endpoint_opt.is_empty() {
            prefix_in_bucket = "";
        }

        format!(
            "{{{}{}, {}='{}'}}",
            test_endpoint_opt, arg_string, prefix_in_bucket_arg, prefix_in_bucket
        )
    }

    fn get_remote_storage_startup_args(
        &self,
        object_storage_config: &HadronObjectStorageConfig,
    ) -> String {
        let test_endpoint_opt = object_storage_config
            .test_params
            .as_ref()
            .and_then(|test_params| test_params.endpoint.as_ref())
            .map(|endpoint| format!("endpoint='{endpoint}', "))
            .unwrap_or_default();

        let mut arg_string = "".to_string();
        // This key is different between AWS and Azure, which is very subtle.
        let mut prefix_in_bucket_arg = "".to_string();

        if object_storage_config.is_aws() {
            arg_string = "bucket_name='$S3_BUCKET_URI', bucket_region='$S3_REGION'".to_string();
            prefix_in_bucket_arg = "prefix_in_bucket".to_string();
        } else if object_storage_config.is_azure() {
            arg_string = "storage_account='$AZURE_STORAGE_ACCOUNT_NAME', container_name='$AZURE_STORAGE_CONTAINER_NAME', container_region='$AZURE_STORAGE_CONTAINER_REGION'".to_string();
            prefix_in_bucket_arg = "prefix_in_container".to_string();
        }

        format!(
            "{{{}{}, {}='$PREFIX_IN_BUCKET'}}",
            test_endpoint_opt, arg_string, prefix_in_bucket_arg
        )
    }

    /// Returns the StatefulSets and Service objects for SafeKeepers.
    fn get_safe_keeper_objs(&self, storage: &HadronCluster) -> anyhow::Result<SafeKeeperObjs> {
        let hadron_cluster_spec = storage
            .hadron_cluster_spec
            .as_ref()
            .ok_or(anyhow::anyhow!("Expected HadronCluster spec"))?;
        let safe_keeper_specs = hadron_cluster_spec
            .safe_keeper_specs
            .as_ref()
            .ok_or(anyhow::anyhow!("Expected SafeKeeper spec"))?;
        let object_storage_config = hadron_cluster_spec
            .object_storage_config
            .as_ref()
            .ok_or(anyhow::anyhow!("Expected ObjectStorageConfig"))?;

        // We reserve 100GB disk space on the safekeeper in case it runs of disk space and needs to recover.
        let reserved_file_cmd = if self.cloud_provider.is_some() {
            "mkdir -p /data/.neon && fallocate -l 100G /data/.neon/reserved_file"
        } else {
            ""
        };

        let wal_reader_fanout = if is_dev_or_staging() {
            "--wal-reader-fanout --max-delta-for-fanout=2147483648"
        } else {
            ""
        };
        let pull_timeline_on_startup = if is_dev_or_staging() {
            "--enable-pull-timeline-on-startup"
        } else {
            ""
        };

        let mut stateful_sets: Vec<StatefulSet> = Vec::new();
        for safe_keeper_spec in safe_keeper_specs {
            let transformed_pool_id = transform_pool_id(safe_keeper_spec.pool_id);
            let remote_storage_opt = self.get_remote_storage_startup_args(object_storage_config);
            let token_verification_key_mount_path =
                brickstore_internal_token_verification_key_mount_path();
            // PS prefers streaming WALs from SK replica in the same AZ.
            let availability_zone = if safe_keeper_spec.availability_zone_suffix.is_some() {
                format!(
                    "--availability-zone='az-{}'",
                    safe_keeper_spec.availability_zone_suffix.clone().unwrap()
                )
            } else {
                "".to_string()
            };
            // TODO: Fully parameterize the port number constants
            let launch_command = format!(
                r#"#!/bin/sh
# Extract the ordinal number from the hostname
ordinal=$(hostname | rev | cut -d- -f1 | rev)

# Set the SAFEKEEPER_ID and PREFIX_IN_BUCKET based on the ordinal number
SAFEKEEPER_ID=$((ordinal + {transformed_pool_id}))
# Do NOT specify a leading / in the prefix (this breaks Azure).
PREFIX_IN_BUCKET="safekeeper/"

SAFEKEEPER_FQDN="${{HOSTNAME}}.safe-keeper.${{MY_NAMESPACE}}.svc.cluster.local"

# Make sure SIGTERM received by the shell is propagated to the safekeeper process.
shutdown() {{
    echo "Shutting down safekeeper running at pid $pid"
    kill -TERM $pid
    wait $pid
}}

trap shutdown TERM

{reserved_file_cmd}

# Start Safekeeper. Notes on ports:
# Port 5454 accpets PG wire protocol connections from compute nodes and only accepts tenant-scoped tokens. This is the only port allowed from the untrusted worker subnet.
# Port 5455 accepts PG wire protocol connections from PS (and maybe other trusted components) and is advertised via the storage broker.
/usr/local/bin/safekeeper --listen-pg='0.0.0.0:5455' \
                            --listen-pg-tenant-only='0.0.0.0:5454' \
                            --advertise-pg-tenant-only="$SAFEKEEPER_FQDN:5454" \
                            --hcc-base-url=$STORAGE_CONTROLLER_URL \
                            --advertise-pg="$SAFEKEEPER_FQDN:5455" \
                            --listen-http='0.0.0.0:7676' \
                            --token-auth-type='HadronJWT' \
                            --pg-tenant-only-auth-public-key-path='{token_verification_key_mount_path}' \
                            --id=$SAFEKEEPER_ID \
                            --broker-endpoint="$BROKER_ENDPOINT" \
                            --max-offloader-lag=1073741824 \
                            --max-reelect-offloader-lag-bytes=4294967296 \
                            --wal-backup-parallel-jobs=64 \
                            -D /data/.neon \
                            {wal_reader_fanout} \
                            {pull_timeline_on_startup} \
                            {availability_zone} \
                            --remote-storage="{remote_storage_opt}" &
pid=$!

wait
"#
            );

            // Get Brickstore secrets we need to mount to safe keepers. Currently it's just the token verification keys,
            // and the azure service principal secret (when appropriate).
            let mut k8s_mounts = vec![brickstore_internal_token_verification_key_secret_mount()];

            if object_storage_config.is_azure() {
                k8s_mounts.push(azure_storage_account_service_principal_secret_mount())
            }

            let K8sSecretVolumesAndMounts {
                volumes: secret_volumes,
                volume_mounts: secret_volume_mounts,
            } = self.get_hadron_volumes_and_mounts(k8s_mounts);

            let stateful_set = StatefulSet {
                metadata: meta::v1::ObjectMeta {
                    name: Some(format!(
                        "{}-{}",
                        "safe-keeper",
                        safe_keeper_spec.pool_id.unwrap_or(0)
                    )),
                    namespace: Some(self.namespace.clone()),
                    annotations: Some(
                        vec![
                            (
                                SKMaintenanceManager::SK_LOW_DOWNTIME_MAINTENANCE_KEY.to_string(),
                                safe_keeper_spec
                                    .enable_low_downtime_maintenance
                                    .unwrap_or(
                                        SKMaintenanceManager::SK_LOW_DOWNTIME_MAINTENANCE_DEFAULT,
                                    )
                                    .to_string(),
                            ),
                            (
                                SKMaintenanceManager::SK_LDTM_SK_STATUS_CHECK_KEY.to_string(),
                                safe_keeper_spec
                                    .enable_ldtm_sk_status_check
                                    .unwrap_or(
                                        SKMaintenanceManager::SK_LDTM_SK_STATUS_CHECK_DEFAULT,
                                    )
                                    .to_string(),
                            ),
                        ]
                        .into_iter()
                        .collect(),
                    ),
                    ..Default::default()
                },
                spec: StatefulSetSpec {
                    replicas: safe_keeper_spec.replicas,
                    selector: meta::v1::LabelSelector {
                        match_labels: Some(
                            vec![("app".to_string(), "safe-keeper".to_string())]
                                .into_iter()
                                .collect(),
                        ),
                        ..Default::default()
                    },
                    service_name: Some("safe-keeper".to_string()),
                    volume_claim_templates: get_volume_claim_template(
                        safe_keeper_spec.resources.clone(),
                        safe_keeper_spec.storage_class_name.clone(),
                    )?,
                    template: PodTemplateSpec {
                        metadata: get_pod_metadata("safe-keeper".to_string(), 7676),
                        spec: Some(PodSpec {
                            affinity: self.compute_node_affinity(
                                safe_keeper_spec.node_selector.as_ref(),
                                safe_keeper_spec.availability_zone_suffix.as_ref(),
                            ),
                            tolerations: self.node_selector_requirement_to_tolerations(
                                safe_keeper_spec.node_selector.as_ref(),
                            ),
                            image_pull_secrets: safe_keeper_spec.image_pull_secrets.clone(),
                            service_account_name: hadron_cluster_spec.service_account_name.clone(),
                            security_context: get_pod_security_context(),
                            volumes: Some(secret_volumes),
                            // Set the priority class to the very-high-priority "pg-compute", which should allow
                            // the safekeeper to preempt all other pods on the same nodes (including log daemon)
                            // if we run low on resources for whatever reason.
                            priority_class_name: Some("pg-compute".to_string()),
                            containers: vec![Container {
                                name: "safe-keeper".to_string(),
                                image: safe_keeper_spec.image.clone(),
                                image_pull_policy: safe_keeper_spec.image_pull_policy.clone(),
                                ports: get_container_ports(vec![5454, 7676]),
                                resources: Some(
                                    self.extract_cpu_memory_resources(
                                        safe_keeper_spec
                                            .resources
                                            .clone()
                                            .ok_or(anyhow::anyhow!("Expected resources"))?,
                                    )?,
                                ),
                                volume_mounts: Some(itertools::concat(vec![
                                    get_local_data_volume_mounts(),
                                    secret_volume_mounts,
                                ])),
                                command: Some(vec!["/bin/sh".to_string(), "-c".to_string()]),
                                args: Some(vec![launch_command]),
                                env: {
                                    let additional_env_vars = vec![
                                        EnvVar {
                                            name: "MY_NAMESPACE".to_string(),
                                            value_from: Some(EnvVarSource {
                                                field_ref: Some(ObjectFieldSelector {
                                                    field_path: "metadata.namespace".to_string(),
                                                    ..Default::default()
                                                }),
                                                ..Default::default()
                                            }),
                                            ..Default::default()
                                        },
                                        EnvVar {
                                            name: "STORAGE_CONTROLLER_URL".to_string(),
                                            value: Some(format!(
                                                "http://{}:{}",
                                                self.hcc_dns_name, self.hcc_listening_port
                                            )),
                                            ..Default::default()
                                        },
                                        EnvVar {
                                            name: HADRON_NODE_IP_ADDRESS.to_string(),
                                            value_from: Some(EnvVarSource {
                                                field_ref: Some(ObjectFieldSelector {
                                                    field_path: "status.podIP".to_string(),
                                                    ..Default::default()
                                                }),
                                                ..Default::default()
                                            }),
                                            ..Default::default()
                                        },
                                    ];

                                    get_env_vars(object_storage_config, additional_env_vars)
                                },
                                ..Default::default()
                            }],
                            ..Default::default()
                        }),
                    },
                    ..Default::default()
                },
                status: None,
            };
            stateful_sets.push(stateful_set);
        }

        let service = get_service(
            "safe-keeper".to_string(),
            self.namespace.clone(),
            5454,
            7676,
        );

        Ok(SafeKeeperObjs {
            stateful_sets,
            service,
        })
    }

    /// Returns the StatefulSets and Service objects for PageServers.
    fn get_page_server_objs(
        &self,
        storage: &HadronCluster,
        billing_metrics_conf: Option<PageServerBillingMetricsConfig>,
    ) -> anyhow::Result<PageServerObjs> {
        let hadron_cluster_spec = storage
            .hadron_cluster_spec
            .as_ref()
            .ok_or(anyhow::anyhow!("Expected HadronCluster spec"))?;
        let page_server_specs = hadron_cluster_spec
            .page_server_specs
            .as_ref()
            .ok_or(anyhow::anyhow!("Expected PageServer spec"))?;
        let object_storage_config = hadron_cluster_spec
            .object_storage_config
            .as_ref()
            .ok_or(anyhow::anyhow!("Expected ObjectStorageConfig"))?;

        let billing_metrics_configs =
            billing_metrics_conf.map_or("".to_string(), move |conf| conf.to_toml());

        let mut image_puller_daemonsets: Vec<ImagePullerDaemonsetInfo> = Vec::new();
        let mut stateful_sets: Vec<StatefulSet> = Vec::new();
        for page_server_spec in page_server_specs {
            // First, generate an "image puller" DaemonSet for this pageserver pool.
            // The image puller DaemonSet's purpose is to pre-download the new pageserver image to the nodes running this pageserver
            // pool before we start shutting down pageservers for the rolling upgrade. This speeds up rolling upgrades and reduces
            // downtime between pageserver shutdown and restart significantly.
            let image_pull_ds_name =
                format!("image-puller-ps-{}", page_server_spec.pool_id.unwrap_or(0));
            let image_puller_daemonset = self.generate_image_puller_daemonset(
                &image_pull_ds_name,
                &page_server_spec.image,
                &page_server_spec.image_pull_secrets,
                &page_server_spec.node_selector,
                &page_server_spec.availability_zone_suffix,
                &page_server_spec.image_prepull_parallelism,
            );
            match image_puller_daemonset {
                Ok(d) => {
                    image_puller_daemonsets.push(ImagePullerDaemonsetInfo {
                        daemonset: d,
                        image_prepull_timeout: page_server_spec
                            .image_prepull_timeout_seconds
                            .map(Duration::from_secs),
                    });
                }
                Err(e) => {
                    // We may not be able to generate an image puller DaemonSet manifest if the cluster config is missing crucial
                    // fields (e.g., doesn't specify an image). When this happens we just skip it with a warning but don't fail
                    // anything else. The ImagePullJob is just a performance optimization and it is not strictly required for the
                    // system to function. Besides, any error here due to malformed cluster spec will likely result in a more permant
                    // error down below where we construct the main workload, the pageserver StatefulSet manifest, so we will bail
                    // out there with a more informative error message if it comes to that.
                    tracing::warn!(
                        "Failed to generate image puller daemonset for page server pool {}, skipping: {:?}",
                        page_server_spec.pool_id.unwrap_or(0),
                        e
                    );
                }
            };

            // Now generate the pageserver StatefulSet spec.
            // Hacky way to convert from Gi to bytes
            let storage_quantity = page_server_spec
                .resources
                .clone()
                .ok_or(anyhow::anyhow!("Expected resources"))?
                .limits
                .clone()
                .ok_or(anyhow::anyhow!("Expected resource limits"))?["storage"]
                .clone();
            let mut storage_size_bytes_str: String =
                serde_json::to_string(&storage_quantity)?.replace("Gi", "");
            storage_size_bytes_str.pop();
            storage_size_bytes_str.remove(0);
            let storage_size_bytes = storage_size_bytes_str.parse::<i64>()? * 1073741824;

            let transformed_pool_id = transform_pool_id(page_server_spec.pool_id);
            // Extract any additional pageserver configs that we should append to pageserver.toml.
            let additional_pageserver_configs = page_server_spec
                .custom_pageserver_toml
                .clone()
                .unwrap_or_default();
            let remote_storage_opt = self.get_remote_storage_startup_args(object_storage_config);
            let token_verification_key_mount_path =
                brickstore_internal_token_verification_key_mount_path();
            let wal_receiver_protocol = if is_dev_or_staging() {
                "wal_receiver_protocol = {type='interpreted', args={format='protobuf', compression={zstd={level=1} } } }"
            } else {
                ""
            };
            let s3_fault_injection = if is_chaos_testing() {
                "test_remote_failures = 10000 \n test_remote_failures_probability = 20"
            } else {
                ""
            };

            let image_layer_force_creation_period = if is_dev_or_staging() {
                ", image_layer_force_creation_period='1d'"
            } else {
                ""
            };
            // PS prefers streaming WALs from SK replica in the same AZ.
            // It also notifies SC about its AZ so that SC can optimize its placements,
            // e.g., placing primary and secondary in different AZs,
            // co-locating the primary shard in the same AZ as the tenant.
            let availability_zone = if page_server_spec.availability_zone_suffix.is_some() {
                format!(
                    "availability_zone='az-{}'",
                    page_server_spec.availability_zone_suffix.clone().unwrap()
                )
            } else {
                "".to_string()
            };

            let launch_command = format!(
                r#"#!/bin/sh
# Extract the ordinal index from the hostname (e.g. "page-server-0" -> 0)
ordinal=$(hostname | rev | cut -d- -f1 | rev)

# Set the id and prefix_in_bucket dynamically
PAGESERVER_ID=$((ordinal + {transformed_pool_id}))
# Do NOT specify a leading / in the prefix (this breaks Azure).
PREFIX_IN_BUCKET="pageserver/"

# Compute the in-cluster FQDN of the page server
PAGESERVER_FQDN="${{HOSTNAME}}.page-server.${{NAMESPACE}}.svc.cluster.local"

# Write the page server identity file.
cat <<EOF > /data/.neon/identity.toml
id=$PAGESERVER_ID
EOF

# Create the page server metadata.json file so that it auto-registers with the storage controller.
cat <<EOF > /data/.neon/metadata.json
{{
    "host": "$PAGESERVER_FQDN",
    "port": 6400,
    "http_host": "$PAGESERVER_FQDN",
    "http_port": 9898,
    "other": {{}}
}}
EOF

# Write the pageserver.toml config file.
cat <<EOF > /data/.neon/pageserver.toml
pg_distrib_dir='/usr/local/'
pg_auth_type='HadronJWT'
auth_validation_public_key_path='{token_verification_key_mount_path}'
broker_endpoint='$BROKER_ENDPOINT'
control_plane_api='$STORAGE_CONTROLLER_ENDPOINT'
listen_pg_addr='0.0.0.0:6400'
listen_http_addr='0.0.0.0:9898'
max_file_descriptors=10000
remote_storage={remote_storage_opt}
{s3_fault_injection}
ephemeral_bytes_per_memory_kb=512
disk_usage_based_eviction={{max_usage_pct=80, min_avail_bytes=$MIN_DISK_AVAIL_BYTES, period='1m'}}
tenant_config = {{checkpoint_distance=1_073_741_824, compaction_target_size=134_217_728 {image_layer_force_creation_period}}}
{availability_zone}
{billing_metrics_configs}
{wal_receiver_protocol}
{additional_pageserver_configs}
EOF

# Make sure SIGTERM received by the shell is propagated to the pageserver process.
shutdown() {{
    echo "Shutting down pageserver running at pid $pid"
    kill -TERM $pid
    wait $pid
}}

trap shutdown TERM

# Start the pageserver binary.
/usr/local/bin/pageserver -D /data/.neon/ &
pid=$!

wait
"#
            );

            // Get Brickstore secerts we need to mount to page servers. Currently it's just the token verification keys,
            // and the azure service principal secret (when appropriate).
            let mut k8s_mounts = vec![brickstore_internal_token_verification_key_secret_mount()];

            if object_storage_config.is_azure() {
                k8s_mounts.push(azure_storage_account_service_principal_secret_mount())
            }

            let K8sSecretVolumesAndMounts {
                volumes: secret_volumes,
                volume_mounts: secret_volume_mounts,
            } = self.get_hadron_volumes_and_mounts(k8s_mounts);

            let stateful_set = StatefulSet {
                metadata: meta::v1::ObjectMeta {
                    annotations: Some(
                        vec![
                            // Set up PersistentPodState for page servers so that they are re-scheduled to the same nodes upon restarts,
                            // if possible. This helps performance because page servers use local SSDs for caches and it would be nice
                            // to not lose this cache due to k8s moving Pods around.
                            // Note that PersistentPodState is an OpenKruise AdvancedStatefulSet feature.
                            // https://openkruise.io/docs/user-manuals/persistentpodstate/#annotation-auto-generate-persistentpodstate
                            (
                                "kruise.io/auto-generate-persistent-pod-state".to_string(),
                                "true".to_string(),
                            ),
                            (
                                "kruise.io/preferred-persistent-topology".to_string(),
                                "kubernetes.io/hostname".to_string(),
                            ),
                            (
                                DrainAndFillManager::DRAIN_AND_FILL_KEY.to_string(),
                                page_server_spec
                                    .use_drain_and_fill
                                    .unwrap_or(DrainAndFillManager::DRAIN_AND_FILL_DEFAULT)
                                    .to_string(),
                            ),
                        ]
                        .into_iter()
                        .collect(),
                    ),
                    name: Some(format!(
                        "{}-{}",
                        "page-server",
                        page_server_spec.pool_id.unwrap_or(0)
                    )),
                    namespace: Some(self.namespace.clone()),
                    ..Default::default()
                },
                spec: StatefulSetSpec {
                    // NB: The pageservers persistent volumes are instance-bound, making
                    // them effectively ephemeral. We want the corresponding PVCs to be automatically
                    // cleaned up on deletion of the StatefulSet or scaling down since we don't
                    // have to worry about data loss.
                    persistent_volume_claim_retention_policy: Some(
                        StatefulSetPersistentVolumeClaimRetentionPolicy {
                            when_deleted: Some("Delete".to_string()),
                            when_scaled: Some("Delete".to_string()),
                        },
                    ),
                    replicas: page_server_spec.replicas,
                    selector: meta::v1::LabelSelector {
                        match_labels: Some(
                            vec![("app".to_string(), "page-server".to_string())]
                                .into_iter()
                                .collect(),
                        ),
                        ..Default::default()
                    },
                    service_name: Some("page-server".to_string()),
                    volume_claim_templates: get_volume_claim_template(
                        page_server_spec.resources.clone(),
                        page_server_spec.storage_class_name.clone(),
                    )?,
                    update_strategy: Some(StatefulSetUpdateStrategy {
                        r#type: Some("RollingUpdate".to_string()),
                        rolling_update: Some(StatefulSetUpdateStrategyRollingUpdate {
                            // TODO(william.huang): Evaluate whether this 30-second wait between pod restarts is warranted.
                            min_ready_seconds: Some(30),
                            max_unavailable: Some(IntOrString::Int(1)),
                            in_place_update_strategy: None,
                            partition: None,
                            // Use the "InPlaceIfPossible" pod update policy to avoid recreating the Pod when updating container images.
                            // Recreating the Pod results in Pod IP changes, which can be disruptive to compute nodes who uses a Cloud
                            // DNS mechanism to locate the pageservers. The compute nodes can experience downtime of O(10 sec) on every
                            // pageserver IP address change due to DNS record TTLs. Recreating the Pod also re-subjects the Pod to cluster
                            // admission control and CNI (network address assignment) delays, which could also lead to potential (and
                            // unnecessary) downtime for routine container image updates.
                            //
                            // Note that the Pod won't be updated in-place (and will be recreated) if any fields unsupported by the
                            // "InPlaceIfPossible" policy are changed. Notably, the launch `command` and `arg` fields of the containers
                            // are NOT supported in in-place updates.
                            pod_update_policy: Some("InPlaceIfPossible".to_string()),
                            paused: None,
                            unordered_update: None,
                        }),
                    }),
                    template: PodTemplateSpec {
                        metadata: get_pod_metadata("page-server".to_string(), 9898),
                        spec: Some(PodSpec {
                            affinity: self.compute_node_affinity(
                                page_server_spec.node_selector.as_ref(),
                                page_server_spec.availability_zone_suffix.as_ref(),
                            ),
                            tolerations: self.node_selector_requirement_to_tolerations(
                                page_server_spec.node_selector.as_ref(),
                            ),
                            image_pull_secrets: page_server_spec.image_pull_secrets.clone(),
                            service_account_name: hadron_cluster_spec.service_account_name.clone(),
                            security_context: get_pod_security_context(),
                            // Set the pririty class to the very-high-priority "pg-compute", which should allow
                            // the safekeeper to preempt all other pods on the same nodes (including log daemon)
                            // if we run low on resources for whatever reason.
                            priority_class_name: Some("pg-compute".to_string()),
                            // The "InPlaceUpdateReady" readiness gate is required to use the "InPlaceIfPossible" pod update policy.
                            // See https://openkruise.io/docs/v1.6/user-manuals/advancedstatefulset/#in-place-update for details.
                            readiness_gates: Some(vec![PodReadinessGate {
                                condition_type: "InPlaceUpdateReady".to_string(),
                            }]),
                            volumes: Some(secret_volumes),
                            containers: vec![Container {
                                name: "page-server".to_string(),
                                image: page_server_spec.image.clone(),
                                image_pull_policy: page_server_spec.image_pull_policy.clone(),
                                ports: get_container_ports(vec![6400, 9898]),
                                resources: Some(
                                    self.extract_cpu_memory_resources(
                                        page_server_spec
                                            .resources
                                            .clone()
                                            .ok_or(anyhow::anyhow!("Expected resources"))?,
                                    )?,
                                ),
                                volume_mounts: Some(itertools::concat(vec![
                                    get_local_data_volume_mounts(),
                                    secret_volume_mounts,
                                ])),
                                command: Some(vec!["/bin/sh".to_string(), "-c".to_string()]),
                                args: Some(vec![launch_command]),
                                env: {
                                    let additional_env_vars = vec![
                                        EnvVar {
                                            name: "STORAGE_CONTROLLER_ENDPOINT".to_string(),
                                            value: Some(format!(
                                                "http://{}:{}/upcall/v1/",
                                                self.hcc_dns_name, self.hcc_listening_port
                                            )),
                                            ..Default::default()
                                        },
                                        EnvVar {
                                            name: "MIN_DISK_AVAIL_BYTES".to_string(),
                                            value: Some(format!("{}", storage_size_bytes / 5)),
                                            ..Default::default()
                                        },
                                        EnvVar {
                                            name: "NAMESPACE".to_string(),
                                            value_from: Some(EnvVarSource {
                                                field_ref: Some(ObjectFieldSelector {
                                                    field_path: "metadata.namespace".to_string(),
                                                    ..Default::default()
                                                }),
                                                ..Default::default()
                                            }),
                                            ..Default::default()
                                        },
                                        EnvVar {
                                            name: HADRON_NODE_IP_ADDRESS.to_string(),
                                            value_from: Some(EnvVarSource {
                                                field_ref: Some(ObjectFieldSelector {
                                                    field_path: "status.podIP".to_string(),
                                                    ..Default::default()
                                                }),
                                                ..Default::default()
                                            }),
                                            ..Default::default()
                                        },
                                    ];

                                    get_env_vars(object_storage_config, additional_env_vars)
                                },
                                ..Default::default()
                            }],
                            ..Default::default()
                        }),
                    },
                    ..Default::default()
                },
                status: None,
            };
            stateful_sets.push(stateful_set);
        }

        let service = get_service(
            "page-server".to_string(),
            self.namespace.clone(),
            6400,
            9898,
        );
        Ok(PageServerObjs {
            image_puller_daemonsets,
            stateful_sets,
            service,
        })
    }

    pub async fn get_http_urls_for_compute_services(&self, service_names: Vec<String>) -> Vec<Url> {
        let namespace = self
            .pg_params
            .read()
            .expect("pg_params lock poisoned")
            .compute_namespace
            .clone();

        service_names
            .into_iter()
            .map(|svc_name| {
                Url::parse(&format!(
                    "http://{svc_name}.{namespace}.svc.cluster.local.:80"
                ))
                .unwrap()
            })
            .collect()
    }
}

#[async_trait]
impl K8sManager for K8sManagerImpl {
    fn get_client(&self) -> Arc<Client> {
        self.client.clone()
    }

    fn get_current_pg_params(&self) -> Result<PgParams, anyhow::Error> {
        // Try to acquire the read lock
        let read_lock = self
            .pg_params
            .read()
            .map_err(|err| anyhow!("Failed to acquire read lock due to err: {:?}", err))?;

        Ok((*read_lock).clone())
    }

    fn set_pg_params(&self, params: PgParams) -> Result<(), anyhow::Error> {
        // Try to acquire the write lock
        let mut write_lock = self
            .pg_params
            .write()
            .map_err(|err| anyhow!("Failed to acquire write lock due to err: {:?}", err))?;

        // If we were able to then override the params
        *write_lock = params;
        Ok(())
    }

    async fn deploy_compute(&self, pg_compute: PgCompute) -> kube::Result<()> {
        self.deploy_compute(pg_compute).await
    }

    async fn delete_compute(
        &self,
        pg_compute_name: &str,
        model: ComputeModel,
    ) -> kube::Result<bool> {
        self.delete_compute(pg_compute_name, model).await
    }

    async fn get_http_urls_for_compute_services(&self, service_names: Vec<String>) -> Vec<Url> {
        self.get_http_urls_for_compute_services(service_names).await
    }

    async fn get_databricks_compute_settings(
        &self,
        workspace_url: Option<Url>,
    ) -> DatabricksSettings {
        self.get_databricks_compute_settings(workspace_url).await
    }

    async fn get_instance_primary_ingress_service(
        &self,
        instance_id: Uuid,
    ) -> kube::Result<Service> {
        self.get_ingress_service(&Self::instance_primary_ingress_service_name(instance_id))
            .await
    }

    async fn create_or_patch_instance_primary_ingress_service(
        &self,
        instance_id: Uuid,
        compute_id: Uuid,
        service_type: K8sServiceType,
    ) -> kube::Result<Service> {
        self.create_or_patch_ingress_service(
            &Self::instance_primary_ingress_service_name(instance_id),
            compute_id,
            service_type,
        )
        .await
    }

    async fn create_or_patch_readable_secondary_ingress_service(
        &self,
        instance_id: Uuid,
    ) -> kube::Result<Service> {
        self.create_if_not_exists_readable_secondary_ingress(
            Self::instance_read_only_ingress_service_name(instance_id),
            instance_id,
        )
        .await
    }

    async fn delete_instance_primary_ingress_service(
        &self,
        instance_id: Uuid,
    ) -> kube::Result<bool> {
        self.delete_ingress_service(&Self::instance_primary_ingress_service_name(instance_id))
            .await
    }
}

#[cfg(test)]
mod tests {
    use crate::hadron_k8s::{CloudProvider, BRICKSTORE_POOL_TYPES_LABEL_KEY};
    use camino_tempfile::Utf8TempDir;
    use compute_api::spec::{DatabricksSettings, PgComputeTlsSettings};
    use http::{Request, Response};
    use k8s_openapi::api::core::v1::ConfigMapVolumeSource;
    use k8s_openapi::api::core::v1::{
        NodeSelectorRequirement, SecretVolumeSource, Volume, VolumeMount,
    };
    use kube::client::Body;
    use kube::Client;
    use reqwest::Url;
    use std::fs::File;
    use std::io::Write;
    use std::sync::Arc;

    use crate::hadron_k8s::HadronObjectStorageConfig;
    use crate::hadron_k8s::{
        endpoint_default_resources, select_node_group_by_tshirt_size, BrcDbletNodeGroup, K8sMount,
        MountType, PgParams,
    };
    use hcc_api::models::EndpointTShirtSize;
    use tower_test::mock;

    use super::K8sManagerImpl;

    use super::ConfigData;

    // Test PgParams parsing behavior.
    #[test]
    fn test_pg_params_parsing() {
        // Test that PgParams parsing fails when the input is empty or malformed.
        assert!(serde_json::from_str::<PgParams>("").is_err());
        assert!(serde_json::from_str::<PgParams>("{").is_err());

        // Sanity check that PgParams now contains required fields and there is no "default" behavior for
        // required fields.
        assert!(serde_json::from_str::<PgParams>("{}").is_err());
        // Test that PgParams parsing fails when a required field is mis-spelled ("comptue_image" instead of "compute_image").
        assert!(serde_json::from_str::<PgParams>(
            r#"{
            "compute_namespace": "hadron-compute",
            "comptue_image": "hadron/compute-image:v1",
            "prometheus_exporter_image": "hadron/prometheus-exporter-image:v1"
        }"#
        )
        .is_err());

        // Tests that once we can parse out the required fields, the rest of the feilds are optional and
        // fall back to expected default values.
        let parsed_config: PgParams = serde_json::from_str(
            r#"{
          "compute_namespace": "hadron-compute",
          "compute_image": "hadron/compute-image:v1",
          "prometheus_exporter_image": "hadron/prometheus-exporter-image:v1"
        }"#,
        )
        .unwrap();
        assert_eq!(
            parsed_config.compute_namespace,
            "hadron-compute".to_string()
        );
        assert_eq!(
            parsed_config.compute_image,
            "hadron/compute-image:v1".to_string()
        );
        assert_eq!(
            parsed_config.prometheus_exporter_image,
            "hadron/prometheus-exporter-image:v1".to_string()
        );
        assert_eq!(
            parsed_config.compute_image_pull_secret,
            Some("harbor-image-pull-secret".to_string())
        );
        assert_eq!(parsed_config.compute_pg_port, Some(55432));
        assert_eq!(parsed_config.compute_http_port, Some(55433));
        assert_eq!(
            parsed_config.compute_mounts,
            Some(vec![
                K8sMount {
                    name: "brickstore-internal-token-public-keys".to_string(),
                    mount_type: MountType::Secret,
                    mount_path: "/databricks/secrets/brickstore-internal-token-public-keys"
                        .to_string(),
                    files: vec!["key1.pem".to_string(), "key2.pem".to_string()],
                },
                K8sMount {
                    name: "brickstore-domain-certs".to_string(),
                    mount_type: MountType::Secret,
                    mount_path: "/databricks/secrets/brickstore-domain-certs".to_string(),
                    files: vec!["server.key".to_string(), "server.crt".to_string()],
                },
                K8sMount {
                    name: "trusted-ca-certificates".to_string(),
                    mount_type: MountType::Secret,
                    mount_path: "/databricks/secrets/trusted-ca".to_string(),
                    files: vec!["data-plane-misc-root-ca-cert.pem".to_string()],
                },
                K8sMount {
                    name: "pg-compute-config".to_string(),
                    mount_type: MountType::ConfigMap,
                    mount_path: "/databricks/pg_config".to_string(),
                    files: vec![
                        "databricks_pg_hba.conf".to_string(),
                        "databricks_pg_ident.conf".to_string()
                    ],
                }
            ])
        );
        assert_eq!(
            parsed_config.pg_compute_tls_settings,
            Some(PgComputeTlsSettings {
                key_file: "/databricks/secrets/brickstore-domain-certs/server.key".to_string(),
                cert_file: "/databricks/secrets/brickstore-domain-certs/server.crt".to_string(),
                ca_file: "/databricks/secrets/trusted-ca/data-plane-misc-root-ca-cert.pem"
                    .to_string(),
            })
        );
        assert_eq!(
            parsed_config.databricks_pg_hba,
            Some("/databricks/pg_config/databricks_pg_hba.conf".to_string())
        );
        assert_eq!(
            parsed_config.databricks_pg_ident,
            Some("/databricks/pg_config/databricks_pg_ident.conf".to_string())
        );
    }

    // Test storage billing params serialization to TOML.
    #[test]
    fn test_page_server_billing_metrics_config_to_toml() {
        let mut billing_metrics_config = super::PageServerBillingMetricsConfig {
            metric_collection_endpoint: Some("http://localhost:8080/metrics".to_string()),
            metric_collection_interval: Some("5 min".to_string()),
            synthetic_size_calculation_interval: Some("5 min".to_string()),
        };

        let mut toml_str = billing_metrics_config.to_toml();
        assert_eq!(
            toml_str,
            r#"metric_collection_endpoint = "http://localhost:8080/metrics"
metric_collection_interval = "5 min"
synthetic_size_calculation_interval = "5 min"
"#
        );

        billing_metrics_config.metric_collection_endpoint = None;
        toml_str = billing_metrics_config.to_toml();

        assert_eq!(
            toml_str,
            r#"metric_collection_interval = "5 min"
synthetic_size_calculation_interval = "5 min"
"#
        );
    }

    // Test demonstrating the cluster-config ConfigMap parsing behavior.
    #[test]
    fn test_cluster_config_parsing() {
        let parsed_config_data: ConfigData = serde_json::from_str(
            r#"{
            "hadron_cluster": {
                "hadron_cluster_spec": {
                    "storage_broker_spec": {
                        "image": "sb-image"
                    },
                    "safe_keeper_specs": [
                        {
                            "pool_id": 0,
                            "replicas": 1,
                            "image": "sk-image",
                            "storage_class_name": "sk-storage-class"
                        }
                    ],
                    "page_server_specs": [
                        {
                            "pool_id": 0,
                            "replicas": 1,
                            "image": "ps-image",
                            "storage_class_name": "ps-storage-class"

                        }
                    ]
                }
            },
            "pg_params": {
                "compute_namespace": "test-ns",
                "compute_image": "compute-image",
                "prometheus_exporter_image": "prometheus-exporter-image",
                "compute_image_pull_secret": "test-image-pull-secret",
                "compute_mounts" : [
                    {
                        "name": "secret",
                        "mount_path": "/databricks/secrets/secret",
                        "mount_type": "secret",
                        "files": ["file1", "file2"]
                    },
                    {
                        "name": "another-secret",
                        "mount_path": "/databricks/secrets/another-secret",
                        "mount_type": "secret",
                        "files": ["another_file1", "another_file2"]
                    },
                    {
                        "name": "config-map",
                        "mount_type": "config_map",
                        "mount_path": "/databricks/pg_config",
                        "files": ["config1", "config2"]
                    }
                ],
                "pg_compute_tls_settings": {
                    "key_file": "/databricks/secrets/some-directory/server.key",
                    "cert_file": "/databricks/secrets/some-directory/server.crt",
                    "ca_file": "/databricks/secrets/some-directory/ca.crt"
                },
                "databricks_pg_hba": "/databricks/pg_config/hba",
                "databricks_pg_ident": "/databricks/pg_config/ident"
            },
            "page_server_billing_metrics_config": {
                "metric_collection_endpoint": "http://localhost:8080/metrics",
                "metric_collection_interval": "5 min",
                "synthetic_size_calculation_interval": "5 min"
            }
        }"#,
        )
        .unwrap();

        let hadron_cluster_spec = parsed_config_data
            .hadron_cluster
            .unwrap()
            .hadron_cluster_spec
            .unwrap();

        let storage_broker_spec = hadron_cluster_spec.storage_broker_spec.unwrap();
        assert_eq!(storage_broker_spec.image, Some("sb-image".to_string()));

        let safe_keeper_specs = hadron_cluster_spec.safe_keeper_specs.unwrap();
        let safe_keeper_spec = safe_keeper_specs.first().unwrap();
        assert_eq!(safe_keeper_spec.pool_id, Some(0));
        assert_eq!(safe_keeper_spec.replicas, Some(1));
        assert_eq!(safe_keeper_spec.image, Some("sk-image".to_string()));
        assert_eq!(
            safe_keeper_spec.storage_class_name,
            Some("sk-storage-class".to_string())
        );

        let page_server_specs = hadron_cluster_spec.page_server_specs.unwrap();
        let page_server_spec = page_server_specs.first().unwrap();
        assert_eq!(page_server_spec.pool_id, Some(0));
        assert_eq!(page_server_spec.replicas, Some(1));
        assert_eq!(page_server_spec.image, Some("ps-image".to_string()));
        assert_eq!(
            page_server_spec.storage_class_name,
            Some("ps-storage-class".to_string())
        );

        let pg_params = parsed_config_data.pg_params.unwrap();
        assert_eq!(pg_params.compute_namespace, "test-ns".to_string());
        assert_eq!(pg_params.compute_image, "compute-image".to_string());
        assert_eq!(
            pg_params.prometheus_exporter_image,
            "prometheus-exporter-image".to_string()
        );
        assert_eq!(
            pg_params.compute_image_pull_secret,
            Some("test-image-pull-secret".to_string())
        );
        assert_eq!(
            pg_params.compute_mounts,
            Some(vec![
                K8sMount {
                    name: "secret".to_string(),
                    mount_type: MountType::Secret,
                    mount_path: "/databricks/secrets/secret".to_string(),
                    files: vec!["file1".to_string(), "file2".to_string()],
                },
                K8sMount {
                    name: "another-secret".to_string(),
                    mount_type: MountType::Secret,
                    mount_path: "/databricks/secrets/another-secret".to_string(),
                    files: vec!["another_file1".to_string(), "another_file2".to_string()],
                },
                K8sMount {
                    name: "config-map".to_string(),
                    mount_type: MountType::ConfigMap,
                    mount_path: "/databricks/pg_config".to_string(),
                    files: vec!["config1".to_string(), "config2".to_string()],
                }
            ])
        );
        assert_eq!(
            pg_params.pg_compute_tls_settings,
            Some(PgComputeTlsSettings {
                key_file: "/databricks/secrets/some-directory/server.key".to_string(),
                cert_file: "/databricks/secrets/some-directory/server.crt".to_string(),
                ca_file: "/databricks/secrets/some-directory/ca.crt".to_string()
            })
        );
        assert_eq!(
            pg_params.databricks_pg_hba,
            Some("/databricks/pg_config/hba".to_string())
        );
        assert_eq!(
            pg_params.databricks_pg_ident,
            Some("/databricks/pg_config/ident".to_string())
        );
    }

    #[test]
    fn test_node_group_to_string() {
        assert_eq!(BrcDbletNodeGroup::Dblet2C.to_string(), "dbletbrc2c");
        assert_eq!(BrcDbletNodeGroup::Dblet4C.to_string(), "dbletbrc4c");
        assert_eq!(BrcDbletNodeGroup::Dblet8C.to_string(), "dbletbrc8c");
        assert_eq!(BrcDbletNodeGroup::Dblet16C.to_string(), "dbletbrc16c");
    }

    #[test]
    fn test_node_group_selection() {
        assert_eq!(
            select_node_group_by_tshirt_size(&EndpointTShirtSize::XSmall),
            BrcDbletNodeGroup::Dblet2C
        );
        assert_eq!(
            select_node_group_by_tshirt_size(&EndpointTShirtSize::Small),
            BrcDbletNodeGroup::Dblet4C
        );
        assert_eq!(
            select_node_group_by_tshirt_size(&EndpointTShirtSize::Medium),
            BrcDbletNodeGroup::Dblet8C
        );
        assert_eq!(
            select_node_group_by_tshirt_size(&EndpointTShirtSize::Large),
            BrcDbletNodeGroup::Dblet16C
        );
        assert_eq!(
            select_node_group_by_tshirt_size(&EndpointTShirtSize::Test),
            BrcDbletNodeGroup::Dblet4C
        );
    }

    #[test]
    fn test_default_endpoint_config() {
        let default_resources = endpoint_default_resources();
        let requests = default_resources
            .requests
            .clone()
            .expect("resources.requests exist");
        assert_eq!(
            requests.get("cpu").expect("resources.requests.cpu exist").0,
            "500m"
        );
        assert_eq!(
            requests
                .get("memory")
                .expect("resources.requests.memory exist")
                .0,
            "4Gi"
        );
        assert!(default_resources.limits.is_none());
    }

    #[tokio::test]
    async fn test_node_affinity_generation() {
        // We don't really use the k8s client in this test, so just use a non-functional mock client so that we can construct a K8sManager object.
        let (mock_service, mut _handle) = mock::pair::<Request<Body>, Response<Body>>();
        let mock_client = Arc::new(Client::new(mock_service, "default"));

        let test_k8s_manager =
            K8sManagerImpl::new_for_test(mock_client, "test-region-1".to_string(), None);

        // Test the trivial case.
        assert!(test_k8s_manager.compute_node_affinity(None, None).is_none());

        let node_group_req = NodeSelectorRequirement {
            key: BRICKSTORE_POOL_TYPES_LABEL_KEY.to_string(),
            operator: "In".to_string(),
            values: Some(vec!["brc16cn".to_string()]),
        };

        // Test that the topology.kubernetes.io/zone label is not added to "matchExpressions" when the availability zone suffix is not specified.
        assert_eq!(
            test_k8s_manager
                .compute_node_affinity(Some(&node_group_req), None)
                .unwrap(),
            serde_json::from_str(
                r#"{
                    "nodeAffinity": {
                        "requiredDuringSchedulingIgnoredDuringExecution": {
                            "nodeSelectorTerms": [
                                {
                                    "matchExpressions": [
                                        {
                                            "key": "brickstore-pool-types",
                                            "operator": "In",
                                            "values": ["brc16cn"]
                                        }
                                    ]
                                }
                            ]
                        }
                    }
                }"#
            )
            .unwrap()
        );

        // Test that specifying the availability zone suffix results in the correct "topology.kubernetes.io/zone" match expression to be added.
        assert_eq!(
            test_k8s_manager
                .compute_node_affinity(Some(&node_group_req), Some(&"a".to_string()))
                .unwrap(),
            serde_json::from_str(
                r#"{
                    "nodeAffinity": {
                        "requiredDuringSchedulingIgnoredDuringExecution": {
                            "nodeSelectorTerms": [
                                {
                                    "matchExpressions": [
                                        {
                                            "key": "brickstore-pool-types",
                                            "operator": "In",
                                            "values": ["brc16cn"]
                                        },
                                        {
                                            "key": "topology.kubernetes.io/zone",
                                            "operator": "In",
                                            "values": ["test-region-1a"]
                                        }
                                    ]
                                }
                            ]
                        }
                    }
                }"#
            )
            .unwrap()
        );
    }

    /// Test that K8sSecretMount is converted to k8s Volume and VolumeMount correctly.
    #[tokio::test]
    async fn test_get_volumes_and_mounts() {
        let (mock_service, mut _handle) = mock::pair::<Request<Body>, Response<Body>>();
        let mock_client = Arc::new(Client::new(mock_service, "default"));
        let test_k8s_manager =
            K8sManagerImpl::new_for_test(mock_client, "test-region-1".to_string(), None);

        let mounts = vec![
            K8sMount {
                name: "secret".to_string(),
                mount_type: MountType::Secret,
                mount_path: "/databricks/secrets/dir1".to_string(),
                files: vec!["file1".to_string(), "file2".to_string()],
            },
            K8sMount {
                name: "config-map".to_string(),
                mount_type: MountType::ConfigMap,
                mount_path: "/databricks/config".to_string(),
                files: vec!["config1".to_string(), "config2".to_string()],
            },
        ];

        let volumes_and_mounts = test_k8s_manager.get_hadron_volumes_and_mounts(mounts);

        let expected_volumes = vec![
            Volume {
                name: "secret".to_string(),
                secret: Some(SecretVolumeSource {
                    secret_name: Some("secret".to_string()),
                    ..Default::default()
                }),
                ..Default::default()
            },
            Volume {
                name: "config-map".to_string(),
                config_map: Some(ConfigMapVolumeSource {
                    name: "config-map".to_string(),
                    ..Default::default()
                }),
                ..Default::default()
            },
        ];

        let expected_volume_mounts = vec![
            VolumeMount {
                name: "secret".to_string(),
                read_only: Some(true),
                mount_path: "/databricks/secrets/dir1".to_string(),
                ..Default::default()
            },
            VolumeMount {
                name: "config-map".to_string(),
                read_only: Some(true),
                mount_path: "/databricks/config".to_string(),
                ..Default::default()
            },
        ];

        assert_eq!(volumes_and_mounts.volumes, expected_volumes);
        assert_eq!(volumes_and_mounts.volume_mounts, expected_volume_mounts);
    }

    /// Test that get_databricks_compute_settings returns the correct settings.
    #[tokio::test]
    async fn test_get_databricks_compute_settings() {
        let (mock_service, mut _handle) = mock::pair::<Request<Body>, Response<Body>>();
        let mock_client = Arc::new(Client::new(mock_service, "default"));
        let test_k8s_manager =
            K8sManagerImpl::new_for_test(mock_client, "test-region-1".to_string(), None);

        let databricks_compute_settings = test_k8s_manager
            .get_databricks_compute_settings(Some(
                Url::parse("https://test-workspace.databricks.com").unwrap(),
            ))
            .await;

        let expected_settings = DatabricksSettings {
            pg_compute_tls_settings: PgComputeTlsSettings {
                key_file: "/databricks/secrets/brickstore-domain-certs/server.key".to_string(),
                cert_file: "/databricks/secrets/brickstore-domain-certs/server.crt".to_string(),
                ca_file: "/databricks/secrets/trusted-ca/data-plane-misc-root-ca-cert.pem"
                    .to_string(),
            },
            databricks_pg_hba: "/databricks/pg_config/databricks_pg_hba.conf".to_string(),
            databricks_pg_ident: "/databricks/pg_config/databricks_pg_ident.conf".to_string(),
            databricks_workspace_host: "test-workspace.databricks.com".to_string(),
        };

        assert_eq!(databricks_compute_settings, expected_settings)
    }

    // Test the functionality of `K8sManager::get_http_urls_for_compute_services()`. Just to make sure we don't have stupid
    // bugs/typos that would cause the function to panic when unwrapping Url::parse() results.
    #[tokio::test]
    async fn test_compute_service_url_generation() {
        let (mock_service, mut _handle) = mock::pair::<Request<Body>, Response<Body>>();
        let mock_client = Arc::new(Client::new(mock_service, "default"));
        let test_k8s_manager =
            K8sManagerImpl::new_for_test(mock_client, "test-region-1".to_string(), None);

        let service_names = vec!["pg-abc-admin".to_string(), "pg-xyz-admin".to_string()];

        let expected_urls = vec![
            Url::parse("http://pg-abc-admin.test-namespace.svc.cluster.local.:80").unwrap(),
            Url::parse("http://pg-xyz-admin.test-namespace.svc.cluster.local.:80").unwrap(),
        ];

        let actual_urls = test_k8s_manager
            .get_http_urls_for_compute_services(service_names)
            .await;

        assert_eq!(actual_urls, expected_urls);
    }

    #[tokio::test]
    async fn test_get_remote_storage_startup_args() {
        let (mock_service, mut _handle) = mock::pair::<Request<Body>, Response<Body>>();
        let mock_client = Arc::new(Client::new(mock_service, "default"));
        let test_k8s_manager =
            K8sManagerImpl::new_for_test(mock_client, "test-region-1".to_string(), None);

        let aws_storage_config = HadronObjectStorageConfig {
            bucket_name: Some("test-bucket".to_string()),
            bucket_region: Some("us-west-2".to_string()),
            ..Default::default()
        };

        let mut result = test_k8s_manager.get_remote_storage_startup_args(&aws_storage_config);

        assert_eq!(result, "{bucket_name='$S3_BUCKET_URI', bucket_region='$S3_REGION', prefix_in_bucket='$PREFIX_IN_BUCKET'}");

        let azure_storage_config = HadronObjectStorageConfig {
            storage_account_resource_id: Some("/subscriptions/123/resourceGroups/xyz/providers/Microsoft.Storage/storageAccounts/def".to_string()),
            azure_tenant_id: Some("456".to_string()),
            storage_container_name: Some("container".to_string()),
            storage_container_region: Some("westus".to_string()),
            ..Default::default()
        };

        result = test_k8s_manager.get_remote_storage_startup_args(&azure_storage_config);

        assert_eq!(result, "{storage_account='$AZURE_STORAGE_ACCOUNT_NAME', container_name='$AZURE_STORAGE_CONTAINER_NAME', container_region='$AZURE_STORAGE_CONTAINER_REGION', prefix_in_container='$PREFIX_IN_BUCKET'}");
    }

    #[tokio::test]
    async fn test_build_compute_service() {
        let (mock_service, mut _handle) = mock::pair::<Request<Body>, Response<Body>>();
        let mock_client = Arc::new(Client::new(mock_service, "default"));
        let mut test_k8s_manager = K8sManagerImpl::new_for_test(
            mock_client.clone(),
            "test-region-1".to_string(),
            Some(CloudProvider::AWS),
        );

        let test_compute_name = "test-pg".to_string();

        let mut service =
            test_k8s_manager.build_loadbalancer_service(1234, test_compute_name.clone());

        // On AWS, the service should have 3 annotations and use an externalTrafficPolicy of "Local".
        assert_eq!(service.metadata.annotations.clone().unwrap().len(), 3);
        // All of the annotations should contain the string "aws".
        for annotation in service.metadata.annotations.clone().unwrap().keys() {
            assert!(annotation.contains("aws"));
        }
        assert_eq!(
            service.spec.unwrap().external_traffic_policy.unwrap(),
            "Local"
        );

        test_k8s_manager = K8sManagerImpl::new_for_test(
            mock_client.clone(),
            "test-region-1".to_string(),
            Some(CloudProvider::Azure),
        );

        service = test_k8s_manager.build_loadbalancer_service(1234, test_compute_name.clone());

        // On Azure, the service should have 2 annotations and use an externalTrafficPolicy of "Cluster".
        assert_eq!(service.metadata.annotations.clone().unwrap().len(), 2);
        // All of the annotations should contain the string "azure".
        for annotation in service.metadata.annotations.clone().unwrap().keys() {
            assert!(annotation.contains("azure"));
        }
        assert_eq!(
            service.spec.unwrap().external_traffic_policy.unwrap(),
            "Cluster"
        );
        // Ensure the DNS label annotation is properly set.
        assert_eq!(
            service
                .metadata
                .annotations
                .clone()
                .unwrap()
                .get("service.beta.kubernetes.io/azure-dns-label-name")
                .unwrap()
                .to_string(),
            test_compute_name.clone()
        );
    }

    #[tokio::test]
    async fn test_node_selector_requirement_to_tolerations() {
        let (mock_service, mut _handle) = mock::pair::<Request<Body>, Response<Body>>();
        let mock_client = Arc::new(Client::new(mock_service, "default"));

        let clouds = vec![CloudProvider::AWS, CloudProvider::Azure];

        for cloud in clouds {
            let test_k8s_manager = K8sManagerImpl::new_for_test(
                mock_client.clone(),
                "test-region-1".to_string(),
                Some(cloud.clone()),
            );

            let node_selector_requirement = NodeSelectorRequirement {
                key: BRICKSTORE_POOL_TYPES_LABEL_KEY.to_string(),
                operator: "In".to_string(),
                values: Some(vec!["brc16cn".to_string()]),
            };

            let tolerations = test_k8s_manager
                .node_selector_requirement_to_tolerations(Some(&node_selector_requirement))
                .unwrap();

            assert_eq!(tolerations.len(), 1);
            assert_eq!(
                tolerations[0].key,
                Some("databricks.com/node-type".to_string())
            );
            assert_eq!(tolerations[0].operator, Some("Equal".to_string()));
            assert_eq!(tolerations[0].value, Some("brc16cn".to_string()));

            if cloud.clone() == CloudProvider::AWS {
                assert_eq!(tolerations[0].effect, Some("NoSchedule".to_string()));
            } else {
                assert_eq!(tolerations[0].effect, Some("PreferNoSchedule".to_string()));
            }
        }
    }

    #[tokio::test]
    async fn test_read_and_validate_cluster_config() {
        // Create a temporary directory to hold test config files. The directory is cleaned up when `tmp_dir` goes out of scope.
        let tmp_dir = Utf8TempDir::new().unwrap();

        // Case 1: Nonexistent file
        let bad_path = tmp_dir.path().join("nonexistent.json");
        let err = K8sManagerImpl::read_and_validate_cluster_config(bad_path.as_str()).await;
        assert!(err.is_err(), "Should fail on nonexistent file");

        // Case 2: Malformed JSON
        let malformed_path = tmp_dir.path().join("malformed.json");
        {
            let mut file = File::create(malformed_path.as_std_path()).unwrap();
            writeln!(file, "{{ not valid json").unwrap();
        }
        let err = K8sManagerImpl::read_and_validate_cluster_config(malformed_path.as_str()).await;
        assert!(err.is_err(), "Should fail on malformed JSON");

        // Case 3: Missing required fields
        let missing_fields_path = tmp_dir.path().join("missing_fields.json");
        {
            let mut file = File::create(missing_fields_path.as_std_path()).unwrap();
            writeln!(
                file,
                r#"{{ "hadron_cluster": {{ "hadron_cluster_spec": null }} }}"#
            )
            .unwrap();
        }
        let err =
            K8sManagerImpl::read_and_validate_cluster_config(missing_fields_path.as_str()).await;
        assert!(
            err.is_err(),
            "Should fail on config missing required fields"
        );

        // Case 4: Happy path
        let valid_path = tmp_dir.path().join("valid.json");
        {
            let mut file = File::create(valid_path.as_std_path()).unwrap();
            writeln!(
                file,
                r#"
            {{
              "hadron_cluster": {{
                "hadron_cluster_spec": {{
                  "object_storage_config": {{
                    "bucket_name": "mybucket"
                  }}
                }}
              }},
              "pg_params": {{
                "compute_namespace": "default",
                "compute_image": "postgres:latest",
                "prometheus_exporter_image": "exporter:latest"
              }}
            }}"#
            )
            .unwrap();
        }
        let result = K8sManagerImpl::read_and_validate_cluster_config(valid_path.as_str()).await;
        assert!(result.is_ok(), "Should succeed on valid config");
        let (cluster, pg_params, hash, _) = result.unwrap();
        assert!(
            cluster.hadron_cluster_spec.is_some(),
            "Cluster spec present"
        );
        assert_eq!(
            pg_params.compute_namespace, "default",
            "Parsed namespace correctly"
        );
        assert!(!hash.is_empty(), "Hash should be populated");
    }
}

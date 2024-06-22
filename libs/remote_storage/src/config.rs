use std::{fmt::Debug, num::NonZeroUsize, str::FromStr, time::Duration};

use anyhow::bail;
use aws_sdk_s3::types::StorageClass;
use camino::Utf8PathBuf;

use serde::{Deserialize, Serialize};

use crate::{
    DEFAULT_MAX_KEYS_PER_LIST_RESPONSE, DEFAULT_REMOTE_STORAGE_AZURE_CONCURRENCY_LIMIT,
    DEFAULT_REMOTE_STORAGE_S3_CONCURRENCY_LIMIT,
};

/// External backup storage configuration, enough for creating a client for that storage.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct RemoteStorageConfig {
    /// The storage connection configuration.
    #[serde(flatten)]
    pub storage: RemoteStorageKind,
    /// A common timeout enforced for all requests after concurrency limiter permit has been
    /// acquired.
    #[serde(
        with = "humantime_serde",
        default = "default_timeout",
        skip_serializing_if = "is_default_timeout"
    )]
    pub timeout: Duration,
}

fn default_timeout() -> Duration {
    RemoteStorageConfig::DEFAULT_TIMEOUT
}

fn is_default_timeout(d: &Duration) -> bool {
    *d == RemoteStorageConfig::DEFAULT_TIMEOUT
}

/// A kind of a remote storage to connect to, with its connection configuration.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(untagged)]
pub enum RemoteStorageKind {
    /// Storage based on local file system.
    /// Specify a root folder to place all stored files into.
    LocalFs { local_path: Utf8PathBuf },
    /// AWS S3 based storage, storing all files in the S3 bucket
    /// specified by the config
    AwsS3(S3Config),
    /// Azure Blob based storage, storing all files in the container
    /// specified by the config
    AzureContainer(AzureConfig),
}

/// AWS S3 bucket coordinates and access credentials to manage the bucket contents (read and write).
#[derive(Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct S3Config {
    /// Name of the bucket to connect to.
    pub bucket_name: String,
    /// The region where the bucket is located at.
    pub bucket_region: String,
    /// A "subfolder" in the bucket, to use the same bucket separately by multiple remote storage users at once.
    pub prefix_in_bucket: Option<String>,
    /// A base URL to send S3 requests to.
    /// By default, the endpoint is derived from a region name, assuming it's
    /// an AWS S3 region name, erroring on wrong region name.
    /// Endpoint provides a way to support other S3 flavors and their regions.
    ///
    /// Example: `http://127.0.0.1:5000`
    pub endpoint: Option<String>,
    /// AWS S3 has various limits on its API calls, we need not to exceed those.
    /// See [`DEFAULT_REMOTE_STORAGE_S3_CONCURRENCY_LIMIT`] for more details.
    #[serde(default = "default_remote_storage_s3_concurrency_limit")]
    pub concurrency_limit: NonZeroUsize,
    #[serde(default = "default_max_keys_per_list_response")]
    pub max_keys_per_list_response: Option<i32>,
    #[serde(
        deserialize_with = "deserialize_storage_class",
        serialize_with = "serialize_storage_class",
        default
    )]
    pub upload_storage_class: Option<StorageClass>,
}

fn default_remote_storage_s3_concurrency_limit() -> NonZeroUsize {
    DEFAULT_REMOTE_STORAGE_S3_CONCURRENCY_LIMIT
        .try_into()
        .unwrap()
}

fn default_max_keys_per_list_response() -> Option<i32> {
    DEFAULT_MAX_KEYS_PER_LIST_RESPONSE
}

impl Debug for S3Config {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S3Config")
            .field("bucket_name", &self.bucket_name)
            .field("bucket_region", &self.bucket_region)
            .field("prefix_in_bucket", &self.prefix_in_bucket)
            .field("concurrency_limit", &self.concurrency_limit)
            .field(
                "max_keys_per_list_response",
                &self.max_keys_per_list_response,
            )
            .finish()
    }
}

/// Azure  bucket coordinates and access credentials to manage the bucket contents (read and write).
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AzureConfig {
    /// Name of the container to connect to.
    pub container_name: String,
    /// Name of the storage account the container is inside of
    pub storage_account: Option<String>,
    /// The region where the bucket is located at.
    pub container_region: String,
    /// A "subfolder" in the container, to use the same container separately by multiple remote storage users at once.
    pub prefix_in_container: Option<String>,
    /// Azure has various limits on its API calls, we need not to exceed those.
    /// See [`DEFAULT_REMOTE_STORAGE_AZURE_CONCURRENCY_LIMIT`] for more details.
    #[serde(default = "default_remote_storage_azure_concurrency_limit")]
    pub concurrency_limit: NonZeroUsize,
    #[serde(default = "default_max_keys_per_list_response")]
    pub max_keys_per_list_response: Option<i32>,
}

fn default_remote_storage_azure_concurrency_limit() -> NonZeroUsize {
    NonZeroUsize::new(DEFAULT_REMOTE_STORAGE_AZURE_CONCURRENCY_LIMIT).unwrap()
}

impl Debug for AzureConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AzureConfig")
            .field("bucket_name", &self.container_name)
            .field("storage_account", &self.storage_account)
            .field("bucket_region", &self.container_region)
            .field("prefix_in_container", &self.prefix_in_container)
            .field("concurrency_limit", &self.concurrency_limit)
            .field(
                "max_keys_per_list_response",
                &self.max_keys_per_list_response,
            )
            .finish()
    }
}

fn deserialize_storage_class<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<StorageClass>, D::Error> {
    Option::<String>::deserialize(deserializer).and_then(|s| {
        if let Some(s) = s {
            use serde::de::Error;
            let storage_class = StorageClass::from_str(&s).expect("infallible");
            #[allow(deprecated)]
            if matches!(storage_class, StorageClass::Unknown(_)) {
                return Err(D::Error::custom(format!(
                    "Specified storage class unknown to SDK: '{s}'. Allowed values: {:?}",
                    StorageClass::values()
                )));
            }
            Ok(Some(storage_class))
        } else {
            Ok(None)
        }
    })
}

fn serialize_storage_class<S: serde::Serializer>(
    val: &Option<StorageClass>,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    let val = val.as_ref().map(StorageClass::as_str);
    Option::<&str>::serialize(&val, serializer)
}

impl RemoteStorageConfig {
    pub const DEFAULT_TIMEOUT: Duration = std::time::Duration::from_secs(120);

    pub fn from_toml(toml: &toml_edit::Item) -> anyhow::Result<Option<RemoteStorageConfig>> {
        let document: toml_edit::Document = match toml {
            toml_edit::Item::Table(toml) => toml.clone().into(),
            toml_edit::Item::Value(toml_edit::Value::InlineTable(toml)) => {
                toml.clone().into_table().into()
            }
            _ => bail!("toml not a table or inline table"),
        };

        if document.is_empty() {
            return Ok(None);
        }

        Ok(Some(toml_edit::de::from_document(document)?))
    }
}

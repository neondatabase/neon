//! A set of generic storage abstractions for the page server to use when backing up and restoring its state from the external storage.
//! No other modules from this tree are supposed to be used directly by the external code.
//!
//! [`RemoteStorage`] trait a CRUD-like generic abstraction to use for adapting external storages with a few implementations:
//!   * [`local_fs`] allows to use local file system as an external storage
//!   * [`s3_bucket`] uses AWS S3 bucket as an external storage
//!   * [`azure_blob`] allows to use Azure Blob storage as an external storage
//!
#![deny(unsafe_code)]
#![deny(clippy::undocumented_unsafe_blocks)]

mod azure_blob;
mod local_fs;
mod s3_bucket;
mod simulate_failures;
mod support;

use std::{
    collections::HashMap, fmt::Debug, num::NonZeroUsize, pin::Pin, sync::Arc, time::SystemTime,
};

use anyhow::{bail, Context};
use camino::{Utf8Path, Utf8PathBuf};

use bytes::Bytes;
use futures::stream::Stream;
use serde::{Deserialize, Serialize};
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;
use toml_edit::Item;
use tracing::info;

pub use self::{
    azure_blob::AzureBlobStorage, local_fs::LocalFs, s3_bucket::S3Bucket,
    simulate_failures::UnreliableWrapper,
};
use s3_bucket::RequestKind;

/// Currently, sync happens with AWS S3, that has two limits on requests per second:
/// ~200 RPS for IAM services
/// <https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/UsingWithRDS.IAMDBAuth.html>
/// ~3500 PUT/COPY/POST/DELETE or 5500 GET/HEAD S3 requests
/// <https://aws.amazon.com/premiumsupport/knowledge-center/s3-request-limit-avoid-throttling/>
pub const DEFAULT_REMOTE_STORAGE_S3_CONCURRENCY_LIMIT: usize = 100;
/// We set this a little bit low as we currently buffer the entire file into RAM
///
/// Here, a limit of max 20k concurrent connections was noted.
/// <https://learn.microsoft.com/en-us/answers/questions/1301863/is-there-any-limitation-to-concurrent-connections>
pub const DEFAULT_REMOTE_STORAGE_AZURE_CONCURRENCY_LIMIT: usize = 30;
/// No limits on the client side, which currenltly means 1000 for AWS S3.
/// <https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectsV2.html#API_ListObjectsV2_RequestSyntax>
pub const DEFAULT_MAX_KEYS_PER_LIST_RESPONSE: Option<i32> = None;

/// As defined in S3 docs
pub const MAX_KEYS_PER_DELETE: usize = 1000;

const REMOTE_STORAGE_PREFIX_SEPARATOR: char = '/';

/// Path on the remote storage, relative to some inner prefix.
/// The prefix is an implementation detail, that allows representing local paths
/// as the remote ones, stripping the local storage prefix away.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RemotePath(Utf8PathBuf);

impl Serialize for RemotePath {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for RemotePath {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let str = String::deserialize(deserializer)?;
        Ok(Self(Utf8PathBuf::from(&str)))
    }
}

impl std::fmt::Display for RemotePath {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.0, f)
    }
}

impl RemotePath {
    pub fn new(relative_path: &Utf8Path) -> anyhow::Result<Self> {
        anyhow::ensure!(
            relative_path.is_relative(),
            "Path {relative_path:?} is not relative"
        );
        Ok(Self(relative_path.to_path_buf()))
    }

    pub fn from_string(relative_path: &str) -> anyhow::Result<Self> {
        Self::new(Utf8Path::new(relative_path))
    }

    pub fn with_base(&self, base_path: &Utf8Path) -> Utf8PathBuf {
        base_path.join(&self.0)
    }

    pub fn object_name(&self) -> Option<&str> {
        self.0.file_name()
    }

    pub fn join(&self, segment: &Utf8Path) -> Self {
        Self(self.0.join(segment))
    }

    pub fn get_path(&self) -> &Utf8PathBuf {
        &self.0
    }

    pub fn extension(&self) -> Option<&str> {
        self.0.extension()
    }

    pub fn strip_prefix(&self, p: &RemotePath) -> Result<&Utf8Path, std::path::StripPrefixError> {
        self.0.strip_prefix(&p.0)
    }
}

/// We don't need callers to be able to pass arbitrary delimiters: just control
/// whether listings will use a '/' separator or not.
///
/// The WithDelimiter mode will populate `prefixes` and `keys` in the result.  The
/// NoDelimiter mode will only populate `keys`.
pub enum ListingMode {
    WithDelimiter,
    NoDelimiter,
}

#[derive(Default)]
pub struct Listing {
    pub prefixes: Vec<RemotePath>,
    pub keys: Vec<RemotePath>,
}

/// Storage (potentially remote) API to manage its state.
/// This storage tries to be unaware of any layered repository context,
/// providing basic CRUD operations for storage files.
#[allow(async_fn_in_trait)]
pub trait RemoteStorage: Send + Sync + 'static {
    /// Lists all top level subdirectories for a given prefix
    /// Note: here we assume that if the prefix is passed it was obtained via remote_object_id
    /// which already takes into account any kind of global prefix (prefix_in_bucket for S3 or storage_root for LocalFS)
    /// so this method doesnt need to.
    async fn list_prefixes(
        &self,
        prefix: Option<&RemotePath>,
    ) -> Result<Vec<RemotePath>, DownloadError> {
        let result = self
            .list(prefix, ListingMode::WithDelimiter)
            .await?
            .prefixes;
        Ok(result)
    }
    /// Lists all files in directory "recursively"
    /// (not really recursively, because AWS has a flat namespace)
    /// Note: This is subtely different than list_prefixes,
    /// because it is for listing files instead of listing
    /// names sharing common prefixes.
    /// For example,
    /// list_files("foo/bar") = ["foo/bar/cat123.txt",
    /// "foo/bar/cat567.txt", "foo/bar/dog123.txt", "foo/bar/dog456.txt"]
    /// whereas,
    /// list_prefixes("foo/bar/") = ["cat", "dog"]
    /// See `test_real_s3.rs` for more details.
    async fn list_files(&self, prefix: Option<&RemotePath>) -> anyhow::Result<Vec<RemotePath>> {
        let result = self.list(prefix, ListingMode::NoDelimiter).await?.keys;
        Ok(result)
    }

    async fn list(
        &self,
        prefix: Option<&RemotePath>,
        _mode: ListingMode,
    ) -> anyhow::Result<Listing, DownloadError>;

    /// Streams the local file contents into remote into the remote storage entry.
    async fn upload(
        &self,
        from: impl Stream<Item = std::io::Result<Bytes>> + Send + Sync + 'static,
        // S3 PUT request requires the content length to be specified,
        // otherwise it starts to fail with the concurrent connection count increasing.
        data_size_bytes: usize,
        to: &RemotePath,
        metadata: Option<StorageMetadata>,
    ) -> anyhow::Result<()>;

    /// Streams the remote storage entry contents into the buffered writer given, returns the filled writer.
    /// Returns the metadata, if any was stored with the file previously.
    async fn download(&self, from: &RemotePath) -> Result<Download, DownloadError>;

    /// Streams a given byte range of the remote storage entry contents into the buffered writer given, returns the filled writer.
    /// Returns the metadata, if any was stored with the file previously.
    async fn download_byte_range(
        &self,
        from: &RemotePath,
        start_inclusive: u64,
        end_exclusive: Option<u64>,
    ) -> Result<Download, DownloadError>;

    async fn delete(&self, path: &RemotePath) -> anyhow::Result<()>;

    async fn delete_objects<'a>(&self, paths: &'a [RemotePath]) -> anyhow::Result<()>;

    /// Copy a remote object inside a bucket from one path to another.
    async fn copy(&self, from: &RemotePath, to: &RemotePath) -> anyhow::Result<()>;

    /// Resets the content of everything with the given prefix to the given state
    async fn time_travel_recover(
        &self,
        prefix: Option<&RemotePath>,
        timestamp: SystemTime,
        done_if_after: SystemTime,
        cancel: &CancellationToken,
    ) -> Result<(), TimeTravelError>;
}

pub type DownloadStream = Pin<Box<dyn Stream<Item = std::io::Result<Bytes>> + Unpin + Send + Sync>>;
pub struct Download {
    pub download_stream: DownloadStream,
    /// The last time the file was modified (`last-modified` HTTP header)
    pub last_modified: Option<SystemTime>,
    /// A way to identify this specific version of the resource (`etag` HTTP header)
    pub etag: Option<String>,
    /// Extra key-value data, associated with the current remote file.
    pub metadata: Option<StorageMetadata>,
}

impl Debug for Download {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Download")
            .field("metadata", &self.metadata)
            .finish()
    }
}

#[derive(Debug)]
pub enum DownloadError {
    /// Validation or other error happened due to user input.
    BadInput(anyhow::Error),
    /// The file was not found in the remote storage.
    NotFound,
    /// A cancellation token aborted the download, typically during
    /// tenant detach or process shutdown.
    Cancelled,
    /// The file was found in the remote storage, but the download failed.
    Other(anyhow::Error),
}

impl std::fmt::Display for DownloadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DownloadError::BadInput(e) => {
                write!(f, "Failed to download a remote file due to user input: {e}")
            }
            DownloadError::Cancelled => write!(f, "Cancelled, shutting down"),
            DownloadError::NotFound => write!(f, "No file found for the remote object id given"),
            DownloadError::Other(e) => write!(f, "Failed to download a remote file: {e:?}"),
        }
    }
}

impl std::error::Error for DownloadError {}

impl DownloadError {
    /// Returns true if the error should not be retried with backoff
    pub fn is_permanent(&self) -> bool {
        use DownloadError::*;
        match self {
            BadInput(_) => true,
            NotFound => true,
            Cancelled => true,
            Other(_) => false,
        }
    }
}

#[derive(Debug)]
pub enum TimeTravelError {
    /// Validation or other error happened due to user input.
    BadInput(anyhow::Error),
    /// The used remote storage does not have time travel recovery implemented
    Unimplemented,
    /// The number of versions/deletion markers is above our limit.
    TooManyVersions,
    /// A cancellation token aborted the process, typically during
    /// request closure or process shutdown.
    Cancelled,
    /// Other errors
    Other(anyhow::Error),
}

impl std::fmt::Display for TimeTravelError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TimeTravelError::BadInput(e) => {
                write!(
                    f,
                    "Failed to time travel recover a prefix due to user input: {e}"
                )
            }
            TimeTravelError::Unimplemented => write!(
                f,
                "time travel recovery is not implemented for the current storage backend"
            ),
            TimeTravelError::Cancelled => write!(f, "Cancelled, shutting down"),
            TimeTravelError::TooManyVersions => {
                write!(f, "Number of versions/delete markers above limit")
            }
            TimeTravelError::Other(e) => write!(f, "Failed to time travel recover a prefix: {e:?}"),
        }
    }
}

impl std::error::Error for TimeTravelError {}

/// Every storage, currently supported.
/// Serves as a simple way to pass around the [`RemoteStorage`] without dealing with generics.
#[derive(Clone)]
// Require Clone for `Other` due to https://github.com/rust-lang/rust/issues/26925
pub enum GenericRemoteStorage<Other: Clone = Arc<UnreliableWrapper>> {
    LocalFs(LocalFs),
    AwsS3(Arc<S3Bucket>),
    AzureBlob(Arc<AzureBlobStorage>),
    Unreliable(Other),
}

impl<Other: RemoteStorage> GenericRemoteStorage<Arc<Other>> {
    pub async fn list(
        &self,
        prefix: Option<&RemotePath>,
        mode: ListingMode,
    ) -> anyhow::Result<Listing, DownloadError> {
        match self {
            Self::LocalFs(s) => s.list(prefix, mode).await,
            Self::AwsS3(s) => s.list(prefix, mode).await,
            Self::AzureBlob(s) => s.list(prefix, mode).await,
            Self::Unreliable(s) => s.list(prefix, mode).await,
        }
    }

    // A function for listing all the files in a "directory"
    // Example:
    // list_files("foo/bar") = ["foo/bar/a.txt", "foo/bar/b.txt"]
    pub async fn list_files(&self, folder: Option<&RemotePath>) -> anyhow::Result<Vec<RemotePath>> {
        match self {
            Self::LocalFs(s) => s.list_files(folder).await,
            Self::AwsS3(s) => s.list_files(folder).await,
            Self::AzureBlob(s) => s.list_files(folder).await,
            Self::Unreliable(s) => s.list_files(folder).await,
        }
    }

    // lists common *prefixes*, if any of files
    // Example:
    // list_prefixes("foo123","foo567","bar123","bar432") = ["foo", "bar"]
    pub async fn list_prefixes(
        &self,
        prefix: Option<&RemotePath>,
    ) -> Result<Vec<RemotePath>, DownloadError> {
        match self {
            Self::LocalFs(s) => s.list_prefixes(prefix).await,
            Self::AwsS3(s) => s.list_prefixes(prefix).await,
            Self::AzureBlob(s) => s.list_prefixes(prefix).await,
            Self::Unreliable(s) => s.list_prefixes(prefix).await,
        }
    }

    pub async fn upload(
        &self,
        from: impl Stream<Item = std::io::Result<Bytes>> + Send + Sync + 'static,
        data_size_bytes: usize,
        to: &RemotePath,
        metadata: Option<StorageMetadata>,
    ) -> anyhow::Result<()> {
        match self {
            Self::LocalFs(s) => s.upload(from, data_size_bytes, to, metadata).await,
            Self::AwsS3(s) => s.upload(from, data_size_bytes, to, metadata).await,
            Self::AzureBlob(s) => s.upload(from, data_size_bytes, to, metadata).await,
            Self::Unreliable(s) => s.upload(from, data_size_bytes, to, metadata).await,
        }
    }

    pub async fn download(&self, from: &RemotePath) -> Result<Download, DownloadError> {
        match self {
            Self::LocalFs(s) => s.download(from).await,
            Self::AwsS3(s) => s.download(from).await,
            Self::AzureBlob(s) => s.download(from).await,
            Self::Unreliable(s) => s.download(from).await,
        }
    }

    pub async fn download_byte_range(
        &self,
        from: &RemotePath,
        start_inclusive: u64,
        end_exclusive: Option<u64>,
    ) -> Result<Download, DownloadError> {
        match self {
            Self::LocalFs(s) => {
                s.download_byte_range(from, start_inclusive, end_exclusive)
                    .await
            }
            Self::AwsS3(s) => {
                s.download_byte_range(from, start_inclusive, end_exclusive)
                    .await
            }
            Self::AzureBlob(s) => {
                s.download_byte_range(from, start_inclusive, end_exclusive)
                    .await
            }
            Self::Unreliable(s) => {
                s.download_byte_range(from, start_inclusive, end_exclusive)
                    .await
            }
        }
    }

    pub async fn delete(&self, path: &RemotePath) -> anyhow::Result<()> {
        match self {
            Self::LocalFs(s) => s.delete(path).await,
            Self::AwsS3(s) => s.delete(path).await,
            Self::AzureBlob(s) => s.delete(path).await,
            Self::Unreliable(s) => s.delete(path).await,
        }
    }

    pub async fn delete_objects<'a>(&self, paths: &'a [RemotePath]) -> anyhow::Result<()> {
        match self {
            Self::LocalFs(s) => s.delete_objects(paths).await,
            Self::AwsS3(s) => s.delete_objects(paths).await,
            Self::AzureBlob(s) => s.delete_objects(paths).await,
            Self::Unreliable(s) => s.delete_objects(paths).await,
        }
    }

    pub async fn copy_object(&self, from: &RemotePath, to: &RemotePath) -> anyhow::Result<()> {
        match self {
            Self::LocalFs(s) => s.copy(from, to).await,
            Self::AwsS3(s) => s.copy(from, to).await,
            Self::AzureBlob(s) => s.copy(from, to).await,
            Self::Unreliable(s) => s.copy(from, to).await,
        }
    }

    pub async fn time_travel_recover(
        &self,
        prefix: Option<&RemotePath>,
        timestamp: SystemTime,
        done_if_after: SystemTime,
        cancel: &CancellationToken,
    ) -> Result<(), TimeTravelError> {
        match self {
            Self::LocalFs(s) => {
                s.time_travel_recover(prefix, timestamp, done_if_after, cancel)
                    .await
            }
            Self::AwsS3(s) => {
                s.time_travel_recover(prefix, timestamp, done_if_after, cancel)
                    .await
            }
            Self::AzureBlob(s) => {
                s.time_travel_recover(prefix, timestamp, done_if_after, cancel)
                    .await
            }
            Self::Unreliable(s) => {
                s.time_travel_recover(prefix, timestamp, done_if_after, cancel)
                    .await
            }
        }
    }
}

impl GenericRemoteStorage {
    pub fn from_config(storage_config: &RemoteStorageConfig) -> anyhow::Result<Self> {
        Ok(match &storage_config.storage {
            RemoteStorageKind::LocalFs(root) => {
                info!("Using fs root '{root}' as a remote storage");
                Self::LocalFs(LocalFs::new(root.clone())?)
            }
            RemoteStorageKind::AwsS3(s3_config) => {
                // The profile and access key id are only printed here for debugging purposes,
                // their values don't indicate the eventually taken choice for auth.
                let profile = std::env::var("AWS_PROFILE").unwrap_or_else(|_| "<none>".into());
                let access_key_id =
                    std::env::var("AWS_ACCESS_KEY_ID").unwrap_or_else(|_| "<none>".into());
                info!("Using s3 bucket '{}' in region '{}' as a remote storage, prefix in bucket: '{:?}', bucket endpoint: '{:?}', profile: {profile}, access_key_id: {access_key_id}",
                      s3_config.bucket_name, s3_config.bucket_region, s3_config.prefix_in_bucket, s3_config.endpoint);
                Self::AwsS3(Arc::new(S3Bucket::new(s3_config)?))
            }
            RemoteStorageKind::AzureContainer(azure_config) => {
                info!("Using azure container '{}' in region '{}' as a remote storage, prefix in container: '{:?}'",
                      azure_config.container_name, azure_config.container_region, azure_config.prefix_in_container);
                Self::AzureBlob(Arc::new(AzureBlobStorage::new(azure_config)?))
            }
        })
    }

    pub fn unreliable_wrapper(s: Self, fail_first: u64) -> Self {
        Self::Unreliable(Arc::new(UnreliableWrapper::new(s, fail_first)))
    }

    /// Takes storage object contents and its size and uploads to remote storage,
    /// mapping `from_path` to the corresponding remote object id in the storage.
    ///
    /// The storage object does not have to be present on the `from_path`,
    /// this path is used for the remote object id conversion only.
    pub async fn upload_storage_object(
        &self,
        from: impl Stream<Item = std::io::Result<Bytes>> + Send + Sync + 'static,
        from_size_bytes: usize,
        to: &RemotePath,
    ) -> anyhow::Result<()> {
        self.upload(from, from_size_bytes, to, None)
            .await
            .with_context(|| {
                format!("Failed to upload data of length {from_size_bytes} to storage path {to:?}")
            })
    }

    /// Downloads the storage object into the `to_path` provided.
    /// `byte_range` could be specified to dowload only a part of the file, if needed.
    pub async fn download_storage_object(
        &self,
        byte_range: Option<(u64, Option<u64>)>,
        from: &RemotePath,
    ) -> Result<Download, DownloadError> {
        match byte_range {
            Some((start, end)) => self.download_byte_range(from, start, end).await,
            None => self.download(from).await,
        }
    }
}

/// Extra set of key-value pairs that contain arbitrary metadata about the storage entry.
/// Immutable, cannot be changed once the file is created.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorageMetadata(HashMap<String, String>);

/// External backup storage configuration, enough for creating a client for that storage.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemoteStorageConfig {
    /// The storage connection configuration.
    pub storage: RemoteStorageKind,
}

/// A kind of a remote storage to connect to, with its connection configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RemoteStorageKind {
    /// Storage based on local file system.
    /// Specify a root folder to place all stored files into.
    LocalFs(Utf8PathBuf),
    /// AWS S3 based storage, storing all files in the S3 bucket
    /// specified by the config
    AwsS3(S3Config),
    /// Azure Blob based storage, storing all files in the container
    /// specified by the config
    AzureContainer(AzureConfig),
}

/// AWS S3 bucket coordinates and access credentials to manage the bucket contents (read and write).
#[derive(Clone, PartialEq, Eq)]
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
    pub concurrency_limit: NonZeroUsize,
    pub max_keys_per_list_response: Option<i32>,
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
#[derive(Clone, PartialEq, Eq)]
pub struct AzureConfig {
    /// Name of the container to connect to.
    pub container_name: String,
    /// The region where the bucket is located at.
    pub container_region: String,
    /// A "subfolder" in the container, to use the same container separately by multiple remote storage users at once.
    pub prefix_in_container: Option<String>,
    /// Azure has various limits on its API calls, we need not to exceed those.
    /// See [`DEFAULT_REMOTE_STORAGE_AZURE_CONCURRENCY_LIMIT`] for more details.
    pub concurrency_limit: NonZeroUsize,
    pub max_keys_per_list_response: Option<i32>,
}

impl Debug for AzureConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AzureConfig")
            .field("bucket_name", &self.container_name)
            .field("bucket_region", &self.container_region)
            .field("prefix_in_bucket", &self.prefix_in_container)
            .field("concurrency_limit", &self.concurrency_limit)
            .field(
                "max_keys_per_list_response",
                &self.max_keys_per_list_response,
            )
            .finish()
    }
}

impl RemoteStorageConfig {
    pub fn from_toml(toml: &toml_edit::Item) -> anyhow::Result<Option<RemoteStorageConfig>> {
        let local_path = toml.get("local_path");
        let bucket_name = toml.get("bucket_name");
        let bucket_region = toml.get("bucket_region");
        let container_name = toml.get("container_name");
        let container_region = toml.get("container_region");

        let use_azure = container_name.is_some() && container_region.is_some();

        let default_concurrency_limit = if use_azure {
            DEFAULT_REMOTE_STORAGE_AZURE_CONCURRENCY_LIMIT
        } else {
            DEFAULT_REMOTE_STORAGE_S3_CONCURRENCY_LIMIT
        };
        let concurrency_limit = NonZeroUsize::new(
            parse_optional_integer("concurrency_limit", toml)?.unwrap_or(default_concurrency_limit),
        )
        .context("Failed to parse 'concurrency_limit' as a positive integer")?;

        let max_keys_per_list_response =
            parse_optional_integer::<i32, _>("max_keys_per_list_response", toml)
                .context("Failed to parse 'max_keys_per_list_response' as a positive integer")?
                .or(DEFAULT_MAX_KEYS_PER_LIST_RESPONSE);

        let endpoint = toml
            .get("endpoint")
            .map(|endpoint| parse_toml_string("endpoint", endpoint))
            .transpose()?;

        let storage = match (
            local_path,
            bucket_name,
            bucket_region,
            container_name,
            container_region,
        ) {
            // no 'local_path' nor 'bucket_name' options are provided, consider this remote storage disabled
            (None, None, None, None, None) => return Ok(None),
            (_, Some(_), None, ..) => {
                bail!("'bucket_region' option is mandatory if 'bucket_name' is given ")
            }
            (_, None, Some(_), ..) => {
                bail!("'bucket_name' option is mandatory if 'bucket_region' is given ")
            }
            (None, Some(bucket_name), Some(bucket_region), ..) => {
                RemoteStorageKind::AwsS3(S3Config {
                    bucket_name: parse_toml_string("bucket_name", bucket_name)?,
                    bucket_region: parse_toml_string("bucket_region", bucket_region)?,
                    prefix_in_bucket: toml
                        .get("prefix_in_bucket")
                        .map(|prefix_in_bucket| {
                            parse_toml_string("prefix_in_bucket", prefix_in_bucket)
                        })
                        .transpose()?,
                    endpoint,
                    concurrency_limit,
                    max_keys_per_list_response,
                })
            }
            (_, _, _, Some(_), None) => {
                bail!("'container_name' option is mandatory if 'container_region' is given ")
            }
            (_, _, _, None, Some(_)) => {
                bail!("'container_name' option is mandatory if 'container_region' is given ")
            }
            (None, None, None, Some(container_name), Some(container_region)) => {
                RemoteStorageKind::AzureContainer(AzureConfig {
                    container_name: parse_toml_string("container_name", container_name)?,
                    container_region: parse_toml_string("container_region", container_region)?,
                    prefix_in_container: toml
                        .get("prefix_in_container")
                        .map(|prefix_in_container| {
                            parse_toml_string("prefix_in_container", prefix_in_container)
                        })
                        .transpose()?,
                    concurrency_limit,
                    max_keys_per_list_response,
                })
            }
            (Some(local_path), None, None, None, None) => RemoteStorageKind::LocalFs(
                Utf8PathBuf::from(parse_toml_string("local_path", local_path)?),
            ),
            (Some(_), Some(_), ..) => {
                bail!("'local_path' and 'bucket_name' are mutually exclusive")
            }
            (Some(_), _, _, Some(_), Some(_)) => {
                bail!("local_path and 'container_name' are mutually exclusive")
            }
        };

        Ok(Some(RemoteStorageConfig { storage }))
    }
}

// Helper functions to parse a toml Item
fn parse_optional_integer<I, E>(name: &str, item: &toml_edit::Item) -> anyhow::Result<Option<I>>
where
    I: TryFrom<i64, Error = E>,
    E: std::error::Error + Send + Sync + 'static,
{
    let toml_integer = match item.get(name) {
        Some(item) => item
            .as_integer()
            .with_context(|| format!("configure option {name} is not an integer"))?,
        None => return Ok(None),
    };

    I::try_from(toml_integer)
        .map(Some)
        .with_context(|| format!("configure option {name} is too large"))
}

fn parse_toml_string(name: &str, item: &Item) -> anyhow::Result<String> {
    let s = item
        .as_str()
        .with_context(|| format!("configure option {name} is not a string"))?;
    Ok(s.to_string())
}

struct ConcurrencyLimiter {
    // Every request to S3 can be throttled or cancelled, if a certain number of requests per second is exceeded.
    // Same goes to IAM, which is queried before every S3 request, if enabled. IAM has even lower RPS threshold.
    // The helps to ensure we don't exceed the thresholds.
    write: Arc<Semaphore>,
    read: Arc<Semaphore>,
}

impl ConcurrencyLimiter {
    fn for_kind(&self, kind: RequestKind) -> &Arc<Semaphore> {
        match kind {
            RequestKind::Get => &self.read,
            RequestKind::Put => &self.write,
            RequestKind::List => &self.read,
            RequestKind::Delete => &self.write,
            RequestKind::Copy => &self.write,
            RequestKind::TimeTravel => &self.write,
        }
    }

    async fn acquire(
        &self,
        kind: RequestKind,
    ) -> Result<tokio::sync::SemaphorePermit<'_>, tokio::sync::AcquireError> {
        self.for_kind(kind).acquire().await
    }

    async fn acquire_owned(
        &self,
        kind: RequestKind,
    ) -> Result<tokio::sync::OwnedSemaphorePermit, tokio::sync::AcquireError> {
        Arc::clone(self.for_kind(kind)).acquire_owned().await
    }

    fn new(limit: usize) -> ConcurrencyLimiter {
        Self {
            read: Arc::new(Semaphore::new(limit)),
            write: Arc::new(Semaphore::new(limit)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_object_name() {
        let k = RemotePath::new(Utf8Path::new("a/b/c")).unwrap();
        assert_eq!(k.object_name(), Some("c"));

        let k = RemotePath::new(Utf8Path::new("a/b/c/")).unwrap();
        assert_eq!(k.object_name(), Some("c"));

        let k = RemotePath::new(Utf8Path::new("a/")).unwrap();
        assert_eq!(k.object_name(), Some("a"));

        // XXX is it impossible to have an empty key?
        let k = RemotePath::new(Utf8Path::new("")).unwrap();
        assert_eq!(k.object_name(), None);
    }

    #[test]
    fn rempte_path_cannot_be_created_from_absolute_ones() {
        let err = RemotePath::new(Utf8Path::new("/")).expect_err("Should fail on absolute paths");
        assert_eq!(err.to_string(), "Path \"/\" is not relative");
    }
}

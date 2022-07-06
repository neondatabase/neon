//! A set of generic storage abstractions for the page server to use when backing up and restoring its state from the external storage.
//! No other modules from this tree are supposed to be used directly by the external code.
//!
//! [`RemoteStorage`] trait a CRUD-like generic abstraction to use for adapting external storages with a few implementations:
//!   * [`local_fs`] allows to use local file system as an external storage
//!   * [`s3_bucket`] uses AWS S3 bucket as an external storage
//!
mod local_fs;
mod s3_bucket;

use std::{
    borrow::Cow,
    collections::HashMap,
    ffi::OsStr,
    fmt::Debug,
    num::{NonZeroU32, NonZeroUsize},
    path::{Path, PathBuf},
    pin::Pin,
};

use anyhow::{bail, Context};

use tokio::io;
use toml_edit::Item;
use tracing::info;

pub use self::{
    local_fs::LocalFs,
    s3_bucket::{S3Bucket, S3ObjectKey},
};

/// How many different timelines can be processed simultaneously when synchronizing layers with the remote storage.
/// During regular work, pageserver produces one layer file per timeline checkpoint, with bursts of concurrency
/// during start (where local and remote timelines are compared and initial sync tasks are scheduled) and timeline attach.
/// Both cases may trigger timeline download, that might download a lot of layers. This concurrency is limited by the clients internally, if needed.
pub const DEFAULT_REMOTE_STORAGE_MAX_CONCURRENT_SYNCS: usize = 50;
pub const DEFAULT_REMOTE_STORAGE_MAX_SYNC_ERRORS: u32 = 10;
/// Currently, sync happens with AWS S3, that has two limits on requests per second:
/// ~200 RPS for IAM services
/// https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/UsingWithRDS.IAMDBAuth.html
/// ~3500 PUT/COPY/POST/DELETE or 5500 GET/HEAD S3 requests
/// https://aws.amazon.com/premiumsupport/knowledge-center/s3-request-limit-avoid-throttling/
pub const DEFAULT_REMOTE_STORAGE_S3_CONCURRENCY_LIMIT: usize = 100;

/// Storage (potentially remote) API to manage its state.
/// This storage tries to be unaware of any layered repository context,
/// providing basic CRUD operations for storage files.
#[async_trait::async_trait]
pub trait RemoteStorage: Send + Sync {
    /// A way to uniquely reference a file in the remote storage.
    type RemoteObjectId;

    /// Attempts to derive the storage path out of the local path, if the latter is correct.
    fn remote_object_id(&self, local_path: &Path) -> anyhow::Result<Self::RemoteObjectId>;

    /// Gets the download path of the given storage file.
    fn local_path(&self, remote_object_id: &Self::RemoteObjectId) -> anyhow::Result<PathBuf>;

    /// Lists all items the storage has right now.
    async fn list(&self) -> anyhow::Result<Vec<Self::RemoteObjectId>>;

    /// Streams the local file contents into remote into the remote storage entry.
    async fn upload(
        &self,
        from: impl io::AsyncRead + Unpin + Send + Sync + 'static,
        // S3 PUT request requires the content length to be specified,
        // otherwise it starts to fail with the concurrent connection count increasing.
        from_size_bytes: usize,
        to: &Self::RemoteObjectId,
        metadata: Option<StorageMetadata>,
    ) -> anyhow::Result<()>;

    /// Streams the remote storage entry contents into the buffered writer given, returns the filled writer.
    /// Returns the metadata, if any was stored with the file previously.
    async fn download(&self, from: &Self::RemoteObjectId) -> Result<Download, DownloadError>;

    /// Streams a given byte range of the remote storage entry contents into the buffered writer given, returns the filled writer.
    /// Returns the metadata, if any was stored with the file previously.
    async fn download_byte_range(
        &self,
        from: &Self::RemoteObjectId,
        start_inclusive: u64,
        end_exclusive: Option<u64>,
    ) -> Result<Download, DownloadError>;

    async fn delete(&self, path: &Self::RemoteObjectId) -> anyhow::Result<()>;
}

pub struct Download {
    pub download_stream: Pin<Box<dyn io::AsyncRead + Unpin + Send>>,
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
    /// The file was found in the remote storage, but the download failed.
    Other(anyhow::Error),
}

impl std::fmt::Display for DownloadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DownloadError::BadInput(e) => {
                write!(f, "Failed to download a remote file due to user input: {e}")
            }
            DownloadError::NotFound => write!(f, "No file found for the remote object id given"),
            DownloadError::Other(e) => write!(f, "Failed to download a remote file: {e}"),
        }
    }
}

impl std::error::Error for DownloadError {}

/// Every storage, currently supported.
/// Serves as a simple way to pass around the [`RemoteStorage`] without dealing with generics.
pub enum GenericRemoteStorage {
    Local(LocalFs),
    S3(S3Bucket),
}

impl GenericRemoteStorage {
    pub fn new(
        working_directory: PathBuf,
        storage_config: &RemoteStorageConfig,
    ) -> anyhow::Result<Self> {
        match &storage_config.storage {
            RemoteStorageKind::LocalFs(root) => {
                info!("Using fs root '{}' as a remote storage", root.display());
                LocalFs::new(root.clone(), working_directory).map(GenericRemoteStorage::Local)
            }
            RemoteStorageKind::AwsS3(s3_config) => {
                info!("Using s3 bucket '{}' in region '{}' as a remote storage, prefix in bucket: '{:?}', bucket endpoint: '{:?}'",
                    s3_config.bucket_name, s3_config.bucket_region, s3_config.prefix_in_bucket, s3_config.endpoint);
                S3Bucket::new(s3_config, working_directory).map(GenericRemoteStorage::S3)
            }
        }
    }
}

/// Extra set of key-value pairs that contain arbitrary metadata about the storage entry.
/// Immutable, cannot be changed once the file is created.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorageMetadata(HashMap<String, String>);

fn strip_path_prefix<'a>(prefix: &'a Path, path: &'a Path) -> anyhow::Result<&'a Path> {
    if prefix == path {
        anyhow::bail!(
            "Prefix and the path are equal, cannot strip: '{}'",
            prefix.display()
        )
    } else {
        path.strip_prefix(prefix).with_context(|| {
            format!(
                "Path '{}' is not prefixed with '{}'",
                path.display(),
                prefix.display(),
            )
        })
    }
}

/// External backup storage configuration, enough for creating a client for that storage.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemoteStorageConfig {
    /// Max allowed number of concurrent sync operations between the API user and the remote storage.
    pub max_concurrent_syncs: NonZeroUsize,
    /// Max allowed errors before the sync task is considered failed and evicted.
    pub max_sync_errors: NonZeroU32,
    /// The storage connection configuration.
    pub storage: RemoteStorageKind,
}

/// A kind of a remote storage to connect to, with its connection configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RemoteStorageKind {
    /// Storage based on local file system.
    /// Specify a root folder to place all stored files into.
    LocalFs(PathBuf),
    /// AWS S3 based storage, storing all files in the S3 bucket
    /// specified by the config
    AwsS3(S3Config),
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
}

impl Debug for S3Config {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S3Config")
            .field("bucket_name", &self.bucket_name)
            .field("bucket_region", &self.bucket_region)
            .field("prefix_in_bucket", &self.prefix_in_bucket)
            .field("concurrency_limit", &self.concurrency_limit)
            .finish()
    }
}

pub fn path_with_suffix_extension(original_path: impl AsRef<Path>, suffix: &str) -> PathBuf {
    let new_extension = match original_path
        .as_ref()
        .extension()
        .map(OsStr::to_string_lossy)
    {
        Some(extension) => Cow::Owned(format!("{extension}.{suffix}")),
        None => Cow::Borrowed(suffix),
    };
    original_path
        .as_ref()
        .with_extension(new_extension.as_ref())
}

impl RemoteStorageConfig {
    pub fn from_toml(toml: &toml_edit::Item) -> anyhow::Result<RemoteStorageConfig> {
        let local_path = toml.get("local_path");
        let bucket_name = toml.get("bucket_name");
        let bucket_region = toml.get("bucket_region");

        let max_concurrent_syncs = NonZeroUsize::new(
            parse_optional_integer("max_concurrent_syncs", toml)?
                .unwrap_or(DEFAULT_REMOTE_STORAGE_MAX_CONCURRENT_SYNCS),
        )
        .context("Failed to parse 'max_concurrent_syncs' as a positive integer")?;

        let max_sync_errors = NonZeroU32::new(
            parse_optional_integer("max_sync_errors", toml)?
                .unwrap_or(DEFAULT_REMOTE_STORAGE_MAX_SYNC_ERRORS),
        )
        .context("Failed to parse 'max_sync_errors' as a positive integer")?;

        let concurrency_limit = NonZeroUsize::new(
            parse_optional_integer("concurrency_limit", toml)?
                .unwrap_or(DEFAULT_REMOTE_STORAGE_S3_CONCURRENCY_LIMIT),
        )
        .context("Failed to parse 'concurrency_limit' as a positive integer")?;

        let storage = match (local_path, bucket_name, bucket_region) {
            (None, None, None) => bail!("no 'local_path' nor 'bucket_name' option"),
            (_, Some(_), None) => {
                bail!("'bucket_region' option is mandatory if 'bucket_name' is given ")
            }
            (_, None, Some(_)) => {
                bail!("'bucket_name' option is mandatory if 'bucket_region' is given ")
            }
            (None, Some(bucket_name), Some(bucket_region)) => RemoteStorageKind::AwsS3(S3Config {
                bucket_name: parse_toml_string("bucket_name", bucket_name)?,
                bucket_region: parse_toml_string("bucket_region", bucket_region)?,
                prefix_in_bucket: toml
                    .get("prefix_in_bucket")
                    .map(|prefix_in_bucket| parse_toml_string("prefix_in_bucket", prefix_in_bucket))
                    .transpose()?,
                endpoint: toml
                    .get("endpoint")
                    .map(|endpoint| parse_toml_string("endpoint", endpoint))
                    .transpose()?,
                concurrency_limit,
            }),
            (Some(local_path), None, None) => RemoteStorageKind::LocalFs(PathBuf::from(
                parse_toml_string("local_path", local_path)?,
            )),
            (Some(_), Some(_), _) => bail!("local_path and bucket_name are mutually exclusive"),
        };

        Ok(RemoteStorageConfig {
            max_concurrent_syncs,
            max_sync_errors,
            storage,
        })
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_path_with_suffix_extension() {
        let p = PathBuf::from("/foo/bar");
        assert_eq!(
            &path_with_suffix_extension(&p, "temp").to_string_lossy(),
            "/foo/bar.temp"
        );
        let p = PathBuf::from("/foo/bar");
        assert_eq!(
            &path_with_suffix_extension(&p, "temp.temp").to_string_lossy(),
            "/foo/bar.temp.temp"
        );
        let p = PathBuf::from("/foo/bar.baz");
        assert_eq!(
            &path_with_suffix_extension(&p, "temp.temp").to_string_lossy(),
            "/foo/bar.baz.temp.temp"
        );
        let p = PathBuf::from("/foo/bar.baz");
        assert_eq!(
            &path_with_suffix_extension(&p, ".temp").to_string_lossy(),
            "/foo/bar.baz..temp"
        );
    }
}

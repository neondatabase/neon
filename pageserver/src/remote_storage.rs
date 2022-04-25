//! A set of generic storage abstractions for the page server to use when backing up and restoring its state from the external storage.
//! This particular module serves as a public API border between pageserver and the internal storage machinery.
//! No other modules from this tree are supposed to be used directly by the external code.
//!
//! There are a few components the storage machinery consists of:
//! * [`RemoteStorage`] trait a CRUD-like generic abstraction to use for adapting external storages with a few implementations:
//!     * [`local_fs`] allows to use local file system as an external storage
//!     * [`s3_bucket`] uses AWS S3 bucket as an external storage
//!
//! * synchronization logic at [`storage_sync`] module that keeps pageserver state (both runtime one and the workdir files) and storage state in sync.
//! Synchronization internals are split into submodules
//!     * [`storage_sync::index`] to keep track of remote tenant files, the metadata and their mappings to local files
//!     * [`storage_sync::upload`] and [`storage_sync::download`] to manage archive creation and upload; download and extraction, respectively
//!
//! * public API via to interact with the external world:
//!     * [`start_local_timeline_sync`] to launch a background async loop to handle the synchronization
//!     * [`schedule_timeline_checkpoint_upload`] and [`schedule_timeline_download`] to enqueue a new upload and download tasks,
//!       to be processed by the async loop
//!
//! Here's a schematic overview of all interactions backup and the rest of the pageserver perform:
//!
//! +------------------------+                                    +--------->-------+
//! |                        |  - - - (init async loop) - - - ->  |                 |
//! |                        |                                    |                 |
//! |                        |  ------------------------------->  |      async      |
//! |       pageserver       |    (enqueue timeline sync task)    | upload/download |
//! |                        |                                    |      loop       |
//! |                        |  <-------------------------------  |                 |
//! |                        |  (apply new timeline sync states)  |                 |
//! +------------------------+                                    +---------<-------+
//!                                                                         |
//!                                                                         |
//!                                          CRUD layer file operations     |
//!                                     (upload/download/delete/list, etc.) |
//!                                                                         V
//!                                                            +------------------------+
//!                                                            |                        |
//!                                                            | [`RemoteStorage`] impl |
//!                                                            |                        |
//!                                                            | pageserver assumes it  |
//!                                                            | owns exclusive write   |
//!                                                            | access to this storage |
//!                                                            +------------------------+
//!
//! First, during startup, the pageserver inits the storage sync thread with the async loop, or leaves the loop uninitialised, if configured so.
//! The loop inits the storage connection and checks the remote files stored.
//! This is done once at startup only, relying on the fact that pageserver uses the storage alone (ergo, nobody else uploads the files to the storage but this server).
//! Based on the remote storage data, the sync logic immediately schedules sync tasks for local timelines and reports about remote only timelines to pageserver, so it can
//! query their downloads later if they are accessed.
//!
//! Some time later, during pageserver checkpoints, in-memory data is flushed onto disk along with its metadata.
//! If the storage sync loop was successfully started before, pageserver schedules the new checkpoint file uploads after every checkpoint.
//! The checkpoint uploads are disabled, if no remote storage configuration is provided (no sync loop is started this way either).
//! See [`crate::layered_repository`] for the upload calls and the adjacent logic.
//!
//! Synchronization logic is able to communicate back with updated timeline sync states, [`crate::repository::TimelineSyncStatusUpdate`],
//! submitted via [`crate::tenant_mgr::apply_timeline_sync_status_updates`] function. Tenant manager applies corresponding timeline updates in pageserver's in-memory state.
//! Such submissions happen in two cases:
//! * once after the sync loop startup, to signal pageserver which timelines will be synchronized in the near future
//! * after every loop step, in case a timeline needs to be reloaded or evicted from pageserver's memory
//!
//! When the pageserver terminates, the sync loop finishes a current sync task (if any) and exits.
//!
//! The storage logic considers `image` as a set of local files (layers), fully representing a certain timeline at given moment (identified with `disk_consistent_lsn` from the corresponding `metadata` file).
//! Timeline can change its state, by adding more files on disk and advancing its `disk_consistent_lsn`: this happens after pageserver checkpointing and is followed
//! by the storage upload, if enabled.
//! Yet timeline cannot alter already existing files, and cannot remove those too: only a GC process is capable of removing unused files.
//! This way, remote storage synchronization relies on the fact that every checkpoint is incremental and local files are "immutable":
//! * when a certain checkpoint gets uploaded, the sync loop remembers the fact, preventing further reuploads of the same state
//! * no files are deleted from either local or remote storage, only the missing ones locally/remotely get downloaded/uploaded, local metadata file will be overwritten
//! when the newer image is downloaded
//!
//! Pageserver maintains similar to the local file structure remotely: all layer files are uploaded with the same names under the same directory structure.
//! Yet instead of keeping the `metadata` file remotely, we wrap it with more data in [`IndexShard`], containing the list of remote files.
//! This file gets read to populate the cache, if the remote timeline data is missing from it and gets updated after every successful download.
//! This way, we optimize S3 storage access by not running the `S3 list` command that could be expencive and slow: knowing both [`ZTenantId`] and [`ZTimelineId`],
//! we can always reconstruct the path to the timeline, use this to get the same path on the remote storage and retrive its shard contents, if needed, same as any layer files.
//!
//! By default, pageserver reads the remote storage index data only for timelines located locally, to synchronize those, if needed.
//! Bulk index data download happens only initially, on pageserer startup. The rest of the remote storage stays unknown to pageserver and loaded on demand only,
//! when a new timeline is scheduled for the download.
//!
//! NOTES:
//! * pageserver assumes it has exclusive write access to the remote storage. If supported, the way multiple pageservers can be separated in the same storage
//! (i.e. using different directories in the local filesystem external storage), but totally up to the storage implementation and not covered with the trait API.
//!
//! * the sync tasks may not processed immediately after the submission: if they error and get re-enqueued, their execution might be backed off to ensure error cap is not exceeded too fast.
//! The sync queue processing also happens in batches, so the sync tasks can wait in the queue for some time.

mod local_fs;
mod s3_bucket;
mod storage_sync;

use std::{
    collections::{HashMap, HashSet},
    ffi, fs,
    path::{Path, PathBuf},
};

use anyhow::{bail, Context};
use tokio::io;
use tracing::{debug, error, info};

pub use self::{
    local_fs::LocalFs,
    s3_bucket::S3Bucket,
    storage_sync::{
        download_index_part,
        index::{IndexPart, RemoteIndex, RemoteTimeline},
        schedule_timeline_checkpoint_upload, schedule_timeline_download,
    },
};
use crate::{
    config::{PageServerConf, RemoteStorageKind},
    layered_repository::{
        ephemeral_file::is_ephemeral_file,
        metadata::{TimelineMetadata, METADATA_FILE_NAME},
    },
};
use utils::zid::{ZTenantId, ZTenantTimelineId, ZTimelineId};

/// A timeline status to share with pageserver's sync counterpart,
/// after comparing local and remote timeline state.
#[derive(Clone, Copy, Debug)]
pub enum LocalTimelineInitStatus {
    /// The timeline has every remote layer present locally.
    /// There could be some layers requiring uploading,
    /// but this does not block the timeline from any user interaction.
    LocallyComplete,
    /// A timeline has some files remotely, that are not present locally and need downloading.
    /// Downloading might update timeline's metadata locally and current pageserver logic deals with local layers only,
    /// so the data needs to be downloaded first before the timeline can be used.
    NeedsSync,
}

type LocalTimelineInitStatuses = HashMap<ZTenantId, HashMap<ZTimelineId, LocalTimelineInitStatus>>;

/// A structure to combine all synchronization data to share with pageserver after a successful sync loop initialization.
/// Successful initialization includes a case when sync loop is not started, in which case the startup data is returned still,
/// to simplify the received code.
pub struct SyncStartupData {
    pub remote_index: RemoteIndex,
    pub local_timeline_init_statuses: LocalTimelineInitStatuses,
}

/// Based on the config, initiates the remote storage connection and starts a separate thread
/// that ensures that pageserver and the remote storage are in sync with each other.
/// If no external configuration connection given, no thread or storage initialization is done.
/// Along with that, scans tenant files local and remote (if the sync gets enabled) to check the initial timeline states.
pub fn start_local_timeline_sync(
    config: &'static PageServerConf,
) -> anyhow::Result<SyncStartupData> {
    let local_timeline_files = local_tenant_timeline_files(config)
        .context("Failed to collect local tenant timeline files")?;

    match &config.remote_storage_config {
        Some(storage_config) => match &storage_config.storage {
            RemoteStorageKind::LocalFs(root) => {
                info!("Using fs root '{}' as a remote storage", root.display());
                storage_sync::spawn_storage_sync_thread(
                    config,
                    local_timeline_files,
                    LocalFs::new(root.clone(), &config.workdir)?,
                    storage_config.max_concurrent_timelines_sync,
                    storage_config.max_sync_errors,
                )
            },
            RemoteStorageKind::AwsS3(s3_config) => {
                info!("Using s3 bucket '{}' in region '{}' as a remote storage, prefix in bucket: '{:?}', bucket endpoint: '{:?}'",
                    s3_config.bucket_name, s3_config.bucket_region, s3_config.prefix_in_bucket, s3_config.endpoint);
                storage_sync::spawn_storage_sync_thread(
                    config,
                    local_timeline_files,
                    S3Bucket::new(s3_config, &config.workdir)?,
                    storage_config.max_concurrent_timelines_sync,
                    storage_config.max_sync_errors,
                )
            },
        }
        .context("Failed to spawn the storage sync thread"),
        None => {
            info!("No remote storage configured, skipping storage sync, considering all local timelines with correct metadata files enabled");
            let mut local_timeline_init_statuses = LocalTimelineInitStatuses::new();
            for (ZTenantTimelineId { tenant_id, timeline_id }, _) in
                local_timeline_files
            {
                local_timeline_init_statuses
                    .entry(tenant_id)
                    .or_default()
                    .insert(timeline_id, LocalTimelineInitStatus::LocallyComplete);
            }
            Ok(SyncStartupData {
                local_timeline_init_statuses,
                remote_index: RemoteIndex::empty(),
            })
        }
    }
}

fn local_tenant_timeline_files(
    config: &'static PageServerConf,
) -> anyhow::Result<HashMap<ZTenantTimelineId, (TimelineMetadata, HashSet<PathBuf>)>> {
    let mut local_tenant_timeline_files = HashMap::new();
    let tenants_dir = config.tenants_path();
    for tenants_dir_entry in fs::read_dir(&tenants_dir)
        .with_context(|| format!("Failed to list tenants dir {}", tenants_dir.display()))?
    {
        match &tenants_dir_entry {
            Ok(tenants_dir_entry) => {
                match collect_timelines_for_tenant(config, &tenants_dir_entry.path()) {
                    Ok(collected_files) => {
                        local_tenant_timeline_files.extend(collected_files.into_iter())
                    }
                    Err(e) => error!(
                        "Failed to collect tenant files from dir '{}' for entry {:?}, reason: {:#}",
                        tenants_dir.display(),
                        tenants_dir_entry,
                        e
                    ),
                }
            }
            Err(e) => error!(
                "Failed to list tenants dir entry {:?} in directory {}, reason: {:?}",
                tenants_dir_entry,
                tenants_dir.display(),
                e
            ),
        }
    }

    Ok(local_tenant_timeline_files)
}

fn collect_timelines_for_tenant(
    config: &'static PageServerConf,
    tenant_path: &Path,
) -> anyhow::Result<HashMap<ZTenantTimelineId, (TimelineMetadata, HashSet<PathBuf>)>> {
    let mut timelines = HashMap::new();
    let tenant_id = tenant_path
        .file_name()
        .and_then(ffi::OsStr::to_str)
        .unwrap_or_default()
        .parse::<ZTenantId>()
        .context("Could not parse tenant id out of the tenant dir name")?;
    let timelines_dir = config.timelines_path(&tenant_id);

    for timelines_dir_entry in fs::read_dir(&timelines_dir).with_context(|| {
        format!(
            "Failed to list timelines dir entry for tenant {}",
            tenant_id
        )
    })? {
        match timelines_dir_entry {
            Ok(timelines_dir_entry) => {
                let timeline_path = timelines_dir_entry.path();
                match collect_timeline_files(&timeline_path) {
                    Ok((timeline_id, metadata, timeline_files)) => {
                        timelines.insert(
                            ZTenantTimelineId {
                                tenant_id,
                                timeline_id,
                            },
                            (metadata, timeline_files),
                        );
                    }
                    Err(e) => error!(
                        "Failed to process timeline dir contents at '{}', reason: {:?}",
                        timeline_path.display(),
                        e
                    ),
                }
            }
            Err(e) => error!(
                "Failed to list timelines for entry tenant {}, reason: {:?}",
                tenant_id, e
            ),
        }
    }

    Ok(timelines)
}

// discover timeline files and extract timeline metadata
//  NOTE: ephemeral files are excluded from the list
fn collect_timeline_files(
    timeline_dir: &Path,
) -> anyhow::Result<(ZTimelineId, TimelineMetadata, HashSet<PathBuf>)> {
    let mut timeline_files = HashSet::new();
    let mut timeline_metadata_path = None;

    let timeline_id = timeline_dir
        .file_name()
        .and_then(ffi::OsStr::to_str)
        .unwrap_or_default()
        .parse::<ZTimelineId>()
        .context("Could not parse timeline id out of the timeline dir name")?;
    let timeline_dir_entries =
        fs::read_dir(&timeline_dir).context("Failed to list timeline dir contents")?;
    for entry in timeline_dir_entries {
        let entry_path = entry.context("Failed to list timeline dir entry")?.path();
        if entry_path.is_file() {
            if entry_path.file_name().and_then(ffi::OsStr::to_str) == Some(METADATA_FILE_NAME) {
                timeline_metadata_path = Some(entry_path);
            } else if is_ephemeral_file(&entry_path.file_name().unwrap().to_string_lossy()) {
                debug!("skipping ephemeral file {}", entry_path.display());
                continue;
            } else {
                timeline_files.insert(entry_path);
            }
        }
    }

    let timeline_metadata_path = match timeline_metadata_path {
        Some(path) => path,
        None => bail!("No metadata file found in the timeline directory"),
    };
    let metadata = TimelineMetadata::from_bytes(
        &fs::read(&timeline_metadata_path).context("Failed to read timeline metadata file")?,
    )
    .context("Failed to parse timeline metadata file bytes")?;

    Ok((timeline_id, metadata, timeline_files))
}

/// Storage (potentially remote) API to manage its state.
/// This storage tries to be unaware of any layered repository context,
/// providing basic CRUD operations for storage files.
#[async_trait::async_trait]
pub trait RemoteStorage: Send + Sync {
    /// A way to uniquely reference a file in the remote storage.
    type StoragePath;

    /// Attempts to derive the storage path out of the local path, if the latter is correct.
    fn storage_path(&self, local_path: &Path) -> anyhow::Result<Self::StoragePath>;

    /// Gets the download path of the given storage file.
    fn local_path(&self, storage_path: &Self::StoragePath) -> anyhow::Result<PathBuf>;

    /// Lists all items the storage has right now.
    async fn list(&self) -> anyhow::Result<Vec<Self::StoragePath>>;

    /// Streams the local file contents into remote into the remote storage entry.
    async fn upload(
        &self,
        from: impl io::AsyncRead + Unpin + Send + Sync + 'static,
        // S3 PUT request requires the content length to be specified,
        // otherwise it starts to fail with the concurrent connection count increasing.
        from_size_bytes: usize,
        to: &Self::StoragePath,
        metadata: Option<StorageMetadata>,
    ) -> anyhow::Result<()>;

    /// Streams the remote storage entry contents into the buffered writer given, returns the filled writer.
    /// Returns the metadata, if any was stored with the file previously.
    async fn download(
        &self,
        from: &Self::StoragePath,
        to: &mut (impl io::AsyncWrite + Unpin + Send + Sync),
    ) -> anyhow::Result<Option<StorageMetadata>>;

    /// Streams a given byte range of the remote storage entry contents into the buffered writer given, returns the filled writer.
    /// Returns the metadata, if any was stored with the file previously.
    async fn download_range(
        &self,
        from: &Self::StoragePath,
        start_inclusive: u64,
        end_exclusive: Option<u64>,
        to: &mut (impl io::AsyncWrite + Unpin + Send + Sync),
    ) -> anyhow::Result<Option<StorageMetadata>>;

    async fn delete(&self, path: &Self::StoragePath) -> anyhow::Result<()>;
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

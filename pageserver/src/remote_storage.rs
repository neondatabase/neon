//! A set of generic storage abstractions for the page server to use when backing up and restoring its state from the external storage.
//! This particular module serves as a public API border between pageserver and the internal storage machinery.
//! No other modules from this tree are supposed to be used directly by the external code.
//!
//! There are a few components the storage machinery consists of:
//! * [`RemoteStorage`] trait a CRUD-like generic abstraction to use for adapting external storages with a few implementations:
//!     * [`local_fs`] allows to use local file system as an external storage
//!     * [`rust_s3`] uses AWS S3 bucket as an external storage
//!
//! * synchronization logic at [`storage_sync`] module that keeps pageserver state (both runtime one and the workdir files) and storage state in sync.
//! Synchronization internals are split into submodules
//!     * [`storage_sync::compression`] for a custom remote storage format used to store timeline files in archives
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
//! Synchronization logic is able to communicate back with updated timeline sync states, [`TimelineSyncState`],
//! submitted via [`crate::tenant_mgr::set_timeline_states`] function. Tenant manager applies corresponding timeline updates in pageserver's in-memory state.
//! Such submissions happen in two cases:
//! * once after the sync loop startup, to signal pageserver which timelines will be synchronized in the near future
//! * after every loop step, in case a timeline needs to be reloaded or evicted from pageserver's memory
//!
//! When the pageserver terminates, the upload loop finishes a current sync task (if any) and exits.
//!
//! The storage logic considers `image` as a set of local files, fully representing a certain timeline at given moment (identified with `disk_consistent_lsn`).
//! Timeline can change its state, by adding more files on disk and advancing its `disk_consistent_lsn`: this happens after pageserver checkpointing and is followed
//! by the storage upload, if enabled.
//! Yet timeline cannot alter already existing files, and normally cannot remote those too: only a GC process is capable of removing unused files.
//! This way, remote storage synchronization relies on the fact that every checkpoint is incremental and local files are "immutable":
//! * when a certain checkpoint gets uploaded, the sync loop remembers the fact, preventing further reuploads of the same state
//! * no files are deleted from either local or remote storage, only the missing ones locally/remotely get downloaded/uploaded, local metadata file will be overwritten
//! when the newer image is downloaded
//!
//! To optimize S3 storage (and access), the sync loop compresses the checkpoint files before placing them to S3, and uncompresses them back, keeping track of timeline files and metadata.
//! Also, the remote file list is queried once only, at startup, to avoid possible extra costs and latency issues.
//!
//! NOTES:
//! * pageserver assumes it has exclusive write access to the remote storage. If supported, the way multiple pageservers can be separated in the same storage
//! (i.e. using different directories in the local filesystem external storage), but totally up to the storage implementation and not covered with the trait API.
//!
//! * the sync tasks may not processed immediately after the submission: if they error and get re-enqueued, their execution might be backed off to ensure error cap is not exceeded too fast.
//! The sync queue processing also happens in batches, so the sync tasks can wait in the queue for some time.

mod local_fs;
mod rust_s3;
mod storage_sync;

use std::{
    collections::HashMap,
    ffi, fs,
    path::{Path, PathBuf},
    thread,
};

use anyhow::{bail, Context};
use tokio::io;
use tracing::{error, info};
use zenith_utils::zid::{ZTenantId, ZTimelineId};

pub use self::storage_sync::{schedule_timeline_checkpoint_upload, schedule_timeline_download};
use self::{local_fs::LocalFs, rust_s3::S3};
use crate::{
    config::{PageServerConf, RemoteStorageKind},
    layered_repository::metadata::{TimelineMetadata, METADATA_FILE_NAME},
    repository::TimelineSyncState,
};

/// Any timeline has its own id and its own tenant it belongs to,
/// the sync processes group timelines by both for simplicity.
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone, Copy, Hash)]
pub struct TimelineSyncId(ZTenantId, ZTimelineId);

impl std::fmt::Display for TimelineSyncId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "(tenant: {}, timeline: {})", self.0, self.1)
    }
}

/// A structure to combine all synchronization data to share with pageserver after a successful sync loop initialization.
/// Successful initialization includes a case when sync loop is not started, in which case the startup data is returned still,
/// to simplify the received code.
pub struct SyncStartupData {
    /// A sync state, derived from initial comparison of local timeline files and the remote archives,
    /// before any sync tasks are executed.
    /// To reuse the local file scan logic, the timeline states are returned even if no sync loop get started during init:
    /// in this case, no remote files exist and all local timelines with correct metadata files are considered ready.
    pub initial_timeline_states: HashMap<ZTenantId, HashMap<ZTimelineId, TimelineSyncState>>,
    /// A handle to the sync loop, if it was started from the configuration provided.
    pub sync_loop_handle: Option<thread::JoinHandle<anyhow::Result<()>>>,
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
            RemoteStorageKind::LocalFs(root) => storage_sync::spawn_storage_sync_thread(
                config,
                local_timeline_files,
                LocalFs::new(root.clone(), &config.workdir)?,
                storage_config.max_concurrent_sync,
                storage_config.max_sync_errors,
            ),
            RemoteStorageKind::AwsS3(s3_config) => storage_sync::spawn_storage_sync_thread(
                config,
                local_timeline_files,
                S3::new(s3_config, &config.workdir)?,
                storage_config.max_concurrent_sync,
                storage_config.max_sync_errors,
            ),
        }
        .context("Failed to spawn the storage sync thread"),
        None => {
            info!("No remote storage configured, skipping storage sync, considering all local timelines with correct metadata files enabled");
            let mut initial_timeline_states: HashMap<
                ZTenantId,
                HashMap<ZTimelineId, TimelineSyncState>,
            > = HashMap::new();
            for (TimelineSyncId(tenant_id, timeline_id), (timeline_metadata, _)) in
                local_timeline_files
            {
                initial_timeline_states
                    .entry(tenant_id)
                    .or_default()
                    .insert(
                        timeline_id,
                        TimelineSyncState::Ready(timeline_metadata.disk_consistent_lsn()),
                    );
            }
            Ok(SyncStartupData {
                initial_timeline_states,
                sync_loop_handle: None,
            })
        }
    }
}

fn local_tenant_timeline_files(
    config: &'static PageServerConf,
) -> anyhow::Result<HashMap<TimelineSyncId, (TimelineMetadata, Vec<PathBuf>)>> {
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
                "Failed to list tenants dir entry {:?} in directory {}, reason: {:#}",
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
) -> anyhow::Result<HashMap<TimelineSyncId, (TimelineMetadata, Vec<PathBuf>)>> {
    let mut timelines: HashMap<TimelineSyncId, (TimelineMetadata, Vec<PathBuf>)> = HashMap::new();
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
                            TimelineSyncId(tenant_id, timeline_id),
                            (metadata, timeline_files),
                        );
                    }
                    Err(e) => error!(
                        "Failed to process timeline dir contents at '{}', reason: {:#}",
                        timeline_path.display(),
                        e
                    ),
                }
            }
            Err(e) => error!(
                "Failed to list timelines for entry tenant {}, reason: {:#}",
                tenant_id, e
            ),
        }
    }

    Ok(timelines)
}

fn collect_timeline_files(
    timeline_dir: &Path,
) -> anyhow::Result<(ZTimelineId, TimelineMetadata, Vec<PathBuf>)> {
    let mut timeline_files = Vec::new();
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
            } else {
                timeline_files.push(entry_path);
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
trait RemoteStorage: Send + Sync {
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
        to: &Self::StoragePath,
    ) -> anyhow::Result<()>;

    /// Streams the remote storage entry contents into the buffered writer given, returns the filled writer.
    async fn download(
        &self,
        from: &Self::StoragePath,
        to: &mut (impl io::AsyncWrite + Unpin + Send + Sync),
    ) -> anyhow::Result<()>;

    /// Streams a given byte range of the remote storage entry contents into the buffered writer given, returns the filled writer.
    async fn download_range(
        &self,
        from: &Self::StoragePath,
        start_inclusive: u64,
        end_exclusive: Option<u64>,
        to: &mut (impl io::AsyncWrite + Unpin + Send + Sync),
    ) -> anyhow::Result<()>;

    async fn delete(&self, path: &Self::StoragePath) -> anyhow::Result<()>;
}

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

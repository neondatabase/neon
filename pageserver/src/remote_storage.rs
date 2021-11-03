//! A set of generic storage abstractions for the page server to use when backing up and restoring its state from the external storage.
//! This particular module serves as a public API border between pageserver and the internal storage machinery.
//! No other modules from this tree are supposed to be used directly by the external code.
//!
//! There are a few components the storage machinery consists of:
//! * [`RemoteStorage`] trait a CRUD-like generic abstraction to use for adapting external storages with a few implementations:
//!     * [`local_fs`] allows to use local file system as an external storage
//!     * [`rust_s3`] uses AWS S3 bucket entirely as an external storage
//!
//! * synchronization logic at [`storage_sync`] module that keeps pageserver state (both runtime one and the workdir files) and storage state in sync.
//!
//! * public API via to interact with the external world: [`run_storage_sync_thread`] and [`schedule_timeline_upload`]
//!
//! Here's a schematic overview of all interactions backup and the rest of the pageserver perform:
//!
//! +------------------------+                                    +--------->-------+
//! |                        |  - - - (init async loop) - - - ->  |                 |
//! |                        |                                    |                 |
//! |                        |  ------------------------------->  |      async      |
//! |       pageserver       |   (schedule frozen layer upload)   | upload/download |
//! |                        |                                    |      loop       |
//! |                        |  <-------------------------------  |                 |
//! |                        |    (register downloaded layers)    |                 |
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
//! First, during startup, the pageserver inits the storage sync thread with the async loop, or leaves the loop unitialised, if configured so.
//! Some time later, during pageserver checkpoints, in-memory data is flushed onto disk along with its metadata.
//! If the storage sync loop was successfully started before, pageserver schedules the new image uploads after every checkpoint.
//! See [`crate::layered_repository`] for the upload calls and the adjacent logic.
//!
//! The storage logic considers `image` as a set of local files, fully representing a certain timeline at given moment (identified with `disk_consistent_lsn`).
//! Timeline can change its state, by adding more files on disk and advancing its `disk_consistent_lsn`: this happens after pageserver checkpointing and is followed
//! by the storage upload, if enabled.
//! When a certain image gets uploaded, the sync loop remembers the fact, preventing further reuploads of the same image state.
//! No files are deleted from either local or remote storage, only the missing ones locally/remotely get downloaded/uploaded, local metadata file will be overwritten
//! when the newer timeline is downloaded.
//!
//! Meanwhile, the loop inits the storage connection and checks the remote files stored.
//! This is done once at startup only, relying on the fact that pageserver uses the storage alone (ergo, nobody else uploads the files to the storage but this server).
//! Based on the remote image data, the storage sync logic queues image downloads, while accepting any potential upload tasks from pageserver and managing the tasks by their priority.
//! On the image download, a [`crate::tenant_mgr::register_relish_download`] function is called to register the new image in pageserver, initializing all related threads and internal state.
//!
//! When the pageserver terminates, the upload loop finishes a current image sync task (if any) and exits.
//!
//! NOTES:
//! * pageserver assumes it has exclusive write access to the remote storage. If supported, the way multiple pageservers can be separated in the same storage
//! (i.e. using different directories in the local filesystem external storage), but totally up to the storage implementation and not covered with the trait API.
//!
//! * the uploads do not happen right after pageserver startup, they are registered when
//!     1. pageserver does the checkpoint, which happens further in the future after the server start
//!     2. pageserver loads the timeline from disk for the first time
//!
//! * the uploads do not happen right after the upload registration: the sync loop might be occupied with other tasks, or tasks with bigger priority could be waiting already
//!
//! * all synchronization tasks (including the public API to register uploads and downloads and the sync queue management) happens on an image scale: a big set of remote files,
//! enough to represent (and recover, if needed) a certain timeline state. On the contrary, all internal storage CRUD calls are made per reilsh file from those images.
//! This way, the synchronization is able to download the image partially, if some state was synced before, but exposes correctly synced images only.

mod local_fs;
mod rust_s3;
mod storage_sync;

use std::{
    path::{Path, PathBuf},
    thread,
};

use anyhow::{anyhow, ensure, Context};
use tokio::io;
use zenith_utils::zid::{ZTenantId, ZTimelineId};

pub use self::storage_sync::schedule_timeline_upload;
use self::{local_fs::LocalFs, rust_s3::S3};
use crate::{
    layered_repository::{TENANTS_SEGMENT_NAME, TIMELINES_SEGMENT_NAME},
    PageServerConf, RemoteStorageKind,
};

/// Based on the config, initiates the remote storage connection and starts a separate thread
/// that ensures that pageserver and the remote storage are in sync with each other.
/// If no external configuraion connection given, no thread or storage initialization is done.
pub fn run_storage_sync_thread(
    config: &'static PageServerConf,
) -> anyhow::Result<Option<thread::JoinHandle<anyhow::Result<()>>>> {
    match &config.remote_storage_config {
        Some(storage_config) => {
            let max_concurrent_sync = storage_config.max_concurrent_sync;
            let handle = match &storage_config.storage {
                RemoteStorageKind::LocalFs(root) => storage_sync::spawn_storage_sync_thread(
                    config,
                    LocalFs::new(root.clone(), &config.workdir)?,
                    max_concurrent_sync,
                ),
                RemoteStorageKind::AwsS3(s3_config) => storage_sync::spawn_storage_sync_thread(
                    config,
                    S3::new(s3_config, &config.workdir)?,
                    max_concurrent_sync,
                ),
            };
            handle.map(Some)
        }
        None => Ok(None),
    }
}

/// Storage (potentially remote) API to manage its state.
/// This storage tries to be unaware of any layered repository context,
/// providing basic CRUD operations with storage files.
#[async_trait::async_trait]
trait RemoteStorage: Send + Sync {
    /// A way to uniquely reference a file in the remote storage.
    type StoragePath;

    /// Attempts to derive the storage path out of the local path, if the latter is correct.
    fn storage_path(&self, local_path: &Path) -> anyhow::Result<Self::StoragePath>;

    /// Gets the layered storage information about the given entry.
    fn info(&self, storage_path: &Self::StoragePath) -> anyhow::Result<RemoteFileInfo>;

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

    async fn delete(&self, path: &Self::StoragePath) -> anyhow::Result<()>;
}

/// Information about a certain remote storage entry.
#[derive(Debug, PartialEq, Eq)]
struct RemoteFileInfo {
    tenant_id: ZTenantId,
    timeline_id: ZTimelineId,
    /// Path in the pageserver workdir where the file should go to.
    download_destination: PathBuf,
    is_metadata: bool,
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

fn parse_ids_from_path<'a, R: std::fmt::Display>(
    path_segments: impl Iterator<Item = &'a str>,
    path_log_representation: &R,
) -> anyhow::Result<(ZTenantId, ZTimelineId)> {
    let mut segments = path_segments.skip_while(|&segment| segment != TENANTS_SEGMENT_NAME);
    let tenants_segment = segments.next().ok_or_else(|| {
        anyhow!(
            "Found no '{}' segment in the storage path '{}'",
            TENANTS_SEGMENT_NAME,
            path_log_representation
        )
    })?;
    ensure!(
        tenants_segment == TENANTS_SEGMENT_NAME,
        "Failed to extract '{}' segment from storage path '{}'",
        TENANTS_SEGMENT_NAME,
        path_log_representation
    );
    let tenant_id = segments
        .next()
        .ok_or_else(|| {
            anyhow!(
                "Found no tenant id in the storage path '{}'",
                path_log_representation
            )
        })?
        .parse::<ZTenantId>()
        .with_context(|| {
            format!(
                "Failed to parse tenant id from storage path '{}'",
                path_log_representation
            )
        })?;

    let timelines_segment = segments.next().ok_or_else(|| {
        anyhow!(
            "Found no '{}' segment in the storage path '{}'",
            TIMELINES_SEGMENT_NAME,
            path_log_representation
        )
    })?;
    ensure!(
        timelines_segment == TIMELINES_SEGMENT_NAME,
        "Failed to extract '{}' segment from storage path '{}'",
        TIMELINES_SEGMENT_NAME,
        path_log_representation
    );
    let timeline_id = segments
        .next()
        .ok_or_else(|| {
            anyhow!(
                "Found no timeline id in the storage path '{}'",
                path_log_representation
            )
        })?
        .parse::<ZTimelineId>()
        .with_context(|| {
            format!(
                "Failed to parse timeline id from storage path '{}'",
                path_log_representation
            )
        })?;

    Ok((tenant_id, timeline_id))
}

/// A set of common test utils to share in unit tests inside the module tree.
#[cfg(test)]
mod test_utils {
    use std::path::{Path, PathBuf};

    use anyhow::ensure;

    use crate::{
        layered_repository::{TENANTS_SEGMENT_NAME, TIMELINES_SEGMENT_NAME},
        repository::repo_harness::{RepoHarness, TIMELINE_ID},
    };

    /// Gives a timeline path with pageserver workdir stripped off.
    pub fn relative_timeline_path(harness: &RepoHarness) -> anyhow::Result<PathBuf> {
        let timeline_path = harness.timeline_path(&TIMELINE_ID);
        Ok(timeline_path
            .strip_prefix(&harness.conf.workdir)?
            .to_path_buf())
    }

    /// Creates a path with custom tenant id in one of its segments.
    /// Useful for emulating paths with wrong ids.
    pub fn custom_tenant_id_path(
        path_with_tenant_id: &Path,
        new_tenant_id: &str,
    ) -> anyhow::Result<PathBuf> {
        let mut new_path = PathBuf::new();
        let mut is_tenant_id = false;
        let mut tenant_id_replaced = false;
        for segment in path_with_tenant_id {
            match segment.to_str() {
                Some(TENANTS_SEGMENT_NAME) => is_tenant_id = true,
                Some(_tenant_id_str) if is_tenant_id => {
                    is_tenant_id = false;
                    new_path.push(new_tenant_id);
                    tenant_id_replaced = true;
                    continue;
                }
                _ => {}
            }
            new_path.push(segment)
        }

        ensure!(tenant_id_replaced, "Found no tenant id segment to replace");
        Ok(new_path)
    }

    /// Creates a path with custom timeline id in one of its segments.
    /// Useful for emulating paths with wrong ids.
    pub fn custom_timeline_id_path(
        path_with_timeline_id: &Path,
        new_timeline_id: &str,
    ) -> anyhow::Result<PathBuf> {
        let mut new_path = PathBuf::new();
        let mut is_timeline_id = false;
        let mut timeline_id_replaced = false;
        for segment in path_with_timeline_id {
            match segment.to_str() {
                Some(TIMELINES_SEGMENT_NAME) => is_timeline_id = true,
                Some(_timeline_id_str) if is_timeline_id => {
                    is_timeline_id = false;
                    new_path.push(new_timeline_id);
                    timeline_id_replaced = true;
                    continue;
                }
                _ => {}
            }
            new_path.push(segment)
        }

        ensure!(
            timeline_id_replaced,
            "Found no timeline id segment to replace"
        );
        Ok(new_path)
    }
}

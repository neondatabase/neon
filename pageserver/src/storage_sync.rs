//! This module manages synchronizing local fs with remote storage. It consists of:
//!
//! * [`RemoteTimelineClient`] provides functions related to upload/download of a particular timeline.
//!   It contains a queue of pending uploads, and manages the queue, performing uploads in parallel
//!   when it's safe to do so.
//!
//! * Stand-alone function, [`list_remote_timelines`], to get list of timelines of a tenant.
//!
//! These functions use the low-level remote storage client, [`RemoteStorage`], which is defined
//! in the remote_storage crate (see libs/remote_storage).
//!

//! FIXME: Update below as needed
//!
//! At system startup, the pageserver will load all tenants that are found on
//! local disk. A RemoteTimelineClient is initialized for each timeline. The
//! upload queue of each RemoteTimelineClient is initially empty, but if there
//! are any files on local filesystem that don't exist remotely, they are added
//! to the upload queue at startup.
//!
//! Some time later, during pageserver checkpoints, in-memory data is flushed to
//! disk along with its metadata.  Whenever a new layer file is created, the
//! pageserver also schedules it for upload, by calling schedule_layer_upload.
//! At a checkpoint, after all the new layer files have been created, the
//! updated metadata file is also scheduled for upload, by calling
//! schedule_index_upload.  See [`crate::tenant::timeline`] for the upload calls
//! and the adjacent logic.
//!
//! If no remote storage configuration is provided, the RemoetTimelineClient is
//! not created and the uploads are skipped.
//!
//! If a timeline is deleted, its RemoteTimelineClient is dropped, cancelling
//! all pending uploads.  If a timeline is detached, TODO what happens ???
//!
//! If the pageserver terminates, TODO what happens ???
//!
//! To have a consistent set of files, it's important that uploads and deletions
//! are performed in the right order. For example, the index file contains a
//! list of layer files, so it must not be uploaded until all the layer files
//! that are in its list have been succesfully uploaded. RemoteTimelineClient
//! maintains a queue of operations, and it knows which operations can be
//! performed in parallel, and which operations act like a "barrier" that
//! require preceding operations to finish. The calling code just needs to call
//! the schedule-functions in the correct order, and RemoteTimelineClient will
//! parallelize the operations in a way that's safe.
//!
//! The caller should be careful with deletion, though. You should not delete
//! files locally that haven't been uploaded yet. Otherwise the upload will
//! fail. After scheduling uploads, you can use the 'wait_completion' function
//! to wait for them to finish.
//!
//! - We rely on read-after write consistency in the remote storage.
//! - Layer files are immutable

//!
//! The "directory structure" in the remote storage mirrors the local directory structure, with paths
//! like `tenants/<tenant_id>/timelines/<timeline_id>/<layer filename>.
//! Yet instead of keeping the `metadata` file remotely, we wrap it with more data in [`IndexPart`], containing the list of remote files.
//! This file gets read to populate the cache, if the remote timeline data is missing from it and gets updated after every successful download.
//! This way, we optimize S3 storage access by not running the `S3 list` command that could be expencive and slow: knowing both [`TenantId`] and [`TimelineId`],
//! we can always reconstruct the path to the timeline, use this to get the same path on the remote storage and retrieve its shard contents, if needed, same as any layer files.
//! TODO: update this paragraph with more details on how the list of remote files is kept up-to-date
//!
//! At pageserver startup, it downloads the index files of every locally-present
//! tenant, to synchronize the local state with remote storage. Other tenants
//! that might be present in the remote storage are ignored. When a tenant is
//! attached to the pageserver, the directory structure for the tenant and all
//! its timelines are created on local disk, and the timeline index files are
//! downloaded, to create the local metadata files. The layer files containing
//! the actual data are downloaded on-demand.
//!
//! NOTES:
//! * pageserver assumes it has exclusive write access to the remote storage. If supported, the way multiple pageservers can be separated in the same storage
//! (i.e. using different directories in the local filesystem external storage), but totally up to the storage implementation and not covered with the trait API.
//!
//! * the sync tasks may not processed immediately after the submission: if they error and get re-enqueued, their execution might be backed off to ensure error cap is not exceeded too fast.
//! The sync queue processing also happens in batches, so the sync tasks can wait in the queue for some time.
//!
//! Uploads are queued and executed in the background and in parallel, enforcing the ordering rules.
//! Downloads are performed immediately, and independently of the uploads.
//!
//! Deletion happens only after a successful upload only, otherwise the compaction output might make the timeline inconsistent until both tasks are fully processed without errors.
//! Upload and download update the remote data (inmemory index and S3 json index part file) only after every layer is successfully synchronized, while the deletion task
//! does otherwise: it requires to have the remote data updated first successfully: blob files will be invisible to pageserver this way.
//!
//! FIXME: how is the initial list of remote files created now? Update this paragraph
//! During the loop startup, an initial [`RemoteTimelineIndex`] state is constructed via downloading and merging the index data for all timelines,
//! present locally.
//! It's enough to poll such timelines' remote state once on startup only, due to an agreement that only one pageserver at a time has an exclusive
//! write access to remote portion of timelines that are attached to the pagegserver.
//! The index state is used to issue initial sync tasks, if needed:
//! * all timelines with local state behind the remote gets download tasks scheduled.
//! Such timelines are considered "remote" before the download succeeds, so a number of operations (gc, checkpoints) on that timeline are unavailable
//! before up-to-date layers and metadata file are downloaded locally.
//! * all newer local state gets scheduled for upload, such timelines are "local" and fully operational
//! * remote timelines not present locally are unknown to pageserver, but can be downloaded on a separate request
//!
//! Then, the index is shared across pageserver under [`RemoteIndex`] guard to ensure proper synchronization.
//! The remote index gets updated after very remote storage change (after an upload), same as the index part files remotely.
//!
//! Remote timeline contains a set of layer files, created during checkpoint(s) and the serialized [`IndexPart`] file with timeline metadata and all remote layer paths inside.
//! Those paths are used instead of `S3 list` command to avoid its slowliness and expenciveness for big amount of files.
//! If the index part does not contain some file path but it's present remotely, such file is invisible to pageserver and ignored.
//! Among other tasks, the index is used to prevent invalid uploads and non-existing downloads on demand, refer to [`index`] for more details.
//!
//! FIXME: update this paragraph
//! Index construction is currently the only place where the storage sync can return an [`Err`] to the user.
//! New sync tasks are accepted via [`schedule_layer_upload`], [`schedule_layer_download`] and [`schedule_layer_delete`] functions.
//! After the initial state is loaded into memory and the loop starts, any further [`Err`] results do not stop the loop, but rather
//! reschedule the same task, with possibly less files to sync:
//! * download tasks currently never replace existing local file with metadata file as an exception
//! (but this is a subject to change when checksum checks are implemented: all files could get overwritten on a checksum mismatch)
//! * download tasks carry the information of skipped acrhives, so resubmissions are not downloading successfully processed layers again
//! * downloads do not contain any actual files to download, so that "external", sync pageserver code is able to schedule the timeline download
//! without accessing any extra information about its files.
//!
//! FIXME: update this paragraph
//! Uploads and downloads sync layer files in arbitrary order, but only after all layer files are synched the local metadada (for download) and remote index part (for upload) are updated,
//! to avoid having a corrupt state without the relevant layer files.
//! Refer to [`upload`] and [`download`] for more details.
//!
//! Synchronization never removes any local files from pageserver workdir or remote files from the remote storage, yet there could be overwrites of the same files (index part and metadata file updates, future checksum mismatch fixes).
//! NOTE: No real contents or checksum check happens right now and is a subject to improve later.

mod delete;
mod download;
pub mod index;
mod upload;

use anyhow::Context;
// re-export this
pub use download::is_temp_download_file;
pub use download::list_remote_timelines;

use std::collections::{HashMap, VecDeque};
use std::fmt::Debug;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};

use anyhow::ensure;
use remote_storage::{DownloadError, GenericRemoteStorage};
use tokio::runtime::Runtime;
use tracing::{error, info, warn};

use utils::lsn::Lsn;

use self::index::IndexPart;

use crate::metrics::MeasureRemoteOp;
use crate::metrics::RemoteOpFileKind;
use crate::metrics::RemoteOpKind;
use crate::{
    config::PageServerConf,
    storage_sync::index::{LayerFileMetadata, RelativePath},
    task_mgr,
    task_mgr::TaskKind,
    task_mgr::BACKGROUND_RUNTIME,
    tenant::metadata::TimelineMetadata,
    {exponential_backoff, DEFAULT_BASE_BACKOFF_SECONDS, DEFAULT_MAX_BACKOFF_SECONDS},
};

use utils::id::{TenantId, TimelineId};

/// A client for accessing a timeline's data in remote storage.
///
/// This takes care of managing the number of connections, and balancing them
/// across tenants. This also handles retries of failed uploads.
///
/// Upload and delete requests are ordered so that before a deletion is
/// performed, we wait for all preceding uploads to finish. This ensures sure
/// that if you perform a compaction operation that reshuffles data in layer
/// files, we don't have a transient state where the old files have already been
/// deleted, but new files have not yet been uploaded.
///
/// Similarly, this enforces an order between index-file uploads, and layer
/// uploads.  Before an index-file upload is performed, all preceding layer
/// uploads must be finished.
///
/// This also maintains a list of remote files, and automatically includes that
/// in the index part file, whenever timeline metadata is uploaded.
///
/// Downloads are not queued, they are performed immediately.
pub struct RemoteTimelineClient {
    conf: &'static PageServerConf,

    runtime: &'static Runtime,

    tenant_id: TenantId,
    timeline_id: TimelineId,

    upload_queue: Mutex<UploadQueue>,

    storage_impl: GenericRemoteStorage,
}

// clippy warns that Uninitialized is much smaller than Initialized, which wastes
// memory for Uninitialized variants. Doesn't matter in practice, there are not
// that many upload queues in a running pageserver, and most of them are initialized
// anyway.
#[allow(clippy::large_enum_variant)]
enum UploadQueue {
    Uninitialized,
    Initialized(UploadQueueInitialized),
}
/// This keeps track of queued and in-progress tasks.
struct UploadQueueInitialized {
    /// Counter to assign task IDs
    task_counter: u64,

    /// All layer files stored in the remote storage, taking into account all
    /// in-progress and queued operations
    latest_files: HashMap<RelativePath, LayerFileMetadata>,

    /// Metadata stored in the remote storage, taking into account all
    /// in-progress and queued operations.
    /// DANGER: do not return to outside world, e.g., safekeepers.
    latest_metadata: TimelineMetadata,

    /// `disk_consistent_lsn` from the last metadata file that was successfully
    /// uploaded. `Lsn(0)` if nothing was uploaded yet.
    /// Unlike `latest_files` or `latest_metadata`, this value is never ahead.
    /// Safekeeper can rely on it to make decisions for WAL storage.
    last_uploaded_consistent_lsn: Lsn,

    // Breakdown of different kinds of tasks currently in-progress
    num_inprogress_layer_uploads: usize,
    num_inprogress_metadata_uploads: usize,
    num_inprogress_deletions: usize,

    /// Tasks that are currently in-progress. In-progress means that a tokio Task
    /// has been launched for it. An in-progress task can be busy uploading, but it can
    /// also be waiting on the `concurrency_limiter` Semaphore in S3Bucket, or it can
    /// be waiting for retry in `exponential_backoff`.
    inprogress_tasks: HashMap<u64, Arc<UploadTask>>,

    /// Queued operations that have not been launched yet. They might depend on previous
    /// tasks to finish. For example, metadata upload cannot be performed before all
    /// preceding layer file uploads have completed.
    queued_operations: VecDeque<UploadOp>,
}

impl UploadQueue {
    fn initialize_empty_remote(
        &mut self,
        metadata: &TimelineMetadata,
    ) -> anyhow::Result<&mut UploadQueueInitialized> {
        match self {
            UploadQueue::Uninitialized => (),
            UploadQueue::Initialized(_) => anyhow::bail!("already initialized"),
        }

        info!("initializing upload queue for empty remote");

        let state = UploadQueueInitialized {
            // As described in the doc comment, it's ok for `latest_files` and `latest_metadata` to be ahead.
            latest_files: Default::default(),
            latest_metadata: metadata.clone(),
            // We haven't uploaded anything yet, so, `last_uploaded_consistent_lsn` must be 0 to prevent
            // safekeepers from garbage-collecting anything.
            last_uploaded_consistent_lsn: Lsn(0),
            // what follows are boring default initializations
            task_counter: Default::default(),
            num_inprogress_layer_uploads: 0,
            num_inprogress_metadata_uploads: 0,
            num_inprogress_deletions: 0,
            inprogress_tasks: Default::default(),
            queued_operations: Default::default(),
        };

        *self = UploadQueue::Initialized(state);
        Ok(self.initialized_mut().expect("we just set it"))
    }

    fn initialize_with_current_remote_index_part(
        &mut self,
        index_part: &IndexPart,
    ) -> anyhow::Result<&mut UploadQueueInitialized> {
        match self {
            UploadQueue::Uninitialized => (),
            UploadQueue::Initialized(_) => anyhow::bail!("already initialized"),
        }

        let mut files = HashMap::new();
        for path in &index_part.timeline_layers {
            let layer_metadata = index_part
                .layer_metadata
                .get(path)
                .map(LayerFileMetadata::from)
                .unwrap_or(LayerFileMetadata::MISSING);
            files.insert(path.clone(), layer_metadata);
        }

        let index_part_metadata = index_part.parse_metadata()?;
        info!(
            "initializing upload queue with remote index_part.disk_consistent_lsn: {}",
            index_part_metadata.disk_consistent_lsn()
        );

        let state = UploadQueueInitialized {
            latest_files: files,
            latest_metadata: index_part_metadata.clone(),
            last_uploaded_consistent_lsn: index_part_metadata.disk_consistent_lsn(),
            // what follows are boring default initializations
            task_counter: 0,
            num_inprogress_layer_uploads: 0,
            num_inprogress_metadata_uploads: 0,
            num_inprogress_deletions: 0,
            inprogress_tasks: Default::default(),
            queued_operations: Default::default(),
        };

        *self = UploadQueue::Initialized(state);
        Ok(self.initialized_mut().expect("we just set it"))
    }

    fn initialized_mut(&mut self) -> Option<&mut UploadQueueInitialized> {
        match self {
            UploadQueue::Uninitialized => None,
            UploadQueue::Initialized(x) => Some(x),
        }
    }
}

/// An in-progress upload or delete task.
#[derive(Debug)]
struct UploadTask {
    /// Unique ID of this task. Used as the key in `inprogress_tasks` above.
    task_id: u64,
    retries: AtomicU32,

    op: UploadOp,
}

#[derive(Debug)]
enum UploadOp {
    /// Upload a layer file
    UploadLayer(PathBuf, LayerFileMetadata),

    /// Upload the metadata file
    UploadMetadata(IndexPart, Lsn),

    /// Delete a file.
    Delete(PathBuf),

    /// Barrier. When the barrier operation is reached,
    Barrier(tokio::sync::watch::Sender<()>),
}

impl std::fmt::Display for UploadOp {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            UploadOp::UploadLayer(path, metadata) => write!(
                f,
                "UploadLayer({}, size={:?})",
                path.display(),
                metadata.file_size()
            ),
            UploadOp::UploadMetadata(_, lsn) => write!(f, "UploadMetadata(lsn: {})", lsn),
            UploadOp::Delete(path) => write!(f, "Delete({})", path.display()),
            UploadOp::Barrier(_) => write!(f, "Barrier"),
        }
    }
}

impl RemoteTimelineClient {
    /// Initialize the upload queue for a remote storage that already received
    /// an index file upload, i.e., it's not empty.
    /// The given `index_part` must be the one on the remote.
    pub fn init_upload_queue(&self, index_part: &IndexPart) -> anyhow::Result<()> {
        let mut upload_queue = self.upload_queue.lock().unwrap();
        upload_queue.initialize_with_current_remote_index_part(index_part)?;
        Ok(())
    }

    /// Initialize the upload queue for the case where the remote storage is empty,
    /// i.e., it doesn't have an `IndexPart`.
    pub fn init_upload_queue_for_empty_remote(
        &self,
        local_metadata: &TimelineMetadata,
    ) -> anyhow::Result<()> {
        let mut upload_queue = self.upload_queue.lock().unwrap();
        upload_queue.initialize_empty_remote(local_metadata)?;
        Ok(())
    }

    pub fn last_uploaded_consistent_lsn(&self) -> Option<Lsn> {
        self.upload_queue
            .lock()
            .unwrap()
            .initialized_mut()
            .map(|q| q.last_uploaded_consistent_lsn)
    }

    //
    // Download operations.
    //
    // These don't use the per-timeline queue. They do use the global semaphore in
    // S3Bucket, to limit the total number of concurrent operations, though.
    //

    /// Download index file
    pub async fn download_index_file(&self) -> Result<IndexPart, DownloadError> {
        download::download_index_part(
            self.conf,
            &self.storage_impl,
            self.tenant_id,
            self.timeline_id,
        )
        .await
    }

    /// Download a (layer) file from `path`, into local filesystem.
    ///
    /// 'layer_metadata' is the metadata from the remote index file.
    pub async fn download_layer_file(
        &self,
        path: &RelativePath,
        layer_metadata: &LayerFileMetadata,
    ) -> anyhow::Result<()> {
        let downloaded_size = download::download_layer_file(
            self.conf,
            &self.storage_impl,
            self.tenant_id,
            self.timeline_id,
            path,
            layer_metadata,
        )
        .await?;

        // Update the metadata for given layer file. The remote index file
        // might be missing some information for the file; this allows us
        // to fill in the missing details.
        if layer_metadata.file_size().is_none() {
            let new_metadata = LayerFileMetadata::new(downloaded_size);
            let mut guard = self.upload_queue.lock().unwrap();
            let upload_queue = guard
                .initialized_mut()
                .context("upload queue is not initialized")?;
            if let Some(upgraded) = upload_queue.latest_files.get_mut(path) {
                upgraded.merge(&new_metadata);
            } else {
                // The file should exist, since we just downloaded it.
                warn!(
                    "downloaded file {:?} not found in local copy of the index file",
                    path
                );
            }
        }
        Ok(())
    }

    //
    // Upload operations.
    //

    ///
    /// Launch an index-file upload operation in the background.
    ///
    /// The upload will be added to the queue immediately, but it
    /// won't be performed until all previosuly scheduled layer file
    /// upload operations have completed successfully.  This is to
    /// ensure that when the index file claims that layers X, Y and Z
    /// exist in remote storage, they really do.
    pub fn schedule_index_upload(
        self: &Arc<Self>,
        metadata: &TimelineMetadata,
    ) -> anyhow::Result<()> {
        let mut guard = self.upload_queue.lock().unwrap();
        let upload_queue = guard
            .initialized_mut()
            .context("upload queue is not initialized")?;

        // As documented in the struct definition, it's ok for latest_metadata to be
        // ahead of what's _actually_ on the remote during index upload.
        upload_queue.latest_metadata = metadata.clone();

        let disk_consistent_lsn = upload_queue.latest_metadata.disk_consistent_lsn();

        let index_part = IndexPart::new(
            upload_queue.latest_files.clone(),
            disk_consistent_lsn,
            upload_queue.latest_metadata.to_bytes()?,
        );
        upload_queue
            .queued_operations
            .push_back(UploadOp::UploadMetadata(index_part, disk_consistent_lsn));

        info!(
            "scheduled metadata upload with {} files",
            upload_queue.latest_files.len()
        );

        // Launch the task immediately, if possible
        self.launch_queued_tasks(upload_queue);

        Ok(())
    }

    ///
    /// Launch an upload operation in the background.
    ///
    pub fn schedule_layer_file_upload(
        self: &Arc<Self>,
        path: &Path,
        layer_metadata: &LayerFileMetadata,
    ) -> anyhow::Result<()> {
        let mut guard = self.upload_queue.lock().unwrap();
        let upload_queue = guard
            .initialized_mut()
            .context("upload queue is not initialized")?;

        // The file size can be missing for files that were created before we tracked that
        // in the metadata, but it should be present for any new files we create.
        ensure!(
            layer_metadata.file_size().is_some(),
            "file size not initialized in metadata"
        );

        let relative_path = RelativePath::from_local_path(
            &self.conf.timeline_path(&self.timeline_id, &self.tenant_id),
            path,
        )?;

        upload_queue
            .latest_files
            .insert(relative_path, layer_metadata.clone());

        upload_queue
            .queued_operations
            .push_back(UploadOp::UploadLayer(
                PathBuf::from(path),
                layer_metadata.clone(),
            ));

        info!("scheduled layer file upload {}", path.display());

        // Launch the task immediately, if possible
        self.launch_queued_tasks(upload_queue);
        Ok(())
    }

    ///
    /// Launch a delete operation in the background.
    ///
    /// The deletion won't actually be performed, until all preceding
    /// upload operations have completed succesfully.
    pub fn schedule_layer_file_deletion(self: &Arc<Self>, paths: &[PathBuf]) -> anyhow::Result<()> {
        let mut guard = self.upload_queue.lock().unwrap();
        let upload_queue = guard
            .initialized_mut()
            .context("upload queue is not initialized")?;

        // Update the remote index file, removing the to-be-deleted files from the index,
        // before deleting the actual files.
        // NB: deleting layers doesn't affect the values stored in TimelineMetadata,
        //     so, we don't need update it.

        for path in paths {
            let relative_path = RelativePath::from_local_path(
                &self.conf.timeline_path(&self.timeline_id, &self.tenant_id),
                path,
            )?;
            upload_queue.latest_files.remove(&relative_path);
        }
        let disk_consistent_lsn = upload_queue.latest_metadata.disk_consistent_lsn();
        let index_part = IndexPart::new(
            upload_queue.latest_files.clone(),
            disk_consistent_lsn,
            upload_queue.latest_metadata.to_bytes()?,
        );
        upload_queue
            .queued_operations
            .push_back(UploadOp::UploadMetadata(index_part, disk_consistent_lsn));

        // schedule the actual deletions
        for path in paths {
            upload_queue
                .queued_operations
                .push_back(UploadOp::Delete(PathBuf::from(path)));
            info!("scheduled layer file deletion {}", path.display());
        }

        // Launch the tasks immediately, if possible
        self.launch_queued_tasks(upload_queue);
        Ok(())
    }

    ///
    /// Wait for all previously scheduled uploads/deletions to complete
    ///
    pub async fn wait_completion(self: &Arc<Self>) -> anyhow::Result<()> {
        let (sender, mut receiver) = tokio::sync::watch::channel(());
        let barrier_op = UploadOp::Barrier(sender);

        {
            let mut guard = self.upload_queue.lock().unwrap();
            let upload_queue = guard
                .initialized_mut()
                .context("upload queue is not initialized")?;
            upload_queue.queued_operations.push_back(barrier_op);
            // Launch the task immediately, if possible
            self.launch_queued_tasks(upload_queue);
        }

        receiver.changed().await?;
        Ok(())
    }

    ///
    /// Pick next tasks from the queue, and start as many of them as possible without violating
    /// the ordering constraints.
    ///
    /// The caller needs to already hold the `upload_queue` lock.
    fn launch_queued_tasks(self: &Arc<Self>, upload_queue: &mut UploadQueueInitialized) {
        while let Some(next_op) = upload_queue.queued_operations.front() {
            // Can we run this task now?
            let can_run_now = match next_op {
                UploadOp::UploadLayer(_, _) => {
                    // Can always be scheduled.
                    true
                }
                UploadOp::UploadMetadata(_, _) => {
                    // These can only be performed after all the preceding operations
                    // have finished.
                    upload_queue.inprogress_tasks.is_empty()
                }
                UploadOp::Delete(_) => {
                    // Wait for preceding uploads to finish. Concurrent deletions are OK, though.
                    upload_queue.num_inprogress_deletions == upload_queue.inprogress_tasks.len()
                }

                UploadOp::Barrier(_) => upload_queue.inprogress_tasks.is_empty(),
            };

            // If we cannot launch this task, don't look any further.
            //
            // In some cases, we could let some non-frontmost tasks to "jump the queue" and launch
            // them now, but we don't try to do that currently.  For example, if the frontmost task
            // is an index-file upload that cannot proceed until preceding uploads have finished, we
            // could still start layer uploads that were scheduled later.
            if !can_run_now {
                break;
            }

            // We can launch this task. Remove it from the queue first.
            let next_op = upload_queue.queued_operations.pop_front().unwrap();

            info!("starting op: {}", next_op);

            // Update the counters
            match next_op {
                UploadOp::UploadLayer(_, _) => {
                    upload_queue.num_inprogress_layer_uploads += 1;
                }
                UploadOp::UploadMetadata(_, _) => {
                    upload_queue.num_inprogress_metadata_uploads += 1;
                }
                UploadOp::Delete(_) => {
                    upload_queue.num_inprogress_deletions += 1;
                }
                UploadOp::Barrier(sender) => {
                    sender.send_replace(());
                    continue;
                }
            };

            // Assign unique ID to this task
            upload_queue.task_counter += 1;
            let task_id = upload_queue.task_counter;

            // Add it to the in-progress map
            let task = Arc::new(UploadTask {
                task_id,
                op: next_op,
                retries: AtomicU32::new(0),
            });
            upload_queue
                .inprogress_tasks
                .insert(task.task_id, Arc::clone(&task));

            // Spawn task to perform the task
            let self_rc = Arc::clone(self);
            task_mgr::spawn(
                self.runtime.handle(),
                TaskKind::RemoteUploadTask,
                Some(self.tenant_id),
                Some(self.timeline_id),
                "remote upload",
                false,
                async move {
                    self_rc.perform_upload_task(task).await;
                    Ok(())
                },
            );

            // Loop back to process next task
        }
    }

    ///
    /// Perform an upload task.
    ///
    /// The task is in the `inprogress_tasks` list. This function will try to
    /// execute it, retrying forever. On successful completion, the task is
    /// removed it from the `inprogress_tasks` list, and any next task(s) in the
    /// queue that were waiting by the completion are launched.
    ///
    async fn perform_upload_task(self: &Arc<Self>, task: Arc<UploadTask>) {
        // Loop to retry until it completes.
        loop {
            let upload_result: anyhow::Result<()> = match task.op {
                UploadOp::UploadLayer(ref path, ref layer_metadata) => {
                    upload::upload_timeline_layer(&self.storage_impl, path, layer_metadata)
                        .measure_remote_op(
                            self.tenant_id,
                            self.timeline_id,
                            RemoteOpFileKind::Layer,
                            RemoteOpKind::Upload,
                        )
                        .await
                }
                UploadOp::UploadMetadata(ref index_part, _lsn) => {
                    upload::upload_index_part(
                        self.conf,
                        &self.storage_impl,
                        self.tenant_id,
                        self.timeline_id,
                        index_part,
                    )
                    .measure_remote_op(
                        self.tenant_id,
                        self.timeline_id,
                        RemoteOpFileKind::Index,
                        RemoteOpKind::Upload,
                    )
                    .await
                }
                UploadOp::Delete(ref path) => {
                    delete::delete_layer(&self.storage_impl, path)
                        .measure_remote_op(
                            self.tenant_id,
                            self.timeline_id,
                            RemoteOpFileKind::Layer,
                            RemoteOpKind::Delete,
                        )
                        .await
                }
                UploadOp::Barrier(_) => {
                    // unreachable. Barrier operations are handled synchronously in
                    // launch_queued_tasks
                    warn!("unexpected Barrier operation in perform_upload_task");
                    break;
                }
            };

            match upload_result {
                Ok(()) => {
                    break;
                }
                Err(e) => {
                    let retries = task.retries.fetch_add(1, Ordering::SeqCst);

                    error!(
                        "failed to perform remote task {}, will retry (attempt {}): {:?}",
                        task.op, retries, e
                    );

                    exponential_backoff(
                        retries,
                        DEFAULT_BASE_BACKOFF_SECONDS,
                        DEFAULT_MAX_BACKOFF_SECONDS,
                    )
                    .await;
                }
            }
        }

        let retries = task.retries.load(Ordering::SeqCst);
        if retries > 0 {
            info!(
                "remote task {} completed successfully after {} retries",
                task.op, retries
            );
        } else {
            info!("remote task {} completed successfully", task.op);
        }

        // The task has completed succesfully. Remove it from the in-progress list.
        {
            let mut upload_queue = self.upload_queue.lock().unwrap();
            let mut upload_queue = upload_queue.initialized_mut().expect(
                "callers are responsible for ensuring this is only called on initialized queue",
            );
            upload_queue.inprogress_tasks.remove(&task.task_id);

            match task.op {
                UploadOp::UploadLayer(_, _) => {
                    upload_queue.num_inprogress_layer_uploads -= 1;
                }
                UploadOp::UploadMetadata(_, lsn) => {
                    upload_queue.num_inprogress_metadata_uploads -= 1;
                    upload_queue.last_uploaded_consistent_lsn = lsn; // XXX monotonicity check?
                }
                UploadOp::Delete(_) => {
                    upload_queue.num_inprogress_deletions -= 1;
                }
                UploadOp::Barrier(_) => unreachable!(),
            };

            // Launch any queued tasks that were unblocked by this one.
            self.launch_queued_tasks(upload_queue);
        }
    }
}

///
/// Create a remote storage client for given timeline
///
/// Note: the caller must initialize the upload queue before any uploads can be scheduled,
/// by calling init_upload_queue.
///
pub fn create_remote_timeline_client(
    remote_storage: GenericRemoteStorage,
    conf: &'static PageServerConf,
    tenant_id: TenantId,
    timeline_id: TimelineId,
) -> anyhow::Result<RemoteTimelineClient> {
    Ok(RemoteTimelineClient {
        conf,
        runtime: &BACKGROUND_RUNTIME,
        tenant_id,
        timeline_id,
        storage_impl: remote_storage,
        upload_queue: Mutex::new(UploadQueue::Uninitialized),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tenant::harness::{TenantHarness, TIMELINE_ID};
    use remote_storage::{RemoteStorageConfig, RemoteStorageKind};
    use std::collections::HashSet;
    use utils::lsn::Lsn;

    pub(super) fn dummy_contents(name: &str) -> Vec<u8> {
        format!("contents for {name}").into()
    }

    pub(super) fn dummy_metadata(disk_consistent_lsn: Lsn) -> TimelineMetadata {
        let metadata = TimelineMetadata::new(
            disk_consistent_lsn,
            None,
            None,
            Lsn(0),
            Lsn(0),
            Lsn(0),
            // Any version will do
            // but it should be consistent with the one in the tests
            crate::DEFAULT_PG_VERSION,
        );

        // go through serialize + deserialize to fix the header, including checksum
        TimelineMetadata::from_bytes(&metadata.to_bytes().unwrap()).unwrap()
    }

    fn assert_file_list(a: &HashSet<RelativePath>, b: &[&str]) {
        let xx = PathBuf::from("");
        let mut avec: Vec<String> = a
            .iter()
            .map(|x| x.to_local_path(&xx).to_string_lossy().into())
            .collect();
        avec.sort();

        let mut bvec = b.to_owned();
        bvec.sort_unstable();

        assert_eq!(avec, bvec);
    }

    fn assert_remote_files(expected: &[&str], remote_path: &Path) {
        let mut expected: Vec<String> = expected.iter().map(|x| String::from(*x)).collect();
        expected.sort();

        let mut found: Vec<String> = Vec::new();
        for entry in std::fs::read_dir(remote_path).unwrap().flatten() {
            let entry_name = entry.file_name();
            let fname = entry_name.to_str().unwrap();
            found.push(String::from(fname));
        }
        found.sort();

        assert_eq!(found, expected);
    }

    // Test scheduling
    #[test]
    fn upload_scheduling() -> anyhow::Result<()> {
        let harness = TenantHarness::create("upload_scheduling")?;
        let timeline_path = harness.timeline_path(&TIMELINE_ID);
        std::fs::create_dir_all(&timeline_path)?;

        let remote_fs_dir = harness.conf.workdir.join("remote_fs");
        std::fs::create_dir_all(remote_fs_dir)?;
        let remote_fs_dir = std::fs::canonicalize(harness.conf.workdir.join("remote_fs"))?;

        let storage_config = RemoteStorageConfig {
            max_concurrent_syncs: std::num::NonZeroUsize::new(
                remote_storage::DEFAULT_REMOTE_STORAGE_MAX_CONCURRENT_SYNCS,
            )
            .unwrap(),
            max_sync_errors: std::num::NonZeroU32::new(
                remote_storage::DEFAULT_REMOTE_STORAGE_MAX_SYNC_ERRORS,
            )
            .unwrap(),
            storage: RemoteStorageKind::LocalFs(remote_fs_dir.clone()),
        };

        // Use a current-thread runtime in the test
        let runtime = Box::leak(Box::new(
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()?,
        ));
        let _entered = runtime.enter();

        // Test outline:
        //
        // Schedule upload of a bunch of layers. Check that they are started immediately, not queued
        // Schedule upload of index. Check that it is queued
        // let the layer file uploads finish. Check that the index-upload is now started
        // let the index-upload finish.
        //
        // Download back the index.json. Check that the list of files is correct
        //
        // Schedule upload. Schedule deletion. Check that the deletion is queued
        // let upload finish. Check that deletion is now started
        // Schedule another deletion. Check that it's launched immediately.
        // Schedule index upload. Check that it's queued

        println!("workdir: {}", harness.conf.workdir.display());

        let storage_impl =
            GenericRemoteStorage::from_config(harness.conf.workdir.clone(), &storage_config)?;
        let client = Arc::new(RemoteTimelineClient {
            conf: harness.conf,
            runtime,
            tenant_id: harness.tenant_id,
            timeline_id: TIMELINE_ID,
            storage_impl,
            upload_queue: Mutex::new(UploadQueue::Uninitialized),
        });

        let remote_timeline_dir =
            remote_fs_dir.join(timeline_path.strip_prefix(&harness.conf.workdir)?);
        println!("remote_timeline_dir: {}", remote_timeline_dir.display());

        let metadata = dummy_metadata(Lsn(0x10));
        client.init_upload_queue_for_empty_remote(&metadata)?;

        // Create a couple of dummy files,  schedule upload for them
        let content_foo = dummy_contents("foo");
        let content_bar = dummy_contents("bar");
        std::fs::write(timeline_path.join("foo"), &content_foo)?;
        std::fs::write(timeline_path.join("bar"), &content_bar)?;

        client.schedule_layer_file_upload(
            &timeline_path.join("foo"),
            &LayerFileMetadata::new(content_foo.len() as u64),
        )?;
        client.schedule_layer_file_upload(
            &timeline_path.join("bar"),
            &LayerFileMetadata::new(content_bar.len() as u64),
        )?;

        // Check that they are started immediately, not queued
        {
            let mut guard = client.upload_queue.lock().unwrap();
            let upload_queue = guard.initialized_mut().unwrap();
            assert!(upload_queue.queued_operations.is_empty());
            assert!(upload_queue.inprogress_tasks.len() == 2);
            assert!(upload_queue.num_inprogress_layer_uploads == 2);
        }

        // Schedule upload of index. Check that it is queued
        let metadata = dummy_metadata(Lsn(0x20));
        client.schedule_index_upload(&metadata)?;
        {
            let mut guard = client.upload_queue.lock().unwrap();
            let upload_queue = guard.initialized_mut().unwrap();
            assert!(upload_queue.queued_operations.len() == 1);
        }

        // Wait for the uploads to finish
        runtime.block_on(client.wait_completion())?;
        {
            let mut guard = client.upload_queue.lock().unwrap();
            let upload_queue = guard.initialized_mut().unwrap();

            assert!(upload_queue.queued_operations.is_empty());
            assert!(upload_queue.inprogress_tasks.is_empty());
        }

        // Download back the index.json, and check that the list of files is correct
        let index_part = runtime.block_on(client.download_index_file())?;
        assert_file_list(&index_part.timeline_layers, &["foo", "bar"]);
        let downloaded_metadata = index_part.parse_metadata()?;
        assert_eq!(downloaded_metadata, metadata);

        // Schedule upload and then a deletion. Check that the deletion is queued
        let content_baz = dummy_contents("baz");
        std::fs::write(timeline_path.join("baz"), &content_baz)?;
        client.schedule_layer_file_upload(
            &timeline_path.join("baz"),
            &LayerFileMetadata::new(content_baz.len() as u64),
        )?;
        client.schedule_layer_file_deletion(&[timeline_path.join("foo")])?;
        {
            let mut guard = client.upload_queue.lock().unwrap();
            let upload_queue = guard.initialized_mut().unwrap();

            // Deletion schedules upload of the index file, and the file deletion itself
            assert!(upload_queue.queued_operations.len() == 2);
            assert!(upload_queue.inprogress_tasks.len() == 1);
            assert!(upload_queue.num_inprogress_layer_uploads == 1);
            assert!(upload_queue.num_inprogress_deletions == 0);
        }
        assert_remote_files(&["foo", "bar", "index_part.json"], &remote_timeline_dir);

        // Finish them
        runtime.block_on(client.wait_completion())?;

        assert_remote_files(&["bar", "baz", "index_part.json"], &remote_timeline_dir);

        Ok(())
    }

    // TODO: Currently, GC can run between upload retries, removing local layers scheduled for upload. Test this scenario.
    // FIXME: used to have a test for this in upload.rs, `layer_upload_after_local_fs_update()`.
    // I didn't understand how it tests that, though.

    // TODO: Test upload failures and retries
}

//! This module manages synchronizing local FS with remote storage.
//!
//! # Overview
//!
//! * [`RemoteTimelineClient`] provides functions related to upload/download of a particular timeline.
//!   It contains a queue of pending uploads, and manages the queue, performing uploads in parallel
//!   when it's safe to do so.
//!
//! * Stand-alone function, [`list_remote_timelines`], to get list of timelines of a tenant.
//!
//! These functions use the low-level remote storage client, [`remote_storage::RemoteStorage`].
//!
//! # APIs & How To Use Them
//!
//! There is a [RemoteTimelineClient] for each [Timeline][`crate::tenant::Timeline`] in the system,
//! unless the pageserver is configured without remote storage.
//!
//! We allocate the client instance in [Timeline][`crate::tenant::Timeline`], i.e.,
//! either in [`crate::tenant_mgr`] during startup or when creating a new
//! timeline.
//! However, the client does not become ready for use until we've initialized its upload queue:
//!
//! - For timelines that already have some state on the remote storage, we use
//!   [`RemoteTimelineClient::init_upload_queue`] .
//! - For newly created timelines, we use
//!   [`RemoteTimelineClient::init_upload_queue_for_empty_remote`].
//!
//! The former takes the remote's [`IndexPart`] as an argument, possibly retrieved
//! using [`list_remote_timelines`]. We'll elaborate on [`IndexPart`] in the next section.
//!
//! Whenever we've created/updated/deleted a file in a timeline directory, we schedule
//! the corresponding remote operation with the timeline's [`RemoteTimelineClient`]:
//!
//! - [`RemoteTimelineClient::schedule_layer_file_upload`]  when we've created a new layer file.
//! - [`RemoteTimelineClient::schedule_index_upload`] when we've updated the timeline metadata file.
//! - [`RemoteTimelineClient::schedule_layer_file_deletion`] when we've deleted one or more layer files.
//!
//! Internally, these functions create [`UploadOp`]s and put them in a queue.
//!
//! There are also APIs for downloading files.
//! These are not part of the aforementioned queuing and will not be discussed
//! further here, except in the section covering tenant attach.
//!
//! # Remote Storage Structure & [`IndexPart`] Index File
//!
//! The "directory structure" in the remote storage mirrors the local directory structure, with paths
//! like `tenants/<tenant_id>/timelines/<timeline_id>/<layer filename>`.
//! Yet instead of keeping the `metadata` file remotely, we wrap it with more
//! data in an "index file" aka [`IndexPart`], containing the list of **all** remote
//! files for a given timeline.
//! If a file is not referenced from [`IndexPart`], it's not part of the remote storage state.
//!
//! Having the `IndexPart` also avoids expensive and slow `S3 list` commands.
//!
//! # Consistency
//!
//! To have a consistent remote structure, it's important that uploads and
//! deletions are performed in the right order. For example, the index file
//! contains a list of layer files, so it must not be uploaded until all the
//! layer files that are in its list have been succesfully uploaded.
//!
//! The contract between client and its user is that the user is responsible of
//! scheduling operations in an order that keeps the remote consistent as
//! described above.
//! From the user's perspective, the operations are executed sequentially.
//! Internally, the client knows which operations can be performed in parallel,
//! and which operations act like a "barrier" that require preceding operations
//! to finish. The calling code just needs to call the schedule-functions in the
//! correct order, and the client will parallelize the operations in a way that
//! is safe.
//!
//! The caller should be careful with deletion, though. They should not delete
//! local files that have been scheduled for upload but not yet finished uploading.
//! Otherwise the upload will fail. To wait for an upload to finish, use
//! the 'wait_completion' function (more on that later.)
//!
//! All of this relies on the following invariants:
//!
//! - We rely on read-after write consistency in the remote storage.
//! - Layer files are immutable
//!
//! NB: Pageserver assumes that it has exclusive write access to the tenant in remote
//! storage. Different tenants can be attached to different pageservers, but if the
//! same tenant is attached to two pageservers at the same time, they will overwrite
//! each other's index file updates, and confusion will ensue. There's no interlock or
//! mechanism to detect that in the pageserver, we rely on the control plane to ensure
//! that that doesn't happen.
//!
//! ## Implementation Note
//!
//! The *actual* remote state lags behind the *desired* remote state while
//! there are in-flight operations.
//! We keep track of the desired remote state in
//! [`UploadQueueInitialized::latest_files`] and [`UploadQueueInitialized::latest_metadata`].
//! It is initialized based on the [`IndexPart`] that was passed during init
//! and updated with every `schedule_*` function call.
//! All this is necessary necessary to compute the future [`IndexPart`]s
//! when scheduling an operation while other operations that also affect the
//! remote [`IndexPart`] are in flight.
//!
//! # Retries & Error Handling
//!
//! The client retries operations indefinitely, using exponential back-off.
//! There is no way to force a retry, i.e., interrupt the back-off.
//! This could be built easily.
//!
//! # Cancellation
//!
//! The operations execute as plain [`task_mgr`] tasks, scoped to
//! the client's tenant and timeline.
//! Dropping the client will drop queued operations but not executing operations.
//! These will complete unless the `task_mgr` tasks are cancelled using `task_mgr`
//! APIs, e.g., during pageserver shutdown, timeline delete, or tenant detach.
//!
//! # Completion
//!
//! Once an operation has completed, we update
//! [`UploadQueueInitialized::last_uploaded_consistent_lsn`] which indicates
//! to safekeepers that they can delete the WAL up to that LSN.
//!
//! The [`RemoteTimelineClient::wait_completion`] method can be used to wait
//! for all pending operations to complete. It does not prevent more
//! operations from getting scheduled.
//!
//! # Crash Consistency
//!
//! We do not persist the upload queue state.
//! If we drop the client, or crash, all unfinished operations are lost.
//!
//! To recover, the following steps need to be taken:
//! - Retrieve the current remote [`IndexPart`]. This gives us a
//!   consistent remote state, assuming the user scheduled the operations in
//!   the correct order.
//! - Initiate upload queue with that [`IndexPart`].
//! - Reschedule all lost operations by comparing the local filesystem state
//!   and remote state as per [`IndexPart`]. This is done in
//!   [`Timeline::setup_timeline`] and [`Timeline::reconcile_with_remote`].
//!
//! Note that if we crash during file deletion between the index update
//! that removes the file from the list of files, and deleting the remote file,
//! the file is leaked in the remote storage. Similarly, if a new file is created
//! and uploaded, but the pageserver dies permantently before updating the
//! remote index file, the new file is leaked in remote storage. We accept and
//! tolerate that for now.
//! Note further that we cannot easily fix this by scheduling deletes for every
//! file that is present only on the remote, because we cannot distinguish the
//! following two cases:
//! - (1) We had the file locally, deleted it locally, scheduled a remote delete,
//!   but crashed before it finished remotely.
//! - (2) We never had the file locally because we were still in tenant attach
//!   when we crashed. (Similar case for on-demand download in the future.)
//!
//! # Downloads (= Tenant Attach)
//!
//! In addition to the upload queue, [`RemoteTimelineClient`] has functions for
//! downloading files from the remote storage. Downloads are performed immediately,
//! independently of the uploads.
//!
//! When we attach a tenant, we perform the following steps:
//! - create `Tenant` object in `TenantState::Attaching` state
//! - List timelines that are present in remote storage, and download their remote [`IndexPart`]s
//! - For each timeline, create `Timeline` struct and a `RemoteTimelineClient`, and initialize the client's upload queue with its `IndexPart`
//! - eagerly download all the remote layers using the client's download APIs
//! - transition tenant from `TenantState::Attaching` to `TenantState::Active` state.
//!
//! Most of the above happens in [`Timeline::reconcile_with_remote`].
//! We keep track of the fact that a client is in `Attaching` state in a marker
//! file on the local disk.
//! However, the distinction is moot for storage sync since we call
//! `reconcile_with_remote` for tenants both with and without the marker file.
//!
//! In the future, downloading will be done on-demand and `reconcile_with_remote`
//! will only be responsible for re-scheduling upload ops after a crash of an
//! `Active` tenant.
//!
//! # Operating Without Remote Storage
//!
//! If no remote storage configuration is provided, the [`RemoteTimelineClient`] is
//! not created and the uploads are skipped.
//! Theoretically, it should be ok to remove and re-add remote storage configuration to
//! the pageserver config at any time, since it doesn't make a difference to
//! `reconcile_with_remote`.
//! Of course, the remote timeline dir must not change while we have de-configured
//! remote storage, i.e., the pageserver must remain the owner of the given prefix
//! in remote storage.
//! But note that we don't test any of this right now.
//!

mod delete;
mod download;
pub mod index;
mod upload;

// re-export these
pub use download::{is_temp_download_file, list_remote_timelines};

use std::collections::{HashMap, VecDeque};
use std::fmt::Debug;
use std::ops::DerefMut;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};

use anyhow::ensure;
use remote_storage::{DownloadError, GenericRemoteStorage};
use tokio::runtime::Runtime;
use tracing::{info, warn};
use tracing::{info_span, Instrument};

use utils::lsn::Lsn;

use self::index::IndexPart;

use crate::metrics::MeasureRemoteOp;
use crate::metrics::RemoteOpFileKind;
use crate::metrics::RemoteOpKind;
use crate::metrics::REMOTE_UPLOAD_QUEUE_UNFINISHED_TASKS;
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
    Stopped(UploadQueueStopped),
}

impl UploadQueue {
    fn as_str(&self) -> &'static str {
        match self {
            UploadQueue::Uninitialized => "Uninitialized",
            UploadQueue::Initialized(_) => "Initialized",
            UploadQueue::Stopped(_) => "Stopped",
        }
    }
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

struct UploadQueueStopped {
    last_uploaded_consistent_lsn: Lsn,
}

impl UploadQueue {
    fn initialize_empty_remote(
        &mut self,
        metadata: &TimelineMetadata,
    ) -> anyhow::Result<&mut UploadQueueInitialized> {
        match self {
            UploadQueue::Uninitialized => (),
            UploadQueue::Initialized(_) | UploadQueue::Stopped(_) => {
                anyhow::bail!("already initialized, state {}", self.as_str())
            }
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
            UploadQueue::Initialized(_) | UploadQueue::Stopped(_) => {
                anyhow::bail!("already initialized, state {}", self.as_str())
            }
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

    fn initialized_mut(&mut self) -> anyhow::Result<&mut UploadQueueInitialized> {
        match self {
            UploadQueue::Uninitialized | UploadQueue::Stopped(_) => {
                anyhow::bail!("queue is in state {}", self.as_str())
            }
            UploadQueue::Initialized(x) => Ok(x),
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
    Delete(RemoteOpFileKind, PathBuf),

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
            UploadOp::Delete(_, path) => write!(f, "Delete({})", path.display()),
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
        match &*self.upload_queue.lock().unwrap() {
            UploadQueue::Uninitialized => None,
            UploadQueue::Initialized(q) => Some(q.last_uploaded_consistent_lsn),
            UploadQueue::Stopped(q) => Some(q.last_uploaded_consistent_lsn),
        }
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
        .measure_remote_op(
            self.tenant_id,
            self.timeline_id,
            RemoteOpFileKind::Index,
            RemoteOpKind::Download,
        )
        .await
    }

    /// Download a (layer) file from `path`, into local filesystem.
    ///
    /// 'layer_metadata' is the metadata from the remote index file.
    ///
    /// On success, returns the size of the downloaded file.
    pub async fn download_layer_file(
        &self,
        path: &RelativePath,
        layer_metadata: &LayerFileMetadata,
    ) -> anyhow::Result<u64> {
        let downloaded_size = download::download_layer_file(
            self.conf,
            &self.storage_impl,
            self.tenant_id,
            self.timeline_id,
            path,
            layer_metadata,
        )
        .measure_remote_op(
            self.tenant_id,
            self.timeline_id,
            RemoteOpFileKind::Layer,
            RemoteOpKind::Download,
        )
        .await?;

        // Update the metadata for given layer file. The remote index file
        // might be missing some information for the file; this allows us
        // to fill in the missing details.
        if layer_metadata.file_size().is_none() {
            let new_metadata = LayerFileMetadata::new(downloaded_size);
            let mut guard = self.upload_queue.lock().unwrap();
            let upload_queue = guard.initialized_mut()?;
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
        Ok(downloaded_size)
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
        let upload_queue = guard.initialized_mut()?;

        // As documented in the struct definition, it's ok for latest_metadata to be
        // ahead of what's _actually_ on the remote during index upload.
        upload_queue.latest_metadata = metadata.clone();

        let disk_consistent_lsn = upload_queue.latest_metadata.disk_consistent_lsn();

        let index_part = IndexPart::new(
            upload_queue.latest_files.clone(),
            disk_consistent_lsn,
            upload_queue.latest_metadata.to_bytes()?,
        );
        let op = UploadOp::UploadMetadata(index_part, disk_consistent_lsn);
        self.update_upload_queue_unfinished_metric(1, &op);
        upload_queue.queued_operations.push_back(op);

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
        let upload_queue = guard.initialized_mut()?;

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

        let op = UploadOp::UploadLayer(PathBuf::from(path), layer_metadata.clone());
        self.update_upload_queue_unfinished_metric(1, &op);
        upload_queue.queued_operations.push_back(op);

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
        let upload_queue = guard.initialized_mut()?;

        // Convert the paths into RelativePaths, and gather other information we need.
        let mut relative_paths = Vec::with_capacity(paths.len());
        for path in paths {
            relative_paths.push(RelativePath::from_local_path(
                &self.conf.timeline_path(&self.timeline_id, &self.tenant_id),
                path,
            )?);
        }

        // Deleting layers doesn't affect the values stored in TimelineMetadata,
        // so we don't need update it. Just serialize it.
        let metadata_bytes = upload_queue.latest_metadata.to_bytes()?;
        let disk_consistent_lsn = upload_queue.latest_metadata.disk_consistent_lsn();

        // Update the remote index file, removing the to-be-deleted files from the index,
        // before deleting the actual files.
        //
        // Once we start removing files from upload_queue.latest_files, there's
        // no going back! Otherwise, some of the files would already be removed
        // from latest_files, but not yet scheduled for deletion. Use a closure
        // to syntactically forbid ? or bail! calls here.
        let no_bail_here = || {
            for relative_path in relative_paths {
                upload_queue.latest_files.remove(&relative_path);
            }

            let index_part = IndexPart::new(
                upload_queue.latest_files.clone(),
                disk_consistent_lsn,
                metadata_bytes,
            );
            let op = UploadOp::UploadMetadata(index_part, disk_consistent_lsn);
            self.update_upload_queue_unfinished_metric(1, &op);
            upload_queue.queued_operations.push_back(op);

            // schedule the actual deletions
            for path in paths {
                let op = UploadOp::Delete(RemoteOpFileKind::Layer, PathBuf::from(path));
                self.update_upload_queue_unfinished_metric(1, &op);
                upload_queue.queued_operations.push_back(op);
                info!("scheduled layer file deletion {}", path.display());
            }

            // Launch the tasks immediately, if possible
            self.launch_queued_tasks(upload_queue);
        };
        no_bail_here();
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
            let upload_queue = guard.initialized_mut()?;
            upload_queue.queued_operations.push_back(barrier_op);
            // Don't count this kind of operation!

            // Launch the task immediately, if possible
            self.launch_queued_tasks(upload_queue);
        }

        if receiver.changed().await.is_err() {
            anyhow::bail!("wait_completion aborted because upload queue was stopped");
        }
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
                UploadOp::Delete(_, _) => {
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
                UploadOp::Delete(_, _) => {
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
                }
                .instrument(info_span!(parent: None, "remote_upload", tenant = %self.tenant_id, timeline = %self.timeline_id, upload_task_id = %task_id)),
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
    /// The task can be shut down, however. That leads to stopping the whole
    /// queue.
    ///
    async fn perform_upload_task(self: &Arc<Self>, task: Arc<UploadTask>) {
        // Loop to retry until it completes.
        loop {
            // If we're requested to shut down, close up shop and exit.
            //
            // Note: We only check for the shutdown requests between retries, so
            // if a shutdown request arrives while we're busy uploading, in the
            // upload::upload:*() call below, we will wait not exit until it has
            // finisheed. We probably could cancel the upload by simply dropping
            // the Future, but we're not 100% sure if the remote storage library
            // is cancellation safe, so we don't dare to do that. Hopefully, the
            // upload finishes or times out soon enough.
            if task_mgr::is_shutdown_requested() {
                info!("upload task cancelled by shutdown request");
                self.update_upload_queue_unfinished_metric(-1, &task.op);
                self.stop();
                return;
            }

            let upload_result: anyhow::Result<()> = match &task.op {
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
                UploadOp::Delete(metric_file_kind, ref path) => {
                    delete::delete_layer(&self.storage_impl, path)
                        .measure_remote_op(
                            self.tenant_id,
                            self.timeline_id,
                            *metric_file_kind,
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

                    // uploads may fail due to rate limts (IAM, S3) or spurious network and external errors
                    // such issues are relatively regular, so don't use WARN or ERROR to avoid alerting
                    // people and tests until the retries are definitely causing delays.
                    if retries < 3 {
                        info!(
                            "failed to perform remote task {}, will retry (attempt {}): {:?}",
                            task.op, retries, e
                        );
                    } else {
                        warn!(
                            "failed to perform remote task {}, will retry (attempt {}): {:?}",
                            task.op, retries, e
                        );
                    }

                    // sleep until it's time to retry, or we're cancelled
                    tokio::select! {
                        _ = task_mgr::shutdown_watcher() => { },
                        _ = exponential_backoff(
                            retries,
                            DEFAULT_BASE_BACKOFF_SECONDS,
                            DEFAULT_MAX_BACKOFF_SECONDS,
                        ) => { },
                    };
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
            let mut upload_queue_guard = self.upload_queue.lock().unwrap();
            let upload_queue = match upload_queue_guard.deref_mut() {
                UploadQueue::Uninitialized => panic!("callers are responsible for ensuring this is only called on an initialized queue"),
                UploadQueue::Stopped(_) => {
                    info!("another concurrent task already stopped the queue");
                    return;
                }, // nothing to do
                UploadQueue::Initialized(qi) => { qi }
            };

            upload_queue.inprogress_tasks.remove(&task.task_id);

            match task.op {
                UploadOp::UploadLayer(_, _) => {
                    upload_queue.num_inprogress_layer_uploads -= 1;
                }
                UploadOp::UploadMetadata(_, lsn) => {
                    upload_queue.num_inprogress_metadata_uploads -= 1;
                    upload_queue.last_uploaded_consistent_lsn = lsn; // XXX monotonicity check?
                }
                UploadOp::Delete(_, _) => {
                    upload_queue.num_inprogress_deletions -= 1;
                }
                UploadOp::Barrier(_) => unreachable!(),
            };

            // Launch any queued tasks that were unblocked by this one.
            self.launch_queued_tasks(upload_queue);
        }
        self.update_upload_queue_unfinished_metric(-1, &task.op);
    }

    fn update_upload_queue_unfinished_metric(&self, delta: i64, op: &UploadOp) {
        let (file_kind, op_kind) = match op {
            UploadOp::UploadLayer(_, _) => (RemoteOpFileKind::Layer, RemoteOpKind::Upload),
            UploadOp::UploadMetadata(_, _) => (RemoteOpFileKind::Index, RemoteOpKind::Upload),
            UploadOp::Delete(file_kind, _) => (*file_kind, RemoteOpKind::Delete),
            UploadOp::Barrier(_) => {
                // we do not account these
                return;
            }
        };
        REMOTE_UPLOAD_QUEUE_UNFINISHED_TASKS
            .get_metric_with_label_values(&[
                &self.tenant_id.to_string(),
                &self.timeline_id.to_string(),
                file_kind.as_str(),
                op_kind.as_str(),
            ])
            .unwrap()
            .add(delta)
    }

    fn stop(&self) {
        // Whichever *task* for this RemoteTimelineClient grabs the mutex first will transition the queue
        // into stopped state, thereby dropping all off the queued *ops* which haven't become *tasks* yet.
        // The other *tasks* will come here and observe an already shut down queue and hence simply wrap up their business.
        let mut guard = self.upload_queue.lock().unwrap();
        match &*guard {
            UploadQueue::Uninitialized => panic!(
                "callers are responsible for ensuring this is only called on initialized queue"
            ),
            UploadQueue::Stopped(_) => {
                // nothing to do
                info!("another concurrent task already shut down the queue");
            }
            UploadQueue::Initialized(qi) => {
                info!("shutting down upload queue");

                // Replace the queue with the Stopped state, taking ownership of the old
                // Initialized queue. We will do some checks on it, and then drop it.
                let qi = {
                    let last_uploaded_consistent_lsn = qi.last_uploaded_consistent_lsn;
                    let upload_queue = std::mem::replace(
                        &mut *guard,
                        UploadQueue::Stopped(UploadQueueStopped {
                            last_uploaded_consistent_lsn,
                        }),
                    );
                    if let UploadQueue::Initialized(qi) = upload_queue {
                        qi
                    } else {
                        unreachable!("we checked in the match above that it is Initialized");
                    }
                };

                // consistency check
                assert_eq!(
                    qi.num_inprogress_layer_uploads
                        + qi.num_inprogress_metadata_uploads
                        + qi.num_inprogress_deletions,
                    qi.inprogress_tasks.len()
                );

                // We don't need to do anything here for in-progress tasks. They will finish
                // on their own, decrement the unfinished-task counter themselves, and observe
                // that the queue is Stopped.
                drop(qi.inprogress_tasks);

                // Tear down queued ops
                for op in qi.queued_operations.into_iter() {
                    self.update_upload_queue_unfinished_metric(-1, &op);
                    // Dropping UploadOp::Barrier() here will make wait_completion() return with an Err()
                    // which is exactly what we want to happen.
                    drop(op);
                }

                // We're done.
                drop(guard);
            }
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
}

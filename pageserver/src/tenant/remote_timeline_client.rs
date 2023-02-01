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
//! either in [`crate::tenant::mgr`] during startup or when creating a new
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
//! - [`RemoteTimelineClient::schedule_index_upload_for_metadata_update`] when we've updated the timeline metadata file.
//! - [`RemoteTimelineClient::schedule_index_upload_for_file_changes`] to upload an updated index file, after we've scheduled file uploads
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
//! layer files that are in its list have been successfully uploaded.
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
//!   [`Timeline::timeline_init_and_sync`] and [`Timeline::reconcile_with_remote`].
//!
//! Note that if we crash during file deletion between the index update
//! that removes the file from the list of files, and deleting the remote file,
//! the file is leaked in the remote storage. Similarly, if a new file is created
//! and uploaded, but the pageserver dies permanently before updating the
//! remote index file, the new file is leaked in remote storage. We accept and
//! tolerate that for now.
//! Note further that we cannot easily fix this by scheduling deletes for every
//! file that is present only on the remote, because we cannot distinguish the
//! following two cases:
//! - (1) We had the file locally, deleted it locally, scheduled a remote delete,
//!   but crashed before it finished remotely.
//! - (2) We never had the file locally because we haven't on-demand downloaded
//!   it yet.
//!
//! # Downloads
//!
//! In addition to the upload queue, [`RemoteTimelineClient`] has functions for
//! downloading files from the remote storage. Downloads are performed immediately
//! against the `RemoteStorage`, independently of the upload queue.
//!
//! When we attach a tenant, we perform the following steps:
//! - create `Tenant` object in `TenantState::Attaching` state
//! - List timelines that are present in remote storage, and for each:
//!   - download their remote [`IndexPart`]s
//!   - create `Timeline` struct and a `RemoteTimelineClient`
//!   - initialize the client's upload queue with its `IndexPart`
//!   - create [`RemoteLayer`] instances for layers that are referenced by `IndexPart`
//!     but not present locally
//!   - schedule uploads for layers that are only present locally.
//!   - if the remote `IndexPart`'s metadata was newer than the metadata in
//!     the local filesystem, write the remote metadata to the local filesystem
//! - After the above is done for each timeline, open the tenant for business by
//!   transitioning it from `TenantState::Attaching` to `TenantState::Active` state.
//!   This starts the timelines' WAL-receivers and the tenant's GC & Compaction loops.
//!
//! Most of the above steps happen in [`Timeline::reconcile_with_remote`] or its callers.
//! We keep track of the fact that a client is in `Attaching` state in a marker
//! file on the local disk. This is critical because, when we restart the pageserver,
//! we do not want to do the `List timelines` step for each tenant that has already
//! been successfully attached (for performance & cost reasons).
//! Instead, for a tenant without the attach marker file, we assume that the
//! local state is in sync or ahead of the remote state. This includes the list
//! of all of the tenant's timelines, which is particularly critical to be up-to-date:
//! if there's a timeline on the remote that the pageserver doesn't know about,
//! the GC will not consider its branch point, leading to data loss.
//! So, for a tenant with the attach marker file, we know that we do not yet have
//! persisted all the remote timeline's metadata files locally. To exclude the
//! risk above, we re-run the procedure for such tenants
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

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};

use anyhow::ensure;
use remote_storage::{DownloadError, GenericRemoteStorage};
use std::ops::DerefMut;
use tokio::runtime::Runtime;
use tracing::{debug, info, warn};
use tracing::{info_span, Instrument};
use utils::lsn::Lsn;

use crate::metrics::RemoteOpFileKind;
use crate::metrics::RemoteOpKind;
use crate::metrics::{MeasureRemoteOp, RemoteTimelineClientMetrics};
use crate::tenant::remote_timeline_client::index::LayerFileMetadata;
use crate::{
    config::PageServerConf,
    task_mgr,
    task_mgr::TaskKind,
    task_mgr::BACKGROUND_RUNTIME,
    tenant::metadata::TimelineMetadata,
    tenant::upload_queue::{
        UploadOp, UploadQueue, UploadQueueInitialized, UploadQueueStopped, UploadTask,
    },
    {exponential_backoff, DEFAULT_BASE_BACKOFF_SECONDS, DEFAULT_MAX_BACKOFF_SECONDS},
};

use utils::id::{TenantId, TimelineId};

use self::index::IndexPart;

use super::storage_layer::LayerFileName;

// Occasional network issues and such can cause remote operations to fail, and
// that's expected. If a download fails, we log it at info-level, and retry.
// But after FAILED_DOWNLOAD_WARN_THRESHOLD retries, we start to log it at WARN
// level instead, as repeated failures can mean a more serious problem. If it
// fails more than FAILED_DOWNLOAD_RETRIES times, we give up
const FAILED_DOWNLOAD_WARN_THRESHOLD: u32 = 3;
const FAILED_DOWNLOAD_RETRIES: u32 = 10;

// Similarly log failed uploads and deletions at WARN level, after this many
// retries. Uploads and deletions are retried forever, though.
const FAILED_UPLOAD_WARN_THRESHOLD: u32 = 3;

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

    metrics: Arc<RemoteTimelineClientMetrics>,

    storage_impl: GenericRemoteStorage,
}

impl RemoteTimelineClient {
    ///
    /// Create a remote storage client for given timeline
    ///
    /// Note: the caller must initialize the upload queue before any uploads can be scheduled,
    /// by calling init_upload_queue.
    ///
    pub fn new(
        remote_storage: GenericRemoteStorage,
        conf: &'static PageServerConf,
        tenant_id: TenantId,
        timeline_id: TimelineId,
    ) -> RemoteTimelineClient {
        RemoteTimelineClient {
            conf,
            runtime: &BACKGROUND_RUNTIME,
            tenant_id,
            timeline_id,
            storage_impl: remote_storage,
            upload_queue: Mutex::new(UploadQueue::Uninitialized),
            metrics: Arc::new(RemoteTimelineClientMetrics::new(&tenant_id, &timeline_id)),
        }
    }

    /// Initialize the upload queue for a remote storage that already received
    /// an index file upload, i.e., it's not empty.
    /// The given `index_part` must be the one on the remote.
    pub fn init_upload_queue(&self, index_part: &IndexPart) -> anyhow::Result<()> {
        let mut upload_queue = self.upload_queue.lock().unwrap();
        upload_queue.initialize_with_current_remote_index_part(index_part)?;
        self.update_remote_physical_size_gauge(Some(index_part));
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
        self.update_remote_physical_size_gauge(None);
        Ok(())
    }

    pub fn last_uploaded_consistent_lsn(&self) -> Option<Lsn> {
        match &*self.upload_queue.lock().unwrap() {
            UploadQueue::Uninitialized => None,
            UploadQueue::Initialized(q) => Some(q.last_uploaded_consistent_lsn),
            UploadQueue::Stopped(q) => Some(q.last_uploaded_consistent_lsn),
        }
    }

    fn update_remote_physical_size_gauge(&self, current_remote_index_part: Option<&IndexPart>) {
        let size: u64 = if let Some(current_remote_index_part) = current_remote_index_part {
            current_remote_index_part
                .layer_metadata
                .values()
                // If we don't have the file size for the layer, don't account for it in the metric.
                .map(|ilmd| ilmd.file_size.unwrap_or(0))
                .sum()
        } else {
            0
        };
        self.metrics.remote_physical_size_gauge().set(size);
    }

    pub fn get_remote_physical_size(&self) -> u64 {
        self.metrics.remote_physical_size_gauge().get()
    }

    //
    // Download operations.
    //
    // These don't use the per-timeline queue. They do use the global semaphore in
    // S3Bucket, to limit the total number of concurrent operations, though.
    //

    /// Download index file
    pub async fn download_index_file(&self) -> Result<IndexPart, DownloadError> {
        let _unfinished_gauge_guard = self
            .metrics
            .call_begin(&RemoteOpFileKind::Index, &RemoteOpKind::Download);

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
            Arc::clone(&self.metrics),
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
        layer_file_name: &LayerFileName,
        layer_metadata: &LayerFileMetadata,
    ) -> anyhow::Result<u64> {
        let downloaded_size = {
            let _unfinished_gauge_guard = self
                .metrics
                .call_begin(&RemoteOpFileKind::Layer, &RemoteOpKind::Download);
            download::download_layer_file(
                self.conf,
                &self.storage_impl,
                self.tenant_id,
                self.timeline_id,
                layer_file_name,
                layer_metadata,
            )
            .measure_remote_op(
                self.tenant_id,
                self.timeline_id,
                RemoteOpFileKind::Layer,
                RemoteOpKind::Download,
                Arc::clone(&self.metrics),
            )
            .await?
        };

        // Update the metadata for given layer file. The remote index file
        // might be missing some information for the file; this allows us
        // to fill in the missing details.
        if layer_metadata.file_size().is_none() {
            let new_metadata = LayerFileMetadata::new(downloaded_size);
            let mut guard = self.upload_queue.lock().unwrap();
            let upload_queue = guard.initialized_mut()?;
            if let Some(upgraded) = upload_queue.latest_files.get_mut(layer_file_name) {
                if upgraded.merge(&new_metadata) {
                    upload_queue.latest_files_changes_since_metadata_upload_scheduled += 1;
                }
                // If we don't do an index file upload inbetween here and restart,
                // the value will go back down after pageserver restart, since we will
                // have lost this data point.
                // But, we upload index part fairly frequently, and restart pageserver rarely.
                // So, by accounting eagerly, we present a most-of-the-time-more-accurate value sooner.
                self.metrics
                    .remote_physical_size_gauge()
                    .add(downloaded_size);
            } else {
                // The file should exist, since we just downloaded it.
                warn!(
                    "downloaded file {:?} not found in local copy of the index file",
                    layer_file_name
                );
            }
        }
        Ok(downloaded_size)
    }

    //
    // Upload operations.
    //

    ///
    /// Launch an index-file upload operation in the background, with
    /// updated metadata.
    ///
    /// The upload will be added to the queue immediately, but it
    /// won't be performed until all previosuly scheduled layer file
    /// upload operations have completed successfully.  This is to
    /// ensure that when the index file claims that layers X, Y and Z
    /// exist in remote storage, they really do. To wait for the upload
    /// to complete, use `wait_completion`.
    ///
    /// If there were any changes to the list of files, i.e. if any
    /// layer file uploads were scheduled, since the last index file
    /// upload, those will be included too.
    pub fn schedule_index_upload_for_metadata_update(
        self: &Arc<Self>,
        metadata: &TimelineMetadata,
    ) -> anyhow::Result<()> {
        let mut guard = self.upload_queue.lock().unwrap();
        let upload_queue = guard.initialized_mut()?;

        // As documented in the struct definition, it's ok for latest_metadata to be
        // ahead of what's _actually_ on the remote during index upload.
        upload_queue.latest_metadata = metadata.clone();

        let metadata_bytes = upload_queue.latest_metadata.to_bytes()?;
        self.schedule_index_upload(upload_queue, metadata_bytes);

        Ok(())
    }

    ///
    /// Launch an index-file upload operation in the background, if necessary.
    ///
    /// Use this function to schedule the update of the index file after
    /// scheduling file uploads or deletions. If no file uploads or deletions
    /// have been scheduled since the last index file upload, this does
    /// nothing.
    ///
    /// Like schedule_index_upload_for_metadata_update(), this merely adds
    /// the upload to the upload queue and returns quickly.
    pub fn schedule_index_upload_for_file_changes(self: &Arc<Self>) -> anyhow::Result<()> {
        let mut guard = self.upload_queue.lock().unwrap();
        let upload_queue = guard.initialized_mut()?;

        if upload_queue.latest_files_changes_since_metadata_upload_scheduled > 0 {
            let metadata_bytes = upload_queue.latest_metadata.to_bytes()?;
            self.schedule_index_upload(upload_queue, metadata_bytes);
        }

        Ok(())
    }

    /// Launch an index-file upload operation in the background (internal function)
    fn schedule_index_upload(
        self: &Arc<Self>,
        upload_queue: &mut UploadQueueInitialized,
        metadata_bytes: Vec<u8>,
    ) {
        info!(
            "scheduling metadata upload with {} files ({} changed)",
            upload_queue.latest_files.len(),
            upload_queue.latest_files_changes_since_metadata_upload_scheduled,
        );

        let disk_consistent_lsn = upload_queue.latest_metadata.disk_consistent_lsn();

        let index_part = IndexPart::new(
            upload_queue.latest_files.clone(),
            disk_consistent_lsn,
            metadata_bytes,
        );
        let op = UploadOp::UploadMetadata(index_part, disk_consistent_lsn);
        self.calls_unfinished_metric_begin(&op);
        upload_queue.queued_operations.push_back(op);
        upload_queue.latest_files_changes_since_metadata_upload_scheduled = 0;

        // Launch the task immediately, if possible
        self.launch_queued_tasks(upload_queue);
    }

    ///
    /// Launch an upload operation in the background.
    ///
    pub fn schedule_layer_file_upload(
        self: &Arc<Self>,
        layer_file_name: &LayerFileName,
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

        upload_queue
            .latest_files
            .insert(layer_file_name.clone(), layer_metadata.clone());
        upload_queue.latest_files_changes_since_metadata_upload_scheduled += 1;

        let op = UploadOp::UploadLayer(layer_file_name.clone(), layer_metadata.clone());
        self.calls_unfinished_metric_begin(&op);
        upload_queue.queued_operations.push_back(op);

        info!(
            "scheduled layer file upload {}",
            layer_file_name.file_name()
        );

        // Launch the task immediately, if possible
        self.launch_queued_tasks(upload_queue);
        Ok(())
    }

    ///
    /// Launch a delete operation in the background.
    ///
    /// Note: This schedules an index file upload before the deletions.  The
    /// deletion won't actually be performed, until any previously scheduled
    /// upload operations, and the index file upload, have completed
    /// succesfully.
    ///
    pub fn schedule_layer_file_deletion(
        self: &Arc<Self>,
        names: &[LayerFileName],
    ) -> anyhow::Result<()> {
        let mut guard = self.upload_queue.lock().unwrap();
        let upload_queue = guard.initialized_mut()?;

        // Deleting layers doesn't affect the values stored in TimelineMetadata,
        // so we don't need update it. Just serialize it.
        let metadata_bytes = upload_queue.latest_metadata.to_bytes()?;

        // Update the remote index file, removing the to-be-deleted files from the index,
        // before deleting the actual files.
        //
        // Once we start removing files from upload_queue.latest_files, there's
        // no going back! Otherwise, some of the files would already be removed
        // from latest_files, but not yet scheduled for deletion. Use a closure
        // to syntactically forbid ? or bail! calls here.
        let no_bail_here = || {
            for name in names {
                upload_queue.latest_files.remove(name);
                upload_queue.latest_files_changes_since_metadata_upload_scheduled += 1;
            }

            if upload_queue.latest_files_changes_since_metadata_upload_scheduled > 0 {
                self.schedule_index_upload(upload_queue, metadata_bytes);
            }

            // schedule the actual deletions
            for name in names {
                let op = UploadOp::Delete(RemoteOpFileKind::Layer, name.clone());
                self.calls_unfinished_metric_begin(&op);
                upload_queue.queued_operations.push_back(op);
                info!("scheduled layer file deletion {}", name.file_name());
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

            debug!("starting op: {}", next_op);

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
            // finished. We probably could cancel the upload by simply dropping
            // the Future, but we're not 100% sure if the remote storage library
            // is cancellation safe, so we don't dare to do that. Hopefully, the
            // upload finishes or times out soon enough.
            if task_mgr::is_shutdown_requested() {
                info!("upload task cancelled by shutdown request");
                self.calls_unfinished_metric_end(&task.op);
                self.stop();
                return;
            }

            let upload_result: anyhow::Result<()> = match &task.op {
                UploadOp::UploadLayer(ref layer_file_name, ref layer_metadata) => {
                    let path = &self
                        .conf
                        .timeline_path(&self.timeline_id, &self.tenant_id)
                        .join(layer_file_name.file_name());
                    upload::upload_timeline_layer(
                        self.conf,
                        &self.storage_impl,
                        path,
                        layer_metadata,
                    )
                    .measure_remote_op(
                        self.tenant_id,
                        self.timeline_id,
                        RemoteOpFileKind::Layer,
                        RemoteOpKind::Upload,
                        Arc::clone(&self.metrics),
                    )
                    .await
                }
                UploadOp::UploadMetadata(ref index_part, _lsn) => {
                    let res = upload::upload_index_part(
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
                        Arc::clone(&self.metrics),
                    )
                    .await;
                    if res.is_ok() {
                        self.update_remote_physical_size_gauge(Some(index_part));
                    }
                    res
                }
                UploadOp::Delete(metric_file_kind, ref layer_file_name) => {
                    let path = &self
                        .conf
                        .timeline_path(&self.timeline_id, &self.tenant_id)
                        .join(layer_file_name.file_name());
                    delete::delete_layer(self.conf, &self.storage_impl, path)
                        .measure_remote_op(
                            self.tenant_id,
                            self.timeline_id,
                            *metric_file_kind,
                            RemoteOpKind::Delete,
                            Arc::clone(&self.metrics),
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

                    // Uploads can fail due to rate limits (IAM, S3), spurious network problems,
                    // or other external reasons. Such issues are relatively regular, so log them
                    // at info level at first, and only WARN if the operation fails repeatedly.
                    //
                    // (See similar logic for downloads in `download::download_retry`)
                    if retries < FAILED_UPLOAD_WARN_THRESHOLD {
                        info!(
                            "failed to perform remote task {}, will retry (attempt {}): {:#}",
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
            debug!("remote task {} completed successfully", task.op);
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
        self.calls_unfinished_metric_end(&task.op);
    }

    fn calls_unfinished_metric_impl(
        &self,
        op: &UploadOp,
    ) -> Option<(RemoteOpFileKind, RemoteOpKind)> {
        let res = match op {
            UploadOp::UploadLayer(_, _) => (RemoteOpFileKind::Layer, RemoteOpKind::Upload),
            UploadOp::UploadMetadata(_, _) => (RemoteOpFileKind::Index, RemoteOpKind::Upload),
            UploadOp::Delete(file_kind, _) => (*file_kind, RemoteOpKind::Delete),
            UploadOp::Barrier(_) => {
                // we do not account these
                return None;
            }
        };
        Some(res)
    }

    fn calls_unfinished_metric_begin(&self, op: &UploadOp) {
        let (file_kind, op_kind) = match self.calls_unfinished_metric_impl(op) {
            Some(x) => x,
            None => return,
        };
        let guard = self.metrics.call_begin(&file_kind, &op_kind);
        guard.will_decrement_manually(); // in unfinished_ops_metric_end()
    }

    fn calls_unfinished_metric_end(&self, op: &UploadOp) {
        let (file_kind, op_kind) = match self.calls_unfinished_metric_impl(op) {
            Some(x) => x,
            None => return,
        };
        self.metrics.call_end(&file_kind, &op_kind);
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
                    self.calls_unfinished_metric_end(&op);
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        tenant::harness::{TenantHarness, TIMELINE_ID},
        DEFAULT_PG_VERSION,
    };
    use remote_storage::{RemoteStorageConfig, RemoteStorageKind};
    use std::{collections::HashSet, path::Path};
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

    fn assert_file_list(a: &HashSet<LayerFileName>, b: &[&str]) {
        let mut avec: Vec<String> = a.iter().map(|x| x.file_name()).collect();
        avec.sort();

        let mut bvec = b.to_vec();
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
        // Use a current-thread runtime in the test
        let runtime = Box::leak(Box::new(
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()?,
        ));
        let _entered = runtime.enter();

        let harness = TenantHarness::create("upload_scheduling")?;
        let (tenant, ctx) = runtime.block_on(harness.load());
        let _timeline =
            tenant.create_empty_timeline(TIMELINE_ID, Lsn(0), DEFAULT_PG_VERSION, &ctx)?;
        let timeline_path = harness.timeline_path(&TIMELINE_ID);

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

        let storage_impl = GenericRemoteStorage::from_config(&storage_config)?;
        let client = Arc::new(RemoteTimelineClient {
            conf: harness.conf,
            runtime,
            tenant_id: harness.tenant_id,
            timeline_id: TIMELINE_ID,
            storage_impl,
            upload_queue: Mutex::new(UploadQueue::Uninitialized),
            metrics: Arc::new(RemoteTimelineClientMetrics::new(
                &harness.tenant_id,
                &TIMELINE_ID,
            )),
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
            &LayerFileName::Test("foo".to_owned()),
            &LayerFileMetadata::new(content_foo.len() as u64),
        )?;
        client.schedule_layer_file_upload(
            &LayerFileName::Test("bar".to_owned()),
            &LayerFileMetadata::new(content_bar.len() as u64),
        )?;

        // Check that they are started immediately, not queued
        {
            let mut guard = client.upload_queue.lock().unwrap();
            let upload_queue = guard.initialized_mut().unwrap();
            assert!(upload_queue.queued_operations.is_empty());
            assert!(upload_queue.inprogress_tasks.len() == 2);
            assert!(upload_queue.num_inprogress_layer_uploads == 2);

            // also check that `latest_file_changes` was updated
            assert!(upload_queue.latest_files_changes_since_metadata_upload_scheduled == 2);
        }

        // Schedule upload of index. Check that it is queued
        let metadata = dummy_metadata(Lsn(0x20));
        client.schedule_index_upload_for_metadata_update(&metadata)?;
        {
            let mut guard = client.upload_queue.lock().unwrap();
            let upload_queue = guard.initialized_mut().unwrap();
            assert!(upload_queue.queued_operations.len() == 1);
            assert!(upload_queue.latest_files_changes_since_metadata_upload_scheduled == 0);
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
            &LayerFileName::Test("baz".to_owned()),
            &LayerFileMetadata::new(content_baz.len() as u64),
        )?;
        client.schedule_layer_file_deletion(&[LayerFileName::Test("foo".to_owned())])?;
        {
            let mut guard = client.upload_queue.lock().unwrap();
            let upload_queue = guard.initialized_mut().unwrap();

            // Deletion schedules upload of the index file, and the file deletion itself
            assert!(upload_queue.queued_operations.len() == 2);
            assert!(upload_queue.inprogress_tasks.len() == 1);
            assert!(upload_queue.num_inprogress_layer_uploads == 1);
            assert!(upload_queue.num_inprogress_deletions == 0);
            assert!(upload_queue.latest_files_changes_since_metadata_upload_scheduled == 0);
        }
        assert_remote_files(&["foo", "bar", "index_part.json"], &remote_timeline_dir);

        // Finish them
        runtime.block_on(client.wait_completion())?;

        assert_remote_files(&["bar", "baz", "index_part.json"], &remote_timeline_dir);

        Ok(())
    }
}

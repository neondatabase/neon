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
//! [`UploadQueueInitialized::projected_remote_consistent_lsn`] immediately,
//! and submit a request through the DeletionQueue to update
//! [`UploadQueueInitialized::visible_remote_consistent_lsn`] after it has
//! validated that our generation is not stale.  It is this visible value
//! that is advertized to safekeepers as a signal that that they can
//! delete the WAL up to that LSN.
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
//!   [`Tenant::timeline_init_and_sync`].
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
//!   - schedule uploads for layers that are only present locally.
//! - After the above is done for each timeline, open the tenant for business by
//!   transitioning it from `TenantState::Attaching` to `TenantState::Active` state.
//!   This starts the timelines' WAL-receivers and the tenant's GC & Compaction loops.
//!
//! # Operating Without Remote Storage
//!
//! If no remote storage configuration is provided, the [`RemoteTimelineClient`] is
//! not created and the uploads are skipped.
//!
//! [`Tenant::timeline_init_and_sync`]: super::Tenant::timeline_init_and_sync
//! [`Timeline::load_layer_map`]: super::Timeline::load_layer_map

pub(crate) mod download;
pub mod index;
pub(crate) mod upload;

use anyhow::Context;
use camino::Utf8Path;
use chrono::{NaiveDateTime, Utc};

pub(crate) use download::download_initdb_tar_zst;
use pageserver_api::shard::{ShardIndex, TenantShardId};
use scopeguard::ScopeGuard;
use tokio_util::sync::CancellationToken;
pub(crate) use upload::upload_initdb_dir;
use utils::backoff::{
    self, exponential_backoff, DEFAULT_BASE_BACKOFF_SECONDS, DEFAULT_MAX_BACKOFF_SECONDS,
};

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};

use remote_storage::{DownloadError, GenericRemoteStorage, RemotePath, TimeoutOrCancel};
use std::ops::DerefMut;
use tracing::{debug, error, info, instrument, warn};
use tracing::{info_span, Instrument};
use utils::lsn::Lsn;

use crate::deletion_queue::DeletionQueueClient;
use crate::metrics::{
    MeasureRemoteOp, RemoteOpFileKind, RemoteOpKind, RemoteTimelineClientMetrics,
    RemoteTimelineClientMetricsCallTrackSize, REMOTE_ONDEMAND_DOWNLOADED_BYTES,
    REMOTE_ONDEMAND_DOWNLOADED_LAYERS,
};
use crate::task_mgr::shutdown_token;
use crate::tenant::debug_assert_current_span_has_tenant_and_timeline_id;
use crate::tenant::remote_timeline_client::download::download_retry;
use crate::tenant::storage_layer::AsLayerDesc;
use crate::tenant::upload_queue::Delete;
use crate::tenant::TIMELINES_SEGMENT_NAME;
use crate::{
    config::PageServerConf,
    task_mgr,
    task_mgr::TaskKind,
    tenant::metadata::TimelineMetadata,
    tenant::upload_queue::{
        UploadOp, UploadQueue, UploadQueueInitialized, UploadQueueStopped, UploadTask,
    },
    TENANT_HEATMAP_BASENAME,
};

use utils::id::{TenantId, TimelineId};

use self::index::IndexPart;

use super::storage_layer::{Layer, LayerFileName, ResidentLayer};
use super::upload_queue::SetDeletedFlagProgress;
use super::Generation;

pub(crate) use download::{is_temp_download_file, list_remote_timelines};
pub(crate) use index::LayerFileMetadata;

// Occasional network issues and such can cause remote operations to fail, and
// that's expected. If a download fails, we log it at info-level, and retry.
// But after FAILED_DOWNLOAD_WARN_THRESHOLD retries, we start to log it at WARN
// level instead, as repeated failures can mean a more serious problem. If it
// fails more than FAILED_DOWNLOAD_RETRIES times, we give up
pub(crate) const FAILED_DOWNLOAD_WARN_THRESHOLD: u32 = 3;
pub(crate) const FAILED_REMOTE_OP_RETRIES: u32 = 10;

// Similarly log failed uploads and deletions at WARN level, after this many
// retries. Uploads and deletions are retried forever, though.
pub(crate) const FAILED_UPLOAD_WARN_THRESHOLD: u32 = 3;

pub(crate) const INITDB_PATH: &str = "initdb.tar.zst";

pub(crate) const INITDB_PRESERVED_PATH: &str = "initdb-preserved.tar.zst";

/// Default buffer size when interfacing with [`tokio::fs::File`].
pub(crate) const BUFFER_SIZE: usize = 32 * 1024;

pub enum MaybeDeletedIndexPart {
    IndexPart(IndexPart),
    Deleted(IndexPart),
}

/// Errors that can arise when calling [`RemoteTimelineClient::stop`].
#[derive(Debug, thiserror::Error)]
pub enum StopError {
    /// Returned if the upload queue was never initialized.
    /// See [`RemoteTimelineClient::init_upload_queue`] and [`RemoteTimelineClient::init_upload_queue_for_empty_remote`].
    #[error("queue is not initialized")]
    QueueUninitialized,
}

#[derive(Debug, thiserror::Error)]
pub enum PersistIndexPartWithDeletedFlagError {
    #[error("another task is already setting the deleted_flag, started at {0:?}")]
    AlreadyInProgress(NaiveDateTime),
    #[error("the deleted_flag was already set, value is {0:?}")]
    AlreadyDeleted(NaiveDateTime),
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

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

    tenant_shard_id: TenantShardId,
    timeline_id: TimelineId,
    generation: Generation,

    upload_queue: Mutex<UploadQueue>,

    metrics: Arc<RemoteTimelineClientMetrics>,

    storage_impl: GenericRemoteStorage,

    deletion_queue_client: DeletionQueueClient,

    cancel: CancellationToken,
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
        deletion_queue_client: DeletionQueueClient,
        conf: &'static PageServerConf,
        tenant_shard_id: TenantShardId,
        timeline_id: TimelineId,
        generation: Generation,
    ) -> RemoteTimelineClient {
        RemoteTimelineClient {
            conf,
            tenant_shard_id,
            timeline_id,
            generation,
            storage_impl: remote_storage,
            deletion_queue_client,
            upload_queue: Mutex::new(UploadQueue::Uninitialized),
            metrics: Arc::new(RemoteTimelineClientMetrics::new(
                &tenant_shard_id,
                &timeline_id,
            )),
            cancel: CancellationToken::new(),
        }
    }

    /// Initialize the upload queue for a remote storage that already received
    /// an index file upload, i.e., it's not empty.
    /// The given `index_part` must be the one on the remote.
    pub fn init_upload_queue(&self, index_part: &IndexPart) -> anyhow::Result<()> {
        let mut upload_queue = self.upload_queue.lock().unwrap();
        upload_queue.initialize_with_current_remote_index_part(index_part)?;
        self.update_remote_physical_size_gauge(Some(index_part));
        info!(
            "initialized upload queue from remote index with {} layer files",
            index_part.layer_metadata.len()
        );
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
        info!("initialized upload queue as empty");
        Ok(())
    }

    /// Initialize the queue in stopped state. Used in startup path
    /// to continue deletion operation interrupted by pageserver crash or restart.
    pub fn init_upload_queue_stopped_to_continue_deletion(
        &self,
        index_part: &IndexPart,
    ) -> anyhow::Result<()> {
        // FIXME: consider newtype for DeletedIndexPart.
        let deleted_at = index_part.deleted_at.ok_or(anyhow::anyhow!(
            "bug: it is responsibility of the caller to provide index part from MaybeDeletedIndexPart::Deleted"
        ))?;

        {
            let mut upload_queue = self.upload_queue.lock().unwrap();
            upload_queue.initialize_with_current_remote_index_part(index_part)?;
            self.update_remote_physical_size_gauge(Some(index_part));
        }
        // also locks upload queue, without dropping the guard above it will be a deadlock
        self.stop().expect("initialized line above");

        let mut upload_queue = self.upload_queue.lock().unwrap();

        upload_queue
            .stopped_mut()
            .expect("stopped above")
            .deleted_at = SetDeletedFlagProgress::Successful(deleted_at);

        Ok(())
    }

    pub fn remote_consistent_lsn_projected(&self) -> Option<Lsn> {
        match &mut *self.upload_queue.lock().unwrap() {
            UploadQueue::Uninitialized => None,
            UploadQueue::Initialized(q) => q.get_last_remote_consistent_lsn_projected(),
            UploadQueue::Stopped(q) => q
                .upload_queue_for_deletion
                .get_last_remote_consistent_lsn_projected(),
        }
    }

    pub fn remote_consistent_lsn_visible(&self) -> Option<Lsn> {
        match &mut *self.upload_queue.lock().unwrap() {
            UploadQueue::Uninitialized => None,
            UploadQueue::Initialized(q) => Some(q.get_last_remote_consistent_lsn_visible()),
            UploadQueue::Stopped(q) => Some(
                q.upload_queue_for_deletion
                    .get_last_remote_consistent_lsn_visible(),
            ),
        }
    }

    fn update_remote_physical_size_gauge(&self, current_remote_index_part: Option<&IndexPart>) {
        let size: u64 = if let Some(current_remote_index_part) = current_remote_index_part {
            current_remote_index_part
                .layer_metadata
                .values()
                // If we don't have the file size for the layer, don't account for it in the metric.
                .map(|ilmd| ilmd.file_size)
                .sum()
        } else {
            0
        };
        self.metrics.remote_physical_size_set(size);
    }

    pub fn get_remote_physical_size(&self) -> u64 {
        self.metrics.remote_physical_size_get()
    }

    //
    // Download operations.
    //
    // These don't use the per-timeline queue. They do use the global semaphore in
    // S3Bucket, to limit the total number of concurrent operations, though.
    //

    /// Download index file
    pub async fn download_index_file(
        &self,
        cancel: &CancellationToken,
    ) -> Result<MaybeDeletedIndexPart, DownloadError> {
        let _unfinished_gauge_guard = self.metrics.call_begin(
            &RemoteOpFileKind::Index,
            &RemoteOpKind::Download,
            crate::metrics::RemoteTimelineClientMetricsCallTrackSize::DontTrackSize {
                reason: "no need for a downloads gauge",
            },
        );

        let index_part = download::download_index_part(
            &self.storage_impl,
            &self.tenant_shard_id,
            &self.timeline_id,
            self.generation,
            cancel,
        )
        .measure_remote_op(
            RemoteOpFileKind::Index,
            RemoteOpKind::Download,
            Arc::clone(&self.metrics),
        )
        .await?;

        if index_part.deleted_at.is_some() {
            Ok(MaybeDeletedIndexPart::Deleted(index_part))
        } else {
            Ok(MaybeDeletedIndexPart::IndexPart(index_part))
        }
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
        cancel: &CancellationToken,
    ) -> anyhow::Result<u64> {
        let downloaded_size = {
            let _unfinished_gauge_guard = self.metrics.call_begin(
                &RemoteOpFileKind::Layer,
                &RemoteOpKind::Download,
                crate::metrics::RemoteTimelineClientMetricsCallTrackSize::DontTrackSize {
                    reason: "no need for a downloads gauge",
                },
            );
            download::download_layer_file(
                self.conf,
                &self.storage_impl,
                self.tenant_shard_id,
                self.timeline_id,
                layer_file_name,
                layer_metadata,
                cancel,
            )
            .measure_remote_op(
                RemoteOpFileKind::Layer,
                RemoteOpKind::Download,
                Arc::clone(&self.metrics),
            )
            .await?
        };

        REMOTE_ONDEMAND_DOWNLOADED_LAYERS.inc();
        REMOTE_ONDEMAND_DOWNLOADED_BYTES.inc_by(downloaded_size);

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
    /// won't be performed until all previously scheduled layer file
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

        self.schedule_index_upload(upload_queue, upload_queue.latest_metadata.clone());

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
            self.schedule_index_upload(upload_queue, upload_queue.latest_metadata.clone());
        }

        Ok(())
    }

    /// Launch an index-file upload operation in the background (internal function)
    fn schedule_index_upload(
        self: &Arc<Self>,
        upload_queue: &mut UploadQueueInitialized,
        metadata: TimelineMetadata,
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
            metadata,
        );
        let op = UploadOp::UploadMetadata(index_part, disk_consistent_lsn);
        self.metric_begin(&op);
        upload_queue.queued_operations.push_back(op);
        upload_queue.latest_files_changes_since_metadata_upload_scheduled = 0;

        // Launch the task immediately, if possible
        self.launch_queued_tasks(upload_queue);
    }

    /// Schedules a remote copy of the given Layer file from another timeline of the same tenant.
    /// The file will be copied under the timeline in the remote storage, and added to the
    /// `index_part.json` files uploaded on the next metadata upload.
    ///
    /// There will be no checking if the copys have already happened. If such is needed, the
    /// existence of a copy needs to be done before calling this method.
    ///
    /// A `index_part.json` upload will not be scheduled, it can be scheduled by
    /// [`Self::schedule_index_upload_for_file_changes`].
    pub(crate) fn schedule_layer_adoption(
        self: &Arc<Self>,
        adopted: Layer,
        target: Layer,
    ) -> anyhow::Result<()> {
        // non-technical rule, we don't have any such support planned
        anyhow::ensure!(
            adopted.layer_desc().tenant_shard_id == target.layer_desc().tenant_shard_id,
            "cross-tenant copying is not supported"
        );
        // currently there is no known reason to do copy within
        anyhow::ensure!(
            adopted.layer_desc().timeline_id != target.layer_desc().timeline_id,
            "copying within a timeline is not supported"
        );

        let mut guard = self.upload_queue.lock().unwrap();
        let upload_queue = guard.initialized_mut()?;

        self.schedule_layer_adoption0(upload_queue, adopted, target);
        self.launch_queued_tasks(upload_queue);
        Ok(())
    }

    pub(crate) fn schedule_layer_adoption0(
        self: &Arc<Self>,
        upload_queue: &mut UploadQueueInitialized,
        adopted: Layer,
        owned: Layer,
    ) {
        let metadata = owned.metadata();

        upload_queue
            .latest_files
            .insert(owned.layer_desc().filename(), metadata.clone());
        upload_queue.latest_files_changes_since_metadata_upload_scheduled += 1;

        info!(
            gen=?metadata.generation,
            shard=?metadata.shard,
            "scheduled layer file copy {adopted}",
        );

        let op = UploadOp::AdoptLayer { adopted, owned };
        self.metric_begin(&op);
        upload_queue.queued_operations.push_back(op);
    }

    ///
    /// Launch an upload operation in the background.
    ///
    pub(crate) fn schedule_layer_file_upload(
        self: &Arc<Self>,
        layer: ResidentLayer,
    ) -> anyhow::Result<()> {
        let mut guard = self.upload_queue.lock().unwrap();
        let upload_queue = guard.initialized_mut()?;

        self.schedule_layer_file_upload0(upload_queue, layer);
        self.launch_queued_tasks(upload_queue);
        Ok(())
    }

    fn schedule_layer_file_upload0(
        self: &Arc<Self>,
        upload_queue: &mut UploadQueueInitialized,
        layer: ResidentLayer,
    ) {
        let metadata = layer.metadata();

        upload_queue
            .latest_files
            .insert(layer.layer_desc().filename(), metadata.clone());
        upload_queue.latest_files_changes_since_metadata_upload_scheduled += 1;

        info!(
            gen=?metadata.generation,
            shard=?metadata.shard,
            "scheduled layer file upload {layer}",
        );

        let op = UploadOp::UploadLayer(layer, metadata);
        self.metric_begin(&op);
        upload_queue.queued_operations.push_back(op);
    }

    /// Launch a delete operation in the background.
    ///
    /// The operation does not modify local filesystem state.
    ///
    /// Note: This schedules an index file upload before the deletions.  The
    /// deletion won't actually be performed, until all previously scheduled
    /// upload operations, and the index file upload, have completed
    /// successfully.
    pub fn schedule_layer_file_deletion(
        self: &Arc<Self>,
        names: &[LayerFileName],
    ) -> anyhow::Result<()> {
        let mut guard = self.upload_queue.lock().unwrap();
        let upload_queue = guard.initialized_mut()?;

        let with_metadata =
            self.schedule_unlinking_of_layers_from_index_part0(upload_queue, names.iter().cloned());

        self.schedule_deletion_of_unlinked0(upload_queue, with_metadata);

        // Launch the tasks immediately, if possible
        self.launch_queued_tasks(upload_queue);
        Ok(())
    }

    /// Unlinks the layer files from `index_part.json` but does not yet schedule deletion for the
    /// layer files, leaving them dangling.
    ///
    /// The files will be leaked in remote storage unless [`Self::schedule_deletion_of_unlinked`]
    /// is invoked on them.
    pub(crate) fn schedule_gc_update(self: &Arc<Self>, gc_layers: &[Layer]) -> anyhow::Result<()> {
        let mut guard = self.upload_queue.lock().unwrap();
        let upload_queue = guard.initialized_mut()?;

        // just forget the return value; after uploading the next index_part.json, we can consider
        // the layer files as "dangling". this is fine, at worst case we create work for the
        // scrubber.

        let names = gc_layers.iter().map(|x| x.layer_desc().filename());

        self.schedule_unlinking_of_layers_from_index_part0(upload_queue, names);

        self.launch_queued_tasks(upload_queue);

        Ok(())
    }

    /// Update the remote index file, removing the to-be-deleted files from the index,
    /// allowing scheduling of actual deletions later.
    fn schedule_unlinking_of_layers_from_index_part0<I>(
        self: &Arc<Self>,
        upload_queue: &mut UploadQueueInitialized,
        names: I,
    ) -> Vec<(LayerFileName, LayerFileMetadata)>
    where
        I: IntoIterator<Item = LayerFileName>,
    {
        // Deleting layers doesn't affect the values stored in TimelineMetadata,
        // so we don't need update it. Just serialize it.
        let metadata = upload_queue.latest_metadata.clone();

        // Decorate our list of names with each name's metadata, dropping
        // names that are unexpectedly missing from our metadata.  This metadata
        // is later used when physically deleting layers, to construct key paths.
        let with_metadata: Vec<_> = names
            .into_iter()
            .filter_map(|name| {
                let meta = upload_queue.latest_files.remove(&name);

                if let Some(meta) = meta {
                    upload_queue.latest_files_changes_since_metadata_upload_scheduled += 1;
                    Some((name, meta))
                } else {
                    // This can only happen if we forgot to to schedule the file upload
                    // before scheduling the delete. Log it because it is a rare/strange
                    // situation, and in case something is misbehaving, we'd like to know which
                    // layers experienced this.
                    info!("Deleting layer {name} not found in latest_files list, never uploaded?");
                    None
                }
            })
            .collect();

        #[cfg(feature = "testing")]
        for (name, metadata) in &with_metadata {
            let gen = metadata.generation;
            if let Some(unexpected) = upload_queue.dangling_files.insert(name.to_owned(), gen) {
                if unexpected == gen {
                    tracing::error!("{name} was unlinked twice with same generation");
                } else {
                    tracing::error!("{name} was unlinked twice with different generations {gen:?} and {unexpected:?}");
                }
            }
        }

        // after unlinking files from the upload_queue.latest_files we must always schedule an
        // index_part update, because that needs to be uploaded before we can actually delete the
        // files.
        if upload_queue.latest_files_changes_since_metadata_upload_scheduled > 0 {
            self.schedule_index_upload(upload_queue, metadata);
        }

        with_metadata
    }

    /// Schedules deletion for layer files which have previously been unlinked from the
    /// `index_part.json` with [`Self::schedule_gc_update`] or [`Self::schedule_compaction_update`].
    pub(crate) fn schedule_deletion_of_unlinked(
        self: &Arc<Self>,
        layers: Vec<(LayerFileName, LayerFileMetadata)>,
    ) -> anyhow::Result<()> {
        let mut guard = self.upload_queue.lock().unwrap();
        let upload_queue = guard.initialized_mut()?;

        self.schedule_deletion_of_unlinked0(upload_queue, layers);
        self.launch_queued_tasks(upload_queue);
        Ok(())
    }

    fn schedule_deletion_of_unlinked0(
        self: &Arc<Self>,
        upload_queue: &mut UploadQueueInitialized,
        mut with_metadata: Vec<(LayerFileName, LayerFileMetadata)>,
    ) {
        // Filter out any layers which were not created by this tenant shard.  These are
        // layers that originate from some ancestor shard after a split, and may still
        // be referenced by other shards. We are free to delete them locally and remove
        // them from our index (and would have already done so when we reach this point
        // in the code), but we may not delete them remotely.
        with_metadata.retain(|(name, meta)| {
            let retain = meta.shard.shard_number == self.tenant_shard_id.shard_number
                && meta.shard.shard_count == self.tenant_shard_id.shard_count;
            if !retain {
                tracing::debug!(
                    "Skipping deletion of ancestor-shard layer {name}, from shard {}",
                    meta.shard
                );
            }
            retain
        });

        for (name, meta) in &with_metadata {
            info!(
                "scheduling deletion of layer {}{} (shard {})",
                name,
                meta.generation.get_suffix(),
                meta.shard
            );
        }

        #[cfg(feature = "testing")]
        for (name, meta) in &with_metadata {
            let gen = meta.generation;
            match upload_queue.dangling_files.remove(name) {
                Some(same) if same == gen => { /* expected */ }
                Some(other) => {
                    tracing::error!("{name} was unlinked with {other:?} but deleted with {gen:?}");
                }
                None => {
                    tracing::error!("{name} was unlinked but was not dangling");
                }
            }
        }

        // schedule the actual deletions
        if with_metadata.is_empty() {
            // avoid scheduling the op & bumping the metric
            return;
        }
        let op = UploadOp::Delete(Delete {
            layers: with_metadata,
        });
        self.metric_begin(&op);
        upload_queue.queued_operations.push_back(op);
    }

    /// Schedules a compaction update to the remote `index_part.json`.
    ///
    /// `compacted_from` represent the L0 names which have been `compacted_to` L1 layers.
    pub(crate) fn schedule_compaction_update(
        self: &Arc<Self>,
        compacted_from: &[Layer],
        compacted_to: &[ResidentLayer],
    ) -> anyhow::Result<()> {
        let mut guard = self.upload_queue.lock().unwrap();
        let upload_queue = guard.initialized_mut()?;

        for layer in compacted_to {
            self.schedule_layer_file_upload0(upload_queue, layer.clone());
        }

        let names = compacted_from.iter().map(|x| x.layer_desc().filename());

        self.schedule_unlinking_of_layers_from_index_part0(upload_queue, names);
        self.launch_queued_tasks(upload_queue);

        Ok(())
    }

    /// Wait for all previously scheduled uploads/deletions to complete
    pub(crate) async fn wait_completion(self: &Arc<Self>) -> anyhow::Result<()> {
        let mut receiver = {
            let mut guard = self.upload_queue.lock().unwrap();
            let upload_queue = guard.initialized_mut()?;
            self.schedule_barrier0(upload_queue)
        };

        if receiver.changed().await.is_err() {
            anyhow::bail!("wait_completion aborted because upload queue was stopped");
        }

        Ok(())
    }

    pub(crate) fn schedule_barrier(self: &Arc<Self>) -> anyhow::Result<()> {
        let mut guard = self.upload_queue.lock().unwrap();
        let upload_queue = guard.initialized_mut()?;
        self.schedule_barrier0(upload_queue);
        Ok(())
    }

    fn schedule_barrier0(
        self: &Arc<Self>,
        upload_queue: &mut UploadQueueInitialized,
    ) -> tokio::sync::watch::Receiver<()> {
        let (sender, receiver) = tokio::sync::watch::channel(());
        let barrier_op = UploadOp::Barrier(sender);

        upload_queue.queued_operations.push_back(barrier_op);
        // Don't count this kind of operation!

        // Launch the task immediately, if possible
        self.launch_queued_tasks(upload_queue);

        receiver
    }

    /// Wait for all previously scheduled operations to complete, and then stop.
    ///
    /// Not cancellation safe
    pub(crate) async fn shutdown(self: &Arc<Self>) -> Result<(), StopError> {
        // On cancellation the queue is left in ackward state of refusing new operations but
        // proper stop is yet to be called. On cancel the original or some later task must call
        // `stop` or `shutdown`.
        let sg = scopeguard::guard((), |_| {
            tracing::error!("RemoteTimelineClient::shutdown was cancelled; this should not happen, do not make this into an allowed_error")
        });

        let fut = {
            let mut guard = self.upload_queue.lock().unwrap();
            let upload_queue = match &mut *guard {
                UploadQueue::Stopped(_) => return Ok(()),
                UploadQueue::Uninitialized => return Err(StopError::QueueUninitialized),
                UploadQueue::Initialized(ref mut init) => init,
            };

            // if the queue is already stuck due to a shutdown operation which was cancelled, then
            // just don't add more of these as they would never complete.
            //
            // TODO: if launch_queued_tasks were to be refactored to accept a &mut UploadQueue
            // in every place we would not have to jump through this hoop, and this method could be
            // made cancellable.
            if !upload_queue.shutting_down {
                upload_queue.shutting_down = true;
                upload_queue.queued_operations.push_back(UploadOp::Shutdown);
                // this operation is not counted similar to Barrier

                self.launch_queued_tasks(upload_queue);
            }

            upload_queue.shutdown_ready.clone().acquire_owned()
        };

        let res = fut.await;

        scopeguard::ScopeGuard::into_inner(sg);

        match res {
            Ok(_permit) => unreachable!("shutdown_ready should not have been added permits"),
            Err(_closed) => {
                // expected
            }
        }

        self.stop()
    }

    /// Set the deleted_at field in the remote index file.
    ///
    /// This fails if the upload queue has not been `stop()`ed.
    ///
    /// The caller is responsible for calling `stop()` AND for waiting
    /// for any ongoing upload tasks to finish after `stop()` has succeeded.
    /// Check method [`RemoteTimelineClient::stop`] for details.
    #[instrument(skip_all)]
    pub(crate) async fn persist_index_part_with_deleted_flag(
        self: &Arc<Self>,
    ) -> Result<(), PersistIndexPartWithDeletedFlagError> {
        let index_part_with_deleted_at = {
            let mut locked = self.upload_queue.lock().unwrap();

            // We must be in stopped state because otherwise
            // we can have inprogress index part upload that can overwrite the file
            // with missing is_deleted flag that we going to set below
            let stopped = locked.stopped_mut()?;

            match stopped.deleted_at {
                SetDeletedFlagProgress::NotRunning => (), // proceed
                SetDeletedFlagProgress::InProgress(at) => {
                    return Err(PersistIndexPartWithDeletedFlagError::AlreadyInProgress(at));
                }
                SetDeletedFlagProgress::Successful(at) => {
                    return Err(PersistIndexPartWithDeletedFlagError::AlreadyDeleted(at));
                }
            };
            let deleted_at = Utc::now().naive_utc();
            stopped.deleted_at = SetDeletedFlagProgress::InProgress(deleted_at);

            let mut index_part = IndexPart::try_from(&stopped.upload_queue_for_deletion)
                .context("IndexPart serialize")?;
            index_part.deleted_at = Some(deleted_at);
            index_part
        };

        let undo_deleted_at = scopeguard::guard(Arc::clone(self), |self_clone| {
            let mut locked = self_clone.upload_queue.lock().unwrap();
            let stopped = locked
                .stopped_mut()
                .expect("there's no way out of Stopping, and we checked it's Stopping above");
            stopped.deleted_at = SetDeletedFlagProgress::NotRunning;
        });

        pausable_failpoint!("persist_deleted_index_part");

        backoff::retry(
            || {
                upload::upload_index_part(
                    &self.storage_impl,
                    &self.tenant_shard_id,
                    &self.timeline_id,
                    self.generation,
                    &index_part_with_deleted_at,
                    &self.cancel,
                )
            },
            |_e| false,
            1,
            // have just a couple of attempts
            // when executed as part of timeline deletion this happens in context of api call
            // when executed as part of tenant deletion this happens in the background
            2,
            "persist_index_part_with_deleted_flag",
            &self.cancel,
        )
        .await
        .ok_or_else(|| anyhow::Error::new(TimeoutOrCancel::Cancel))
        .and_then(|x| x)?;

        // all good, disarm the guard and mark as success
        ScopeGuard::into_inner(undo_deleted_at);
        {
            let mut locked = self.upload_queue.lock().unwrap();

            let stopped = locked
                .stopped_mut()
                .expect("there's no way out of Stopping, and we checked it's Stopping above");
            stopped.deleted_at = SetDeletedFlagProgress::Successful(
                index_part_with_deleted_at
                    .deleted_at
                    .expect("we set it above"),
            );
        }

        Ok(())
    }

    pub(crate) async fn preserve_initdb_archive(
        self: &Arc<Self>,
        tenant_id: &TenantId,
        timeline_id: &TimelineId,
        cancel: &CancellationToken,
    ) -> anyhow::Result<()> {
        backoff::retry(
            || async {
                upload::preserve_initdb_archive(&self.storage_impl, tenant_id, timeline_id, cancel)
                    .await
            },
            TimeoutOrCancel::caused_by_cancel,
            FAILED_DOWNLOAD_WARN_THRESHOLD,
            FAILED_REMOTE_OP_RETRIES,
            "preserve_initdb_tar_zst",
            &cancel.clone(),
        )
        .await
        .ok_or_else(|| anyhow::Error::new(TimeoutOrCancel::Cancel))
        .and_then(|x| x)
        .context("backing up initdb archive")?;
        Ok(())
    }

    /// Prerequisites: UploadQueue should be in stopped state and deleted_at should be successfuly set.
    /// The function deletes layer files one by one, then lists the prefix to see if we leaked something
    /// deletes leaked files if any and proceeds with deletion of index file at the end.
    pub(crate) async fn delete_all(self: &Arc<Self>) -> anyhow::Result<()> {
        debug_assert_current_span_has_tenant_and_timeline_id();

        let layers: Vec<RemotePath> = {
            let mut locked = self.upload_queue.lock().unwrap();
            let stopped = locked.stopped_mut()?;

            if !matches!(stopped.deleted_at, SetDeletedFlagProgress::Successful(_)) {
                anyhow::bail!("deleted_at is not set")
            }

            debug_assert!(stopped.upload_queue_for_deletion.no_pending_work());

            stopped
                .upload_queue_for_deletion
                .latest_files
                .drain()
                .map(|(file_name, meta)| {
                    remote_layer_path(
                        &self.tenant_shard_id.tenant_id,
                        &self.timeline_id,
                        meta.shard,
                        &file_name,
                        meta.generation,
                    )
                })
                .collect()
        };

        let layer_deletion_count = layers.len();
        self.deletion_queue_client.push_immediate(layers).await?;

        // Delete the initdb.tar.zst, which is not always present, but deletion attempts of
        // inexistant objects are not considered errors.
        let initdb_path =
            remote_initdb_archive_path(&self.tenant_shard_id.tenant_id, &self.timeline_id);
        self.deletion_queue_client
            .push_immediate(vec![initdb_path])
            .await?;

        // Do not delete index part yet, it is needed for possible retry. If we remove it first
        // and retry will arrive to different pageserver there wont be any traces of it on remote storage
        let timeline_storage_path = remote_timeline_path(&self.tenant_shard_id, &self.timeline_id);

        // Execute all pending deletions, so that when we proceed to do a list_prefixes below, we aren't
        // taking the burden of listing all the layers that we already know we should delete.
        self.deletion_queue_client.flush_immediate().await?;

        let cancel = shutdown_token();

        let remaining = download_retry(
            || async {
                self.storage_impl
                    .list_files(Some(&timeline_storage_path), None, &cancel)
                    .await
            },
            "list remaining files",
            &cancel,
        )
        .await
        .context("list files remaining files")?;

        // We will delete the current index_part object last, since it acts as a deletion
        // marker via its deleted_at attribute
        let latest_index = remaining
            .iter()
            .filter(|p| {
                p.object_name()
                    .map(|n| n.starts_with(IndexPart::FILE_NAME))
                    .unwrap_or(false)
            })
            .filter_map(|path| parse_remote_index_path(path.clone()).map(|gen| (path, gen)))
            .max_by_key(|i| i.1)
            .map(|i| i.0.clone())
            .unwrap_or(
                // No generation-suffixed indices, assume we are dealing with
                // a legacy index.
                remote_index_path(&self.tenant_shard_id, &self.timeline_id, Generation::none()),
            );

        let remaining_layers: Vec<RemotePath> = remaining
            .into_iter()
            .filter(|p| {
                if p == &latest_index {
                    return false;
                }
                if p.object_name() == Some(INITDB_PRESERVED_PATH) {
                    return false;
                }
                true
            })
            .inspect(|path| {
                if let Some(name) = path.object_name() {
                    info!(%name, "deleting a file not referenced from index_part.json");
                } else {
                    warn!(%path, "deleting a nameless or non-utf8 object not referenced from index_part.json");
                }
            })
            .collect();

        let not_referenced_count = remaining_layers.len();
        if !remaining_layers.is_empty() {
            self.deletion_queue_client
                .push_immediate(remaining_layers)
                .await?;
        }

        fail::fail_point!("timeline-delete-before-index-delete", |_| {
            Err(anyhow::anyhow!(
                "failpoint: timeline-delete-before-index-delete"
            ))?
        });

        debug!("enqueuing index part deletion");
        self.deletion_queue_client
            .push_immediate([latest_index].to_vec())
            .await?;

        // Timeline deletion is rare and we have probably emitted a reasonably number of objects: wait
        // for a flush to a persistent deletion list so that we may be sure deletion will occur.
        self.deletion_queue_client.flush_immediate().await?;

        fail::fail_point!("timeline-delete-after-index-delete", |_| {
            Err(anyhow::anyhow!(
                "failpoint: timeline-delete-after-index-delete"
            ))?
        });

        info!(prefix=%timeline_storage_path, referenced=layer_deletion_count, not_referenced=%not_referenced_count, "done deleting in timeline prefix, including index_part.json");

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
                UploadOp::UploadLayer(..) | UploadOp::AdoptLayer { .. } => {
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

                UploadOp::Barrier(_) | UploadOp::Shutdown => {
                    upload_queue.inprogress_tasks.is_empty()
                }
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

            if let UploadOp::Shutdown = next_op {
                // leave the op in the queue but do not start more tasks; it will be dropped when
                // the stop is called.
                upload_queue.shutdown_ready.close();
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
                UploadOp::AdoptLayer { .. } => upload_queue.num_inprogress_layer_copies += 1,
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
                UploadOp::Shutdown => unreachable!("shutdown is intentionally never popped off"),
            };

            // Assign unique ID to this task
            upload_queue.task_counter += 1;
            let upload_task_id = upload_queue.task_counter;

            // Add it to the in-progress map
            let task = Arc::new(UploadTask {
                task_id: upload_task_id,
                op: next_op,
                retries: AtomicU32::new(0),
            });
            upload_queue
                .inprogress_tasks
                .insert(task.task_id, Arc::clone(&task));

            // Spawn task to perform the task
            let self_rc = Arc::clone(self);
            let tenant_shard_id = self.tenant_shard_id;
            let timeline_id = self.timeline_id;
            task_mgr::spawn(
                TaskKind::RemoteUploadTask,
                Some(self.tenant_shard_id),
                Some(self.timeline_id),
                "remote upload",
                false,
                async move {
                    self_rc.perform_upload_task(task).await;
                    Ok(())
                }
                .instrument(info_span!(parent: None, "remote_upload", tenant_id=%tenant_shard_id.tenant_id, shard_id=%tenant_shard_id.shard_slug(), %timeline_id, %upload_task_id)),
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
        let cancel = shutdown_token();
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
            if cancel.is_cancelled() {
                info!("upload task cancelled by shutdown request");
                match self.stop() {
                    Ok(()) => {}
                    Err(StopError::QueueUninitialized) => {
                        unreachable!("we never launch an upload task if the queue is uninitialized, and once it is initialized, we never go back")
                    }
                }
                return;
            }

            let upload_result: anyhow::Result<()> = match &task.op {
                UploadOp::UploadLayer(ref layer, ref layer_metadata) => {
                    let path = layer.local_path();
                    upload::upload_timeline_layer(
                        self.conf,
                        &self.storage_impl,
                        path,
                        layer_metadata,
                        self.generation,
                        &self.cancel,
                    )
                    .measure_remote_op(
                        RemoteOpFileKind::Layer,
                        RemoteOpKind::Upload,
                        Arc::clone(&self.metrics),
                    )
                    .await
                }
                UploadOp::AdoptLayer {
                    ref adopted,
                    ref owned,
                } => {
                    let source = adopted.local_path();
                    let target = owned.local_path();
                    upload::copy_timeline_layer(
                        self.conf,
                        &self.storage_impl,
                        source,
                        &adopted.metadata(),
                        target,
                        &owned.metadata(),
                        &self.cancel,
                    )
                    .measure_remote_op(
                        RemoteOpFileKind::Layer,
                        RemoteOpKind::Copy,
                        Arc::clone(&self.metrics),
                    )
                    .await
                }
                UploadOp::UploadMetadata(ref index_part, _lsn) => {
                    let mention_having_future_layers = if cfg!(feature = "testing") {
                        index_part
                            .layer_metadata
                            .keys()
                            .any(|x| x.is_in_future(*_lsn))
                    } else {
                        false
                    };

                    let res = upload::upload_index_part(
                        &self.storage_impl,
                        &self.tenant_shard_id,
                        &self.timeline_id,
                        self.generation,
                        index_part,
                        &self.cancel,
                    )
                    .measure_remote_op(
                        RemoteOpFileKind::Index,
                        RemoteOpKind::Upload,
                        Arc::clone(&self.metrics),
                    )
                    .await;
                    if res.is_ok() {
                        self.update_remote_physical_size_gauge(Some(index_part));
                        if mention_having_future_layers {
                            // find rationale near crate::tenant::timeline::init::cleanup_future_layer
                            tracing::info!(disk_consistent_lsn=%_lsn, "uploaded an index_part.json with future layers -- this is ok! if shutdown now, expect future layer cleanup");
                        }
                    }
                    res
                }
                UploadOp::Delete(delete) => {
                    pausable_failpoint!("before-delete-layer-pausable");
                    self.deletion_queue_client
                        .push_layers(
                            self.tenant_shard_id,
                            self.timeline_id,
                            self.generation,
                            delete.layers.clone(),
                        )
                        .await
                        .map_err(|e| anyhow::anyhow!(e))
                }
                unexpected @ UploadOp::Barrier(_) | unexpected @ UploadOp::Shutdown => {
                    // unreachable. Barrier operations are handled synchronously in
                    // launch_queued_tasks
                    warn!("unexpected {unexpected:?} operation in perform_upload_task");
                    break;
                }
            };

            match upload_result {
                Ok(()) => {
                    break;
                }
                Err(e) if TimeoutOrCancel::caused_by_cancel(&e) => {
                    // loop around to do the proper stopping
                    continue;
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
                    exponential_backoff(
                        retries,
                        DEFAULT_BASE_BACKOFF_SECONDS,
                        DEFAULT_MAX_BACKOFF_SECONDS,
                        &cancel,
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
            debug!("remote task {} completed successfully", task.op);
        }

        // The task has completed successfully. Remove it from the in-progress list.
        let lsn_update = {
            let mut upload_queue_guard = self.upload_queue.lock().unwrap();
            let upload_queue = match upload_queue_guard.deref_mut() {
                UploadQueue::Uninitialized => panic!("callers are responsible for ensuring this is only called on an initialized queue"),
                UploadQueue::Stopped(_stopped) => {
                    None
                },
                UploadQueue::Initialized(qi) => { Some(qi) }
            };

            let upload_queue = match upload_queue {
                Some(upload_queue) => upload_queue,
                None => {
                    info!("another concurrent task already stopped the queue");
                    return;
                }
            };

            upload_queue.inprogress_tasks.remove(&task.task_id);

            let lsn_update = match task.op {
                UploadOp::UploadLayer(_, _) => {
                    upload_queue.num_inprogress_layer_uploads -= 1;
                    None
                }
                UploadOp::AdoptLayer { .. } => {
                    upload_queue.num_inprogress_layer_copies -= 1;
                    None
                }
                UploadOp::UploadMetadata(_, lsn) => {
                    upload_queue.num_inprogress_metadata_uploads -= 1;
                    // XXX monotonicity check?

                    upload_queue.projected_remote_consistent_lsn = Some(lsn);
                    if self.generation.is_none() {
                        // Legacy mode: skip validating generation
                        upload_queue.visible_remote_consistent_lsn.store(lsn);
                        None
                    } else {
                        Some((lsn, upload_queue.visible_remote_consistent_lsn.clone()))
                    }
                }
                UploadOp::Delete(_) => {
                    upload_queue.num_inprogress_deletions -= 1;
                    None
                }
                UploadOp::Barrier(..) | UploadOp::Shutdown => unreachable!(),
            };

            // Launch any queued tasks that were unblocked by this one.
            self.launch_queued_tasks(upload_queue);
            lsn_update
        };

        if let Some((lsn, slot)) = lsn_update {
            // Updates to the remote_consistent_lsn we advertise to pageservers
            // are all routed through the DeletionQueue, to enforce important
            // data safety guarantees (see docs/rfcs/025-generation-numbers.md)
            self.deletion_queue_client
                .update_remote_consistent_lsn(
                    self.tenant_shard_id,
                    self.timeline_id,
                    self.generation,
                    lsn,
                    slot,
                )
                .await;
        }

        self.metric_end(&task.op);
    }

    fn metric_impl(
        &self,
        op: &UploadOp,
    ) -> Option<(
        RemoteOpFileKind,
        RemoteOpKind,
        RemoteTimelineClientMetricsCallTrackSize,
    )> {
        use RemoteTimelineClientMetricsCallTrackSize::DontTrackSize;
        let res = match op {
            UploadOp::UploadLayer(_, m) => (
                RemoteOpFileKind::Layer,
                RemoteOpKind::Upload,
                RemoteTimelineClientMetricsCallTrackSize::Bytes(m.file_size()),
            ),
            UploadOp::AdoptLayer { .. } => (
                RemoteOpFileKind::Layer,
                RemoteOpKind::Copy,
                DontTrackSize {
                    reason: "remote is doing the copying, not us",
                },
            ),
            UploadOp::UploadMetadata(_, _) => (
                RemoteOpFileKind::Index,
                RemoteOpKind::Upload,
                DontTrackSize {
                    reason: "metadata uploads are tiny",
                },
            ),
            UploadOp::Delete(_delete) => (
                RemoteOpFileKind::Layer,
                RemoteOpKind::Delete,
                DontTrackSize {
                    reason: "should we track deletes? positive or negative sign?",
                },
            ),
            UploadOp::Barrier(..) | UploadOp::Shutdown => {
                // we do not account these
                return None;
            }
        };
        Some(res)
    }

    fn metric_begin(&self, op: &UploadOp) {
        let (file_kind, op_kind, track_bytes) = match self.metric_impl(op) {
            Some(x) => x,
            None => return,
        };
        let guard = self.metrics.call_begin(&file_kind, &op_kind, track_bytes);
        guard.will_decrement_manually(); // in metric_end(), see right below
    }

    fn metric_end(&self, op: &UploadOp) {
        let (file_kind, op_kind, track_bytes) = match self.metric_impl(op) {
            Some(x) => x,
            None => return,
        };
        self.metrics.call_end(&file_kind, &op_kind, track_bytes);
    }

    /// Close the upload queue for new operations and cancel queued operations.
    ///
    /// Use [`RemoteTimelineClient::shutdown`] for graceful stop.
    ///
    /// In-progress operations will still be running after this function returns.
    /// Use `task_mgr::shutdown_tasks(None, Some(self.tenant_id), Some(timeline_id))`
    /// to wait for them to complete, after calling this function.
    pub(crate) fn stop(&self) -> Result<(), StopError> {
        // Whichever *task* for this RemoteTimelineClient grabs the mutex first will transition the queue
        // into stopped state, thereby dropping all off the queued *ops* which haven't become *tasks* yet.
        // The other *tasks* will come here and observe an already shut down queue and hence simply wrap up their business.
        let mut guard = self.upload_queue.lock().unwrap();
        match &mut *guard {
            UploadQueue::Uninitialized => Err(StopError::QueueUninitialized),
            UploadQueue::Stopped(_) => {
                // nothing to do
                info!("another concurrent task already shut down the queue");
                Ok(())
            }
            UploadQueue::Initialized(initialized) => {
                info!("shutting down upload queue");

                // Replace the queue with the Stopped state, taking ownership of the old
                // Initialized queue. We will do some checks on it, and then drop it.
                let qi = {
                    // Here we preserve working version of the upload queue for possible use during deletions.
                    // In-place replace of Initialized to Stopped can be done with the help of https://github.com/Sgeo/take_mut
                    // but for this use case it doesnt really makes sense to bring unsafe code only for this usage point.
                    // Deletion is not really perf sensitive so there shouldnt be any problems with cloning a fraction of it.
                    let upload_queue_for_deletion = UploadQueueInitialized {
                        task_counter: 0,
                        latest_files: initialized.latest_files.clone(),
                        latest_files_changes_since_metadata_upload_scheduled: 0,
                        latest_metadata: initialized.latest_metadata.clone(),
                        projected_remote_consistent_lsn: None,
                        visible_remote_consistent_lsn: initialized
                            .visible_remote_consistent_lsn
                            .clone(),
                        num_inprogress_layer_uploads: 0,
                        num_inprogress_layer_copies: 0,
                        num_inprogress_metadata_uploads: 0,
                        num_inprogress_deletions: 0,
                        inprogress_tasks: HashMap::default(),
                        queued_operations: VecDeque::default(),
                        #[cfg(feature = "testing")]
                        dangling_files: HashMap::default(),
                        shutting_down: false,
                        shutdown_ready: Arc::new(tokio::sync::Semaphore::new(0)),
                    };

                    let upload_queue = std::mem::replace(
                        &mut *guard,
                        UploadQueue::Stopped(UploadQueueStopped {
                            upload_queue_for_deletion,
                            deleted_at: SetDeletedFlagProgress::NotRunning,
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
                    self.metric_end(&op);
                    // Dropping UploadOp::Barrier() here will make wait_completion() return with an Err()
                    // which is exactly what we want to happen.
                    drop(op);
                }

                // We're done.
                drop(guard);
                Ok(())
            }
        }
    }
}

pub fn remote_timelines_path(tenant_shard_id: &TenantShardId) -> RemotePath {
    let path = format!("tenants/{tenant_shard_id}/{TIMELINES_SEGMENT_NAME}");
    RemotePath::from_string(&path).expect("Failed to construct path")
}

fn remote_timelines_path_unsharded(tenant_id: &TenantId) -> RemotePath {
    let path = format!("tenants/{tenant_id}/{TIMELINES_SEGMENT_NAME}");
    RemotePath::from_string(&path).expect("Failed to construct path")
}

pub fn remote_timeline_path(
    tenant_shard_id: &TenantShardId,
    timeline_id: &TimelineId,
) -> RemotePath {
    remote_timelines_path(tenant_shard_id).join(Utf8Path::new(&timeline_id.to_string()))
}

/// Note that the shard component of a remote layer path is _not_ always the same
/// as in the TenantShardId of the caller: tenants may reference layers from a different
/// ShardIndex.  Use the ShardIndex from the layer's metadata.
pub fn remote_layer_path(
    tenant_id: &TenantId,
    timeline_id: &TimelineId,
    shard: ShardIndex,
    layer_file_name: &LayerFileName,
    generation: Generation,
) -> RemotePath {
    // Generation-aware key format
    let path = format!(
        "tenants/{tenant_id}{0}/{TIMELINES_SEGMENT_NAME}/{timeline_id}/{1}{2}",
        shard.get_suffix(),
        layer_file_name.file_name(),
        generation.get_suffix()
    );

    RemotePath::from_string(&path).expect("Failed to construct path")
}

pub fn remote_initdb_archive_path(tenant_id: &TenantId, timeline_id: &TimelineId) -> RemotePath {
    RemotePath::from_string(&format!(
        "tenants/{tenant_id}/{TIMELINES_SEGMENT_NAME}/{timeline_id}/{INITDB_PATH}"
    ))
    .expect("Failed to construct path")
}

pub fn remote_initdb_preserved_archive_path(
    tenant_id: &TenantId,
    timeline_id: &TimelineId,
) -> RemotePath {
    RemotePath::from_string(&format!(
        "tenants/{tenant_id}/{TIMELINES_SEGMENT_NAME}/{timeline_id}/{INITDB_PRESERVED_PATH}"
    ))
    .expect("Failed to construct path")
}

pub fn remote_index_path(
    tenant_shard_id: &TenantShardId,
    timeline_id: &TimelineId,
    generation: Generation,
) -> RemotePath {
    RemotePath::from_string(&format!(
        "tenants/{tenant_shard_id}/{TIMELINES_SEGMENT_NAME}/{timeline_id}/{0}{1}",
        IndexPart::FILE_NAME,
        generation.get_suffix()
    ))
    .expect("Failed to construct path")
}

pub(crate) fn remote_heatmap_path(tenant_shard_id: &TenantShardId) -> RemotePath {
    RemotePath::from_string(&format!(
        "tenants/{tenant_shard_id}/{TENANT_HEATMAP_BASENAME}"
    ))
    .expect("Failed to construct path")
}

/// Given the key of an index, parse out the generation part of the name
pub fn parse_remote_index_path(path: RemotePath) -> Option<Generation> {
    let file_name = match path.get_path().file_name() {
        Some(f) => f,
        None => {
            // Unexpected: we should be seeing index_part.json paths only
            tracing::warn!("Malformed index key {}", path);
            return None;
        }
    };

    match file_name.split_once('-') {
        Some((_, gen_suffix)) => Generation::parse_suffix(gen_suffix),
        None => None,
    }
}

/// Files on the remote storage are stored with paths, relative to the workdir.
/// That path includes in itself both tenant and timeline ids, allowing to have a unique remote storage path.
///
/// Errors if the path provided does not start from pageserver's workdir.
pub fn remote_path(
    conf: &PageServerConf,
    local_path: &Utf8Path,
    generation: Generation,
) -> anyhow::Result<RemotePath> {
    let stripped = local_path
        .strip_prefix(&conf.workdir)
        .context("Failed to strip workdir prefix")?;

    let suffixed = format!("{0}{1}", stripped, generation.get_suffix());

    RemotePath::new(Utf8Path::new(&suffixed)).with_context(|| {
        format!(
            "to resolve remote part of path {:?} for base {:?}",
            local_path, conf.workdir
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        context::RequestContext,
        tenant::{
            harness::{TenantHarness, TIMELINE_ID},
            Tenant, Timeline,
        },
        DEFAULT_PG_VERSION,
    };

    use std::collections::HashSet;

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

    fn assert_remote_files(expected: &[&str], remote_path: &Utf8Path, generation: Generation) {
        let mut expected: Vec<String> = expected
            .iter()
            .map(|x| format!("{}{}", x, generation.get_suffix()))
            .collect();
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

    struct TestSetup {
        harness: TenantHarness,
        tenant: Arc<Tenant>,
        timeline: Arc<Timeline>,
        tenant_ctx: RequestContext,
    }

    impl TestSetup {
        async fn new(test_name: &str) -> anyhow::Result<Self> {
            let test_name = Box::leak(Box::new(format!("remote_timeline_client__{test_name}")));
            let harness = TenantHarness::create(test_name)?;
            let (tenant, ctx) = harness.load().await;

            let timeline = tenant
                .create_test_timeline(TIMELINE_ID, Lsn(8), DEFAULT_PG_VERSION, &ctx)
                .await?;

            Ok(Self {
                harness,
                tenant,
                timeline,
                tenant_ctx: ctx,
            })
        }

        /// Construct a RemoteTimelineClient in an arbitrary generation
        fn build_client(&self, generation: Generation) -> Arc<RemoteTimelineClient> {
            Arc::new(RemoteTimelineClient {
                conf: self.harness.conf,
                tenant_shard_id: self.harness.tenant_shard_id,
                timeline_id: TIMELINE_ID,
                generation,
                storage_impl: self.harness.remote_storage.clone(),
                deletion_queue_client: self.harness.deletion_queue.new_client(),
                upload_queue: Mutex::new(UploadQueue::Uninitialized),
                metrics: Arc::new(RemoteTimelineClientMetrics::new(
                    &self.harness.tenant_shard_id,
                    &TIMELINE_ID,
                )),
                cancel: CancellationToken::new(),
            })
        }

        /// A tracing::Span that satisfies remote_timeline_client methods that assert tenant_id
        /// and timeline_id are present.
        fn span(&self) -> tracing::Span {
            tracing::info_span!(
                "test",
                tenant_id = %self.harness.tenant_shard_id.tenant_id,
                shard_id = %self.harness.tenant_shard_id.shard_slug(),
                timeline_id = %TIMELINE_ID
            )
        }
    }

    // Test scheduling
    #[tokio::test]
    async fn upload_scheduling() {
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

        let test_setup = TestSetup::new("upload_scheduling").await.unwrap();
        let span = test_setup.span();
        let _guard = span.enter();

        let TestSetup {
            harness,
            tenant: _tenant,
            timeline,
            tenant_ctx: _tenant_ctx,
        } = test_setup;

        let client = timeline.remote_client.as_ref().unwrap();

        // Download back the index.json, and check that the list of files is correct
        let initial_index_part = match client
            .download_index_file(&CancellationToken::new())
            .await
            .unwrap()
        {
            MaybeDeletedIndexPart::IndexPart(index_part) => index_part,
            MaybeDeletedIndexPart::Deleted(_) => panic!("unexpectedly got deleted index part"),
        };
        let initial_layers = initial_index_part
            .layer_metadata
            .keys()
            .map(|f| f.to_owned())
            .collect::<HashSet<LayerFileName>>();
        let initial_layer = {
            assert!(initial_layers.len() == 1);
            initial_layers.into_iter().next().unwrap()
        };

        let timeline_path = harness.timeline_path(&TIMELINE_ID);

        println!("workdir: {}", harness.conf.workdir);

        let remote_timeline_dir = harness
            .remote_fs_dir
            .join(timeline_path.strip_prefix(&harness.conf.workdir).unwrap());
        println!("remote_timeline_dir: {remote_timeline_dir}");

        let generation = harness.generation;
        let shard = harness.shard;

        // Create a couple of dummy files,  schedule upload for them

        let layers = [
            ("000000000000000000000000000000000000-FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF__00000000016B59D8-00000000016B5A51".parse().unwrap(), dummy_contents("foo")),
            ("000000000000000000000000000000000000-FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF__00000000016B59D9-00000000016B5A52".parse().unwrap(), dummy_contents("bar")),
            ("000000000000000000000000000000000000-FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF__00000000016B59DA-00000000016B5A53".parse().unwrap(), dummy_contents("baz"))
        ]
        .into_iter()
        .map(|(name, contents): (LayerFileName, Vec<u8>)| {
            std::fs::write(timeline_path.join(name.file_name()), &contents).unwrap();

            Layer::for_resident(
                harness.conf,
                &timeline,
                name,
                LayerFileMetadata::new(contents.len() as u64, generation, shard),
            )
        }).collect::<Vec<_>>();

        client
            .schedule_layer_file_upload(layers[0].clone())
            .unwrap();
        client
            .schedule_layer_file_upload(layers[1].clone())
            .unwrap();

        // Check that they are started immediately, not queued
        //
        // this works because we running within block_on, so any futures are now queued up until
        // our next await point.
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
        client
            .schedule_index_upload_for_metadata_update(&metadata)
            .unwrap();
        {
            let mut guard = client.upload_queue.lock().unwrap();
            let upload_queue = guard.initialized_mut().unwrap();
            assert!(upload_queue.queued_operations.len() == 1);
            assert!(upload_queue.latest_files_changes_since_metadata_upload_scheduled == 0);
        }

        // Wait for the uploads to finish
        client.wait_completion().await.unwrap();
        {
            let mut guard = client.upload_queue.lock().unwrap();
            let upload_queue = guard.initialized_mut().unwrap();

            assert!(upload_queue.queued_operations.is_empty());
            assert!(upload_queue.inprogress_tasks.is_empty());
        }

        // Download back the index.json, and check that the list of files is correct
        let index_part = match client
            .download_index_file(&CancellationToken::new())
            .await
            .unwrap()
        {
            MaybeDeletedIndexPart::IndexPart(index_part) => index_part,
            MaybeDeletedIndexPart::Deleted(_) => panic!("unexpectedly got deleted index part"),
        };

        assert_file_list(
            &index_part
                .layer_metadata
                .keys()
                .map(|f| f.to_owned())
                .collect(),
            &[
                &initial_layer.file_name(),
                &layers[0].layer_desc().filename().file_name(),
                &layers[1].layer_desc().filename().file_name(),
            ],
        );
        assert_eq!(index_part.metadata, metadata);

        // Schedule upload and then a deletion. Check that the deletion is queued
        client
            .schedule_layer_file_upload(layers[2].clone())
            .unwrap();

        // this is no longer consistent with how deletion works with Layer::drop, but in this test
        // keep using schedule_layer_file_deletion because we don't have a way to wait for the
        // spawn_blocking started by the drop.
        client
            .schedule_layer_file_deletion(&[layers[0].layer_desc().filename()])
            .unwrap();
        {
            let mut guard = client.upload_queue.lock().unwrap();
            let upload_queue = guard.initialized_mut().unwrap();

            // Deletion schedules upload of the index file, and the file deletion itself
            assert_eq!(upload_queue.queued_operations.len(), 2);
            assert_eq!(upload_queue.inprogress_tasks.len(), 1);
            assert_eq!(upload_queue.num_inprogress_layer_uploads, 1);
            assert_eq!(upload_queue.num_inprogress_deletions, 0);
            assert_eq!(
                upload_queue.latest_files_changes_since_metadata_upload_scheduled,
                0
            );
        }
        assert_remote_files(
            &[
                &initial_layer.file_name(),
                &layers[0].layer_desc().filename().file_name(),
                &layers[1].layer_desc().filename().file_name(),
                "index_part.json",
            ],
            &remote_timeline_dir,
            generation,
        );

        // Finish them
        client.wait_completion().await.unwrap();
        harness.deletion_queue.pump().await;

        assert_remote_files(
            &[
                &initial_layer.file_name(),
                &layers[1].layer_desc().filename().file_name(),
                &layers[2].layer_desc().filename().file_name(),
                "index_part.json",
            ],
            &remote_timeline_dir,
            generation,
        );
    }

    #[tokio::test]
    async fn bytes_unfinished_gauge_for_layer_file_uploads() {
        // Setup

        let TestSetup {
            harness,
            tenant: _tenant,
            timeline,
            ..
        } = TestSetup::new("metrics").await.unwrap();
        let client = timeline.remote_client.as_ref().unwrap();
        let timeline_path = harness.timeline_path(&TIMELINE_ID);

        let layer_file_name_1: LayerFileName = "000000000000000000000000000000000000-FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF__00000000016B59D8-00000000016B5A51".parse().unwrap();
        let content_1 = dummy_contents("foo");
        std::fs::write(
            timeline_path.join(layer_file_name_1.file_name()),
            &content_1,
        )
        .unwrap();

        let layer_file_1 = Layer::for_resident(
            harness.conf,
            &timeline,
            layer_file_name_1.clone(),
            LayerFileMetadata::new(content_1.len() as u64, harness.generation, harness.shard),
        );

        #[derive(Debug, PartialEq, Clone, Copy)]
        struct BytesStartedFinished {
            started: Option<usize>,
            finished: Option<usize>,
        }
        impl std::ops::Add for BytesStartedFinished {
            type Output = Self;
            fn add(self, rhs: Self) -> Self::Output {
                Self {
                    started: self.started.map(|v| v + rhs.started.unwrap_or(0)),
                    finished: self.finished.map(|v| v + rhs.finished.unwrap_or(0)),
                }
            }
        }
        let get_bytes_started_stopped = || {
            let started = client
                .metrics
                .get_bytes_started_counter_value(&RemoteOpFileKind::Layer, &RemoteOpKind::Upload)
                .map(|v| v.try_into().unwrap());
            let stopped = client
                .metrics
                .get_bytes_finished_counter_value(&RemoteOpFileKind::Layer, &RemoteOpKind::Upload)
                .map(|v| v.try_into().unwrap());
            BytesStartedFinished {
                started,
                finished: stopped,
            }
        };

        // Test
        tracing::info!("now doing actual test");

        let actual_a = get_bytes_started_stopped();

        client
            .schedule_layer_file_upload(layer_file_1.clone())
            .unwrap();

        let actual_b = get_bytes_started_stopped();

        client.wait_completion().await.unwrap();

        let actual_c = get_bytes_started_stopped();

        // Validate

        let expected_b = actual_a
            + BytesStartedFinished {
                started: Some(content_1.len()),
                // assert that the _finished metric is created eagerly so that subtractions work on first sample
                finished: Some(0),
            };
        assert_eq!(actual_b, expected_b);

        let expected_c = actual_a
            + BytesStartedFinished {
                started: Some(content_1.len()),
                finished: Some(content_1.len()),
            };
        assert_eq!(actual_c, expected_c);
    }

    async fn inject_index_part(test_state: &TestSetup, generation: Generation) -> IndexPart {
        // An empty IndexPart, just sufficient to ensure deserialization will succeed
        let example_metadata = TimelineMetadata::example();
        let example_index_part = IndexPart::new(
            HashMap::new(),
            example_metadata.disk_consistent_lsn(),
            example_metadata,
        );

        let index_part_bytes = serde_json::to_vec(&example_index_part).unwrap();

        let index_path = test_state.harness.remote_fs_dir.join(
            remote_index_path(
                &test_state.harness.tenant_shard_id,
                &TIMELINE_ID,
                generation,
            )
            .get_path(),
        );

        std::fs::create_dir_all(index_path.parent().unwrap())
            .expect("creating test dir should work");

        eprintln!("Writing {index_path}");
        std::fs::write(&index_path, index_part_bytes).unwrap();
        example_index_part
    }

    /// Assert that when a RemoteTimelineclient in generation `get_generation` fetches its
    /// index, the IndexPart returned is equal to `expected`
    async fn assert_got_index_part(
        test_state: &TestSetup,
        get_generation: Generation,
        expected: &IndexPart,
    ) {
        let client = test_state.build_client(get_generation);

        let download_r = client
            .download_index_file(&CancellationToken::new())
            .await
            .expect("download should always succeed");
        assert!(matches!(download_r, MaybeDeletedIndexPart::IndexPart(_)));
        match download_r {
            MaybeDeletedIndexPart::IndexPart(index_part) => {
                assert_eq!(&index_part, expected);
            }
            MaybeDeletedIndexPart::Deleted(_index_part) => panic!("Test doesn't set deleted_at"),
        }
    }

    #[tokio::test]
    async fn index_part_download_simple() -> anyhow::Result<()> {
        let test_state = TestSetup::new("index_part_download_simple").await.unwrap();
        let span = test_state.span();
        let _guard = span.enter();

        // Simple case: we are in generation N, load the index from generation N - 1
        let generation_n = 5;
        let injected = inject_index_part(&test_state, Generation::new(generation_n - 1)).await;

        assert_got_index_part(&test_state, Generation::new(generation_n), &injected).await;

        Ok(())
    }

    #[tokio::test]
    async fn index_part_download_ordering() -> anyhow::Result<()> {
        let test_state = TestSetup::new("index_part_download_ordering")
            .await
            .unwrap();

        let span = test_state.span();
        let _guard = span.enter();

        // A generation-less IndexPart exists in the bucket, we should find it
        let generation_n = 5;
        let injected_none = inject_index_part(&test_state, Generation::none()).await;
        assert_got_index_part(&test_state, Generation::new(generation_n), &injected_none).await;

        // If a more recent-than-none generation exists, we should prefer to load that
        let injected_1 = inject_index_part(&test_state, Generation::new(1)).await;
        assert_got_index_part(&test_state, Generation::new(generation_n), &injected_1).await;

        // If a more-recent-than-me generation exists, we should ignore it.
        let _injected_10 = inject_index_part(&test_state, Generation::new(10)).await;
        assert_got_index_part(&test_state, Generation::new(generation_n), &injected_1).await;

        // If a directly previous generation exists, _and_ an index exists in my own
        // generation, I should prefer my own generation.
        let _injected_prev =
            inject_index_part(&test_state, Generation::new(generation_n - 1)).await;
        let injected_current = inject_index_part(&test_state, Generation::new(generation_n)).await;
        assert_got_index_part(
            &test_state,
            Generation::new(generation_n),
            &injected_current,
        )
        .await;

        Ok(())
    }
}

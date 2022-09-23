//! There are a few components the storage machinery consists of:
//!
//! * [`RemoteStorage`] that is used to interact with an arbitrary external storage
//!
//! * synchronization logic at [`storage_sync`] module that keeps pageserver state (both runtime one and the workdir files) and storage state in sync.
//! Synchronization internals are split into submodules
//!     * [`storage_sync::index`] to keep track of remote tenant files, the metadata and their mappings to local files
//!     * [`storage_sync::upload`] and [`storage_sync::download`] to manage archive creation and upload; download and extraction, respectively
//!
//! * public API via to interact with the external world:
//!     * [`start_local_timeline_sync`] to launch a background async loop to handle the synchronization
//!     * [`schedule_layer_upload`], [`schedule_layer_download`], and[`schedule_layer_delete`] to enqueue a new task
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
//! First, during startup, the pageserver inits the storage sync task with the async loop, or leaves the loop uninitialised, if configured so.
//! The loop inits the storage connection and checks the remote files stored.
//! This is done once at startup only, relying on the fact that pageserver uses the storage alone (ergo, nobody else uploads the files to the storage but this server).
//! Based on the remote storage data, the sync logic immediately schedules sync tasks for local timelines and reports about remote only timelines to pageserver, so it can
//! query their downloads later if they are accessed.
//!
//! Some time later, during pageserver checkpoints, in-memory data is flushed onto disk along with its metadata.
//! If the storage sync loop was successfully started before, pageserver schedules the layer files and the updated metadata file for upload, every time a layer is flushed to disk.
//! The uploads are disabled, if no remote storage configuration is provided (no sync loop is started this way either).
//! See [`crate::tenant`] for the upload calls and the adjacent logic.
//!
//! Synchronization logic is able to communicate back with updated timeline sync states, submitted via [`crate::tenant_mgr::attach_local_tenants`] function.
//! Tenant manager applies corresponding timeline updates in pageserver's in-memory state.
//! Such submissions happen in two cases:
//! * once after the sync loop startup, to signal pageserver which timelines will be synchronized in the near future
//! * after every loop step, in case a timeline needs to be reloaded or evicted from pageserver's memory
//!
//! When the pageserver terminates, the sync loop finishes current sync task (if any) and exits.
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
//! Yet instead of keeping the `metadata` file remotely, we wrap it with more data in [`IndexPart`], containing the list of remote files.
//! This file gets read to populate the cache, if the remote timeline data is missing from it and gets updated after every successful download.
//! This way, we optimize S3 storage access by not running the `S3 list` command that could be expencive and slow: knowing both [`TenantId`] and [`TimelineId`],
//! we can always reconstruct the path to the timeline, use this to get the same path on the remote storage and retrieve its shard contents, if needed, same as any layer files.
//!
//! By default, pageserver reads the remote storage index data only for timelines located locally, to synchronize those, if needed.
//! Bulk index data download happens only initially, on pageserver startup. The rest of the remote storage stays unknown to pageserver and loaded on demand only,
//! when a new timeline is scheduled for the download.
//!
//! NOTES:
//! * pageserver assumes it has exclusive write access to the remote storage. If supported, the way multiple pageservers can be separated in the same storage
//! (i.e. using different directories in the local filesystem external storage), but totally up to the storage implementation and not covered with the trait API.
//!
//! * the sync tasks may not processed immediately after the submission: if they error and get re-enqueued, their execution might be backed off to ensure error cap is not exceeded too fast.
//! The sync queue processing also happens in batches, so the sync tasks can wait in the queue for some time.
//!
//! A synchronization logic for the [`RemoteStorage`] and pageserver in-memory state to ensure correct synchronizations
//! between local tenant files and their counterparts from the remote storage.
//!
//! The synchronization does not aim to be immediate, yet eventually consistent.
//! Synchronization is done with the queue being emptied via separate thread asynchronously,
//! attempting to fully store pageserver's local data on the remote storage in a custom format, beneficial for storing.
//!
//! A queue is implemented in the [`sync_queue`] module as a VecDeque to hold the tasks, and a condition variable for blocking when the queue is empty.
//!
//! The queue gets emptied by a single thread with the loop, that polls the tasks in batches of deduplicated tasks.
//! A task from the batch corresponds to a single timeline, with its files to sync merged together: given that only one task sync loop step is active at a time,
//! timeline uploads and downloads can happen concurrently, in no particular order due to incremental nature of the timeline layers.
//! Deletion happens only after a successful upload only, otherwise the compaction output might make the timeline inconsistent until both tasks are fully processed without errors.
//! Upload and download update the remote data (inmemory index and S3 json index part file) only after every layer is successfully synchronized, while the deletion task
//! does otherwise: it requires to have the remote data updated first successfully: blob files will be invisible to pageserver this way.
//!
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
//! Index construction is currently the only place where the storage sync can return an [`Err`] to the user.
//! New sync tasks are accepted via [`schedule_layer_upload`], [`schedule_layer_download`] and [`schedule_layer_delete`] functions,
//! disregarding of the corresponding loop startup.
//! It's up to the caller to avoid synchronizations if the loop is disabled: otherwise, the sync tasks will be ignored.
//! After the initial state is loaded into memory and the loop starts, any further [`Err`] results do not stop the loop, but rather
//! reschedule the same task, with possibly less files to sync:
//! * download tasks currently never replace existing local file with metadata file as an exception
//! (but this is a subject to change when checksum checks are implemented: all files could get overwritten on a checksum mismatch)
//! * download tasks carry the information of skipped acrhives, so resubmissions are not downloading successfully processed layers again
//! * downloads do not contain any actual files to download, so that "external", sync pageserver code is able to schedule the timeline download
//! without accessing any extra information about its files.
//!
//! Uploads and downloads sync layer files in arbitrary order, but only after all layer files are synched the local metadada (for download) and remote index part (for upload) are updated,
//! to avoid having a corrupt state without the relevant layer files.
//! Refer to [`upload`] and [`download`] for more details.
//!
//! Synchronization never removes any local files from pageserver workdir or remote files from the remote storage, yet there could be overwrites of the same files (index part and metadata file updates, future checksum mismatch fixes).
//! NOTE: No real contents or checksum check happens right now and is a subject to improve later.
//!
//! After the whole timeline is downloaded, [`crate::tenant_mgr::apply_timeline_sync_status_updates`] function is used to update pageserver memory stage for the timeline processed.

mod delete;
mod download;
pub mod index;
mod upload;

use std::{
    collections::{hash_map, HashMap, HashSet, VecDeque},
    fmt::Debug,
    num::{NonZeroU32, NonZeroUsize},
    ops::ControlFlow,
    path::{Path, PathBuf},
    sync::{Condvar, Mutex},
};

use anyhow::{anyhow, bail, Context};
use futures::stream::{FuturesUnordered, StreamExt};
use once_cell::sync::OnceCell;
use remote_storage::GenericRemoteStorage;
use tokio::{
    fs,
    time::{Duration, Instant},
};
use tracing::*;

use self::{
    delete::delete_timeline_layers,
    download::{download_timeline_layers, DownloadedTimeline},
    index::{IndexPart, RemoteTimeline, RemoteTimelineIndex},
    upload::{upload_index_part, upload_timeline_layers, UploadedTimeline},
};
use crate::{
    config::PageServerConf, exponential_backoff, storage_sync::index::RemoteIndex, task_mgr,
    task_mgr::TaskKind, task_mgr::BACKGROUND_RUNTIME, tenant::metadata::TimelineMetadata,
    tenant_mgr::attach_local_tenants,
};
use crate::{
    metrics::{IMAGE_SYNC_TIME, REMAINING_SYNC_ITEMS, REMOTE_INDEX_UPLOAD},
    TenantTimelineValues,
};

use crate::metrics::{IMAGE_SYNC_COUNT, IMAGE_SYNC_TIME_HISTOGRAM};
use utils::id::{TenantId, TenantTimelineId, TimelineId};

use self::download::download_index_parts;
pub use self::download::gather_tenant_timelines_index_parts;

static SYNC_QUEUE: OnceCell<SyncQueue> = OnceCell::new();

/// A timeline status to share with pageserver's sync counterpart,
/// after comparing local and remote timeline state.
#[derive(Clone)]
pub enum LocalTimelineInitStatus {
    /// The timeline has every remote layer present locally.
    /// There could be some layers requiring uploading,
    /// but this does not block the timeline from any user interaction.
    LocallyComplete(TimelineMetadata),
    /// A timeline has some files remotely, that are not present locally and need downloading.
    /// Downloading might update timeline's metadata locally and current pageserver logic deals with local layers only,
    /// so the data needs to be downloaded first before the timeline can be used.
    NeedsSync,
}

impl std::fmt::Debug for LocalTimelineInitStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::LocallyComplete(_) => write!(f, "LocallyComplete"),
            Self::NeedsSync => write!(f, "NeedsSync"),
        }
    }
}

/// A structure to combine all synchronization data to share with pageserver after a successful sync loop initialization.
/// Successful initialization includes a case when sync loop is not started, in which case the startup data is returned still,
/// to simplify the received code.
pub struct SyncStartupData {
    pub remote_index: RemoteIndex,
    pub local_timeline_init_statuses: TenantTimelineValues<LocalTimelineInitStatus>,
}

/// Global queue of sync tasks.
///
/// 'queue' is protected by a mutex, and 'condvar' is used to wait for tasks to arrive.
struct SyncQueue {
    max_timelines_per_batch: NonZeroUsize,

    queue: Mutex<VecDeque<(TenantTimelineId, SyncTask)>>,
    condvar: Condvar,
}

impl SyncQueue {
    fn new(max_timelines_per_batch: NonZeroUsize) -> Self {
        Self {
            max_timelines_per_batch,
            queue: Mutex::new(VecDeque::new()),
            condvar: Condvar::new(),
        }
    }

    /// Queue a new task
    fn push(&self, sync_id: TenantTimelineId, new_task: SyncTask) {
        let mut q = self.queue.lock().unwrap();

        q.push_back((sync_id, new_task));
        if q.len() <= 1 {
            self.condvar.notify_one();
        }
    }

    /// Fetches a task batch, getting every existing entry from the queue, grouping by timelines and merging the tasks for every timeline.
    /// A timeline has to care to not to delete certain layers from the remote storage before the corresponding uploads happen.
    /// Other than that, due to "immutable" nature of the layers, the order of their deletion/uploading/downloading does not matter.
    /// Hence, we merge the layers together into single task per timeline and run those concurrently (with the deletion happening only after successful uploading).
    fn next_task_batch(&self) -> (HashMap<TenantTimelineId, SyncTaskBatch>, usize) {
        // Wait for the first task in blocking fashion
        let mut q = self.queue.lock().unwrap();
        while q.is_empty() {
            q = self
                .condvar
                .wait_timeout(q, Duration::from_millis(1000))
                .unwrap()
                .0;

            if task_mgr::is_shutdown_requested() {
                return (HashMap::new(), q.len());
            }
        }
        let (first_sync_id, first_task) = q.pop_front().unwrap();

        let mut timelines_left_to_batch = self.max_timelines_per_batch.get() - 1;
        let tasks_to_process = q.len();

        let mut batches = HashMap::with_capacity(tasks_to_process);
        batches.insert(first_sync_id, SyncTaskBatch::new(first_task));

        let mut tasks_to_reenqueue = Vec::with_capacity(tasks_to_process);

        // Greedily grab as many other tasks that we can.
        // Yet do not put all timelines in the batch, but only the first ones that fit the timeline limit.
        // Re-enqueue the tasks that don't fit in this batch.
        while let Some((sync_id, new_task)) = q.pop_front() {
            match batches.entry(sync_id) {
                hash_map::Entry::Occupied(mut v) => v.get_mut().add(new_task),
                hash_map::Entry::Vacant(v) => {
                    timelines_left_to_batch = timelines_left_to_batch.saturating_sub(1);
                    if timelines_left_to_batch == 0 {
                        tasks_to_reenqueue.push((sync_id, new_task));
                    } else {
                        v.insert(SyncTaskBatch::new(new_task));
                    }
                }
            }
        }

        debug!(
            "Batched {} timelines, reenqueuing {}",
            batches.len(),
            tasks_to_reenqueue.len()
        );
        for (id, task) in tasks_to_reenqueue {
            q.push_back((id, task));
        }

        (batches, q.len())
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.queue.lock().unwrap().len()
    }
}

/// A task to run in the async download/upload loop.
/// Limited by the number of retries, after certain threshold the failing task gets evicted and the timeline disabled.
#[derive(Debug, Clone)]
enum SyncTask {
    /// A checkpoint outcome with possible local file updates that need actualization in the remote storage.
    /// Not necessary more fresh than the one already uploaded.
    Download(SyncData<LayersDownload>),
    /// A certain amount of image files to download.
    Upload(SyncData<LayersUpload>),
    /// Delete remote files.
    Delete(SyncData<LayersDeletion>),
}

/// Stores the data to synd and its retries, to evict the tasks failing to frequently.
#[derive(Debug, Clone, PartialEq, Eq)]
struct SyncData<T> {
    retries: u32,
    data: T,
}

impl<T> SyncData<T> {
    fn new(retries: u32, data: T) -> Self {
        Self { retries, data }
    }
}

impl SyncTask {
    fn download(download_task: LayersDownload) -> Self {
        Self::Download(SyncData::new(0, download_task))
    }

    fn upload(upload_task: LayersUpload) -> Self {
        Self::Upload(SyncData::new(0, upload_task))
    }

    fn delete(delete_task: LayersDeletion) -> Self {
        Self::Delete(SyncData::new(0, delete_task))
    }
}

#[derive(Debug, Default, PartialEq, Eq)]
struct SyncTaskBatch {
    upload: Option<SyncData<LayersUpload>>,
    download: Option<SyncData<LayersDownload>>,
    delete: Option<SyncData<LayersDeletion>>,
}

impl SyncTaskBatch {
    fn new(task: SyncTask) -> Self {
        let mut new_self = Self::default();
        new_self.add(task);
        new_self
    }

    fn add(&mut self, task: SyncTask) {
        match task {
            SyncTask::Download(new_download) => match &mut self.download {
                Some(batch_download) => {
                    batch_download.retries = batch_download.retries.min(new_download.retries);
                    batch_download
                        .data
                        .layers_to_skip
                        .extend(new_download.data.layers_to_skip.into_iter());
                }
                None => self.download = Some(new_download),
            },
            SyncTask::Upload(new_upload) => match &mut self.upload {
                Some(batch_upload) => {
                    batch_upload.retries = batch_upload.retries.min(new_upload.retries);

                    let batch_data = &mut batch_upload.data;
                    let new_data = new_upload.data;
                    batch_data
                        .layers_to_upload
                        .extend(new_data.layers_to_upload.into_iter());
                    batch_data
                        .uploaded_layers
                        .extend(new_data.uploaded_layers.into_iter());
                    if batch_data
                        .metadata
                        .as_ref()
                        .map(|meta| meta.disk_consistent_lsn())
                        <= new_data
                            .metadata
                            .as_ref()
                            .map(|meta| meta.disk_consistent_lsn())
                    {
                        batch_data.metadata = new_data.metadata;
                    }
                }
                None => self.upload = Some(new_upload),
            },
            SyncTask::Delete(new_delete) => match &mut self.delete {
                Some(batch_delete) => {
                    batch_delete.retries = batch_delete.retries.min(new_delete.retries);
                    // Need to reregister deletions, but it's ok to register already deleted files once again, they will be skipped.
                    batch_delete.data.deletion_registered = batch_delete
                        .data
                        .deletion_registered
                        .min(new_delete.data.deletion_registered);

                    // Do not download and upload the layers getting removed in the same batch
                    if let Some(batch_download) = &mut self.download {
                        batch_download
                            .data
                            .layers_to_skip
                            .extend(new_delete.data.layers_to_delete.iter().cloned());
                        batch_download
                            .data
                            .layers_to_skip
                            .extend(new_delete.data.deleted_layers.iter().cloned());
                    }
                    if let Some(batch_upload) = &mut self.upload {
                        let not_deleted = |layer: &PathBuf| {
                            !new_delete.data.layers_to_delete.contains(layer)
                                && !new_delete.data.deleted_layers.contains(layer)
                        };
                        batch_upload.data.layers_to_upload.retain(not_deleted);
                        batch_upload.data.uploaded_layers.retain(not_deleted);
                    }

                    batch_delete
                        .data
                        .layers_to_delete
                        .extend(new_delete.data.layers_to_delete.into_iter());
                    batch_delete
                        .data
                        .deleted_layers
                        .extend(new_delete.data.deleted_layers.into_iter());
                }
                None => self.delete = Some(new_delete),
            },
        }
    }
}

/// Local timeline files for upload, appeared after the new checkpoint.
/// Current checkpoint design assumes new files are added only, no deletions or amendment happens.
#[derive(Debug, Clone, PartialEq, Eq)]
struct LayersUpload {
    /// Layer file path in the pageserver workdir, that were added for the corresponding checkpoint.
    layers_to_upload: HashSet<PathBuf>,
    /// Already uploaded layers. Used to store the data about the uploads between task retries
    /// and to record the data into the remote index after the task got completed or evicted.
    uploaded_layers: HashSet<PathBuf>,
    metadata: Option<TimelineMetadata>,
}

/// A timeline download task.
/// Does not contain the file list to download, to allow other
/// parts of the pageserer code to schedule the task
/// without using the remote index or any other ways to list the remote timleine files.
/// Skips the files that are already downloaded.
#[derive(Debug, Clone, PartialEq, Eq)]
struct LayersDownload {
    layers_to_skip: HashSet<PathBuf>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct LayersDeletion {
    layers_to_delete: HashSet<PathBuf>,
    deleted_layers: HashSet<PathBuf>,
    /// Pageserver uses [`IndexPart`] as a source of truth for listing the files per timeline.
    /// This object gets serialized and placed into the remote storage.
    /// So if we manage to update pageserver's [`RemoteIndex`] and update the index part on the remote storage,
    /// the corresponding files on S3 won't exist for pageserver albeit being physically present on that remote storage still.
    /// Then all that's left is to remove the files from the remote storage, without concerns about consistency.
    deletion_registered: bool,
}

/// Adds the new checkpoint files as an upload sync task to the queue.
/// On task failure, it gets retried again from the start a number of times.
///
/// Ensure that the loop is started otherwise the task is never processed.
pub fn schedule_layer_upload(
    tenant_id: TenantId,
    timeline_id: TimelineId,
    layers_to_upload: HashSet<PathBuf>,
    metadata: Option<TimelineMetadata>,
) {
    let sync_queue = match SYNC_QUEUE.get() {
        Some(queue) => queue,
        None => {
            warn!("Could not send an upload task for tenant {tenant_id}, timeline {timeline_id}");
            return;
        }
    };
    sync_queue.push(
        TenantTimelineId {
            tenant_id,
            timeline_id,
        },
        SyncTask::upload(LayersUpload {
            layers_to_upload,
            uploaded_layers: HashSet::new(),
            metadata,
        }),
    );
    debug!("Upload task for tenant {tenant_id}, timeline {timeline_id} sent")
}

/// Adds the new files to delete as a deletion task to the queue.
/// On task failure, it gets retried again from the start a number of times.
///
/// Ensure that the loop is started otherwise the task is never processed.
pub fn schedule_layer_delete(
    tenant_id: TenantId,
    timeline_id: TimelineId,
    layers_to_delete: HashSet<PathBuf>,
) {
    let sync_queue = match SYNC_QUEUE.get() {
        Some(queue) => queue,
        None => {
            warn!("Could not send deletion task for tenant {tenant_id}, timeline {timeline_id}");
            return;
        }
    };
    sync_queue.push(
        TenantTimelineId {
            tenant_id,
            timeline_id,
        },
        SyncTask::delete(LayersDeletion {
            layers_to_delete,
            deleted_layers: HashSet::new(),
            deletion_registered: false,
        }),
    );
    debug!("Deletion task for tenant {tenant_id}, timeline {timeline_id} sent")
}

/// Requests the download of the entire timeline for a given tenant.
/// No existing local files are currently overwritten, except the metadata file (if its disk_consistent_lsn is less than the downloaded one).
/// The metadata file is always updated last, to avoid inconsistencies.
///
/// On any failure, the task gets retried, omitting already downloaded layers.
///
/// Ensure that the loop is started otherwise the task is never processed.
pub fn schedule_layer_download(tenant_id: TenantId, timeline_id: TimelineId) {
    debug!("Scheduling layer download for tenant {tenant_id}, timeline {timeline_id}");
    let sync_queue = match SYNC_QUEUE.get() {
        Some(queue) => queue,
        None => {
            warn!("Could not send download task for tenant {tenant_id}, timeline {timeline_id}");
            return;
        }
    };
    sync_queue.push(
        TenantTimelineId {
            tenant_id,
            timeline_id,
        },
        SyncTask::download(LayersDownload {
            layers_to_skip: HashSet::new(),
        }),
    );
    debug!("Download task for tenant {tenant_id}, timeline {timeline_id} sent")
}

/// Launch a thread to perform remote storage sync tasks.
/// See module docs for loop step description.
pub fn spawn_storage_sync_task(
    conf: &'static PageServerConf,
    local_timeline_files: TenantTimelineValues<(TimelineMetadata, HashSet<PathBuf>)>,
    storage: GenericRemoteStorage,
    max_concurrent_timelines_sync: NonZeroUsize,
    max_sync_errors: NonZeroU32,
) -> anyhow::Result<SyncStartupData> {
    let sync_queue = SyncQueue::new(max_concurrent_timelines_sync);
    SYNC_QUEUE
        .set(sync_queue)
        .map_err(|_queue| anyhow!("Could not initialize sync queue"))?;
    let sync_queue = match SYNC_QUEUE.get() {
        Some(queue) => queue,
        None => bail!("Could not get sync queue during the sync loop step, aborting"),
    };

    // TODO we are able to "attach" empty tenants, but not doing it now since it might require big wait time:
    // * we need to list every timeline for tenant on S3, that might be a costly operation
    // * we need to download every timeline for the tenant, to activate it in memory
    //
    // When on-demand download gets merged, we're able to do this fast by storing timeline metadata only.
    let mut empty_tenants = TenantTimelineValues::<LocalTimelineInitStatus>::new();
    let mut keys_for_index_part_downloads = HashSet::new();
    let mut timelines_to_sync = HashMap::new();

    for (tenant_id, timeline_data) in local_timeline_files.0 {
        if timeline_data.is_empty() {
            info!("got empty tenant {}", tenant_id);
            let _ = empty_tenants.0.entry(tenant_id).or_default();
        } else {
            for (timeline_id, timeline_data) in timeline_data {
                let id = TenantTimelineId::new(tenant_id, timeline_id);
                keys_for_index_part_downloads.insert(id);
                timelines_to_sync.insert(id, timeline_data);
            }
        }
    }

    let applicable_index_parts = BACKGROUND_RUNTIME.block_on(download_index_parts(
        conf,
        &storage,
        keys_for_index_part_downloads,
    ));

    let remote_index = RemoteIndex::from_parts(conf, applicable_index_parts)?;

    let mut local_timeline_init_statuses = schedule_first_sync_tasks(
        &mut BACKGROUND_RUNTIME.block_on(remote_index.write()),
        sync_queue,
        timelines_to_sync,
    );
    local_timeline_init_statuses
        .0
        .extend(empty_tenants.0.into_iter());

    let remote_index_clone = remote_index.clone();
    task_mgr::spawn(
        BACKGROUND_RUNTIME.handle(),
        TaskKind::StorageSync,
        None,
        None,
        "Remote storage sync task",
        false,
        async move {
            storage_sync_loop(
                conf,
                (storage, remote_index_clone, sync_queue),
                max_sync_errors,
            )
            .await;
            Ok(())
        },
    );
    Ok(SyncStartupData {
        remote_index,
        local_timeline_init_statuses,
    })
}

async fn storage_sync_loop(
    conf: &'static PageServerConf,
    (storage, index, sync_queue): (GenericRemoteStorage, RemoteIndex, &SyncQueue),
    max_sync_errors: NonZeroU32,
) {
    info!("Starting remote storage sync loop");
    loop {
        let loop_storage = storage.clone();

        let (batched_tasks, remaining_queue_length) = sync_queue.next_task_batch();

        if task_mgr::is_shutdown_requested() {
            info!("Shutdown requested, stopping");
            break;
        }

        REMAINING_SYNC_ITEMS.set(remaining_queue_length as i64);
        if remaining_queue_length > 0 || !batched_tasks.is_empty() {
            debug!("Processing tasks for {} timelines in batch, more tasks left to process: {remaining_queue_length}", batched_tasks.len());
        } else {
            debug!("No tasks to process");
            continue;
        }

        // Concurrently perform all the tasks in the batch
        let loop_step = tokio::select! {
            step = process_batches(
                conf,
                max_sync_errors,
                loop_storage,
                &index,
                batched_tasks,
                sync_queue,
            )
                .instrument(info_span!("storage_sync_loop_step")) => ControlFlow::Continue(step)
                ,
            _ = task_mgr::shutdown_watcher() => ControlFlow::Break(()),
        };

        match loop_step {
            ControlFlow::Continue(updated_tenants) => {
                if updated_tenants.is_empty() {
                    debug!("Sync loop step completed, no new tenant states");
                } else {
                    info!(
                        "Sync loop step completed, {} new tenant state update(s)",
                        updated_tenants.len()
                    );
                    let mut timelines_to_attach = TenantTimelineValues::new();
                    let index_accessor = index.read().await;
                    for tenant_id in updated_tenants {
                        let tenant_entry = match index_accessor.tenant_entry(&tenant_id) {
                            Some(tenant_entry) => tenant_entry,
                            None => {
                                error!(
                                    "cannot find tenant in remote index for timeline sync update"
                                );
                                continue;
                            }
                        };

                        if tenant_entry.has_in_progress_downloads() {
                            info!("Tenant {tenant_id} has pending timeline downloads, skipping tenant registration");
                            continue;
                        } else {
                            info!(
                                "Tenant {tenant_id} download completed. Picking to register in tenant"
                            );
                            // Here we assume that if tenant has no in-progress downloads that
                            // means that it is the last completed timeline download that triggered
                            // sync status update. So we look at the index for available timelines
                            // and register them all at once in a tenant for download
                            // to be submitted in a single operation to tenant
                            // so it can apply them at once to internal timeline map.
                            timelines_to_attach.0.insert(
                                tenant_id,
                                tenant_entry
                                    .iter()
                                    .map(|(&id, entry)| (id, entry.metadata.clone()))
                                    .collect(),
                            );
                        }
                    }
                    drop(index_accessor);
                    // Batch timeline download registration to ensure that the external registration code won't block any running tasks before.
                    attach_local_tenants(conf, &index, timelines_to_attach);
                }
            }
            ControlFlow::Break(()) => {
                info!("Shutdown requested, stopping");
                break;
            }
        }
    }
}

#[derive(Debug)]
enum DownloadStatus {
    Downloaded,
    Nothing,
}

#[derive(Debug)]
enum UploadStatus {
    Uploaded,
    Failed(anyhow::Error),
    Nothing,
}

async fn process_batches(
    conf: &'static PageServerConf,
    max_sync_errors: NonZeroU32,
    storage: GenericRemoteStorage,
    index: &RemoteIndex,
    batched_tasks: HashMap<TenantTimelineId, SyncTaskBatch>,
    sync_queue: &SyncQueue,
) -> HashSet<TenantId> {
    let mut sync_results = batched_tasks
        .into_iter()
        .map(|(sync_id, batch)| {
            let storage = storage.clone();
            let index = index.clone();
            async move {
                let state_update = process_sync_task_batch(
                    conf,
                    (storage, index, sync_queue),
                    max_sync_errors,
                    sync_id,
                    batch,
                )
                .instrument(info_span!("process_sync_task_batch", sync_id = %sync_id))
                .await;
                (sync_id, state_update)
            }
        })
        .collect::<FuturesUnordered<_>>();

    let mut downloaded_timelines = HashSet::new();

    while let Some((sync_id, download_marker)) = sync_results.next().await {
        debug!(
            "Finished storage sync task for sync id {sync_id} download marker {:?}",
            download_marker
        );
        if matches!(download_marker, DownloadStatus::Downloaded) {
            downloaded_timelines.insert(sync_id.tenant_id);
        }
    }

    downloaded_timelines
}

async fn process_sync_task_batch(
    conf: &'static PageServerConf,
    (storage, index, sync_queue): (GenericRemoteStorage, RemoteIndex, &SyncQueue),
    max_sync_errors: NonZeroU32,
    sync_id: TenantTimelineId,
    batch: SyncTaskBatch,
) -> DownloadStatus {
    let sync_start = Instant::now();
    let current_remote_timeline = { index.read().await.timeline_entry(&sync_id).cloned() };

    let upload_data = batch.upload.clone();
    let download_data = batch.download.clone();
    // Run both upload and download tasks concurrently (not in parallel):
    // download and upload tasks do not conflict and spoil the pageserver state even if they are executed in parallel.
    // Under "spoiling" here means potentially inconsistent layer set that misses some of the layers, declared present
    // in local (implicitly, via Lsn values and related memory state) or remote (explicitly via remote layer file paths) metadata.
    // When operating in a system without tasks failing over the error threshold,
    // current batching and task processing systems aim to update the layer set and metadata files (remote and local),
    // without "losing" such layer files.
    let (upload_status, download_status) = tokio::join!(
        async {
            if let Some(upload_data) = upload_data {
                let upload_retries = upload_data.retries;
                match validate_task_retries(upload_retries, max_sync_errors)
                    .instrument(info_span!("retries_validation"))
                    .await
                {
                    ControlFlow::Continue(()) => {
                        upload_timeline_data(
                            conf,
                            (&storage, &index, sync_queue),
                            current_remote_timeline.as_ref(),
                            sync_id,
                            upload_data,
                            sync_start,
                        )
                        .await
                    }
                    ControlFlow::Break(()) => match update_remote_data(
                        conf,
                        &storage,
                        &index,
                        sync_id,
                        RemoteDataUpdate::Upload {
                            uploaded_data: upload_data.data,
                            upload_failed: true,
                        },
                    )
                    .await
                    {
                        Ok(()) => UploadStatus::Failed(anyhow::anyhow!(
                            "Aborted after retries validation, current retries: {upload_retries}, max retries allowed: {max_sync_errors}"
                        )),
                        Err(e) => {
                            error!("Failed to update remote timeline {sync_id}: {e:?}");
                            UploadStatus::Failed(e)
                        }
                    },
                }
            } else {
                UploadStatus::Nothing
            }
        }
        .instrument(info_span!("upload_timeline_data")),
        async {
            if let Some(download_data) = download_data {
                match validate_task_retries(download_data.retries, max_sync_errors)
                    .instrument(info_span!("retries_validation"))
                    .await
                {
                    ControlFlow::Continue(()) => {
                        return download_timeline_data(
                            conf,
                            (&storage, &index, sync_queue),
                            current_remote_timeline.as_ref(),
                            sync_id,
                            download_data,
                            sync_start,
                        )
                        .await;
                    }
                    ControlFlow::Break(()) => {
                        index
                            .write()
                            .await
                            .set_awaits_download(&sync_id, false)
                            .ok();
                    }
                }
            }
            DownloadStatus::Nothing
        }
        .instrument(info_span!("download_timeline_data")),
    );

    if let Some(delete_data) = batch.delete {
        match upload_status {
            UploadStatus::Uploaded | UploadStatus::Nothing => {
                match validate_task_retries(delete_data.retries, max_sync_errors)
                    .instrument(info_span!("retries_validation"))
                    .await
                {
                    ControlFlow::Continue(()) => {
                        delete_timeline_data(
                            conf,
                            (&storage, &index, sync_queue),
                            sync_id,
                            delete_data,
                            sync_start,
                        )
                        .instrument(info_span!("delete_timeline_data"))
                        .await;
                    }
                    ControlFlow::Break(()) => {
                        if let Err(e) = update_remote_data(
                            conf,
                            &storage,
                            &index,
                            sync_id,
                            RemoteDataUpdate::Delete(&delete_data.data.deleted_layers),
                        )
                        .await
                        {
                            error!("Failed to update remote timeline {sync_id}: {e:?}");
                        }
                    }
                }
            }
            UploadStatus::Failed(e) => {
                warn!("Skipping delete task due to failed upload tasks, reenqueuing. Upload data: {:?}, delete data: {delete_data:?}. Upload failure: {e:#}", batch.upload);
                sync_queue.push(sync_id, SyncTask::Delete(delete_data));
            }
        }
    }

    download_status
}

async fn download_timeline_data(
    conf: &'static PageServerConf,
    (storage, index, sync_queue): (&GenericRemoteStorage, &RemoteIndex, &SyncQueue),
    current_remote_timeline: Option<&RemoteTimeline>,
    sync_id: TenantTimelineId,
    new_download_data: SyncData<LayersDownload>,
    sync_start: Instant,
) -> DownloadStatus {
    static TASK_NAME: &str = "download";

    match download_timeline_layers(
        conf,
        storage,
        sync_queue,
        current_remote_timeline,
        sync_id,
        new_download_data,
    )
    .await
    {
        DownloadedTimeline::Abort => {
            register_sync_status(sync_id, sync_start, TASK_NAME, None);
            if let Err(e) = index.write().await.set_awaits_download(&sync_id, false) {
                error!("Timeline {sync_id} was expected to be in the remote index after a download attempt, but it's absent: {e:?}");
            }
        }
        DownloadedTimeline::FailedAndRescheduled => {
            register_sync_status(sync_id, sync_start, TASK_NAME, Some(false));
        }
        DownloadedTimeline::Successful(mut download_data) => {
            match update_local_metadata(conf, sync_id, current_remote_timeline).await {
                Ok(()) => match index.write().await.set_awaits_download(&sync_id, false) {
                    Ok(()) => {
                        register_sync_status(sync_id, sync_start, TASK_NAME, Some(true));
                        return DownloadStatus::Downloaded;
                    }
                    Err(e) => {
                        error!("Timeline {sync_id} was expected to be in the remote index after a successful download, but it's absent: {e:?}");
                    }
                },
                Err(e) => {
                    error!("Failed to update local timeline metadata: {e:?}");
                    download_data.retries += 1;
                    sync_queue.push(sync_id, SyncTask::Download(download_data));
                    register_sync_status(sync_id, sync_start, TASK_NAME, Some(false));
                }
            }
        }
    }

    DownloadStatus::Nothing
}

async fn update_local_metadata(
    conf: &'static PageServerConf,
    sync_id: TenantTimelineId,
    remote_timeline: Option<&RemoteTimeline>,
) -> anyhow::Result<()> {
    let remote_metadata = match remote_timeline {
        Some(timeline) => &timeline.metadata,
        None => {
            debug!("No remote timeline to update local metadata from, skipping the update");
            return Ok(());
        }
    };
    let remote_lsn = remote_metadata.disk_consistent_lsn();

    let local_metadata_path = conf.metadata_path(sync_id.timeline_id, sync_id.tenant_id);
    let local_lsn = if local_metadata_path.exists() {
        let local_metadata = read_metadata_file(&local_metadata_path)
            .await
            .with_context(|| {
                format!(
                    "Failed to load local metadata from path '{}'",
                    local_metadata_path.display()
                )
            })?;

        Some(local_metadata.disk_consistent_lsn())
    } else {
        None
    };

    if local_lsn < Some(remote_lsn) {
        info!("Updating local timeline metadata from remote timeline: local disk_consistent_lsn={local_lsn:?}, remote disk_consistent_lsn={remote_lsn}");
        // clone because spawn_blocking requires static lifetime
        let cloned_metadata = remote_metadata.to_owned();
        let TenantTimelineId {
            tenant_id,
            timeline_id,
        } = sync_id;
        tokio::task::spawn_blocking(move || {
            crate::tenant::save_metadata(conf, timeline_id, tenant_id, &cloned_metadata, true)
        })
        .await
        .with_context(|| {
            format!(
                "failed to join save_metadata task for {}",
                local_metadata_path.display()
            )
        })?
        .with_context(|| {
            format!(
                "Failed to write remote metadata bytes locally to path '{}'",
                local_metadata_path.display()
            )
        })?;
    } else {
        info!("Local metadata at path '{}' has later disk consistent Lsn ({local_lsn:?}) than the remote one ({remote_lsn}), skipping the update", local_metadata_path.display());
    }

    Ok(())
}

async fn delete_timeline_data(
    conf: &'static PageServerConf,
    (storage, index, sync_queue): (&GenericRemoteStorage, &RemoteIndex, &SyncQueue),
    sync_id: TenantTimelineId,
    mut new_delete_data: SyncData<LayersDeletion>,
    sync_start: Instant,
) {
    static TASK_NAME: &str = "delete";

    let timeline_delete = &mut new_delete_data.data;

    if !timeline_delete.deletion_registered {
        if let Err(e) = update_remote_data(
            conf,
            storage,
            index,
            sync_id,
            RemoteDataUpdate::Delete(&timeline_delete.layers_to_delete),
        )
        .await
        {
            error!("Failed to update remote timeline {sync_id}: {e:?}");
            new_delete_data.retries += 1;
            sync_queue.push(sync_id, SyncTask::Delete(new_delete_data));
            register_sync_status(sync_id, sync_start, TASK_NAME, Some(false));
            return;
        }
    }
    timeline_delete.deletion_registered = true;

    let sync_status = delete_timeline_layers(storage, sync_queue, sync_id, new_delete_data).await;
    register_sync_status(sync_id, sync_start, TASK_NAME, Some(sync_status));
}

async fn read_metadata_file(metadata_path: &Path) -> anyhow::Result<TimelineMetadata> {
    TimelineMetadata::from_bytes(
        &fs::read(metadata_path)
            .await
            .context("Failed to read local metadata bytes from fs")?,
    )
    .context("Failed to parse metadata bytes")
}

async fn upload_timeline_data(
    conf: &'static PageServerConf,
    (storage, index, sync_queue): (&GenericRemoteStorage, &RemoteIndex, &SyncQueue),
    current_remote_timeline: Option<&RemoteTimeline>,
    sync_id: TenantTimelineId,
    new_upload_data: SyncData<LayersUpload>,
    sync_start: Instant,
) -> UploadStatus {
    static TASK_NAME: &str = "upload";
    let mut uploaded_data = match upload_timeline_layers(
        storage,
        sync_queue,
        current_remote_timeline,
        sync_id,
        new_upload_data,
    )
    .await
    {
        UploadedTimeline::FailedAndRescheduled(e) => {
            register_sync_status(sync_id, sync_start, TASK_NAME, Some(false));
            return UploadStatus::Failed(e);
        }
        UploadedTimeline::Successful(upload_data) => upload_data,
    };

    match update_remote_data(
        conf,
        storage,
        index,
        sync_id,
        RemoteDataUpdate::Upload {
            uploaded_data: uploaded_data.data.clone(),
            upload_failed: false,
        },
    )
    .await
    {
        Ok(()) => {
            register_sync_status(sync_id, sync_start, TASK_NAME, Some(true));
            UploadStatus::Uploaded
        }
        Err(e) => {
            error!("Failed to update remote timeline {sync_id}: {e:?}");
            uploaded_data.retries += 1;
            sync_queue.push(sync_id, SyncTask::Upload(uploaded_data));
            register_sync_status(sync_id, sync_start, TASK_NAME, Some(false));
            UploadStatus::Failed(e)
        }
    }
}

enum RemoteDataUpdate<'a> {
    Upload {
        uploaded_data: LayersUpload,
        upload_failed: bool,
    },
    Delete(&'a HashSet<PathBuf>),
}

async fn update_remote_data(
    conf: &'static PageServerConf,
    storage: &GenericRemoteStorage,
    index: &RemoteIndex,
    sync_id: TenantTimelineId,
    update: RemoteDataUpdate<'_>,
) -> anyhow::Result<()> {
    let updated_remote_timeline = {
        let mut index_accessor = index.write().await;

        match index_accessor.timeline_entry_mut(&sync_id) {
            Some(existing_entry) => {
                match update {
                    RemoteDataUpdate::Upload {
                        uploaded_data,
                        upload_failed,
                    } => {
                        if let Some(new_metadata) = uploaded_data.metadata.as_ref() {
                            if existing_entry.metadata.disk_consistent_lsn()
                                < new_metadata.disk_consistent_lsn()
                            {
                                existing_entry.metadata = new_metadata.clone();
                            }
                        }
                        if upload_failed {
                            existing_entry.add_upload_failures(
                                uploaded_data.layers_to_upload.iter().cloned(),
                            );
                        } else {
                            existing_entry
                                .add_timeline_layers(uploaded_data.uploaded_layers.iter().cloned());
                        }
                    }
                    RemoteDataUpdate::Delete(layers_to_remove) => {
                        existing_entry.remove_layers(layers_to_remove)
                    }
                }
                existing_entry.clone()
            }
            None => match update {
                RemoteDataUpdate::Upload {
                    uploaded_data,
                    upload_failed,
                } => {
                    let new_metadata = match uploaded_data.metadata.as_ref() {
                        Some(new_metadata) => new_metadata,
                        None => bail!("For timeline {sync_id} upload, there's no upload metadata and no remote index entry, cannot create a new one"),
                    };
                    let mut new_remote_timeline = RemoteTimeline::new(new_metadata.clone());
                    if upload_failed {
                        new_remote_timeline
                            .add_upload_failures(uploaded_data.layers_to_upload.iter().cloned());
                    } else {
                        new_remote_timeline
                            .add_timeline_layers(uploaded_data.uploaded_layers.iter().cloned());
                    }

                    index_accessor.add_timeline_entry(sync_id, new_remote_timeline.clone());
                    new_remote_timeline
                }
                RemoteDataUpdate::Delete(_) => {
                    warn!("No remote index entry for timeline {sync_id}, skipping deletion");
                    return Ok(());
                }
            },
        }
    };

    let timeline_path = conf.timeline_path(&sync_id.timeline_id, &sync_id.tenant_id);
    let new_index_part =
        IndexPart::from_remote_timeline(&timeline_path, updated_remote_timeline)
            .context("Failed to create an index part from the updated remote timeline")?;

    debug!("Uploading remote index for the timeline");
    REMOTE_INDEX_UPLOAD
        .with_label_values(&[
            &sync_id.tenant_id.to_string(),
            &sync_id.timeline_id.to_string(),
        ])
        .inc();

    upload_index_part(conf, storage, sync_id, new_index_part)
        .await
        .context("Failed to upload new index part")
}

async fn validate_task_retries(
    current_attempt: u32,
    max_sync_errors: NonZeroU32,
) -> ControlFlow<(), ()> {
    let max_sync_errors = max_sync_errors.get();
    if current_attempt >= max_sync_errors {
        return ControlFlow::Break(());
    }

    exponential_backoff(current_attempt, 1.0, 30.0).await;
    ControlFlow::Continue(())
}

fn schedule_first_sync_tasks(
    index: &mut RemoteTimelineIndex,
    sync_queue: &SyncQueue,
    local_timeline_files: HashMap<TenantTimelineId, (TimelineMetadata, HashSet<PathBuf>)>,
) -> TenantTimelineValues<LocalTimelineInitStatus> {
    let mut local_timeline_init_statuses = TenantTimelineValues::new();

    let mut new_sync_tasks = VecDeque::with_capacity(local_timeline_files.len());

    for (sync_id, (local_metadata, local_files)) in local_timeline_files {
        match index.timeline_entry_mut(&sync_id) {
            Some(remote_timeline) => {
                let (timeline_status, awaits_download) = compare_local_and_remote_timeline(
                    &mut new_sync_tasks,
                    sync_id,
                    local_metadata,
                    local_files,
                    remote_timeline,
                );
                match local_timeline_init_statuses
                    .0
                    .entry(sync_id.tenant_id)
                    .or_default()
                    .entry(sync_id.timeline_id)
                {
                    hash_map::Entry::Occupied(mut o) => {
                        {
                            // defensive check
                            warn!(
                                "Overwriting timeline init sync status. Status {timeline_status:?}, timeline {}",
                                sync_id.timeline_id
                            );
                        }
                        o.insert(timeline_status);
                    }
                    hash_map::Entry::Vacant(v) => {
                        v.insert(timeline_status);
                    }
                }

                remote_timeline.awaits_download = awaits_download;
            }
            None => {
                // TODO (rodionov) does this mean that we've crashed during tenant creation?
                //  is it safe to upload this checkpoint? could it be half broken?
                warn!(
                    "marking {} as locally complete, while it doesnt exist in remote index",
                    sync_id
                );
                new_sync_tasks.push_back((
                    sync_id,
                    SyncTask::upload(LayersUpload {
                        layers_to_upload: local_files,
                        uploaded_layers: HashSet::new(),
                        metadata: Some(local_metadata.clone()),
                    }),
                ));
                local_timeline_init_statuses
                    .0
                    .entry(sync_id.tenant_id)
                    .or_default()
                    .insert(
                        sync_id.timeline_id,
                        LocalTimelineInitStatus::LocallyComplete(local_metadata),
                    );
            }
        }
    }

    new_sync_tasks.into_iter().for_each(|(sync_id, task)| {
        sync_queue.push(sync_id, task);
    });
    local_timeline_init_statuses
}

/// bool in return value stands for awaits_download
fn compare_local_and_remote_timeline(
    new_sync_tasks: &mut VecDeque<(TenantTimelineId, SyncTask)>,
    sync_id: TenantTimelineId,
    local_metadata: TimelineMetadata,
    local_files: HashSet<PathBuf>,
    remote_entry: &RemoteTimeline,
) -> (LocalTimelineInitStatus, bool) {
    let _entered = info_span!("compare_local_and_remote_timeline", sync_id = %sync_id).entered();

    let remote_files = remote_entry.stored_files();

    let number_of_layers_to_download = remote_files.difference(&local_files).count();
    let (initial_timeline_status, awaits_download) = if number_of_layers_to_download > 0 {
        new_sync_tasks.push_back((
            sync_id,
            SyncTask::download(LayersDownload {
                layers_to_skip: local_files.clone(),
            }),
        ));
        info!("NeedsSync");
        (LocalTimelineInitStatus::NeedsSync, true)
        // we do not need to manipulate with remote consistent lsn here
        // because it will be updated when sync will be completed
    } else {
        info!("LocallyComplete");
        (
            LocalTimelineInitStatus::LocallyComplete(local_metadata.clone()),
            false,
        )
    };

    let layers_to_upload = local_files
        .difference(remote_files)
        .cloned()
        .collect::<HashSet<_>>();
    if !layers_to_upload.is_empty() {
        new_sync_tasks.push_back((
            sync_id,
            SyncTask::upload(LayersUpload {
                layers_to_upload,
                uploaded_layers: HashSet::new(),
                metadata: Some(local_metadata),
            }),
        ));
        // Note that status here doesn't change.
    }

    (initial_timeline_status, awaits_download)
}

fn register_sync_status(
    sync_id: TenantTimelineId,
    sync_start: Instant,
    sync_name: &str,
    sync_status: Option<bool>,
) {
    let secs_elapsed = sync_start.elapsed().as_secs_f64();
    debug!("Processed a sync task in {secs_elapsed:.2} seconds");

    let tenant_id = sync_id.tenant_id.to_string();
    let timeline_id = sync_id.timeline_id.to_string();

    let sync_status = match sync_status {
        Some(true) => "success",
        Some(false) => "failure",
        None => "abort",
    };

    IMAGE_SYNC_TIME_HISTOGRAM
        .with_label_values(&[sync_name, sync_status])
        .observe(secs_elapsed);
    IMAGE_SYNC_TIME
        .with_label_values(&[&tenant_id, &timeline_id])
        .add(secs_elapsed);
    IMAGE_SYNC_COUNT
        .with_label_values(&[&tenant_id, &timeline_id, sync_name, sync_status])
        .inc();
}

#[cfg(test)]
mod test_utils {
    use utils::lsn::Lsn;

    use crate::tenant::harness::TenantHarness;

    use super::*;

    pub(super) async fn create_local_timeline(
        harness: &TenantHarness<'_>,
        timeline_id: TimelineId,
        filenames: &[&str],
        metadata: TimelineMetadata,
    ) -> anyhow::Result<LayersUpload> {
        let timeline_path = harness.timeline_path(&timeline_id);
        fs::create_dir_all(&timeline_path).await?;

        let mut layers_to_upload = HashSet::with_capacity(filenames.len());
        for &file in filenames {
            let file_path = timeline_path.join(file);
            fs::write(&file_path, dummy_contents(file).into_bytes()).await?;
            layers_to_upload.insert(file_path);
        }

        fs::write(
            harness.conf.metadata_path(timeline_id, harness.tenant_id),
            metadata.to_bytes()?,
        )
        .await?;

        Ok(LayersUpload {
            layers_to_upload,
            uploaded_layers: HashSet::new(),
            metadata: Some(metadata),
        })
    }

    pub(super) fn dummy_contents(name: &str) -> String {
        format!("contents for {name}")
    }

    pub(super) fn dummy_metadata(disk_consistent_lsn: Lsn) -> TimelineMetadata {
        TimelineMetadata::new(
            disk_consistent_lsn,
            None,
            None,
            Lsn(0),
            Lsn(0),
            Lsn(0),
            // Any version will do
            // but it should be consistent with the one in the tests
            crate::DEFAULT_PG_VERSION,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::test_utils::dummy_metadata;
    use crate::tenant::harness::TIMELINE_ID;
    use hex_literal::hex;
    use utils::lsn::Lsn;

    use super::*;

    const TEST_SYNC_ID: TenantTimelineId = TenantTimelineId {
        tenant_id: TenantId::from_array(hex!("11223344556677881122334455667788")),
        timeline_id: TIMELINE_ID,
    };

    #[tokio::test]
    async fn separate_task_ids_batch() {
        let sync_queue = SyncQueue::new(NonZeroUsize::new(100).unwrap());
        assert_eq!(sync_queue.len(), 0);

        let sync_id_2 = TenantTimelineId {
            tenant_id: TenantId::from_array(hex!("22223344556677881122334455667788")),
            timeline_id: TIMELINE_ID,
        };
        let sync_id_3 = TenantTimelineId {
            tenant_id: TenantId::from_array(hex!("33223344556677881122334455667788")),
            timeline_id: TIMELINE_ID,
        };
        assert!(sync_id_2 != TEST_SYNC_ID);
        assert!(sync_id_2 != sync_id_3);
        assert!(sync_id_3 != TEST_SYNC_ID);

        let download_task = SyncTask::download(LayersDownload {
            layers_to_skip: HashSet::from([PathBuf::from("sk")]),
        });
        let upload_task = SyncTask::upload(LayersUpload {
            layers_to_upload: HashSet::from([PathBuf::from("up")]),
            uploaded_layers: HashSet::from([PathBuf::from("upl")]),
            metadata: Some(dummy_metadata(Lsn(2))),
        });
        let delete_task = SyncTask::delete(LayersDeletion {
            layers_to_delete: HashSet::from([PathBuf::from("de")]),
            deleted_layers: HashSet::from([PathBuf::from("del")]),
            deletion_registered: false,
        });

        sync_queue.push(TEST_SYNC_ID, download_task.clone());
        sync_queue.push(sync_id_2, upload_task.clone());
        sync_queue.push(sync_id_3, delete_task.clone());

        let submitted_tasks_count = sync_queue.len();
        assert_eq!(submitted_tasks_count, 3);
        let (mut batch, _) = sync_queue.next_task_batch();
        assert_eq!(
            batch.len(),
            submitted_tasks_count,
            "Batch should consist of all tasks submitted"
        );

        assert_eq!(
            Some(SyncTaskBatch::new(download_task)),
            batch.remove(&TEST_SYNC_ID)
        );
        assert_eq!(
            Some(SyncTaskBatch::new(upload_task)),
            batch.remove(&sync_id_2)
        );
        assert_eq!(
            Some(SyncTaskBatch::new(delete_task)),
            batch.remove(&sync_id_3)
        );

        assert!(batch.is_empty(), "Should check all batch tasks");
        assert_eq!(sync_queue.len(), 0);
    }

    #[tokio::test]
    async fn same_task_id_separate_tasks_batch() {
        let sync_queue = SyncQueue::new(NonZeroUsize::new(100).unwrap());
        assert_eq!(sync_queue.len(), 0);

        let download = LayersDownload {
            layers_to_skip: HashSet::from([PathBuf::from("sk")]),
        };
        let upload = LayersUpload {
            layers_to_upload: HashSet::from([PathBuf::from("up")]),
            uploaded_layers: HashSet::from([PathBuf::from("upl")]),
            metadata: Some(dummy_metadata(Lsn(2))),
        };
        let delete = LayersDeletion {
            layers_to_delete: HashSet::from([PathBuf::from("de")]),
            deleted_layers: HashSet::from([PathBuf::from("del")]),
            deletion_registered: false,
        };

        sync_queue.push(TEST_SYNC_ID, SyncTask::download(download.clone()));
        sync_queue.push(TEST_SYNC_ID, SyncTask::upload(upload.clone()));
        sync_queue.push(TEST_SYNC_ID, SyncTask::delete(delete.clone()));

        let submitted_tasks_count = sync_queue.len();
        assert_eq!(submitted_tasks_count, 3);
        let (mut batch, _) = sync_queue.next_task_batch();
        assert_eq!(
            batch.len(),
            1,
            "Queue should have one batch merged from 3 sync tasks of the same user"
        );

        assert_eq!(
            Some(SyncTaskBatch {
                upload: Some(SyncData {
                    retries: 0,
                    data: upload
                }),
                download: Some(SyncData {
                    retries: 0,
                    data: download
                }),
                delete: Some(SyncData {
                    retries: 0,
                    data: delete
                }),
            }),
            batch.remove(&TEST_SYNC_ID),
            "Should have one batch containing all tasks unchanged"
        );

        assert!(batch.is_empty(), "Should check all batch tasks");
        assert_eq!(sync_queue.len(), 0);
    }

    #[tokio::test]
    async fn same_task_id_same_tasks_batch() {
        let sync_queue = SyncQueue::new(NonZeroUsize::new(1).unwrap());
        let download_1 = LayersDownload {
            layers_to_skip: HashSet::from([PathBuf::from("sk1")]),
        };
        let download_2 = LayersDownload {
            layers_to_skip: HashSet::from([PathBuf::from("sk2")]),
        };
        let download_3 = LayersDownload {
            layers_to_skip: HashSet::from([PathBuf::from("sk3")]),
        };
        let download_4 = LayersDownload {
            layers_to_skip: HashSet::from([PathBuf::from("sk4")]),
        };

        let sync_id_2 = TenantTimelineId {
            tenant_id: TenantId::from_array(hex!("22223344556677881122334455667788")),
            timeline_id: TIMELINE_ID,
        };
        assert!(sync_id_2 != TEST_SYNC_ID);

        sync_queue.push(TEST_SYNC_ID, SyncTask::download(download_1.clone()));
        sync_queue.push(TEST_SYNC_ID, SyncTask::download(download_2.clone()));
        sync_queue.push(sync_id_2, SyncTask::download(download_3));
        sync_queue.push(TEST_SYNC_ID, SyncTask::download(download_4.clone()));
        assert_eq!(sync_queue.len(), 4);

        let (mut smallest_batch, _) = sync_queue.next_task_batch();
        assert_eq!(
            smallest_batch.len(),
            1,
            "Queue should have one batch merged from the all sync tasks, but not the other user's task"
        );
        assert_eq!(
            Some(SyncTaskBatch {
                download: Some(SyncData {
                    retries: 0,
                    data: LayersDownload {
                        layers_to_skip: {
                            let mut set = HashSet::new();
                            set.extend(download_1.layers_to_skip.into_iter());
                            set.extend(download_2.layers_to_skip.into_iter());
                            set.extend(download_4.layers_to_skip.into_iter());
                            set
                        },
                    }
                }),
                upload: None,
                delete: None,
            }),
            smallest_batch.remove(&TEST_SYNC_ID),
            "Should have one batch containing all tasks merged for the tenant first appeared in the batch"
        );

        assert!(smallest_batch.is_empty(), "Should check all batch tasks");
        assert_eq!(
            sync_queue.len(),
            1,
            "Should have one task left out of the batch"
        );
    }
}

//! A synchronization logic for the [`RemoteStorage`] and pageserver in-memory state to ensure correct synchronizations
//! between local tenant files and their counterparts from the remote storage.
//!
//! The synchronization does not aim to be immediate, yet eventually consistent.
//! Synchronization is done with the queue being emptied via separate thread asynchronously,
//! attempting to fully store pageserver's local data on the remote storage in a custom format, beneficial for storing.
//!
//! A queue is implemented in the [`sync_queue`] module as a pair of sender and receiver channels, to block on zero tasks instead of checking the queue.
//! The pair's shared buffer of a fixed size serves as an implicit queue, holding [`SyncTask`] for local files upload/download operations.
//!
//! The queue gets emptied by a single thread with the loop, that polls the tasks in batches of deduplicated tasks (size configurable).
//! A task from the batch corresponds to a single timeline, with its files to sync merged together.
//! Every batch task and layer file in the task is processed concurrently, which is possible due to incremental nature of the timelines:
//! it's not asserted, but assumed that timeline's checkpoints only add the files locally, not removing or amending the existing ones.
//! Only GC removes local timeline files, the GC support is not added to sync currently,
//! yet downloading extra files is not critically bad at this stage, GC can remove those again.
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
//! New sync tasks are accepted via [`schedule_timeline_checkpoint_upload`] and [`schedule_timeline_download`] functions,
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
//!
//! When pageserver signals shutdown, current sync task gets finished and the loop exists.

mod download;
pub mod index;
mod upload;

use std::{
    collections::{hash_map, HashMap, HashSet, VecDeque},
    fmt::Debug,
    num::{NonZeroU32, NonZeroUsize},
    ops::ControlFlow,
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::Context;
use futures::stream::{FuturesUnordered, StreamExt};
use lazy_static::lazy_static;
use tokio::{
    fs,
    runtime::Runtime,
    sync::mpsc::{self, UnboundedReceiver},
    time::{Duration, Instant},
};
use tracing::*;

use self::{
    download::{download_timeline_layers, DownloadedTimeline},
    index::{IndexPart, RemoteIndex, RemoteTimeline, RemoteTimelineIndex},
    upload::{upload_index_part, upload_timeline_layers, UploadedTimeline},
};
use super::{LocalTimelineInitStatus, LocalTimelineInitStatuses, RemoteStorage, SyncStartupData};
use crate::{
    config::PageServerConf,
    layered_repository::metadata::{metadata_path, TimelineMetadata},
    repository::TimelineSyncStatusUpdate,
    tenant_mgr::apply_timeline_sync_status_updates,
    thread_mgr,
    thread_mgr::ThreadKind,
};

use metrics::{
    register_histogram_vec, register_int_counter, register_int_gauge, HistogramVec, IntCounter,
    IntGauge,
};
use utils::zid::{ZTenantId, ZTenantTimelineId, ZTimelineId};

pub use self::download::download_index_part;

lazy_static! {
    static ref REMAINING_SYNC_ITEMS: IntGauge = register_int_gauge!(
        "pageserver_remote_storage_remaining_sync_items",
        "Number of storage sync items left in the queue"
    )
    .expect("failed to register pageserver remote storage remaining sync items int gauge");
    static ref FATAL_TASK_FAILURES: IntCounter = register_int_counter!(
        "pageserver_remote_storage_fatal_task_failures",
        "Number of critically failed tasks"
    )
    .expect("failed to register pageserver remote storage remaining sync items int gauge");
    static ref IMAGE_SYNC_TIME: HistogramVec = register_histogram_vec!(
        "pageserver_remote_storage_image_sync_time",
        "Time took to synchronize (download or upload) a whole pageserver image. \
        Grouped by `operation_kind` (upload|download) and `status` (success|failure)",
        &["operation_kind", "status"],
        vec![
            0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 1.5, 2.0, 2.5, 3.0, 4.0, 5.0, 6.0, 7.0,
            8.0, 9.0, 10.0, 12.5, 15.0, 17.5, 20.0
        ]
    )
    .expect("failed to register pageserver image sync time histogram vec");
}

/// Wraps mpsc channel bits around into a queue interface.
/// mpsc approach was picked to allow blocking the sync loop if no tasks are present, to avoid meaningless spinning.
mod sync_queue {
    use std::{
        collections::{hash_map, HashMap},
        sync::atomic::{AtomicUsize, Ordering},
    };

    use anyhow::anyhow;
    use once_cell::sync::OnceCell;
    use tokio::sync::mpsc::{error::TryRecvError, UnboundedReceiver, UnboundedSender};
    use tracing::{debug, warn};

    use super::SyncTask;
    use utils::zid::ZTenantTimelineId;

    static SENDER: OnceCell<UnboundedSender<(ZTenantTimelineId, SyncTask)>> = OnceCell::new();
    static LENGTH: AtomicUsize = AtomicUsize::new(0);

    /// Initializes the queue with the given sender channel that is used to put the tasks into later.
    /// Errors if called more than once.
    pub fn init(sender: UnboundedSender<(ZTenantTimelineId, SyncTask)>) -> anyhow::Result<()> {
        SENDER
            .set(sender)
            .map_err(|_sender| anyhow!("sync queue was already initialized"))?;
        Ok(())
    }

    /// Adds a new task to the queue, if the queue was initialized, returning `true` on success.
    /// On any error, or if the queue was not initialized, the task gets dropped (not scheduled) and `false` is returned.
    pub fn push(sync_id: ZTenantTimelineId, new_task: SyncTask) -> bool {
        if let Some(sender) = SENDER.get() {
            match sender.send((sync_id, new_task)) {
                Err(e) => {
                    warn!("Failed to enqueue a sync task: the receiver is dropped: {e}");
                    false
                }
                Ok(()) => {
                    LENGTH.fetch_add(1, Ordering::Relaxed);
                    true
                }
            }
        } else {
            warn!("Failed to enqueue a sync task: the sender is not initialized");
            false
        }
    }

    /// Polls a new task from the queue, using its receiver counterpart.
    /// Does not block if the queue is empty, returning [`None`] instead.
    /// Needed to correctly track the queue length.
    pub async fn next_task(
        receiver: &mut UnboundedReceiver<(ZTenantTimelineId, SyncTask)>,
    ) -> Option<(ZTenantTimelineId, SyncTask)> {
        let task = receiver.recv().await;
        if task.is_some() {
            LENGTH.fetch_sub(1, Ordering::Relaxed);
        }
        task
    }

    /// Fetches a task batch, not bigger than the given limit.
    /// Not blocking, can return fewer tasks if the queue does not contain enough.
    /// Batch tasks are split by timelines, with all related tasks merged into one (download/upload)
    /// or two (download and upload, if both were found in the queue during batch construction).
    pub async fn next_task_batch(
        receiver: &mut UnboundedReceiver<(ZTenantTimelineId, SyncTask)>,
        mut max_batch_size: usize,
    ) -> HashMap<ZTenantTimelineId, SyncTask> {
        if max_batch_size == 0 {
            return HashMap::new();
        }
        let mut tasks: HashMap<ZTenantTimelineId, SyncTask> =
            HashMap::with_capacity(max_batch_size);

        loop {
            match receiver.try_recv() {
                Ok((sync_id, new_task)) => {
                    LENGTH.fetch_sub(1, Ordering::Relaxed);
                    match tasks.entry(sync_id) {
                        hash_map::Entry::Occupied(o) => {
                            let current = o.remove();
                            tasks.insert(sync_id, current.merge(new_task));
                        }
                        hash_map::Entry::Vacant(v) => {
                            v.insert(new_task);
                        }
                    }

                    max_batch_size -= 1;
                    if max_batch_size == 0 {
                        break;
                    }
                }
                Err(TryRecvError::Disconnected) => {
                    debug!("Sender disconnected, batch collection aborted");
                    break;
                }
                Err(TryRecvError::Empty) => {
                    debug!("No more data in the sync queue, task batch is not full");
                    break;
                }
            }
        }

        tasks
    }

    /// Length of the queue, assuming that all receiver counterparts were only called using the queue api.
    pub fn len() -> usize {
        LENGTH.load(Ordering::Relaxed)
    }
}

/// A task to run in the async download/upload loop.
/// Limited by the number of retries, after certain threshold the failing task gets evicted and the timeline disabled.
#[derive(Debug)]
pub enum SyncTask {
    /// A checkpoint outcome with possible local file updates that need actualization in the remote storage.
    /// Not necessary more fresh than the one already uploaded.
    Download(SyncData<TimelineDownload>),
    /// A certain amount of image files to download.
    Upload(SyncData<TimelineUpload>),
    /// Both upload and download layers need to be synced.
    DownloadAndUpload(SyncData<TimelineDownload>, SyncData<TimelineUpload>),
}

/// Stores the data to synd and its retries, to evict the tasks failing to frequently.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SyncData<T> {
    retries: u32,
    data: T,
}

impl<T> SyncData<T> {
    fn new(retries: u32, data: T) -> Self {
        Self { retries, data }
    }
}

impl SyncTask {
    fn download(download_task: TimelineDownload) -> Self {
        Self::Download(SyncData::new(0, download_task))
    }

    fn upload(upload_task: TimelineUpload) -> Self {
        Self::Upload(SyncData::new(0, upload_task))
    }

    /// Merges two tasks into one with the following rules:
    ///
    /// * Download + Download = Download with the retry counter reset and the layers to skip combined
    /// * DownloadAndUpload + Download = DownloadAndUpload with Upload unchanged and the Download counterparts united by the same rules
    /// * Upload + Upload = Upload with the retry counter reset and the layers to upload and the uploaded layers combined
    /// * DownloadAndUpload + Upload = DownloadAndUpload with Download unchanged and the Upload counterparts united by the same rules
    /// * Upload + Download = DownloadAndUpload with both tasks unchanged
    /// * DownloadAndUpload + DownloadAndUpload = DownloadAndUpload with both parts united by the same rules
    fn merge(mut self, other: Self) -> Self {
        match (&mut self, other) {
            (
                SyncTask::DownloadAndUpload(download_data, _) | SyncTask::Download(download_data),
                SyncTask::Download(new_download_data),
            )
            | (
                SyncTask::Download(download_data),
                SyncTask::DownloadAndUpload(new_download_data, _),
            ) => {
                download_data
                    .data
                    .layers_to_skip
                    .extend(new_download_data.data.layers_to_skip.into_iter());
                download_data.retries = 0;
            }
            (SyncTask::Upload(upload), SyncTask::Download(new_download_data)) => {
                self = SyncTask::DownloadAndUpload(new_download_data, upload.clone());
            }

            (
                SyncTask::DownloadAndUpload(_, upload_data) | SyncTask::Upload(upload_data),
                SyncTask::Upload(new_upload_data),
            )
            | (SyncTask::Upload(upload_data), SyncTask::DownloadAndUpload(_, new_upload_data)) => {
                upload_data
                    .data
                    .layers_to_upload
                    .extend(new_upload_data.data.layers_to_upload.into_iter());
                upload_data
                    .data
                    .uploaded_layers
                    .extend(new_upload_data.data.uploaded_layers.into_iter());
                upload_data.retries = 0;

                if new_upload_data.data.metadata.disk_consistent_lsn()
                    > upload_data.data.metadata.disk_consistent_lsn()
                {
                    upload_data.data.metadata = new_upload_data.data.metadata;
                }
            }
            (SyncTask::Download(download), SyncTask::Upload(new_upload_data)) => {
                self = SyncTask::DownloadAndUpload(download.clone(), new_upload_data)
            }

            (
                SyncTask::DownloadAndUpload(download_data, upload_data),
                SyncTask::DownloadAndUpload(new_download_data, new_upload_data),
            ) => {
                download_data
                    .data
                    .layers_to_skip
                    .extend(new_download_data.data.layers_to_skip.into_iter());
                download_data.retries = 0;

                upload_data
                    .data
                    .layers_to_upload
                    .extend(new_upload_data.data.layers_to_upload.into_iter());
                upload_data
                    .data
                    .uploaded_layers
                    .extend(new_upload_data.data.uploaded_layers.into_iter());
                upload_data.retries = 0;

                if new_upload_data.data.metadata.disk_consistent_lsn()
                    > upload_data.data.metadata.disk_consistent_lsn()
                {
                    upload_data.data.metadata = new_upload_data.data.metadata;
                }
            }
        }

        self
    }

    fn name(&self) -> &'static str {
        match self {
            SyncTask::Download(_) => "download",
            SyncTask::Upload(_) => "upload",
            SyncTask::DownloadAndUpload(_, _) => "download and upload",
        }
    }

    fn retries(&self) -> u32 {
        match self {
            SyncTask::Download(data) => data.retries,
            SyncTask::Upload(data) => data.retries,
            SyncTask::DownloadAndUpload(download_data, upload_data) => {
                download_data.retries.max(upload_data.retries)
            }
        }
    }
}

/// Local timeline files for upload, appeared after the new checkpoint.
/// Current checkpoint design assumes new files are added only, no deletions or amendment happens.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TimelineUpload {
    /// Layer file path in the pageserver workdir, that were added for the corresponding checkpoint.
    layers_to_upload: HashSet<PathBuf>,
    /// Already uploaded layers. Used to store the data about the uploads between task retries
    /// and to record the data into the remote index after the task got completed or evicted.
    uploaded_layers: HashSet<PathBuf>,
    metadata: TimelineMetadata,
}

/// A timeline download task.
/// Does not contain the file list to download, to allow other
/// parts of the pageserer code to schedule the task
/// without using the remote index or any other ways to list the remote timleine files.
/// Skips the files that are already downloaded.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TimelineDownload {
    layers_to_skip: HashSet<PathBuf>,
}

/// Adds the new checkpoint files as an upload sync task to the queue.
/// On task failure, it gets retried again from the start a number of times.
///
/// Ensure that the loop is started otherwise the task is never processed.
pub fn schedule_timeline_checkpoint_upload(
    tenant_id: ZTenantId,
    timeline_id: ZTimelineId,
    new_layer: PathBuf,
    metadata: TimelineMetadata,
) {
    if !sync_queue::push(
        ZTenantTimelineId {
            tenant_id,
            timeline_id,
        },
        SyncTask::upload(TimelineUpload {
            layers_to_upload: HashSet::from([new_layer]),
            uploaded_layers: HashSet::new(),
            metadata,
        }),
    ) {
        warn!("Could not send an upload task for tenant {tenant_id}, timeline {timeline_id}",)
    } else {
        debug!("Upload task for tenant {tenant_id}, timeline {timeline_id} sent")
    }
}

/// Requests the download of the entire timeline for a given tenant.
/// No existing local files are currently overwritten, except the metadata file (if its disk_consistent_lsn is less than the downloaded one).
/// The metadata file is always updated last, to avoid inconsistencies.
///
/// On any failure, the task gets retried, omitting already downloaded layers.
///
/// Ensure that the loop is started otherwise the task is never processed.
pub fn schedule_timeline_download(tenant_id: ZTenantId, timeline_id: ZTimelineId) {
    debug!("Scheduling timeline download for tenant {tenant_id}, timeline {timeline_id}");
    sync_queue::push(
        ZTenantTimelineId {
            tenant_id,
            timeline_id,
        },
        SyncTask::download(TimelineDownload {
            layers_to_skip: HashSet::new(),
        }),
    );
}

/// Uses a remote storage given to start the storage sync loop.
/// See module docs for loop step description.
pub(super) fn spawn_storage_sync_thread<P, S>(
    conf: &'static PageServerConf,
    local_timeline_files: HashMap<ZTenantTimelineId, (TimelineMetadata, HashSet<PathBuf>)>,
    storage: S,
    max_concurrent_sync: NonZeroUsize,
    max_sync_errors: NonZeroU32,
) -> anyhow::Result<SyncStartupData>
where
    P: Debug + Send + Sync + 'static,
    S: RemoteStorage<StoragePath = P> + Send + Sync + 'static,
{
    let (sender, receiver) = mpsc::unbounded_channel();
    sync_queue::init(sender)?;

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .context("Failed to create storage sync runtime")?;

    let applicable_index_parts = runtime.block_on(try_fetch_index_parts(
        conf,
        &storage,
        local_timeline_files.keys().copied().collect(),
    ));

    let remote_index = RemoteIndex::from_parts(conf, applicable_index_parts)?;

    let local_timeline_init_statuses = schedule_first_sync_tasks(
        &mut runtime.block_on(remote_index.write()),
        local_timeline_files,
    );

    let loop_index = remote_index.clone();
    thread_mgr::spawn(
        ThreadKind::StorageSync,
        None,
        None,
        "Remote storage sync thread",
        false,
        move || {
            storage_sync_loop(
                runtime,
                conf,
                receiver,
                Arc::new(storage),
                loop_index,
                max_concurrent_sync,
                max_sync_errors,
            );
            Ok(())
        },
    )
    .context("Failed to spawn remote storage sync thread")?;
    Ok(SyncStartupData {
        remote_index,
        local_timeline_init_statuses,
    })
}

#[allow(clippy::too_many_arguments)]
fn storage_sync_loop<P, S>(
    runtime: Runtime,
    conf: &'static PageServerConf,
    mut receiver: UnboundedReceiver<(ZTenantTimelineId, SyncTask)>,
    storage: Arc<S>,
    index: RemoteIndex,
    max_concurrent_sync: NonZeroUsize,
    max_sync_errors: NonZeroU32,
) where
    P: Debug + Send + Sync + 'static,
    S: RemoteStorage<StoragePath = P> + Send + Sync + 'static,
{
    info!("Starting remote storage sync loop");
    loop {
        let loop_index = index.clone();
        let storage = Arc::clone(&storage);
        let loop_step = runtime.block_on(async {
            tokio::select! {
                step = loop_step(
                    conf,
                    &mut receiver,
                    storage,
                    loop_index,
                    max_concurrent_sync,
                    max_sync_errors,
                )
                .instrument(info_span!("storage_sync_loop_step")) => step,
                _ = thread_mgr::shutdown_watcher() => ControlFlow::Break(()),
            }
        });

        match loop_step {
            ControlFlow::Continue(new_timeline_states) => {
                if new_timeline_states.is_empty() {
                    debug!("Sync loop step completed, no new timeline states");
                } else {
                    info!(
                        "Sync loop step completed, {} new timeline state update(s)",
                        new_timeline_states.len()
                    );
                    // Batch timeline download registration to ensure that the external registration code won't block any running tasks before.
                    apply_timeline_sync_status_updates(conf, &index, new_timeline_states);
                }
            }
            ControlFlow::Break(()) => {
                info!("Shutdown requested, stopping");
                break;
            }
        }
    }
}

async fn loop_step<P, S>(
    conf: &'static PageServerConf,
    receiver: &mut UnboundedReceiver<(ZTenantTimelineId, SyncTask)>,
    storage: Arc<S>,
    index: RemoteIndex,
    max_concurrent_sync: NonZeroUsize,
    max_sync_errors: NonZeroU32,
) -> ControlFlow<(), HashMap<ZTenantId, HashMap<ZTimelineId, TimelineSyncStatusUpdate>>>
where
    P: Debug + Send + Sync + 'static,
    S: RemoteStorage<StoragePath = P> + Send + Sync + 'static,
{
    let max_concurrent_sync = max_concurrent_sync.get();

    // request the first task in blocking fashion to do less meaningless work
    let (first_sync_id, first_task) =
        if let Some(first_task) = sync_queue::next_task(receiver).await {
            first_task
        } else {
            return ControlFlow::Break(());
        };

    let mut batched_tasks = sync_queue::next_task_batch(receiver, max_concurrent_sync - 1).await;
    match batched_tasks.entry(first_sync_id) {
        hash_map::Entry::Occupied(o) => {
            let current = o.remove();
            batched_tasks.insert(first_sync_id, current.merge(first_task));
        }
        hash_map::Entry::Vacant(v) => {
            v.insert(first_task);
        }
    }

    let remaining_queue_length = sync_queue::len();
    REMAINING_SYNC_ITEMS.set(remaining_queue_length as i64);
    if remaining_queue_length > 0 || !batched_tasks.is_empty() {
        info!("Processing tasks for {} timelines in batch, more tasks left to process: {remaining_queue_length}", batched_tasks.len());
    } else {
        debug!("No tasks to process");
        return ControlFlow::Continue(HashMap::new());
    }

    let mut sync_results = batched_tasks
        .into_iter()
        .map(|(sync_id, task)| {
            let storage = Arc::clone(&storage);
            let index = index.clone();
            async move {
                let state_update =
                    process_sync_task(conf, storage, index, max_sync_errors, sync_id, task)
                        .instrument(info_span!("process_sync_tasks", sync_id = %sync_id))
                        .await;
                (sync_id, state_update)
            }
        })
        .collect::<FuturesUnordered<_>>();

    let mut new_timeline_states: HashMap<
        ZTenantId,
        HashMap<ZTimelineId, TimelineSyncStatusUpdate>,
    > = HashMap::with_capacity(max_concurrent_sync);
    while let Some((sync_id, state_update)) = sync_results.next().await {
        debug!("Finished storage sync task for sync id {sync_id}");
        if let Some(state_update) = state_update {
            new_timeline_states
                .entry(sync_id.tenant_id)
                .or_default()
                .insert(sync_id.timeline_id, state_update);
        }
    }

    ControlFlow::Continue(new_timeline_states)
}

async fn process_sync_task<P, S>(
    conf: &'static PageServerConf,
    storage: Arc<S>,
    index: RemoteIndex,
    max_sync_errors: NonZeroU32,
    sync_id: ZTenantTimelineId,
    task: SyncTask,
) -> Option<TimelineSyncStatusUpdate>
where
    P: Debug + Send + Sync + 'static,
    S: RemoteStorage<StoragePath = P> + Send + Sync + 'static,
{
    let sync_start = Instant::now();
    let current_remote_timeline = { index.read().await.timeline_entry(&sync_id).cloned() };

    let task = match validate_task_retries(sync_id, task, max_sync_errors) {
        ControlFlow::Continue(task) => task,
        ControlFlow::Break(aborted_task) => {
            match aborted_task {
                SyncTask::Download(_) => {
                    index
                        .write()
                        .await
                        .set_awaits_download(&sync_id, false)
                        .ok();
                }
                SyncTask::Upload(failed_upload_data) => {
                    if let Err(e) = update_remote_data(
                        conf,
                        storage.as_ref(),
                        &index,
                        sync_id,
                        &failed_upload_data.data,
                        true,
                    )
                    .await
                    {
                        error!("Failed to update remote timeline {sync_id}: {e:?}");
                    }
                }
                SyncTask::DownloadAndUpload(_, failed_upload_data) => {
                    index
                        .write()
                        .await
                        .set_awaits_download(&sync_id, false)
                        .ok();
                    if let Err(e) = update_remote_data(
                        conf,
                        storage.as_ref(),
                        &index,
                        sync_id,
                        &failed_upload_data.data,
                        true,
                    )
                    .await
                    {
                        error!("Failed to update remote timeline {sync_id}: {e:?}");
                    }
                }
            }
            return None;
        }
    };

    let task_name = task.name();
    let current_task_attempt = task.retries();
    info!("Sync task '{task_name}' processing started, attempt #{current_task_attempt}");

    if current_task_attempt > 0 {
        let seconds_to_wait = 2.0_f64.powf(current_task_attempt as f64 - 1.0).min(30.0);
        info!("Waiting {seconds_to_wait} seconds before starting the '{task_name}' task");
        tokio::time::sleep(Duration::from_secs_f64(seconds_to_wait)).await;
    }

    let status_update = match task {
        SyncTask::Download(new_download_data) => {
            download_timeline(
                conf,
                (storage.as_ref(), &index),
                current_remote_timeline.as_ref(),
                sync_id,
                new_download_data,
                sync_start,
                task_name,
            )
            .await
        }
        SyncTask::Upload(new_upload_data) => {
            upload_timeline(
                conf,
                (storage.as_ref(), &index),
                current_remote_timeline.as_ref(),
                sync_id,
                new_upload_data,
                sync_start,
                task_name,
            )
            .await;
            None
        }
        SyncTask::DownloadAndUpload(new_download_data, new_upload_data) => {
            let status_update = download_timeline(
                conf,
                (storage.as_ref(), &index),
                current_remote_timeline.as_ref(),
                sync_id,
                new_download_data,
                sync_start,
                task_name,
            )
            .await;

            upload_timeline(
                conf,
                (storage.as_ref(), &index),
                current_remote_timeline.as_ref(),
                sync_id,
                new_upload_data,
                sync_start,
                task_name,
            )
            .await;

            status_update
        }
    };

    info!("Finished processing the task");

    status_update
}

async fn download_timeline<P, S>(
    conf: &'static PageServerConf,
    (storage, index): (&S, &RemoteIndex),
    current_remote_timeline: Option<&RemoteTimeline>,
    sync_id: ZTenantTimelineId,
    new_download_data: SyncData<TimelineDownload>,
    sync_start: Instant,
    task_name: &str,
) -> Option<TimelineSyncStatusUpdate>
where
    P: Debug + Send + Sync + 'static,
    S: RemoteStorage<StoragePath = P> + Send + Sync + 'static,
{
    match download_timeline_layers(storage, current_remote_timeline, sync_id, new_download_data)
        .await
    {
        DownloadedTimeline::Abort => {
            register_sync_status(sync_start, task_name, None);
            if let Err(e) = index.write().await.set_awaits_download(&sync_id, false) {
                error!("Timeline {sync_id} was expected to be in the remote index after a download attempt, but it's absent: {e:?}");
            }
            None
        }
        DownloadedTimeline::FailedAndRescheduled => {
            register_sync_status(sync_start, task_name, Some(false));
            None
        }
        DownloadedTimeline::Successful(mut download_data) => {
            match update_local_metadata(conf, sync_id, current_remote_timeline).await {
                Ok(()) => match index.write().await.set_awaits_download(&sync_id, false) {
                    Ok(()) => {
                        register_sync_status(sync_start, task_name, Some(true));
                        Some(TimelineSyncStatusUpdate::Downloaded)
                    }
                    Err(e) => {
                        error!("Timeline {sync_id} was expected to be in the remote index after a sucessful download, but it's absent: {e:?}");
                        None
                    }
                },
                Err(e) => {
                    error!("Failed to update local timeline metadata: {e:?}");
                    download_data.retries += 1;
                    sync_queue::push(sync_id, SyncTask::Download(download_data));
                    register_sync_status(sync_start, task_name, Some(false));
                    None
                }
            }
        }
    }
}

async fn update_local_metadata(
    conf: &'static PageServerConf,
    sync_id: ZTenantTimelineId,
    remote_timeline: Option<&RemoteTimeline>,
) -> anyhow::Result<()> {
    let remote_metadata = match remote_timeline {
        Some(timeline) => &timeline.metadata,
        None => {
            info!("No remote timeline to update local metadata from, skipping the update");
            return Ok(());
        }
    };
    let remote_lsn = remote_metadata.disk_consistent_lsn();

    let local_metadata_path = metadata_path(conf, sync_id.timeline_id, sync_id.tenant_id);
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

        let remote_metadata_bytes = remote_metadata
            .to_bytes()
            .context("Failed to serialize remote metadata to bytes")?;
        fs::write(&local_metadata_path, &remote_metadata_bytes)
            .await
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

async fn read_metadata_file(metadata_path: &Path) -> anyhow::Result<TimelineMetadata> {
    TimelineMetadata::from_bytes(
        &fs::read(metadata_path)
            .await
            .context("Failed to read local metadata bytes from fs")?,
    )
    .context("Failed to parse metadata bytes")
}

async fn upload_timeline<P, S>(
    conf: &'static PageServerConf,
    (storage, index): (&S, &RemoteIndex),
    current_remote_timeline: Option<&RemoteTimeline>,
    sync_id: ZTenantTimelineId,
    new_upload_data: SyncData<TimelineUpload>,
    sync_start: Instant,
    task_name: &str,
) where
    P: Debug + Send + Sync + 'static,
    S: RemoteStorage<StoragePath = P> + Send + Sync + 'static,
{
    let mut uploaded_data =
        match upload_timeline_layers(storage, current_remote_timeline, sync_id, new_upload_data)
            .await
        {
            UploadedTimeline::FailedAndRescheduled => {
                register_sync_status(sync_start, task_name, Some(false));
                return;
            }
            UploadedTimeline::Successful(upload_data) => upload_data,
            UploadedTimeline::SuccessfulAfterLocalFsUpdate(mut outdated_upload_data) => {
                let local_metadata_path =
                    metadata_path(conf, sync_id.timeline_id, sync_id.tenant_id);
                let local_metadata = match read_metadata_file(&local_metadata_path).await {
                    Ok(metadata) => metadata,
                    Err(e) => {
                        error!(
                            "Failed to load local metadata from path '{}': {e:?}",
                            local_metadata_path.display()
                        );
                        outdated_upload_data.retries += 1;
                        sync_queue::push(sync_id, SyncTask::Upload(outdated_upload_data));
                        register_sync_status(sync_start, task_name, Some(false));
                        return;
                    }
                };

                outdated_upload_data.data.metadata = local_metadata;
                outdated_upload_data
            }
        };

    match update_remote_data(conf, storage, index, sync_id, &uploaded_data.data, false).await {
        Ok(()) => register_sync_status(sync_start, task_name, Some(true)),
        Err(e) => {
            error!("Failed to update remote timeline {sync_id}: {e:?}");
            uploaded_data.retries += 1;
            sync_queue::push(sync_id, SyncTask::Upload(uploaded_data));
            register_sync_status(sync_start, task_name, Some(false));
        }
    }
}

async fn update_remote_data<P, S>(
    conf: &'static PageServerConf,
    storage: &S,
    index: &RemoteIndex,
    sync_id: ZTenantTimelineId,
    uploaded_data: &TimelineUpload,
    upload_failed: bool,
) -> anyhow::Result<()>
where
    P: Debug + Send + Sync + 'static,
    S: RemoteStorage<StoragePath = P> + Send + Sync + 'static,
{
    info!("Updating remote index for the timeline");
    let updated_remote_timeline = {
        let mut index_accessor = index.write().await;

        match index_accessor.timeline_entry_mut(&sync_id) {
            Some(existing_entry) => {
                if existing_entry.metadata.disk_consistent_lsn()
                    < uploaded_data.metadata.disk_consistent_lsn()
                {
                    existing_entry.metadata = uploaded_data.metadata.clone();
                }
                if upload_failed {
                    existing_entry
                        .add_upload_failures(uploaded_data.layers_to_upload.iter().cloned());
                } else {
                    existing_entry
                        .add_timeline_layers(uploaded_data.uploaded_layers.iter().cloned());
                }
                existing_entry.clone()
            }
            None => {
                let mut new_remote_timeline = RemoteTimeline::new(uploaded_data.metadata.clone());
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
        }
    };

    let timeline_path = conf.timeline_path(&sync_id.timeline_id, &sync_id.tenant_id);
    let new_index_part =
        IndexPart::from_remote_timeline(&timeline_path, updated_remote_timeline)
            .context("Failed to create an index part from the updated remote timeline")?;

    info!("Uploading remote data for the timeline");
    upload_index_part(conf, storage, sync_id, new_index_part)
        .await
        .context("Failed to upload new index part")
}

fn validate_task_retries(
    sync_id: ZTenantTimelineId,
    task: SyncTask,
    max_sync_errors: NonZeroU32,
) -> ControlFlow<SyncTask, SyncTask> {
    let max_sync_errors = max_sync_errors.get();
    let mut skip_upload = false;
    let mut skip_download = false;

    match &task {
        SyncTask::Download(download_data) | SyncTask::DownloadAndUpload(download_data, _)
            if download_data.retries > max_sync_errors =>
        {
            error!(
                    "Evicting download task for timeline {sync_id} that failed {} times, exceeding the error threshold {max_sync_errors}",
                    download_data.retries
                );
            skip_download = true;
        }
        SyncTask::Upload(upload_data) | SyncTask::DownloadAndUpload(_, upload_data)
            if upload_data.retries > max_sync_errors =>
        {
            error!(
                "Evicting upload task for timeline {sync_id} that failed {} times, exceeding the error threshold {max_sync_errors}",
                upload_data.retries,
            );
            skip_upload = true;
        }
        _ => {}
    }

    match task {
        aborted_task @ SyncTask::Download(_) if skip_download => ControlFlow::Break(aborted_task),
        aborted_task @ SyncTask::Upload(_) if skip_upload => ControlFlow::Break(aborted_task),
        aborted_task @ SyncTask::DownloadAndUpload(_, _) if skip_upload && skip_download => {
            ControlFlow::Break(aborted_task)
        }
        SyncTask::DownloadAndUpload(download_task, _) if skip_upload => {
            ControlFlow::Continue(SyncTask::Download(download_task))
        }
        SyncTask::DownloadAndUpload(_, upload_task) if skip_download => {
            ControlFlow::Continue(SyncTask::Upload(upload_task))
        }
        not_skipped => ControlFlow::Continue(not_skipped),
    }
}

async fn try_fetch_index_parts<P, S>(
    conf: &'static PageServerConf,
    storage: &S,
    keys: HashSet<ZTenantTimelineId>,
) -> HashMap<ZTenantTimelineId, IndexPart>
where
    P: Debug + Send + Sync + 'static,
    S: RemoteStorage<StoragePath = P> + Send + Sync + 'static,
{
    let mut index_parts = HashMap::with_capacity(keys.len());

    let mut part_downloads = keys
        .into_iter()
        .map(|id| async move { (id, download_index_part(conf, storage, id).await) })
        .collect::<FuturesUnordered<_>>();

    while let Some((id, part_upload_result)) = part_downloads.next().await {
        match part_upload_result {
            Ok(index_part) => {
                debug!("Successfully fetched index part for {id}");
                index_parts.insert(id, index_part);
            }
            Err(e) => warn!("Failed to fetch index part for {id}: {e:?}"),
        }
    }

    index_parts
}

fn schedule_first_sync_tasks(
    index: &mut RemoteTimelineIndex,
    local_timeline_files: HashMap<ZTenantTimelineId, (TimelineMetadata, HashSet<PathBuf>)>,
) -> LocalTimelineInitStatuses {
    let mut local_timeline_init_statuses = LocalTimelineInitStatuses::new();

    let mut new_sync_tasks =
        VecDeque::with_capacity(local_timeline_files.len().max(local_timeline_files.len()));

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
                let was_there = local_timeline_init_statuses
                    .entry(sync_id.tenant_id)
                    .or_default()
                    .insert(sync_id.timeline_id, timeline_status);

                if was_there.is_some() {
                    // defensive check
                    warn!(
                        "Overwriting timeline init sync status. Status {timeline_status:?}, timeline {}",
                        sync_id.timeline_id
                    );
                }
                remote_timeline.awaits_download = awaits_download;
            }
            None => {
                // TODO (rodionov) does this mean that we've crashed during tenant creation?
                //  is it safe to upload this checkpoint? could it be half broken?
                new_sync_tasks.push_back((
                    sync_id,
                    SyncTask::upload(TimelineUpload {
                        layers_to_upload: local_files,
                        uploaded_layers: HashSet::new(),
                        metadata: local_metadata,
                    }),
                ));
                local_timeline_init_statuses
                    .entry(sync_id.tenant_id)
                    .or_default()
                    .insert(
                        sync_id.timeline_id,
                        LocalTimelineInitStatus::LocallyComplete,
                    );
            }
        }
    }

    new_sync_tasks.into_iter().for_each(|(sync_id, task)| {
        sync_queue::push(sync_id, task);
    });
    local_timeline_init_statuses
}

fn compare_local_and_remote_timeline(
    new_sync_tasks: &mut VecDeque<(ZTenantTimelineId, SyncTask)>,
    sync_id: ZTenantTimelineId,
    local_metadata: TimelineMetadata,
    local_files: HashSet<PathBuf>,
    remote_entry: &RemoteTimeline,
) -> (LocalTimelineInitStatus, bool) {
    let remote_files = remote_entry.stored_files();

    // TODO probably here we need more sophisticated logic,
    //   if more data is available remotely can we just download whats there?
    //   without trying to upload something. It may be tricky, needs further investigation.
    //   For now looks strange that we can request upload
    //   and dowload for the same timeline simultaneously.
    //   (upload needs to be only for previously unsynced files, not whole timeline dir).
    //   If one of the tasks fails they will be reordered in the queue which can lead
    //   to timeline being stuck in evicted state
    let number_of_layers_to_download = remote_files.difference(&local_files).count();
    let (initial_timeline_status, awaits_download) = if number_of_layers_to_download > 0 {
        new_sync_tasks.push_back((
            sync_id,
            SyncTask::download(TimelineDownload {
                layers_to_skip: local_files.clone(),
            }),
        ));
        (LocalTimelineInitStatus::NeedsSync, true)
        // we do not need to manupulate with remote consistent lsn here
        // because it will be updated when sync will be completed
    } else {
        (LocalTimelineInitStatus::LocallyComplete, false)
    };

    let layers_to_upload = local_files
        .difference(remote_files)
        .cloned()
        .collect::<HashSet<_>>();
    if !layers_to_upload.is_empty() {
        new_sync_tasks.push_back((
            sync_id,
            SyncTask::upload(TimelineUpload {
                layers_to_upload,
                uploaded_layers: HashSet::new(),
                metadata: local_metadata,
            }),
        ));
        // Note that status here doesn't change.
    }

    (initial_timeline_status, awaits_download)
}

fn register_sync_status(sync_start: Instant, sync_name: &str, sync_status: Option<bool>) {
    let secs_elapsed = sync_start.elapsed().as_secs_f64();
    info!("Processed a sync task in {secs_elapsed:.2} seconds");
    match sync_status {
        Some(true) => IMAGE_SYNC_TIME.with_label_values(&[sync_name, "success"]),
        Some(false) => IMAGE_SYNC_TIME.with_label_values(&[sync_name, "failure"]),
        None => return,
    }
    .observe(secs_elapsed)
}

#[cfg(test)]
mod test_utils {
    use utils::lsn::Lsn;

    use crate::repository::repo_harness::RepoHarness;

    use super::*;

    pub async fn create_local_timeline(
        harness: &RepoHarness<'_>,
        timeline_id: ZTimelineId,
        filenames: &[&str],
        metadata: TimelineMetadata,
    ) -> anyhow::Result<TimelineUpload> {
        let timeline_path = harness.timeline_path(&timeline_id);
        fs::create_dir_all(&timeline_path).await?;

        let mut layers_to_upload = HashSet::with_capacity(filenames.len());
        for &file in filenames {
            let file_path = timeline_path.join(file);
            fs::write(&file_path, dummy_contents(file).into_bytes()).await?;
            layers_to_upload.insert(file_path);
        }

        fs::write(
            metadata_path(harness.conf, timeline_id, harness.tenant_id),
            metadata.to_bytes()?,
        )
        .await?;

        Ok(TimelineUpload {
            layers_to_upload,
            uploaded_layers: HashSet::new(),
            metadata,
        })
    }

    pub fn dummy_contents(name: &str) -> String {
        format!("contents for {name}")
    }

    pub fn dummy_metadata(disk_consistent_lsn: Lsn) -> TimelineMetadata {
        TimelineMetadata::new(disk_consistent_lsn, None, None, Lsn(0), Lsn(0), Lsn(0))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::{test_utils::dummy_metadata, *};
    use utils::lsn::Lsn;

    #[test]
    fn download_sync_tasks_merge() {
        let download_1 = SyncTask::Download(SyncData::new(
            2,
            TimelineDownload {
                layers_to_skip: HashSet::from([PathBuf::from("one")]),
            },
        ));
        let download_2 = SyncTask::Download(SyncData::new(
            6,
            TimelineDownload {
                layers_to_skip: HashSet::from([PathBuf::from("two"), PathBuf::from("three")]),
            },
        ));

        let merged_download = match download_1.merge(download_2) {
            SyncTask::Download(merged_download) => merged_download,
            wrong_merge_result => panic!("Unexpected merge result: {wrong_merge_result:?}"),
        };

        assert_eq!(
            merged_download.retries, 0,
            "Merged task should have its retries counter reset"
        );

        assert_eq!(
            merged_download
                .data
                .layers_to_skip
                .into_iter()
                .collect::<BTreeSet<_>>(),
            BTreeSet::from([
                PathBuf::from("one"),
                PathBuf::from("two"),
                PathBuf::from("three")
            ]),
            "Merged download tasks should a combined set of layers to skip"
        );
    }

    #[test]
    fn upload_sync_tasks_merge() {
        let metadata_1 = dummy_metadata(Lsn(1));
        let metadata_2 = dummy_metadata(Lsn(2));
        assert!(metadata_2.disk_consistent_lsn() > metadata_1.disk_consistent_lsn());

        let upload_1 = SyncTask::Upload(SyncData::new(
            2,
            TimelineUpload {
                layers_to_upload: HashSet::from([PathBuf::from("one")]),
                uploaded_layers: HashSet::from([PathBuf::from("u_one")]),
                metadata: metadata_1,
            },
        ));
        let upload_2 = SyncTask::Upload(SyncData::new(
            6,
            TimelineUpload {
                layers_to_upload: HashSet::from([PathBuf::from("two"), PathBuf::from("three")]),
                uploaded_layers: HashSet::from([PathBuf::from("u_two")]),
                metadata: metadata_2.clone(),
            },
        ));

        let merged_upload = match upload_1.merge(upload_2) {
            SyncTask::Upload(merged_upload) => merged_upload,
            wrong_merge_result => panic!("Unexpected merge result: {wrong_merge_result:?}"),
        };

        assert_eq!(
            merged_upload.retries, 0,
            "Merged task should have its retries counter reset"
        );

        let upload = merged_upload.data;
        assert_eq!(
            upload.layers_to_upload.into_iter().collect::<BTreeSet<_>>(),
            BTreeSet::from([
                PathBuf::from("one"),
                PathBuf::from("two"),
                PathBuf::from("three")
            ]),
            "Merged upload tasks should a combined set of layers to upload"
        );

        assert_eq!(
            upload.uploaded_layers.into_iter().collect::<BTreeSet<_>>(),
            BTreeSet::from([PathBuf::from("u_one"), PathBuf::from("u_two"),]),
            "Merged upload tasks should a combined set of uploaded layers"
        );

        assert_eq!(
            upload.metadata, metadata_2,
            "Merged upload tasks should have a metadata with biggest disk_consistent_lsn"
        );
    }

    #[test]
    fn upload_and_download_sync_tasks_merge() {
        let download_data = SyncData::new(
            3,
            TimelineDownload {
                layers_to_skip: HashSet::from([PathBuf::from("d_one")]),
            },
        );

        let upload_data = SyncData::new(
            2,
            TimelineUpload {
                layers_to_upload: HashSet::from([PathBuf::from("u_one")]),
                uploaded_layers: HashSet::from([PathBuf::from("u_one_2")]),
                metadata: dummy_metadata(Lsn(1)),
            },
        );

        let (merged_download, merged_upload) = match SyncTask::Download(download_data.clone())
            .merge(SyncTask::Upload(upload_data.clone()))
        {
            SyncTask::DownloadAndUpload(merged_download, merged_upload) => {
                (merged_download, merged_upload)
            }
            wrong_merge_result => panic!("Unexpected merge result: {wrong_merge_result:?}"),
        };

        assert_eq!(
            merged_download, download_data,
            "When upload and dowload are merged, both should be unchanged"
        );
        assert_eq!(
            merged_upload, upload_data,
            "When upload and dowload are merged, both should be unchanged"
        );
    }

    #[test]
    fn uploaddownload_and_upload_sync_tasks_merge() {
        let download_data = SyncData::new(
            3,
            TimelineDownload {
                layers_to_skip: HashSet::from([PathBuf::from("d_one")]),
            },
        );

        let metadata_1 = dummy_metadata(Lsn(5));
        let metadata_2 = dummy_metadata(Lsn(2));
        assert!(metadata_1.disk_consistent_lsn() > metadata_2.disk_consistent_lsn());

        let upload_download = SyncTask::DownloadAndUpload(
            download_data.clone(),
            SyncData::new(
                2,
                TimelineUpload {
                    layers_to_upload: HashSet::from([PathBuf::from("one")]),
                    uploaded_layers: HashSet::from([PathBuf::from("u_one")]),
                    metadata: metadata_1.clone(),
                },
            ),
        );

        let new_upload = SyncTask::Upload(SyncData::new(
            6,
            TimelineUpload {
                layers_to_upload: HashSet::from([PathBuf::from("two"), PathBuf::from("three")]),
                uploaded_layers: HashSet::from([PathBuf::from("u_two")]),
                metadata: metadata_2,
            },
        ));

        let (merged_download, merged_upload) = match upload_download.merge(new_upload) {
            SyncTask::DownloadAndUpload(merged_download, merged_upload) => {
                (merged_download, merged_upload)
            }
            wrong_merge_result => panic!("Unexpected merge result: {wrong_merge_result:?}"),
        };

        assert_eq!(
            merged_download, download_data,
            "When uploaddowload and upload tasks are merged, download should be unchanged"
        );

        assert_eq!(
            merged_upload.retries, 0,
            "Merged task should have its retries counter reset"
        );
        let upload = merged_upload.data;
        assert_eq!(
            upload.layers_to_upload.into_iter().collect::<BTreeSet<_>>(),
            BTreeSet::from([
                PathBuf::from("one"),
                PathBuf::from("two"),
                PathBuf::from("three")
            ]),
            "Merged upload tasks should a combined set of layers to upload"
        );

        assert_eq!(
            upload.uploaded_layers.into_iter().collect::<BTreeSet<_>>(),
            BTreeSet::from([PathBuf::from("u_one"), PathBuf::from("u_two"),]),
            "Merged upload tasks should a combined set of uploaded layers"
        );

        assert_eq!(
            upload.metadata, metadata_1,
            "Merged upload tasks should have a metadata with biggest disk_consistent_lsn"
        );
    }

    #[test]
    fn uploaddownload_and_download_sync_tasks_merge() {
        let upload_data = SyncData::new(
            22,
            TimelineUpload {
                layers_to_upload: HashSet::from([PathBuf::from("one")]),
                uploaded_layers: HashSet::from([PathBuf::from("u_one")]),
                metadata: dummy_metadata(Lsn(22)),
            },
        );

        let upload_download = SyncTask::DownloadAndUpload(
            SyncData::new(
                2,
                TimelineDownload {
                    layers_to_skip: HashSet::from([PathBuf::from("one")]),
                },
            ),
            upload_data.clone(),
        );

        let new_download = SyncTask::Download(SyncData::new(
            6,
            TimelineDownload {
                layers_to_skip: HashSet::from([PathBuf::from("two"), PathBuf::from("three")]),
            },
        ));

        let (merged_download, merged_upload) = match upload_download.merge(new_download) {
            SyncTask::DownloadAndUpload(merged_download, merged_upload) => {
                (merged_download, merged_upload)
            }
            wrong_merge_result => panic!("Unexpected merge result: {wrong_merge_result:?}"),
        };

        assert_eq!(
            merged_upload, upload_data,
            "When uploaddowload and download tasks are merged, upload should be unchanged"
        );

        assert_eq!(
            merged_download.retries, 0,
            "Merged task should have its retries counter reset"
        );
        assert_eq!(
            merged_download
                .data
                .layers_to_skip
                .into_iter()
                .collect::<BTreeSet<_>>(),
            BTreeSet::from([
                PathBuf::from("one"),
                PathBuf::from("two"),
                PathBuf::from("three")
            ]),
            "Merged download tasks should a combined set of layers to skip"
        );
    }

    #[test]
    fn uploaddownload_sync_tasks_merge() {
        let metadata_1 = dummy_metadata(Lsn(1));
        let metadata_2 = dummy_metadata(Lsn(2));
        assert!(metadata_2.disk_consistent_lsn() > metadata_1.disk_consistent_lsn());

        let upload_download = SyncTask::DownloadAndUpload(
            SyncData::new(
                2,
                TimelineDownload {
                    layers_to_skip: HashSet::from([PathBuf::from("one")]),
                },
            ),
            SyncData::new(
                2,
                TimelineUpload {
                    layers_to_upload: HashSet::from([PathBuf::from("one")]),
                    uploaded_layers: HashSet::from([PathBuf::from("u_one")]),
                    metadata: metadata_1,
                },
            ),
        );
        let new_upload_download = SyncTask::DownloadAndUpload(
            SyncData::new(
                6,
                TimelineDownload {
                    layers_to_skip: HashSet::from([PathBuf::from("two"), PathBuf::from("three")]),
                },
            ),
            SyncData::new(
                6,
                TimelineUpload {
                    layers_to_upload: HashSet::from([PathBuf::from("two"), PathBuf::from("three")]),
                    uploaded_layers: HashSet::from([PathBuf::from("u_two")]),
                    metadata: metadata_2.clone(),
                },
            ),
        );

        let (merged_download, merged_upload) = match upload_download.merge(new_upload_download) {
            SyncTask::DownloadAndUpload(merged_download, merged_upload) => {
                (merged_download, merged_upload)
            }
            wrong_merge_result => panic!("Unexpected merge result: {wrong_merge_result:?}"),
        };

        assert_eq!(
            merged_download.retries, 0,
            "Merged task should have its retries counter reset"
        );
        assert_eq!(
            merged_download
                .data
                .layers_to_skip
                .into_iter()
                .collect::<BTreeSet<_>>(),
            BTreeSet::from([
                PathBuf::from("one"),
                PathBuf::from("two"),
                PathBuf::from("three")
            ]),
            "Merged download tasks should a combined set of layers to skip"
        );

        assert_eq!(
            merged_upload.retries, 0,
            "Merged task should have its retries counter reset"
        );
        let upload = merged_upload.data;
        assert_eq!(
            upload.layers_to_upload.into_iter().collect::<BTreeSet<_>>(),
            BTreeSet::from([
                PathBuf::from("one"),
                PathBuf::from("two"),
                PathBuf::from("three")
            ]),
            "Merged upload tasks should a combined set of layers to upload"
        );

        assert_eq!(
            upload.uploaded_layers.into_iter().collect::<BTreeSet<_>>(),
            BTreeSet::from([PathBuf::from("u_one"), PathBuf::from("u_two"),]),
            "Merged upload tasks should a combined set of uploaded layers"
        );

        assert_eq!(
            upload.metadata, metadata_2,
            "Merged upload tasks should have a metadata with biggest disk_consistent_lsn"
        );
    }
}

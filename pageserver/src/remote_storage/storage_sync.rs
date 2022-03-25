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
//! Every task in a batch processed concurrently, which is possible due to incremental nature of the timelines:
//! it's not asserted, but assumed that timeline's checkpoints only add the files locally, not removing or amending the existing ones.
//! Only GC removes local timeline files, the GC support is not added to sync currently,
//! yet downloading extra files is not critically bad at this stage, GC can remove those again.
//!
//! During the loop startup, an initial [`RemoteTimelineIndex`] state is constructed via listing the remote storage contents.
//! It's enough to poll the remote state once on startup only, due to agreement that the pageserver has
//! an exclusive write access to the remote storage: new files appear in the storage only after the same
//! pageserver writes them.
//! It's important to do so, since storages like S3 can get slower and more expensive as the number of files grows.
//! The index state is used to issue initial sync tasks, if needed:
//! * all timelines with local state behind the remote gets download tasks scheduled.
//! Such timelines are considered "remote" before the download succeeds, so a number of operations (gc, checkpoints) on that timeline are unavailable.
//! * all never local state gets scheduled for upload, such timelines are "local" and fully operational
//! * the rest of the remote timelines are reported to pageserver, but not downloaded before they are actually accessed in pageserver,
//! it may schedule the download on such occasions.
//!
//! The synchronization unit is an archive: a set of layer files and a special metadata file, all compressed into a blob.
//! Currently, there's no way to process an archive partially, if the archive processing fails, it has to be started from zero next time again.
//! An archive contains set of files of a certain timeline, added during checkpoint(s) and the timeline metadata at that moment.
//! The archive contains that metadata's `disk_consistent_lsn` in its name, to be able to restore partial index information from just a remote storage file list.
//! The index is created at startup (possible due to exclusive ownership over the remote storage by the pageserver) and keeps track of which files were stored
//! in what remote archives.
//! Among other tasks, the index is used to prevent invalid uploads and non-existing downloads on demand.
//! Refer to [`compression`] and [`index`] for more details on the archives and index respectively.
//!
//! The list construction is currently the only place where the storage sync can return an [`Err`] to the user.
//! New sync tasks are accepted via [`schedule_timeline_checkpoint_upload`] and [`schedule_timeline_download`] functions,
//! disregarding of the corresponding loop startup.
//! It's up to the caller to avoid synchronizations if the loop is disabled: otherwise, the sync tasks will be ignored.
//! After the initial state is loaded into memory and the loop starts, any further [`Err`] results do not stop the loop, but rather
//! reschedule the same task, with possibly less files to sync:
//! * download tasks currently never replace existing local file with metadata file as an exception
//! (but this is a subject to change when checksum checks are implemented: all files could get overwritten on a checksum mismatch)
//! * download tasks carry the information of skipped acrhives, so resubmissions are not downloading successfully processed archives again
//!
//! Not every upload of the same timeline gets processed: if the checkpoint with the same `disk_consistent_lsn` was already uploaded, no reuploads happen, as checkpoints
//! are considered to be immutable. The order of `lsn` during upload submissions is allowed to be arbitrary and not required to be ascending.
//! Refer to [`upload`] and [`download`] for more details.
//!
//! Current uploads are per-checkpoint and don't accumulate any data with optimal size for storing on S3.
//! The downloaded archives get processed sequentially, from smaller `disk_consistent_lsn` to larger, with metadata files being added as last.
//! The archive unpacking is designed to unpack metadata as the last file, so the risk of leaving the corrupt timeline due to uncompression error is small (while not eliminated entirely and that should be improved).
//! There's a reschedule threshold that evicts tasks that fail too much and stops the corresponding timeline so it does not diverge from the state on the remote storage.
//! Among other pageserver-specific changes to such evicted timelines, no uploads are expected to come from them to ensure the remote storage state does not get corrupted.
//!
//! Synchronization never removes any local from pageserver workdir or remote files from the remote storage, yet there could be overwrites of the same files (metadata file updates; future checksum mismatch fixes).
//! NOTE: No real contents or checksum check happens right now and is a subject to improve later.
//!
//! After the whole timeline is downloaded, [`crate::tenant_mgr::apply_timeline_sync_status_updates`] function is used to update pageserver memory stage for the timeline processed.
//!
//! When pageserver signals shutdown, current sync task gets finished and the loop exists.

/// Expose the module for a binary CLI tool that deals with the corresponding blobs.
pub mod compression;
mod download;
pub mod index;
mod upload;

use std::{
    collections::{BTreeSet, HashMap, VecDeque},
    num::{NonZeroU32, NonZeroUsize},
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::{bail, Context};
use futures::stream::{FuturesUnordered, StreamExt};
use lazy_static::lazy_static;
use tokio::{
    runtime::Runtime,
    sync::{
        mpsc::{self, UnboundedReceiver},
        RwLock,
    },
    time::{Duration, Instant},
};
use tracing::*;

use self::{
    compression::ArchiveHeader,
    download::{download_timeline, DownloadedTimeline},
    index::{
        ArchiveDescription, ArchiveId, RemoteTimeline, RemoteTimelineIndex, TimelineIndexEntry,
        TimelineIndexEntryInner,
    },
    upload::upload_timeline_checkpoint,
};
use super::{
    LocalTimelineInitStatus, LocalTimelineInitStatuses, RemoteStorage, SyncStartupData,
    ZTenantTimelineId,
};
use crate::{
    config::PageServerConf, layered_repository::metadata::TimelineMetadata,
    remote_storage::storage_sync::compression::read_archive_header,
    repository::TimelineSyncStatusUpdate, tenant_mgr::apply_timeline_sync_status_updates,
    thread_mgr, thread_mgr::ThreadKind,
};

use zenith_metrics::{
    register_histogram_vec, register_int_counter, register_int_gauge, HistogramVec, IntCounter,
    IntGauge,
};
use zenith_utils::zid::{ZTenantId, ZTimelineId};

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
        collections::HashMap,
        sync::atomic::{AtomicUsize, Ordering},
    };

    use anyhow::anyhow;
    use once_cell::sync::OnceCell;
    use tokio::sync::mpsc::{error::TryRecvError, UnboundedReceiver, UnboundedSender};
    use tracing::{debug, warn};

    use super::SyncTask;

    static SENDER: OnceCell<UnboundedSender<SyncTask>> = OnceCell::new();
    static LENGTH: AtomicUsize = AtomicUsize::new(0);

    /// Initializes the queue with the given sender channel that is used to put the tasks into later.
    /// Errors if called more than once.
    pub fn init(sender: UnboundedSender<SyncTask>) -> anyhow::Result<()> {
        SENDER
            .set(sender)
            .map_err(|_sender| anyhow!("sync queue was already initialized"))?;
        Ok(())
    }

    /// Adds a new task to the queue, if the queue was initialized, returning `true` on success.
    /// On any error, or if the queue was not initialized, the task gets dropped (not scheduled) and `false` is returned.
    pub fn push(new_task: SyncTask) -> bool {
        if let Some(sender) = SENDER.get() {
            match sender.send(new_task) {
                Err(e) => {
                    warn!(
                        "Failed to enqueue a sync task: the receiver is dropped: {}",
                        e
                    );
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
    pub async fn next_task(receiver: &mut UnboundedReceiver<SyncTask>) -> Option<SyncTask> {
        let task = receiver.recv().await;
        if task.is_some() {
            LENGTH.fetch_sub(1, Ordering::Relaxed);
        }
        task
    }

    /// Fetches a task batch, not bigger than the given limit.
    /// Not blocking, can return fewer tasks if the queue does not contain enough.
    /// Duplicate entries are eliminated and not considered in batch size calculations.
    pub async fn next_task_batch(
        receiver: &mut UnboundedReceiver<SyncTask>,
        mut max_batch_size: usize,
    ) -> Vec<SyncTask> {
        if max_batch_size == 0 {
            return Vec::new();
        }
        let mut tasks = HashMap::with_capacity(max_batch_size);

        loop {
            match receiver.try_recv() {
                Ok(new_task) => {
                    LENGTH.fetch_sub(1, Ordering::Relaxed);
                    if tasks.insert(new_task.sync_id, new_task).is_none() {
                        max_batch_size -= 1;
                        if max_batch_size == 0 {
                            break;
                        }
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

        tasks.into_values().collect()
    }

    /// Length of the queue, assuming that all receiver counterparts were only called using the queue api.
    pub fn len() -> usize {
        LENGTH.load(Ordering::Relaxed)
    }
}

/// A task to run in the async download/upload loop.
/// Limited by the number of retries, after certain threshold the failing task gets evicted and the timeline disabled.
#[derive(Debug, Clone)]
pub struct SyncTask {
    sync_id: ZTenantTimelineId,
    retries: u32,
    kind: SyncKind,
}

impl SyncTask {
    fn new(sync_id: ZTenantTimelineId, retries: u32, kind: SyncKind) -> Self {
        Self {
            sync_id,
            retries,
            kind,
        }
    }
}

#[derive(Debug, Clone)]
enum SyncKind {
    /// A certain amount of images (archive files) to download.
    Download(TimelineDownload),
    /// A checkpoint outcome with possible local file updates that need actualization in the remote storage.
    /// Not necessary more fresh than the one already uploaded.
    Upload(NewCheckpoint),
}

impl SyncKind {
    fn sync_name(&self) -> &'static str {
        match self {
            Self::Download(_) => "download",
            Self::Upload(_) => "upload",
        }
    }
}

/// Local timeline files for upload, appeared after the new checkpoint.
/// Current checkpoint design assumes new files are added only, no deletions or amendment happens.
#[derive(Debug, Clone)]
pub struct NewCheckpoint {
    /// layer file paths in the pageserver workdir, that were added for the corresponding checkpoint.
    layers: Vec<PathBuf>,
    metadata: TimelineMetadata,
}

/// Info about the remote image files.
#[derive(Debug, Clone)]
struct TimelineDownload {
    files_to_skip: Arc<BTreeSet<PathBuf>>,
    archives_to_skip: BTreeSet<ArchiveId>,
}

/// Adds the new checkpoint files as an upload sync task to the queue.
/// On task failure, it gets retried again from the start a number of times.
///
/// Ensure that the loop is started otherwise the task is never processed.
pub fn schedule_timeline_checkpoint_upload(
    tenant_id: ZTenantId,
    timeline_id: ZTimelineId,
    layers: Vec<PathBuf>,
    metadata: TimelineMetadata,
) {
    if layers.is_empty() {
        debug!("Skipping empty layers upload task");
        return;
    }

    if !sync_queue::push(SyncTask::new(
        ZTenantTimelineId {
            tenant_id,
            timeline_id,
        },
        0,
        SyncKind::Upload(NewCheckpoint { layers, metadata }),
    )) {
        warn!(
            "Could not send an upload task for tenant {}, timeline {}",
            tenant_id, timeline_id
        )
    } else {
        warn!(
            "Could not send an upload task for tenant {}, timeline {}: the sync queue is not initialized",
            tenant_id, timeline_id
        )
    }
}

/// Requests the download of the entire timeline for a given tenant.
/// No existing local files are currently owerwritten, except the metadata file.
/// The timeline downloads checkpoint archives, from the earliest `disc_consistent_lsn` to the latest,
/// replacing the metadata file as the lasat file in every archive uncompression result.
///
/// On any failure, the task gets retried, omitting already downloaded archives and files
/// (yet requiring to download the entire archive even if it got partially extracted before the failure).
///
/// Ensure that the loop is started otherwise the task is never processed.
pub fn schedule_timeline_download(tenant_id: ZTenantId, timeline_id: ZTimelineId) {
    debug!(
        "Scheduling timeline download for tenant {}, timeline {}",
        tenant_id, timeline_id
    );
    sync_queue::push(SyncTask::new(
        ZTenantTimelineId {
            tenant_id,
            timeline_id,
        },
        0,
        SyncKind::Download(TimelineDownload {
            files_to_skip: Arc::new(BTreeSet::new()),
            archives_to_skip: BTreeSet::new(),
        }),
    ));
}

/// Uses a remote storage given to start the storage sync loop.
/// See module docs for loop step description.
pub(super) fn spawn_storage_sync_thread<
    P: std::fmt::Debug + Send + Sync + 'static,
    S: RemoteStorage<StoragePath = P> + Send + Sync + 'static,
>(
    conf: &'static PageServerConf,
    local_timeline_files: HashMap<ZTenantTimelineId, (TimelineMetadata, Vec<PathBuf>)>,
    storage: S,
    max_concurrent_sync: NonZeroUsize,
    max_sync_errors: NonZeroU32,
) -> anyhow::Result<SyncStartupData> {
    let (sender, receiver) = mpsc::unbounded_channel();
    sync_queue::init(sender)?;

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .context("Failed to create storage sync runtime")?;

    let download_paths = runtime
        // TODO could take long time, consider [de]serializing [`RemoteTimelineIndex`] instead
        .block_on(storage.list())
        .context("Failed to list remote storage files")?
        .into_iter()
        .filter_map(|remote_path| match storage.local_path(&remote_path) {
            Ok(local_path) => Some(local_path),
            Err(e) => {
                error!(
                    "Failed to find local path for remote path {:?}: {:?}",
                    remote_path, e
                );
                None
            }
        });
    let mut remote_index =
        RemoteTimelineIndex::try_parse_descriptions_from_paths(conf, download_paths);

    let local_timeline_init_statuses =
        schedule_first_sync_tasks(&mut remote_index, local_timeline_files);
    let remote_index = Arc::new(RwLock::new(remote_index));
    let remote_index_cloned = Arc::clone(&remote_index);
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
                remote_index_cloned,
                storage,
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

enum LoopStep {
    SyncStatusUpdates(HashMap<ZTenantId, HashMap<ZTimelineId, TimelineSyncStatusUpdate>>),
    Shutdown,
}

#[allow(clippy::too_many_arguments)]
fn storage_sync_loop<
    P: std::fmt::Debug + Send + Sync + 'static,
    S: RemoteStorage<StoragePath = P> + Send + Sync + 'static,
>(
    runtime: Runtime,
    conf: &'static PageServerConf,
    mut receiver: UnboundedReceiver<SyncTask>,
    index: Arc<RwLock<RemoteTimelineIndex>>,
    storage: S,
    max_concurrent_sync: NonZeroUsize,
    max_sync_errors: NonZeroU32,
) {
    let remote_assets = Arc::new((storage, Arc::clone(&index)));
    loop {
        let index = Arc::clone(&index);
        let loop_step = runtime.block_on(async {
            tokio::select! {
                new_timeline_states = loop_step(
                    conf,
                    &mut receiver,
                    Arc::clone(&remote_assets),
                    max_concurrent_sync,
                    max_sync_errors,
                )
                .instrument(debug_span!("storage_sync_loop_step")) => LoopStep::SyncStatusUpdates(new_timeline_states),
                _ = thread_mgr::shutdown_watcher() => LoopStep::Shutdown,
            }
        });

        match loop_step {
            LoopStep::SyncStatusUpdates(new_timeline_states) => {
                // Batch timeline download registration to ensure that the external registration code won't block any running tasks before.
                apply_timeline_sync_status_updates(conf, index, new_timeline_states);
                debug!("Sync loop step completed");
            }
            LoopStep::Shutdown => {
                debug!("Shutdown requested, stopping");
                break;
            }
        }
    }
}

async fn loop_step<
    P: std::fmt::Debug + Send + Sync + 'static,
    S: RemoteStorage<StoragePath = P> + Send + Sync + 'static,
>(
    conf: &'static PageServerConf,
    receiver: &mut UnboundedReceiver<SyncTask>,
    remote_assets: Arc<(S, Arc<RwLock<RemoteTimelineIndex>>)>,
    max_concurrent_sync: NonZeroUsize,
    max_sync_errors: NonZeroU32,
) -> HashMap<ZTenantId, HashMap<ZTimelineId, TimelineSyncStatusUpdate>> {
    let max_concurrent_sync = max_concurrent_sync.get();
    let mut next_tasks = Vec::new();

    // request the first task in blocking fashion to do less meaningless work
    if let Some(first_task) = sync_queue::next_task(receiver).await {
        next_tasks.push(first_task);
    } else {
        debug!("Shutdown requested, stopping");
        return HashMap::new();
    };
    next_tasks.extend(
        sync_queue::next_task_batch(receiver, max_concurrent_sync - 1)
            .await
            .into_iter(),
    );

    let remaining_queue_length = sync_queue::len();
    debug!(
        "Processing {} tasks in batch, more tasks left to process: {}",
        next_tasks.len(),
        remaining_queue_length
    );
    REMAINING_SYNC_ITEMS.set(remaining_queue_length as i64);

    let mut task_batch = next_tasks
        .into_iter()
        .map(|task| async {
            let sync_id = task.sync_id;
            let attempt = task.retries;
            let sync_name = task.kind.sync_name();

            let extra_step = match tokio::spawn(
                process_task(conf, Arc::clone(&remote_assets), task, max_sync_errors)
                    .instrument(debug_span!("", sync_id = %sync_id, attempt, sync_name)),
            )
            .await
            {
                Ok(extra_step) => extra_step,
                Err(e) => {
                    error!(
                        "Failed to process storage sync task for tenant {}, timeline {}: {:?}",
                        sync_id.tenant_id, sync_id.timeline_id, e
                    );
                    None
                }
            };
            (sync_id, extra_step)
        })
        .collect::<FuturesUnordered<_>>();

    let mut new_timeline_states: HashMap<
        ZTenantId,
        HashMap<ZTimelineId, TimelineSyncStatusUpdate>,
    > = HashMap::with_capacity(max_concurrent_sync);
    while let Some((sync_id, state_update)) = task_batch.next().await {
        debug!("Finished storage sync task for sync id {}", sync_id);
        if let Some(state_update) = state_update {
            let ZTenantTimelineId {
                tenant_id,
                timeline_id,
            } = sync_id;
            new_timeline_states
                .entry(tenant_id)
                .or_default()
                .insert(timeline_id, state_update);
        }
    }

    new_timeline_states
}

async fn process_task<
    P: std::fmt::Debug + Send + Sync + 'static,
    S: RemoteStorage<StoragePath = P> + Send + Sync + 'static,
>(
    conf: &'static PageServerConf,
    remote_assets: Arc<(S, Arc<RwLock<RemoteTimelineIndex>>)>,
    task: SyncTask,
    max_sync_errors: NonZeroU32,
) -> Option<TimelineSyncStatusUpdate> {
    if task.retries > max_sync_errors.get() {
        error!(
            "Evicting task {:?} that failed {} times, exceeding the error threshold",
            task.kind, task.retries
        );
        FATAL_TASK_FAILURES.inc();
        // FIXME (rodionov) this can potentially leave holes in timeline uploads
        //    planneed to be fixed as part of https://github.com/zenithdb/zenith/issues/977
        return None;
    }

    if task.retries > 0 {
        let seconds_to_wait = 2.0_f64.powf(task.retries as f64 - 1.0).min(30.0);
        debug!(
            "Waiting {} seconds before starting the task",
            seconds_to_wait
        );
        tokio::time::sleep(Duration::from_secs_f64(seconds_to_wait)).await;
    }

    let remote_index = Arc::clone(&remote_assets.1);

    let sync_start = Instant::now();
    let sync_name = task.kind.sync_name();
    match task.kind {
        SyncKind::Download(download_data) => {
            let download_result = download_timeline(
                conf,
                remote_assets,
                task.sync_id,
                download_data,
                task.retries + 1,
            )
            .await;

            match download_result {
                DownloadedTimeline::Abort => {
                    register_sync_status(sync_start, sync_name, None);
                    remote_index
                        .write()
                        .await
                        .set_awaits_download(&task.sync_id, false)
                        .expect("timeline should be present in remote index");
                    None
                }
                DownloadedTimeline::FailedAndRescheduled => {
                    register_sync_status(sync_start, sync_name, Some(false));
                    None
                }
                DownloadedTimeline::Successful => {
                    register_sync_status(sync_start, sync_name, Some(true));
                    remote_index
                        .write()
                        .await
                        .set_awaits_download(&task.sync_id, false)
                        .expect("timeline should be present in remote index");
                    Some(TimelineSyncStatusUpdate::Downloaded)
                }
            }
        }
        SyncKind::Upload(layer_upload) => {
            let sync_status = upload_timeline_checkpoint(
                conf,
                remote_assets,
                task.sync_id,
                layer_upload,
                task.retries + 1,
            )
            .await;
            register_sync_status(sync_start, sync_name, sync_status);
            None
        }
    }
}

fn schedule_first_sync_tasks(
    index: &mut RemoteTimelineIndex,
    local_timeline_files: HashMap<ZTenantTimelineId, (TimelineMetadata, Vec<PathBuf>)>,
) -> LocalTimelineInitStatuses {
    let mut local_timeline_init_statuses = LocalTimelineInitStatuses::new();

    let mut new_sync_tasks =
        VecDeque::with_capacity(local_timeline_files.len().max(local_timeline_files.len()));

    for (sync_id, (local_metadata, local_files)) in local_timeline_files {
        let ZTenantTimelineId {
            tenant_id,
            timeline_id,
        } = sync_id;
        match index.timeline_entry_mut(&sync_id) {
            Some(index_entry) => {
                let (timeline_status, awaits_download) = compare_local_and_remote_timeline(
                    &mut new_sync_tasks,
                    sync_id,
                    local_metadata,
                    local_files,
                    index_entry,
                );
                let was_there = local_timeline_init_statuses
                    .entry(tenant_id)
                    .or_default()
                    .insert(timeline_id, timeline_status);

                if was_there.is_some() {
                    // defensive check
                    warn!(
                        "Overwriting timeline init sync status. Status {:?} Timeline {}",
                        timeline_status, timeline_id
                    );
                }
                index_entry.set_awaits_download(awaits_download);
            }
            None => {
                // TODO (rodionov) does this mean that we've crashed during tenant creation?
                //  is it safe to upload this checkpoint? could it be half broken?
                new_sync_tasks.push_back(SyncTask::new(
                    sync_id,
                    0,
                    SyncKind::Upload(NewCheckpoint {
                        layers: local_files,
                        metadata: local_metadata,
                    }),
                ));
                local_timeline_init_statuses
                    .entry(tenant_id)
                    .or_default()
                    .insert(timeline_id, LocalTimelineInitStatus::LocallyComplete);
            }
        }
    }

    new_sync_tasks.into_iter().for_each(|task| {
        sync_queue::push(task);
    });
    local_timeline_init_statuses
}

fn compare_local_and_remote_timeline(
    new_sync_tasks: &mut VecDeque<SyncTask>,
    sync_id: ZTenantTimelineId,
    local_metadata: TimelineMetadata,
    local_files: Vec<PathBuf>,
    remote_entry: &TimelineIndexEntry,
) -> (LocalTimelineInitStatus, bool) {
    let local_lsn = local_metadata.disk_consistent_lsn();
    let uploads = remote_entry.uploaded_checkpoints();

    let mut initial_timeline_status = LocalTimelineInitStatus::LocallyComplete;

    let mut awaits_download = false;
    // TODO probably here we need more sophisticated logic,
    //   if more data is available remotely can we just download whats there?
    //   without trying to upload something. It may be tricky, needs further investigation.
    //   For now looks strange that we can request upload
    //   and dowload for the same timeline simultaneously.
    //   (upload needs to be only for previously unsynced files, not whole timeline dir).
    //   If one of the tasks fails they will be reordered in the queue which can lead
    //   to timeline being stuck in evicted state
    if !uploads.contains(&local_lsn) {
        new_sync_tasks.push_back(SyncTask::new(
            sync_id,
            0,
            SyncKind::Upload(NewCheckpoint {
                layers: local_files.clone(),
                metadata: local_metadata,
            }),
        ));
        // Note that status here doesnt change.
    }

    let uploads_count = uploads.len();
    let archives_to_skip: BTreeSet<ArchiveId> = uploads
        .into_iter()
        .filter(|upload_lsn| upload_lsn <= &local_lsn)
        .map(ArchiveId)
        .collect();
    if archives_to_skip.len() != uploads_count {
        new_sync_tasks.push_back(SyncTask::new(
            sync_id,
            0,
            SyncKind::Download(TimelineDownload {
                files_to_skip: Arc::new(local_files.into_iter().collect()),
                archives_to_skip,
            }),
        ));
        initial_timeline_status = LocalTimelineInitStatus::NeedsSync;
        awaits_download = true;
        // we do not need to manupulate with remote consistent lsn here
        // because it will be updated when sync will be completed
    }
    (initial_timeline_status, awaits_download)
}

fn register_sync_status(sync_start: Instant, sync_name: &str, sync_status: Option<bool>) {
    let secs_elapsed = sync_start.elapsed().as_secs_f64();
    debug!("Processed a sync task in {} seconds", secs_elapsed);
    match sync_status {
        Some(true) => IMAGE_SYNC_TIME.with_label_values(&[sync_name, "success"]),
        Some(false) => IMAGE_SYNC_TIME.with_label_values(&[sync_name, "failure"]),
        None => return,
    }
    .observe(secs_elapsed)
}

async fn fetch_full_index<
    P: Send + Sync + 'static,
    S: RemoteStorage<StoragePath = P> + Send + Sync + 'static,
>(
    (storage, index): &(S, Arc<RwLock<RemoteTimelineIndex>>),
    timeline_dir: &Path,
    id: ZTenantTimelineId,
) -> anyhow::Result<RemoteTimeline> {
    let index_read = index.read().await;
    let full_index = match index_read.timeline_entry(&id).map(|e| e.inner()) {
        None => bail!("Timeline not found for sync id {}", id),
        Some(TimelineIndexEntryInner::Full(_)) => {
            bail!("Index is already populated for sync id {}", id)
        }
        Some(TimelineIndexEntryInner::Description(description)) => {
            let mut archive_header_downloads = FuturesUnordered::new();
            for (archive_id, description) in description {
                archive_header_downloads.push(async move {
                    let header = download_archive_header(storage, timeline_dir, description)
                        .await
                        .map_err(|e| (e, archive_id))?;
                    Ok((archive_id, description.header_size, header))
                });
            }

            let mut full_index = RemoteTimeline::empty();
            while let Some(header_data) = archive_header_downloads.next().await {
                match header_data {
                    Ok((archive_id, header_size, header)) => full_index.update_archive_contents(archive_id.0, header, header_size),
                    Err((e, archive_id)) => bail!(
                        "Failed to download archive header for tenant {}, timeline {}, archive for Lsn {}: {}",
                        id.tenant_id, id.timeline_id, archive_id.0,
                        e
                    ),
                }
            }
            full_index
        }
    };
    drop(index_read); // tokio rw lock is not upgradeable
    let mut index_write = index.write().await;
    index_write
        .upgrade_timeline_entry(&id, full_index.clone())
        .context("cannot upgrade timeline entry in remote index")?;
    Ok(full_index)
}

async fn download_archive_header<
    P: Send + Sync + 'static,
    S: RemoteStorage<StoragePath = P> + Send + Sync + 'static,
>(
    storage: &S,
    timeline_dir: &Path,
    description: &ArchiveDescription,
) -> anyhow::Result<ArchiveHeader> {
    let mut header_buf = std::io::Cursor::new(Vec::new());
    let remote_path = storage.storage_path(&timeline_dir.join(&description.archive_name))?;
    storage
        .download_range(
            &remote_path,
            0,
            Some(description.header_size),
            &mut header_buf,
        )
        .await?;
    let header_buf = header_buf.into_inner();
    let header = read_archive_header(&description.archive_name, &mut header_buf.as_slice()).await?;
    Ok(header)
}

#[cfg(test)]
mod test_utils {
    use std::{
        collections::{BTreeMap, BTreeSet},
        fs,
    };

    use super::*;
    use crate::{
        layered_repository::metadata::metadata_path, remote_storage::local_fs::LocalFs,
        repository::repo_harness::RepoHarness,
    };
    use zenith_utils::lsn::Lsn;

    #[track_caller]
    pub async fn ensure_correct_timeline_upload(
        harness: &RepoHarness<'_>,
        remote_assets: Arc<(LocalFs, Arc<RwLock<RemoteTimelineIndex>>)>,
        timeline_id: ZTimelineId,
        new_upload: NewCheckpoint,
    ) {
        let sync_id = ZTenantTimelineId::new(harness.tenant_id, timeline_id);
        upload_timeline_checkpoint(
            harness.conf,
            Arc::clone(&remote_assets),
            sync_id,
            new_upload.clone(),
            0,
        )
        .await;

        let (storage, index) = remote_assets.as_ref();
        assert_index_descriptions(
            index,
            RemoteTimelineIndex::try_parse_descriptions_from_paths(
                harness.conf,
                remote_assets
                    .0
                    .list()
                    .await
                    .unwrap()
                    .into_iter()
                    .map(|storage_path| storage.local_path(&storage_path).unwrap()),
            ),
        )
        .await;

        let new_remote_timeline = expect_timeline(index, sync_id).await;
        let new_remote_lsn = new_remote_timeline
            .checkpoints()
            .max()
            .expect("Remote timeline should have an lsn after reupload");
        let upload_lsn = new_upload.metadata.disk_consistent_lsn();
        assert!(
            new_remote_lsn >= upload_lsn,
            "Remote timeline after upload should have the biggest Lsn out of all uploads"
        );
        assert!(
            new_remote_timeline.contains_checkpoint_at(upload_lsn),
            "Should contain upload lsn among the remote ones"
        );

        let remote_files_after_upload = new_remote_timeline
            .stored_files(&harness.conf.timeline_path(&timeline_id, &harness.tenant_id));
        for new_uploaded_layer in &new_upload.layers {
            assert!(
                remote_files_after_upload.contains(new_uploaded_layer),
                "Remote files do not contain layer that should be uploaded: '{}'",
                new_uploaded_layer.display()
            );
        }

        assert_timeline_files_match(harness, timeline_id, new_remote_timeline);
    }

    pub async fn expect_timeline(
        index: &Arc<RwLock<RemoteTimelineIndex>>,
        sync_id: ZTenantTimelineId,
    ) -> RemoteTimeline {
        if let Some(TimelineIndexEntryInner::Full(remote_timeline)) = index
            .read()
            .await
            .timeline_entry(&sync_id)
            .map(|e| e.inner())
        {
            remote_timeline.clone()
        } else {
            panic!(
                "Expect to have a full remote timeline in the index for sync id {}",
                sync_id
            )
        }
    }

    #[track_caller]
    pub async fn assert_index_descriptions(
        index: &Arc<RwLock<RemoteTimelineIndex>>,
        expected_index_with_descriptions: RemoteTimelineIndex,
    ) {
        let index_read = index.read().await;
        let actual_sync_ids = index_read.all_sync_ids().collect::<BTreeSet<_>>();
        let expected_sync_ids = expected_index_with_descriptions
            .all_sync_ids()
            .collect::<BTreeSet<_>>();
        assert_eq!(
            actual_sync_ids, expected_sync_ids,
            "Index contains unexpected sync ids"
        );

        let mut actual_timeline_entries = BTreeMap::new();
        let mut expected_timeline_entries = BTreeMap::new();
        for sync_id in actual_sync_ids {
            actual_timeline_entries.insert(
                sync_id,
                index_read.timeline_entry(&sync_id).unwrap().clone(),
            );
            expected_timeline_entries.insert(
                sync_id,
                expected_index_with_descriptions
                    .timeline_entry(&sync_id)
                    .unwrap()
                    .clone(),
            );
        }
        drop(index_read);

        for (sync_id, actual_timeline_entry) in actual_timeline_entries {
            let expected_timeline_description = expected_timeline_entries
                .remove(&sync_id)
                .unwrap_or_else(|| {
                    panic!(
                        "Failed to find an expected timeline with id {} in the index",
                        sync_id
                    )
                });
            let expected_timeline_description = match expected_timeline_description.inner() {
                TimelineIndexEntryInner::Description(description) => description,
                TimelineIndexEntryInner::Full(_) => panic!("Expected index entry for sync id {} is a full entry, while a description was expected", sync_id),
            };

            match actual_timeline_entry.inner() {
                TimelineIndexEntryInner::Description(description) => {
                    assert_eq!(
                        description, expected_timeline_description,
                        "Index contains unexpected descriptions entry for sync id {}",
                        sync_id
                    )
                }
                TimelineIndexEntryInner::Full(remote_timeline) => {
                    let expected_lsns = expected_timeline_description
                        .values()
                        .map(|description| description.disk_consistent_lsn)
                        .collect::<BTreeSet<_>>();
                    assert_eq!(
                        remote_timeline.checkpoints().collect::<BTreeSet<_>>(),
                        expected_lsns,
                        "Timeline {} should have the same checkpoints uploaded",
                        sync_id,
                    )
                }
            }
        }
    }

    pub fn assert_timeline_files_match(
        harness: &RepoHarness,
        remote_timeline_id: ZTimelineId,
        remote_timeline: RemoteTimeline,
    ) {
        let local_timeline_dir = harness.timeline_path(&remote_timeline_id);
        let local_paths = fs::read_dir(&local_timeline_dir)
            .unwrap()
            .map(|dir| dir.unwrap().path())
            .collect::<BTreeSet<_>>();
        let mut reported_remote_files = remote_timeline.stored_files(&local_timeline_dir);
        let local_metadata_path =
            metadata_path(harness.conf, remote_timeline_id, harness.tenant_id);
        let local_metadata = TimelineMetadata::from_bytes(
            &fs::read(&local_metadata_path)
                .expect("Failed to read metadata file when comparing remote and local image files"),
        )
        .expect(
            "Failed to parse metadata file contents when comparing remote and local image files",
        );
        assert!(
            remote_timeline.contains_checkpoint_at(local_metadata.disk_consistent_lsn()),
            "Should contain local lsn among the remote ones after the upload"
        );
        reported_remote_files.insert(local_metadata_path);

        assert_eq!(
            local_paths, reported_remote_files,
            "Remote image files and local image files are different, missing locally: {:?}, missing remotely: {:?}",
            reported_remote_files.difference(&local_paths).collect::<Vec<_>>(),
            local_paths.difference(&reported_remote_files).collect::<Vec<_>>(),
        );

        if let Some(remote_file) = reported_remote_files.iter().next() {
            let actual_remote_paths = fs::read_dir(
                remote_file
                    .parent()
                    .expect("Remote files are expected to have their timeline dir as parent"),
            )
            .unwrap()
            .map(|dir| dir.unwrap().path())
            .collect::<BTreeSet<_>>();

            let unreported_remote_files = actual_remote_paths
                .difference(&reported_remote_files)
                .collect::<Vec<_>>();
            assert!(
                unreported_remote_files.is_empty(),
                "Unexpected extra remote files that were not listed: {:?}",
                unreported_remote_files
            )
        }
    }

    pub fn create_local_timeline(
        harness: &RepoHarness,
        timeline_id: ZTimelineId,
        filenames: &[&str],
        metadata: TimelineMetadata,
    ) -> anyhow::Result<NewCheckpoint> {
        let timeline_path = harness.timeline_path(&timeline_id);
        fs::create_dir_all(&timeline_path)?;

        let mut layers = Vec::with_capacity(filenames.len());
        for &file in filenames {
            let file_path = timeline_path.join(file);
            fs::write(&file_path, dummy_contents(file).into_bytes())?;
            layers.push(file_path);
        }

        fs::write(
            metadata_path(harness.conf, timeline_id, harness.tenant_id),
            metadata.to_bytes()?,
        )?;

        Ok(NewCheckpoint { layers, metadata })
    }

    fn dummy_contents(name: &str) -> String {
        format!("contents for {}", name)
    }

    pub fn dummy_metadata(disk_consistent_lsn: Lsn) -> TimelineMetadata {
        TimelineMetadata::new(disk_consistent_lsn, None, None, Lsn(0), Lsn(0), Lsn(0))
    }
}

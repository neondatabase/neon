//! A synchronization logic for the [`RemoteStorage`] and the state to ensure the correct synchronizations.
//!
//! The synchronization does not aim to be immediate, instead
//! doing all the job in a separate thread asynchronously, attempting to fully replicate the
//! pageserver timeline workdir data on the remote storage in a custom format, beneficial for storing.
//!
//! [`SYNC_QUEUE`] is a deque to hold [`SyncTask`] for image upload/download.
//! The queue gets emptied by a single thread with the loop, that polls the tasks in batches (size configurable).
//! Every task in a batch processed concurrently, which is possible due to incremental nature of the timelines.
//!
//! During the loop startup, an initial loop state is constructed from all remote storage entries.
//! It's enough to poll the remote state once on startup only, due to agreement that the pageserver has
//! an exclusive write access to the remote storage: new files appear in the storage only after the same
//! pageserver writes them.
//!
//! The list construction is currently the only place where the storage sync can return an [`Err`] to the user.
//! New upload tasks are accepted via [`schedule_timeline_checkpoint_upload`] function disregarding of the corresponding loop startup,
//! it's up to the caller to avoid uploading of the new file, if that caller did not enable the loop.
//! After the initial state is loaded into memory and the loop starts, any further [`Err`] results do not stop the loop, but rather
//! reschedules the same task, with possibly less files to sync in it.
//!
//! The synchronization unit is an archive: a set of timeline files (or relishes) and a special metadata file, all compressed into a blob.
//! An archive contains set of files of a certain timeline, added during checkpoint(s) and the timeline metadata at that moment.
//! The archive contains that metadata's `disk_consistent_lsn` in its name, to be able to restore partial index information from just a remote storage file list.
//! The index is created at startup (possible due to exclusive ownership over the remote storage by the pageserver) and keeps track of which files were stored
//! in what remote archives.
//! Among other tasks, the index is used to prevent invalid uploads and non-existing downloads on demand.
//! Refer to [`compression`] and [`index`] for more details on the archives and index respectively.
//!
//! After pageserver parforms a succesful image checkpoint and produces new local files, it schedules an upload with
//! the list of the files and its metadata file contents at the moment of checkpointing.
//! Pageserver needs both the file list and metadata to load the timeline, so both are mandatory for the upload, that's why the uploads happen after checkpointing.
//! Not every upload of the same timeline gets processed: if `disk_consistent_lsn` is unchanged due to checkpointing for some reason, the remote data is not updated.
//!
//! Current uploads are per-checkpoint and don't accumulate any data with optimal size for storing on S3.
//! The upload is atomic and gets rescheduled entirely, if fails along the way.
//! The downloads are per-timeline and download all missing timeline files.
//! The archives get processed sequentially, from smaller `disk_consistent_lsn` to larger, with metadata files being added as last.
//! If any of the archive processing fails along the way, all the remaining archives are rescheduled for the next attempt.
//! There's a reschedule threshold that evicts tasks that fail too much and stops the corresponding timeline so it does not diverge from the state on the remote storage.
//! The archive unpacking is designed to unpack metadata as the last file, so the risk of leaving the corrupt timeline due to uncompression error is small (while not eliminated entirely and that should be improved).
//!
//! Synchronization never removes any local from pageserver workdir or remote files from the remote storage, yet there could be overwrites of the same files (metadata file updates; archive redownloads).
//! NOTE: No real contents or checksum check happens right now and is a subject to improve later.
//!
//! After the whole timeline is downloaded, [`crate::tenant_mgr::register_timeline_download`] function is used to register the image in pageserver.
//!
//! When pageserver signals shutdown, current sync task gets finished and the loop exists.
//!
//! Currently there's no other way to download a remote relish if it was not downloaded after initial remote storage files check.
//! This is a subject to change in the near future.

mod compression;
pub mod index;

use std::{
    collections::{BTreeSet, HashMap},
    num::{NonZeroU32, NonZeroUsize},
    ops::{Deref, DerefMut},
    path::PathBuf,
    sync::Arc,
    thread,
};

use anyhow::{anyhow, ensure, Context};
use futures::stream::{FuturesUnordered, StreamExt};
use lazy_static::lazy_static;
use tokio::sync::RwLock;
use tokio::{
    sync::mpsc::{self, UnboundedReceiver},
    time::Instant,
};
use tracing::*;

use self::{
    compression::ArchiveHeader,
    index::{ArchiveId, RemoteTimeline},
};
use super::{RemoteStorage, TimelineSyncId};
use crate::{
    layered_repository::metadata::TimelineMetadata,
    tenant_mgr::{perform_post_timeline_sync_steps, TimelineRegistration},
    PageServerConf,
};

use zenith_metrics::{register_histogram_vec, register_int_gauge, HistogramVec, IntGauge};
use zenith_utils::zid::{ZTenantId, ZTimelineId};

lazy_static! {
    static ref REMAINING_SYNC_ITEMS: IntGauge = register_int_gauge!(
        "pageserver_remote_storage_remaining_sync_items",
        "Number of storage sync items left in the queue"
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
/// mpsc approach was picked to allow blocking the sync loop if no tasks are present, to avoud meaningless spinning.
mod sync_queue {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use anyhow::anyhow;
    use once_cell::sync::OnceCell;
    use tokio::sync::mpsc::{error::TryRecvError, UnboundedReceiver, UnboundedSender};
    use tracing::{debug, warn};

    use super::SyncTask;

    static SENDER: OnceCell<UnboundedSender<SyncTask>> = OnceCell::new();
    static LENGTH: AtomicUsize = AtomicUsize::new(0);

    pub fn init(sender: UnboundedSender<SyncTask>) -> anyhow::Result<()> {
        SENDER
            .set(sender)
            .map_err(|_| anyhow!("sync queue was already initialized"))?;
        Ok(())
    }

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
            warn!("Failed to enqueue a sync task: the receiver is not initialized");
            false
        }
    }

    pub async fn next_task(receiver: &mut UnboundedReceiver<SyncTask>) -> Option<SyncTask> {
        let task = receiver.recv().await;
        LENGTH.fetch_sub(1, Ordering::Relaxed);
        task
    }

    pub async fn next_task_batch(
        receiver: &mut UnboundedReceiver<SyncTask>,
        mut max_batch_size: usize,
    ) -> Vec<SyncTask> {
        let mut tasks = Vec::with_capacity(max_batch_size);

        if max_batch_size == 0 {
            return tasks;
        }

        loop {
            match receiver.try_recv() {
                Ok(new_task) => {
                    max_batch_size -= 1;
                    LENGTH.fetch_sub(1, Ordering::Relaxed);
                    tasks.push(new_task);
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

    pub fn len() -> usize {
        LENGTH.load(Ordering::Relaxed)
    }
}

/// A task to run in the async download/upload loop.
/// Limited by the number of retries, after certain threshold the failing task gets evicted and the timeline disabled.
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct SyncTask {
    sync_id: TimelineSyncId,
    retries: u32,
    kind: SyncKind,
}

impl SyncTask {
    fn new(sync_id: TimelineSyncId, retries: u32, kind: SyncKind) -> Self {
        Self {
            sync_id,
            retries,
            kind,
        }
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
enum SyncKind {
    /// A certain amount of images (archive files) to download.
    Download(TimelineDownload),
    /// A checkpoint outcome with possible local file updates that need actualization in the remote storage.
    /// Not necessary more fresh than the one already uploaded.
    Upload(NewCheckpoint),
}

/// Local timeline files for upload, appeared after the new checkpoint.
/// Current checkpoint design assumes new files are added only, no deletions or amendment happens.
#[derive(Debug, PartialEq, Eq, Clone)]
struct NewCheckpoint {
    /// Relish file paths in the pageserver workdir, that were added for the corresponding checkpoint.
    layers: Vec<PathBuf>,
    metadata: TimelineMetadata,
}

/// Info about the remote image files.
#[derive(Clone, Debug, PartialEq, Eq)]
struct TimelineDownload {
    files_to_skip: Arc<BTreeSet<PathBuf>>,
    archives_to_download: Vec<ArchiveId>,
}

/// Adds the new checkpoint files as an upload sync task to the queue.
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
        TimelineSyncId(tenant_id, timeline_id),
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

/// Uses a remote storage given to start the storage sync loop.
/// See module docs for loop step description.
pub(super) fn spawn_storage_sync_thread<
    P: std::fmt::Debug + Send + Sync + 'static,
    S: RemoteStorage<StoragePath = P> + Send + Sync + 'static,
>(
    config: &'static PageServerConf,
    remote_storage: S,
    max_concurrent_sync: NonZeroUsize,
    max_sync_errors: NonZeroU32,
) -> anyhow::Result<thread::JoinHandle<anyhow::Result<()>>> {
    let (sender, receiver) = mpsc::unbounded_channel();
    sync_queue::init(sender)?;

    let handle = thread::Builder::new()
        .name("Queue based remote storage sync".to_string())
        .spawn(move || {
            let thread_result = storage_sync_loop(
                config,
                receiver,
                remote_storage,
                max_concurrent_sync,
                max_sync_errors,
            );
            if let Err(e) = &thread_result {
                error!("Failed to run storage sync thread: {:#}", e);
            }
            thread_result
        })?;
    Ok(handle)
}

fn storage_sync_loop<
    P: std::fmt::Debug + Send + Sync + 'static,
    S: RemoteStorage<StoragePath = P> + Send + Sync + 'static,
>(
    config: &'static PageServerConf,
    mut receiver: UnboundedReceiver<SyncTask>,
    remote_storage: S,
    max_concurrent_sync: NonZeroUsize,
    max_sync_errors: NonZeroU32,
) -> anyhow::Result<()> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let remote_timelines = runtime
        .block_on(index::reconstruct_from_storage(&remote_storage))
        .context("Failed to determine previously uploaded timelines")?;

    schedule_first_tasks(config, &remote_timelines);

    // TODO kb return it back under a single Arc?
    let remote_storage = Arc::new(remote_storage);
    let remote_timelines = Arc::new(RwLock::new(remote_timelines));
    while !crate::tenant_mgr::shutdown_requested() {
        let registration_steps = runtime.block_on(loop_step(
            config,
            &mut receiver,
            Arc::clone(&remote_storage),
            Arc::clone(&remote_timelines),
            max_concurrent_sync,
            max_sync_errors,
        ));
        // Batch timeline download registration to ensure that the external registration code won't block any running tasks before.
        perform_post_timeline_sync_steps(config, registration_steps);
    }

    debug!("Shutdown requested, stopping");
    Ok(())
}

async fn loop_step<
    P: std::fmt::Debug + Send + Sync + 'static,
    S: RemoteStorage<StoragePath = P> + Send + Sync + 'static,
>(
    config: &'static PageServerConf,
    receiver: &mut UnboundedReceiver<SyncTask>,
    remote_storage: Arc<S>,
    remote_timelines: Arc<RwLock<HashMap<TimelineSyncId, RemoteTimeline>>>,
    max_concurrent_sync: NonZeroUsize,
    max_sync_errors: NonZeroU32,
) -> HashMap<(ZTenantId, ZTimelineId), TimelineRegistration> {
    let max_concurrent_sync = max_concurrent_sync.get();
    let mut next_tasks = Vec::with_capacity(max_concurrent_sync);

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
            let extra_step = match tokio::spawn(process_task(
                config,
                Arc::clone(&remote_storage),
                task,
                max_sync_errors,
                Arc::clone(&remote_timelines),
            ))
            .await
            {
                Ok(extra_step) => extra_step,
                Err(e) => {
                    error!(
                        "Failed to process storage sync task for tenant {}, timeline {}: {:#}",
                        sync_id.0, sync_id.1, e
                    );
                    None
                }
            };
            (sync_id, extra_step)
        })
        .collect::<FuturesUnordered<_>>();

    let mut extra_sync_steps = HashMap::with_capacity(max_concurrent_sync);
    while let Some((sync_id, extra_step)) = task_batch.next().await {
        let TimelineSyncId(tenant_id, timeline_id) = sync_id;
        debug!(
            "Finished storage sync task for tenant {}, timeline {}",
            tenant_id, timeline_id
        );
        if let Some(extra_step) = extra_step {
            extra_sync_steps.insert((tenant_id, timeline_id), extra_step);
        }
    }

    extra_sync_steps
}

async fn process_task<
    P: std::fmt::Debug + Send + Sync + 'static,
    S: RemoteStorage<StoragePath = P> + Send + Sync + 'static,
>(
    config: &'static PageServerConf,
    remote_storage: Arc<S>,
    task: SyncTask,
    max_sync_errors: NonZeroU32,
    remote_timelines: Arc<RwLock<HashMap<TimelineSyncId, RemoteTimeline>>>,
) -> Option<TimelineRegistration> {
    if task.retries > max_sync_errors.get() {
        error!(
            "Evicting task {:?} that failed {} times, exceeding the error theshold",
            task.kind, task.retries
        );
        return Some(TimelineRegistration::Evict);
    }

    if task.retries > 0 {
        let seconds_to_wait = 2.0_f64.powf(task.retries as f64 - 1.0).min(30.0);
        debug!(
            "Waiting {} seconds before starting the task",
            seconds_to_wait
        );
        tokio::time::sleep(tokio::time::Duration::from_secs_f64(seconds_to_wait)).await;
    }

    let sync_start = Instant::now();
    match task.kind {
        SyncKind::Download(download_data) => {
            let sync_status = download_timeline(
                config,
                remote_timelines.read().await.deref(),
                Arc::clone(&remote_storage),
                task.sync_id,
                download_data,
                task.retries + 1,
            )
            .await;
            register_sync_status(sync_start, "download", sync_status);
            Some(TimelineRegistration::Download)
        }
        SyncKind::Upload(layer_upload) => {
            let sync_status = upload_timeline_checkpoint(
                config,
                remote_timelines.write().await.deref_mut(),
                Arc::clone(&remote_storage),
                task.sync_id,
                layer_upload,
                task.retries + 1,
            )
            .await;
            register_sync_status(sync_start, "upload", sync_status);
            None
        }
    }
}

fn schedule_first_tasks(
    config: &'static PageServerConf,
    remote_timelines: &HashMap<TimelineSyncId, RemoteTimeline>,
) {
    for (&sync_id, timeline) in remote_timelines {
        if !config.timeline_path(&sync_id.1, &sync_id.0).exists() {
            sync_queue::push(SyncTask::new(
                sync_id,
                0,
                SyncKind::Download(TimelineDownload {
                    files_to_skip: Arc::new(BTreeSet::new()),
                    archives_to_download: timeline.stored_archives(),
                }),
            ));
        } else {
            debug!(
                "Timeline with tenant id {}, timeline id {} exists locally, not downloading",
                sync_id.0, sync_id.1
            );
        }
    }
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

async fn download_timeline<
    P: Send + Sync + 'static,
    S: RemoteStorage<StoragePath = P> + Send + Sync + 'static,
>(
    config: &'static PageServerConf,
    remote_timelines: &HashMap<TimelineSyncId, RemoteTimeline>,
    remote_storage: Arc<S>,
    sync_id: TimelineSyncId,
    mut download: TimelineDownload,
    retries: u32,
) -> Option<bool> {
    let TimelineSyncId(tenant_id, timeline_id) = sync_id;
    debug!(
        "Downloading layers for tenant {}, timeline {}",
        tenant_id, timeline_id
    );

    let remote_timeline = remote_timelines.get(&sync_id)?;

    let archives_total = download.archives_to_download.len();
    debug!("Downloading {} archives of a timeline", archives_total);
    while let Some(archive_id) = download.archives_to_download.pop() {
        if let Err(e) = try_download_archive(
            Arc::clone(&remote_storage),
            config.timeline_path(&timeline_id, &tenant_id),
            remote_timeline,
            archive_id,
            Arc::clone(&download.files_to_skip),
        )
        .await
        {
            // add the failed archive back
            download.archives_to_download.push(archive_id);
            let archives_left = download.archives_to_download.len();
            error!(
                "Failed to download archive {:?} for tenant {} timeline {} : {:#}, requeueing the download ({} archives left out of {})",
                archive_id, tenant_id, timeline_id, e, archives_left, archives_total
            );
            sync_queue::push(SyncTask::new(
                sync_id,
                retries,
                SyncKind::Download(download),
            ));
            return Some(false);
        }
    }
    debug!("Finished downloading all timeline's archives");
    Some(true)
}

async fn try_download_archive<
    P: Send + Sync + 'static,
    S: RemoteStorage<StoragePath = P> + Send + Sync + 'static,
>(
    remote_storage: Arc<S>,
    timeline_dir: PathBuf,
    remote_timeline: &RemoteTimeline,
    archive_id: ArchiveId,
    files_to_skip: Arc<BTreeSet<PathBuf>>,
) -> anyhow::Result<()> {
    debug!("Downloading archive {:?}", archive_id);
    let remote_archive = remote_timeline
        .archive_data(archive_id)
        .ok_or_else(|| anyhow!("Archive {:?} not found in remote storage", archive_id))?;
    let (archive_header, header_size) = remote_timeline
        .restore_header(archive_id)
        .context("Failed to restore header when downloading an archive")?;

    compression::uncompress_file_stream_with_index(
        timeline_dir.clone(),
        files_to_skip,
        remote_archive.disk_consistent_lsn(),
        archive_header,
        header_size,
        move |mut archive_target, archive_name| async move {
            let archive_local_path = timeline_dir.join(&archive_name);
            remote_storage
                .download_range(
                    &remote_storage.storage_path(&archive_local_path)?,
                    header_size,
                    None,
                    &mut archive_target,
                )
                .await
        },
    )
    .await?;

    Ok(())
}

async fn upload_timeline_checkpoint<
    P: Send + Sync + 'static,
    S: RemoteStorage<StoragePath = P> + Send + Sync + 'static,
>(
    config: &'static PageServerConf,
    remote_timelines: &mut HashMap<TimelineSyncId, RemoteTimeline>,
    remote_storage: Arc<S>,
    sync_id: TimelineSyncId,
    new_checkpoint: NewCheckpoint,
    retries: u32,
) -> Option<bool> {
    let TimelineSyncId(tenant_id, timeline_id) = sync_id;
    debug!(
        "Uploading checkpoint for tenant {}, timeline {}",
        tenant_id, timeline_id
    );
    let remote_timeline = remote_timelines.get(&sync_id);

    let new_upload_lsn = new_checkpoint.metadata.disk_consistent_lsn();
    let already_contains_upload_lsn = remote_timeline
        .map(|remote_timeline| remote_timeline.contains_archive(new_upload_lsn))
        .unwrap_or(false);
    if already_contains_upload_lsn {
        warn!(
            "Received a checkpoint witn Lsn {} that's already been uploaded to remote storage, skipping the upload.",
            new_upload_lsn
        );
        return None;
    }

    let timeline_dir = config.timeline_path(&timeline_id, &tenant_id);
    let already_uploaded_files = remote_timeline
        .map(|timeline| timeline.stored_files(&timeline_dir))
        .unwrap_or_default();
    match try_upload_checkpoint(
        config,
        remote_storage,
        sync_id,
        &new_checkpoint,
        already_uploaded_files,
    )
    .await
    {
        Ok((archive_header, header_size)) => {
            remote_timelines
                .entry(sync_id)
                .or_insert_with(RemoteTimeline::empty)
                .set_archive_contents(
                    new_checkpoint.metadata.disk_consistent_lsn(),
                    archive_header,
                    header_size,
                );
            debug!("Checkpoint uploaded successfully");
            Some(true)
        }
        Err(e) => {
            error!(
                "Failed to upload checkpoint: {:#}, requeueing the upload",
                e
            );
            sync_queue::push(SyncTask::new(
                sync_id,
                retries,
                SyncKind::Upload(new_checkpoint),
            ));
            Some(false)
        }
    }
}

async fn try_upload_checkpoint<
    P: Send + Sync + 'static,
    S: RemoteStorage<StoragePath = P> + Send + Sync + 'static,
>(
    config: &'static PageServerConf,
    remote_storage: Arc<S>,
    sync_id: TimelineSyncId,
    new_checkpoint: &NewCheckpoint,
    files_to_skip: BTreeSet<PathBuf>,
) -> anyhow::Result<(ArchiveHeader, u64)> {
    let TimelineSyncId(tenant_id, timeline_id) = sync_id;
    let timeline_dir = config.timeline_path(&timeline_id, &tenant_id);

    let files_to_upload = new_checkpoint
        .layers
        .iter()
        .filter(|&path_to_upload| {
            if files_to_skip.contains(path_to_upload) {
                error!(
                    "Skipping file upload '{}', since it was already uploaded",
                    path_to_upload.display()
                );
                false
            } else {
                true
            }
        })
        .collect::<Vec<_>>();
    ensure!(!files_to_upload.is_empty(), "No files to upload");

    compression::archive_files_as_stream(
        &timeline_dir,
        files_to_upload.into_iter(),
        &new_checkpoint.metadata,
        move |archive_streamer, archive_name| async move {
            let timeline_dir = config.timeline_path(&timeline_id, &tenant_id);
            remote_storage
                .upload(
                    archive_streamer,
                    &remote_storage.storage_path(&timeline_dir.join(&archive_name))?,
                )
                .await
        },
    )
    .await
    .map(|(header, header_size, _)| (header, header_size))
}

#[cfg(test)]
mod tests {
    use std::{
        collections::{BTreeMap, BTreeSet},
        fs,
    };

    use super::{index::reconstruct_from_storage, *};
    use crate::{
        layered_repository::metadata::metadata_path,
        remote_storage::local_fs::LocalFs,
        repository::repo_harness::{RepoHarness, TIMELINE_ID},
    };
    use tempfile::tempdir;
    use zenith_utils::lsn::Lsn;

    #[tokio::test]
    async fn reupload_timeline() -> anyhow::Result<()> {
        let repo_harness = RepoHarness::create("reupload_timeline")?;
        let sync_id = TimelineSyncId(repo_harness.tenant_id, TIMELINE_ID);
        let storage = Arc::new(LocalFs::new(
            tempdir()?.path().to_owned(),
            &repo_harness.conf.workdir,
        )?);
        let mut remote_timelines = HashMap::new();

        let first_upload_metadata = dummy_metadata(Lsn(0x10));
        let first_checkpoint = create_local_timeline(
            &repo_harness,
            TIMELINE_ID,
            &["a", "b"],
            first_upload_metadata.clone(),
        )?;
        let local_timeline_path = repo_harness.timeline_path(&TIMELINE_ID);
        ensure_correct_timeline_upload(
            &repo_harness,
            &mut remote_timelines,
            Arc::clone(&storage),
            TIMELINE_ID,
            first_checkpoint,
        )
        .await;

        let uploaded_timeline = remote_timelines
            .get(&sync_id)
            .expect("Should have the timeline after the corresponding checkpoint upload");
        let uploaded_archives = uploaded_timeline.stored_archives();
        assert_eq!(
            uploaded_archives.len(),
            1,
            "Only one archive is expected after a first upload"
        );
        let first_uploaded_archive = uploaded_archives.first().copied().unwrap();
        assert_eq!(
            uploaded_timeline.latest_disk_consistent_lsn(),
            Some(first_upload_metadata.disk_consistent_lsn()),
            "Metadata that was uploaded, should have its Lsn stored"
        );
        assert_eq!(
            uploaded_timeline
                .archive_data(uploaded_archives.first().copied().unwrap())
                .unwrap()
                .disk_consistent_lsn(),
            first_upload_metadata.disk_consistent_lsn(),
            "Uploaded archive should have corresponding Lsn"
        );
        assert_eq!(
            uploaded_timeline.stored_files(&local_timeline_path),
            vec![local_timeline_path.join("a"), local_timeline_path.join("b")]
                .into_iter()
                .collect(),
            "Should have all files from the first checkpoint"
        );

        let second_upload_metadata = dummy_metadata(Lsn(0x40));
        let second_checkpoint = create_local_timeline(
            &repo_harness,
            TIMELINE_ID,
            &["b", "c"],
            second_upload_metadata.clone(),
        )?;
        assert!(
            first_upload_metadata.disk_consistent_lsn()
                < second_upload_metadata.disk_consistent_lsn()
        );
        ensure_correct_timeline_upload(
            &repo_harness,
            &mut remote_timelines,
            Arc::clone(&storage),
            TIMELINE_ID,
            second_checkpoint,
        )
        .await;

        let updated_timeline = remote_timelines
            .get(&sync_id)
            .expect("Should have the timeline after 2 checkpoints are uploaded");
        let mut updated_archives = updated_timeline.stored_archives();
        assert_eq!(
            updated_archives.len(),
            2,
            "Two archives are expected after a successful update of the upload"
        );
        updated_archives.retain(|archive_id| archive_id != &first_uploaded_archive);
        assert_eq!(
            updated_archives.len(),
            1,
            "Only one new archive is expected among the uploaded"
        );
        let second_uploaded_archive = updated_archives.last().copied().unwrap();
        assert_eq!(
            updated_timeline.latest_disk_consistent_lsn(),
            Some(second_upload_metadata.disk_consistent_lsn()),
            "Metadata that was uploaded, should have its Lsn stored"
        );
        assert_eq!(
            updated_timeline
                .archive_data(second_uploaded_archive)
                .unwrap()
                .disk_consistent_lsn(),
            second_upload_metadata.disk_consistent_lsn(),
            "Uploaded archive should have corresponding Lsn"
        );
        assert_eq!(
            updated_timeline.stored_files(&local_timeline_path),
            vec![
                local_timeline_path.join("a"),
                local_timeline_path.join("b"),
                local_timeline_path.join("c"),
            ]
            .into_iter()
            .collect(),
            "Should have all files from both checkpoints without duplicates"
        );

        let third_upload_metadata = dummy_metadata(Lsn(0x20));
        let third_checkpoint = create_local_timeline(
            &repo_harness,
            TIMELINE_ID,
            &["d"],
            third_upload_metadata.clone(),
        )?;
        assert_ne!(
            third_upload_metadata.disk_consistent_lsn(),
            first_upload_metadata.disk_consistent_lsn()
        );
        assert!(
            third_upload_metadata.disk_consistent_lsn()
                < second_upload_metadata.disk_consistent_lsn()
        );
        ensure_correct_timeline_upload(
            &repo_harness,
            &mut remote_timelines,
            Arc::clone(&storage),
            TIMELINE_ID,
            third_checkpoint,
        )
        .await;

        let updated_timeline = remote_timelines
            .get(&sync_id)
            .expect("Should have the timeline after 3 checkpoints are uploaded");
        let mut updated_archives = updated_timeline.stored_archives();
        assert_eq!(
            updated_archives.len(),
            3,
            "Three archives are expected after two successful updates of the upload"
        );
        updated_archives.retain(|archive_id| {
            archive_id != &first_uploaded_archive && archive_id != &second_uploaded_archive
        });
        assert_eq!(
            updated_archives.len(),
            1,
            "Only one new archive is expected among the uploaded"
        );
        let third_uploaded_archive = updated_archives.last().copied().unwrap();
        assert!(
            updated_timeline.latest_disk_consistent_lsn().unwrap()
                > third_upload_metadata.disk_consistent_lsn(),
            "Should not influence the last lsn by uploading an older checkpoint"
        );
        assert_eq!(
            updated_timeline
                .archive_data(third_uploaded_archive)
                .unwrap()
                .disk_consistent_lsn(),
            third_upload_metadata.disk_consistent_lsn(),
            "Uploaded archive should have corresponding Lsn"
        );
        assert_eq!(
            updated_timeline.stored_files(&local_timeline_path),
            vec![
                local_timeline_path.join("a"),
                local_timeline_path.join("b"),
                local_timeline_path.join("c"),
                local_timeline_path.join("d"),
            ]
            .into_iter()
            .collect(),
            "Should have all files from three checkpoints without duplicates"
        );

        Ok(())
    }

    #[tokio::test]
    async fn reupload_timeline_rejected() -> anyhow::Result<()> {
        let repo_harness = RepoHarness::create("reupload_timeline")?;
        let sync_id = TimelineSyncId(repo_harness.tenant_id, TIMELINE_ID);
        let storage = Arc::new(LocalFs::new(
            tempdir()?.path().to_owned(),
            &repo_harness.conf.workdir,
        )?);
        let mut remote_timelines = HashMap::new();

        let first_upload_metadata = dummy_metadata(Lsn(0x10));
        let first_checkpoint = create_local_timeline(
            &repo_harness,
            TIMELINE_ID,
            &["a", "b"],
            first_upload_metadata.clone(),
        )?;
        ensure_correct_timeline_upload(
            &repo_harness,
            &mut remote_timelines,
            Arc::clone(&storage),
            TIMELINE_ID,
            first_checkpoint,
        )
        .await;
        let after_first_uploads = remote_timelines.clone();

        let normal_upload_metadata = dummy_metadata(Lsn(0x20));
        assert_ne!(
            normal_upload_metadata.disk_consistent_lsn(),
            first_upload_metadata.disk_consistent_lsn()
        );

        let checkpoint_with_no_files = create_local_timeline(
            &repo_harness,
            TIMELINE_ID,
            &[],
            normal_upload_metadata.clone(),
        )?;
        upload_timeline_checkpoint(
            repo_harness.conf,
            &mut remote_timelines,
            Arc::clone(&storage),
            sync_id,
            checkpoint_with_no_files,
            0,
        )
        .await;
        assert_timelines_equal(after_first_uploads.clone(), remote_timelines.clone());

        let checkpoint_with_uploaded_lsn = create_local_timeline(
            &repo_harness,
            TIMELINE_ID,
            &["something", "new"],
            first_upload_metadata.clone(),
        )?;
        upload_timeline_checkpoint(
            repo_harness.conf,
            &mut remote_timelines,
            Arc::clone(&storage),
            sync_id,
            checkpoint_with_uploaded_lsn,
            0,
        )
        .await;
        assert_timelines_equal(after_first_uploads, remote_timelines);

        Ok(())
    }

    #[tokio::test]
    async fn test_download_timeline() -> anyhow::Result<()> {
        let repo_harness = RepoHarness::create("test_download_timeline")?;
        let sync_id = TimelineSyncId(repo_harness.tenant_id, TIMELINE_ID);
        let storage = Arc::new(LocalFs::new(
            tempdir()?.path().to_owned(),
            &repo_harness.conf.workdir,
        )?);
        let mut remote_timelines = HashMap::new();

        let regular_timeline_path = repo_harness.timeline_path(&TIMELINE_ID);
        let regular_timeline = create_local_timeline(
            &repo_harness,
            TIMELINE_ID,
            &["a", "b"],
            dummy_metadata(Lsn(0x30)),
        )?;
        ensure_correct_timeline_upload(
            &repo_harness,
            &mut remote_timelines,
            Arc::clone(&storage),
            TIMELINE_ID,
            regular_timeline,
        )
        .await;
        fs::remove_dir_all(&regular_timeline_path)?;
        let remote_regular_timeline = remote_timelines.get(&sync_id).unwrap().clone();

        download_timeline(
            repo_harness.conf,
            &remote_timelines,
            Arc::clone(&storage),
            sync_id,
            TimelineDownload {
                files_to_skip: Arc::new(BTreeSet::new()),
                archives_to_download: remote_regular_timeline.stored_archives(),
            },
            0,
        )
        .await;
        assert_timelines_equal(
            remote_timelines,
            reconstruct_from_storage(storage.as_ref()).await?,
        );
        assert_timeline_files_match(&repo_harness, TIMELINE_ID, remote_regular_timeline);

        Ok(())
    }

    #[track_caller]
    async fn ensure_correct_timeline_upload(
        harness: &RepoHarness,
        remote_timelines: &mut HashMap<TimelineSyncId, RemoteTimeline>,
        remote_storage: Arc<LocalFs>,
        timeline_id: ZTimelineId,
        new_upload: NewCheckpoint,
    ) {
        let sync_id = TimelineSyncId(harness.tenant_id, timeline_id);
        upload_timeline_checkpoint(
            harness.conf,
            remote_timelines,
            Arc::clone(&remote_storage),
            sync_id,
            new_upload.clone(),
            0,
        )
        .await;
        assert_timelines_equal(
            remote_timelines.clone(),
            reconstruct_from_storage(remote_storage.as_ref())
                .await
                .unwrap(),
        );

        let new_remote_timeline = remote_timelines.get(&sync_id).unwrap().clone();
        let new_remote_lsn = new_remote_timeline
            .latest_disk_consistent_lsn()
            .expect("Remote timeline should have an lsn after reupload");
        let upload_lsn = new_upload.metadata.disk_consistent_lsn();
        assert!(
            new_remote_lsn >= upload_lsn,
            "Remote timeline after upload should have the biggest Lsn out of all uploads"
        );
        assert!(
            new_remote_timeline
                .stored_archives()
                .contains(&ArchiveId(upload_lsn)),
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

    #[track_caller]
    fn assert_timelines_equal(
        expected: HashMap<TimelineSyncId, RemoteTimeline>,
        actual: HashMap<TimelineSyncId, RemoteTimeline>,
    ) {
        let expected_sorted = expected.iter().collect::<BTreeMap<_, _>>();
        let actual_sorted = actual.iter().collect::<BTreeMap<_, _>>();
        assert_eq!(
            expected_sorted, actual_sorted,
            "Different timeline contents"
        );
    }

    fn assert_timeline_files_match(
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
            remote_timeline
                .stored_archives()
                .contains(&ArchiveId(local_metadata.disk_consistent_lsn())),
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

    fn create_local_timeline(
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

    fn dummy_metadata(disk_consistent_lsn: Lsn) -> TimelineMetadata {
        TimelineMetadata::new(disk_consistent_lsn, None, None, Lsn(0), Lsn(0), Lsn(0))
    }
}

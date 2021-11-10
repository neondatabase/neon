//! A synchronization logic for the [`RemoteStorage`] and the state to ensure the correct synchronizations.
//!
//! The synchronization does not aim to be immediate, instead
//! doing all the job in a separate thread asynchronously, attempting to fully replicate the
//! pageserver timeline workdir data on the remote storage.
//!
//! [`SYNC_QUEUE`] is a priority queue to hold [`SyncTask`] for image upload/download.
//! The queue gets emptied by a single thread with the loop, that polls the tasks one by one.
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
//! The synchronization unit is an image: a set of layer files (or relishes) and a special metadata file.
//! Both upload and download tasks consider image in a similar way ([`LocalTimeline`] and [`RemoteTimeline`]):
//! * a set of relishes (both upload and download tasks store the files as local pageserver paths, ergo [`PathBuf`] is used).
//! * a set of ids to distinguish the images ([`ZTenantId`] and [`ZTimelineId`])
//! * `disk_consistent_lsn` which indicates the last [`Lsn`] applicable to the data stored in this image.
//!
//! The same relish has identical layer paths in both structs, since both represent the relish path in pageserver's workdir.
//! This way, the sync can compare remote and local images seamlessly, downloading/uploading missing files if needed.
//!
//! After pageserver parforms a succesful image checkpoint and detects that image state had updated, it reports an upload with
//! the list of image new files and its incremented `disk_consistent_lsn` (that also gets stored into image metadata file).
//! Both the file list and `disk_consistent_lsn` are mandatory for the upload, that's why the uploads happen after checkpointing.
//! Timelines with no such [`Lsn`] cannot guarantee their local file consistency and are not considered for backups.
//! Not every upload of the same timeline gets processed: if `disk_consistent_lsn` is unchanged, the remote timeline is not updated.
//!
//! Remote timelines may lack `disk_consistent_lsn` if their metadata file is corrupt or missing.
//! Such timelines are not downloaded and their layer paths are entirely replaced with the ones from a newer upload for the same timeline.
//! Intact remote timelines are stored in the sync loop memory to avoid duplicate reuploads and then get queried for downloading, if no
//! timeline with the same id is found in the local workdir already.
//!
//! Besides all sync tasks operating images, internally every image is split to its underlying relish files which are synced independently.
//! The sync logic does not distinguish the relishes between each other, uploading/downloading them all via [`FuturesUnordered`] and registering all failures.
//! A single special exception is a metadata file, that is always uploaded/downloaded last (source images with no metadata are ignored), only after the rest
//! of the relishes are successfully synced.
//! If there are relish or metadata sync errors, the task gets resubmitted with all failed layers only, with all the successful layers stored in the loop state.
//! NOTE: No backpressure or eviction is implemented for tasks that always fail, it will be improved later.
//!
//! Synchronization never removes any local from pageserver workdir or remote files from the remote storage: the files from previous
//! uploads that are not mentioned in the new upload lists, are still considered as part of the corresponding image.
//! When determining which files to upload/download, the local file paths (for remote files, that is the same as their download destination) is compared,
//! and two files are considered "equal", if their paths match. Such files are uploaded/downloaded over, no real contents checks are done.
//! NOTE: No real contents or checksum check happens right now and is a subject to improve later.
//!
//! After the whole timeline is downloaded, [`crate::tenant_mgr::register_timeline_download`] function is used to register the image in pageserver.
//!
//! When pageserver signals shutdown, current sync task gets finished and the loop exists.
//!
//! Currently there's no other way to download a remote relish if it was not downloaded after initial remote storage files check.
//! This is a subject to change in the near future, but requires more changes to [`crate::tenant_mgr`] before it can happen.

use std::{
    collections::{hash_map, HashMap, HashSet},
    num::{NonZeroU32, NonZeroUsize},
    ops::DerefMut,
    path::{Path, PathBuf},
    sync::Arc,
    thread,
};

use anyhow::{anyhow, ensure, Context};
use futures::stream::{FuturesUnordered, StreamExt};
use lazy_static::lazy_static;
use tokio::{
    fs,
    io::{self, AsyncWriteExt},
    sync::{
        mpsc::{self, UnboundedReceiver},
        Mutex,
    },
    time::Instant,
};
use tracing::*;

use super::{RemoteStorage, TimelineSyncId};
use crate::{
    layered_repository::{
        metadata::{metadata_path, TimelineMetadata, METADATA_FILE_NAME},
        TENANTS_SEGMENT_NAME, TIMELINES_SEGMENT_NAME,
    },
    tenant_mgr::{perform_post_timeline_sync_steps, PostTimelineSyncStep},
    PageServerConf,
};
use zenith_metrics::{register_histogram_vec, register_int_gauge, HistogramVec, IntGauge};
use zenith_utils::{
    lsn::Lsn,
    zid::{ZTenantId, ZTimelineId},
};

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
    /// Regular image download, that is not critical for running, but still needed.
    Download(RemoteTimeline),
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
struct RemoteTimeline {
    /// Same paths as in [`LocalTimeline`], pointing at the download
    /// destination of every of the remote timeline layers.
    layers: Vec<PathBuf>,
    /// If metadata file is uploaded, the corresponding field from this file.
    /// On the contrast with [`LocalTimeline`], remote timeline's metadata may be missing
    /// due to various upload errors or abrupt pageserver shutdowns that obstructed
    /// the file storing.
    metadata: Option<TimelineMetadata>,
}

impl RemoteTimeline {
    fn disk_consistent_lsn(&self) -> Option<Lsn> {
        self.metadata
            .as_ref()
            .map(|meta| meta.disk_consistent_lsn())
    }
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
        .block_on(fetch_existing_uploads(&remote_storage))
        .context("Failed to determine previously uploaded timelines")?;

    schedule_first_tasks(config, &remote_timelines);

    // placing the two variables, shared between the async loop tasks. Main reason for using `Arc` is `tokio::spawn` with its `'static` requirements.
    let remote_timelines_and_storage = Arc::new((Mutex::new(remote_timelines), remote_storage));

    while !crate::tenant_mgr::shutdown_requested() {
        let registration_steps = runtime.block_on(loop_step(
            config,
            &mut receiver,
            Arc::clone(&remote_timelines_and_storage),
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
    remote_timelines_and_storage: Arc<(Mutex<HashMap<TimelineSyncId, RemoteTimeline>>, S)>,
    max_concurrent_sync: NonZeroUsize,
    max_sync_errors: NonZeroU32,
) -> HashMap<(ZTenantId, ZTimelineId), PostTimelineSyncStep> {
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
        "Processing {} tasks, more tasks left to process: {}",
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
                Arc::clone(&remote_timelines_and_storage),
                task,
                max_sync_errors,
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

type TaskSharedRemotes<S> = (Mutex<HashMap<TimelineSyncId, RemoteTimeline>>, S);

async fn process_task<
    P: std::fmt::Debug + Send + Sync + 'static,
    S: RemoteStorage<StoragePath = P> + Send + Sync + 'static,
>(
    config: &'static PageServerConf,
    remote_timelines_and_storage: Arc<TaskSharedRemotes<S>>,
    task: SyncTask,
    max_sync_errors: NonZeroU32,
) -> Option<PostTimelineSyncStep> {
    let (remote_timelines, remote_storage) = remote_timelines_and_storage.as_ref();
    if task.retries > max_sync_errors.get() {
        error!(
            "Evicting task {:?} that failed {} times, exceeding the error theshold",
            task.kind, task.retries
        );
        return Some(PostTimelineSyncStep::Evict);
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
                remote_storage,
                task.sync_id,
                download_data,
                task.retries + 1,
            )
            .await;
            register_sync_status(sync_start, "download", sync_status);
            Some(PostTimelineSyncStep::RegisterDownload)
        }
        SyncKind::Upload(layer_upload) => {
            let sync_status = upload_timeline(
                config,
                remote_timelines.lock().await.deref_mut(),
                remote_storage,
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
    latest_timelines(remote_timelines)
        .iter()
        .filter(|sync_id| {
            let exists_locally = config.timeline_path(&sync_id.1, &sync_id.0).exists();
            if exists_locally {
                debug!(
                    "Timeline with tenant id {}, timeline id {} exists locally, not downloading",
                    sync_id.0, sync_id.1
                );
                false
            } else {
                true
            }
        })
        .filter_map(|&sync_id| {
            let remote_timeline = remote_timelines.get(&sync_id)?;
            Some(SyncTask::new(
                sync_id,
                0,
                SyncKind::Download(remote_timeline.clone()),
            ))
        })
        .for_each(|task| {
            sync_queue::push(task);
        });
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

fn latest_timelines(
    remote_timelines: &HashMap<TimelineSyncId, RemoteTimeline>,
) -> HashSet<TimelineSyncId> {
    let mut latest_timelines_for_tenants = HashMap::with_capacity(remote_timelines.len());
    for (&sync_id, remote_timeline_data) in remote_timelines {
        let (latest_timeline_id, timeline_metadata) = latest_timelines_for_tenants
            .entry(sync_id.0)
            .or_insert_with(|| (sync_id.1, remote_timeline_data.metadata.clone()));
        if latest_timeline_id != &sync_id.1
            && timeline_metadata
                .as_ref()
                .map(|metadata| metadata.disk_consistent_lsn())
                < remote_timeline_data.disk_consistent_lsn()
        {
            *latest_timeline_id = sync_id.1;
            *timeline_metadata = remote_timeline_data.metadata.clone();
        }
    }

    latest_timelines_for_tenants
        .into_iter()
        .map(|(tenant_id, (timeline_id, _))| TimelineSyncId(tenant_id, timeline_id))
        .collect()
}

async fn fetch_existing_uploads<P: std::fmt::Debug, S: 'static + RemoteStorage<StoragePath = P>>(
    remote_storage: &S,
) -> anyhow::Result<HashMap<TimelineSyncId, RemoteTimeline>> {
    let uploaded_files = remote_storage
        .list()
        .await
        .context("Failed to list the uploads")?;

    let mut data_fetches = uploaded_files
        .into_iter()
        .map(|remote_path| async {
            let local_path = match remote_storage.local_path(&remote_path) {
                Ok(path) => path,
                Err(e) => return Err((e, remote_path)),
            };
            let metadata = if local_path
                .file_name()
                .and_then(|os_str| os_str.to_str())
                .unwrap_or_default()
                == METADATA_FILE_NAME
            {
                let mut metadata_bytes = Vec::new();
                if let Err(e) = remote_storage
                    .download(&remote_path, &mut metadata_bytes)
                    .await
                {
                    return Err((e, remote_path));
                };
                let metadata = match TimelineMetadata::from_bytes(&metadata_bytes) {
                    Ok(metadata) => metadata,
                    Err(e) => return Err((e, remote_path)),
                };
                Some(metadata)
            } else {
                None
            };
            let sync_id = parse_sync_id(&local_path).map_err(|e| (e, remote_path))?;
            Ok::<_, (anyhow::Error, P)>((local_path, sync_id, metadata))
        })
        .collect::<FuturesUnordered<_>>();

    let mut fetched = HashMap::new();
    while let Some(fetch_result) = data_fetches.next().await {
        match fetch_result {
            Ok((local_path, sync_id, remote_metadata)) => {
                let remote_timeline = fetched.entry(sync_id).or_insert_with(|| RemoteTimeline {
                    layers: Vec::new(),
                    metadata: None,
                });
                if remote_metadata.is_some() {
                    remote_timeline.metadata = remote_metadata;
                } else {
                    remote_timeline.layers.push(local_path);
                }
            }
            Err((e, remote_path)) => {
                warn!(
                    "Failed to fetch file info for path {:?}, reason: {:#}",
                    remote_path, e
                );
                continue;
            }
        }
    }

    Ok(fetched)
}

fn parse_sync_id(path: &Path) -> anyhow::Result<TimelineSyncId> {
    let mut segments = path
        .iter()
        .flat_map(|segment| segment.to_str())
        .skip_while(|&segment| segment != TENANTS_SEGMENT_NAME);
    let tenants_segment = segments.next().ok_or_else(|| {
        anyhow!(
            "Found no '{}' segment in the storage path '{}'",
            TENANTS_SEGMENT_NAME,
            path.display()
        )
    })?;
    ensure!(
        tenants_segment == TENANTS_SEGMENT_NAME,
        "Failed to extract '{}' segment from storage path '{}'",
        TENANTS_SEGMENT_NAME,
        path.display()
    );
    let tenant_id = segments
        .next()
        .ok_or_else(|| {
            anyhow!(
                "Found no tenant id in the storage path '{}'",
                path.display()
            )
        })?
        .parse::<ZTenantId>()
        .with_context(|| {
            format!(
                "Failed to parse tenant id from storage path '{}'",
                path.display()
            )
        })?;

    let timelines_segment = segments.next().ok_or_else(|| {
        anyhow!(
            "Found no '{}' segment in the storage path '{}'",
            TIMELINES_SEGMENT_NAME,
            path.display()
        )
    })?;
    ensure!(
        timelines_segment == TIMELINES_SEGMENT_NAME,
        "Failed to extract '{}' segment from storage path '{}'",
        TIMELINES_SEGMENT_NAME,
        path.display()
    );
    let timeline_id = segments
        .next()
        .ok_or_else(|| {
            anyhow!(
                "Found no timeline id in the storage path '{}'",
                path.display()
            )
        })?
        .parse::<ZTimelineId>()
        .with_context(|| {
            format!(
                "Failed to parse timeline id from storage path '{}'",
                path.display()
            )
        })?;

    Ok(TimelineSyncId(tenant_id, timeline_id))
}

async fn download_timeline<P, S: 'static + RemoteStorage<StoragePath = P>>(
    config: &'static PageServerConf,
    remote_storage: &S,
    sync_id: TimelineSyncId,
    remote_timeline: RemoteTimeline,
    current_retry: u32,
) -> Option<bool> {
    debug!("Downloading layers for timeline {}", sync_id.1);

    let new_metadata = if let Some(metadata) = remote_timeline.metadata {
        metadata
    } else {
        warn!("Remote timeline incomplete: no metadata found, aborting the download");
        return None;
    };
    debug!("Downloading {} layers", remote_timeline.layers.len());

    let sync_result = synchronize_layers(
        config,
        remote_storage,
        remote_timeline.layers.into_iter(),
        SyncOperation::Download,
        &new_metadata,
        sync_id,
    )
    .await;

    match sync_result {
        SyncResult::Success { .. } => Some(true),
        SyncResult::MetadataSyncError { .. } => {
            let download = RemoteTimeline {
                layers: Vec::new(),
                metadata: Some(new_metadata),
            };
            sync_queue::push(SyncTask::new(
                sync_id,
                current_retry,
                SyncKind::Download(download),
            ));
            Some(false)
        }
        SyncResult::LayerSyncError { not_synced, .. } => {
            let download = RemoteTimeline {
                layers: not_synced,
                metadata: Some(new_metadata),
            };
            sync_queue::push(SyncTask::new(
                sync_id,
                current_retry,
                SyncKind::Download(download),
            ));
            Some(false)
        }
    }
}

async fn upload_timeline<'a, P, S: 'static + RemoteStorage<StoragePath = P>>(
    config: &'static PageServerConf,
    remote_timelines: &'a mut HashMap<TimelineSyncId, RemoteTimeline>,
    remote_storage: &'a S,
    sync_id: TimelineSyncId,
    mut new_upload: NewCheckpoint,
    current_retry: u32,
) -> Option<bool> {
    debug!("Uploading layers for timeline {}", sync_id.1);

    if let hash_map::Entry::Occupied(o) = remote_timelines.entry(sync_id) {
        let uploaded_timeline_files = o.get();
        let uploaded_layers = uploaded_timeline_files
            .layers
            .iter()
            .collect::<HashSet<_>>();
        new_upload
            .layers
            .retain(|path_to_upload| !uploaded_layers.contains(path_to_upload));
        match &uploaded_timeline_files.metadata {
            None => debug!("Partially uploaded timeline found, downloading missing files only"),
            Some(remote_metadata) => {
                let new_lsn = new_upload.metadata.disk_consistent_lsn();
                let remote_lsn = remote_metadata.disk_consistent_lsn();
                if new_lsn <= remote_lsn {
                    warn!(
                        "Received a timeline witn LSN {} that's not later than the one from remote storage {}, not uploading",
                        new_lsn, remote_lsn
                    );
                    return None;
                } else {
                    debug!(
                        "Received a timeline with newer LSN {} (storage LSN {}), updating the upload",
                        new_lsn, remote_lsn
                    )
                }
            }
        }
    }

    let NewCheckpoint {
        layers: new_layers,
        metadata: new_metadata,
        ..
    } = new_upload;
    let sync_result = synchronize_layers(
        config,
        remote_storage,
        new_layers.into_iter(),
        SyncOperation::Upload,
        &new_metadata,
        sync_id,
    )
    .await;

    let entry_to_update = remote_timelines
        .entry(sync_id)
        .or_insert_with(|| RemoteTimeline {
            layers: Vec::new(),
            metadata: Some(new_metadata.clone()),
        });
    match sync_result {
        SyncResult::Success { synced } => {
            entry_to_update.layers.extend(synced.into_iter());
            entry_to_update.metadata = Some(new_metadata);
            Some(true)
        }
        SyncResult::MetadataSyncError { synced } => {
            entry_to_update.layers.extend(synced.into_iter());
            sync_queue::push(SyncTask::new(
                sync_id,
                current_retry,
                SyncKind::Upload(NewCheckpoint {
                    layers: Vec::new(),
                    metadata: new_metadata,
                }),
            ));
            Some(false)
        }
        SyncResult::LayerSyncError { synced, not_synced } => {
            entry_to_update.layers.extend(synced.into_iter());
            sync_queue::push(SyncTask::new(
                sync_id,
                current_retry,
                SyncKind::Upload(NewCheckpoint {
                    layers: not_synced,
                    metadata: new_metadata,
                }),
            ));
            Some(false)
        }
    }
}

/// Layer sync operation kind.
///
/// This enum allows to unify the logic for image uploads and downloads.
/// When image's layers are synchronized, the only difference
/// between downloads and uploads is the [`RemoteStorage`] method we need to call.
#[derive(Debug, Copy, Clone)]
enum SyncOperation {
    Download,
    Upload,
}

/// Image sync result.
#[derive(Debug)]
enum SyncResult {
    /// All regular files are synced (their paths returned).
    /// Metadata file is synced too (path not returned).
    Success { synced: Vec<PathBuf> },
    /// All regular files are synced (their paths returned).
    /// Metadata file is not synced (path not returned).
    MetadataSyncError { synced: Vec<PathBuf> },
    /// Some regular files are not synced, some are (paths returned).
    /// Metadata file is not synced (path not returned).
    LayerSyncError {
        synced: Vec<PathBuf>,
        not_synced: Vec<PathBuf>,
    },
}

/// Synchronizes given layers and metadata contents of a certain image.
/// Regular files are always synced before metadata files are, the latter gets synced only if
/// the rest of the files are successfully processed.
#[allow(clippy::too_many_arguments)]
async fn synchronize_layers<'a, P, S: 'static + RemoteStorage<StoragePath = P>>(
    config: &'static PageServerConf,
    remote_storage: &'a S,
    layers: impl Iterator<Item = PathBuf>,
    sync_operation: SyncOperation,
    new_metadata: &'a TimelineMetadata,
    sync_id: TimelineSyncId,
) -> SyncResult {
    let mut sync_operations = layers
        .into_iter()
        .map(|layer_path| async move {
            let sync_result = match sync_operation {
                SyncOperation::Download => download(remote_storage, &layer_path).await,
                SyncOperation::Upload => upload(remote_storage, &layer_path).await,
            };
            (layer_path, sync_result)
        })
        .collect::<FuturesUnordered<_>>();

    let mut synced = Vec::new();
    let mut not_synced = Vec::new();
    while let Some((layer_path, layer_download_result)) = sync_operations.next().await {
        match layer_download_result {
            Ok(()) => synced.push(layer_path),
            Err(e) => {
                error!(
                    "Failed to sync ({:?}) layer with local path '{}', reason: {:#}",
                    sync_operation,
                    layer_path.display(),
                    e,
                );
                not_synced.push(layer_path);
            }
        }
    }

    if not_synced.is_empty() {
        debug!(
            "Successfully synced ({:?}) all {} layers",
            sync_operation,
            synced.len(),
        );
        trace!("Synced layers: {:?}", synced);
        match sync_metadata(
            config,
            remote_storage,
            sync_operation,
            new_metadata,
            sync_id,
        )
        .await
        {
            Ok(()) => {
                debug!("Metadata file synced successfully");
                SyncResult::Success { synced }
            }
            Err(e) => {
                error!(
                    "Failed to sync ({:?}) new metadata, reason: {:#}",
                    sync_operation, e
                );
                SyncResult::MetadataSyncError { synced }
            }
        }
    } else {
        SyncResult::LayerSyncError { synced, not_synced }
    }
}

async fn sync_metadata<'a, P, S: 'static + RemoteStorage<StoragePath = P>>(
    config: &'static PageServerConf,
    remote_storage: &'a S,
    sync_operation: SyncOperation,
    new_metadata: &'a TimelineMetadata,
    sync_id: TimelineSyncId,
) -> anyhow::Result<()> {
    debug!("Synchronizing ({:?}) metadata file", sync_operation);

    let local_metadata_path = metadata_path(config, sync_id.1, sync_id.0);
    let new_metadata_bytes = new_metadata.to_bytes()?;
    match sync_operation {
        SyncOperation::Download => {
            tokio::fs::write(&local_metadata_path, new_metadata_bytes).await?;
            tokio::fs::File::open(
                local_metadata_path
                    .parent()
                    .expect("Metadata should always have a parent"),
            )
            .await?
            .sync_all()
            .await?;
        }
        SyncOperation::Upload => {
            let remote_path = remote_storage
                .storage_path(&local_metadata_path)
                .with_context(|| {
                    format!(
                        "Failed to get remote storage path for local metadata path '{}'",
                        local_metadata_path.display()
                    )
                })?;
            remote_storage
                .upload(
                    io::BufReader::new(std::io::Cursor::new(new_metadata_bytes)),
                    &remote_path,
                )
                .await?;
        }
    }
    Ok(())
}

async fn upload<P, S: 'static + RemoteStorage<StoragePath = P>>(
    remote_storage: &S,
    source: &Path,
) -> anyhow::Result<()> {
    let destination = remote_storage.storage_path(source).with_context(|| {
        format!(
            "Failed to derive storage destination out of upload path {}",
            source.display()
        )
    })?;
    let source_file = io::BufReader::new(
        tokio::fs::OpenOptions::new()
            .read(true)
            .open(source)
            .await
            .with_context(|| {
                format!(
                    "Failed to open target s3 destination at {}",
                    source.display()
                )
            })?,
    );
    remote_storage.upload(source_file, &destination).await
}

async fn download<P, S: 'static + RemoteStorage<StoragePath = P>>(
    remote_storage: &S,
    destination: &Path,
) -> anyhow::Result<()> {
    if destination.exists() {
        Ok(())
    } else {
        let source = remote_storage.storage_path(destination).with_context(|| {
            format!(
                "Failed to derive storage source out of download destination '{}'",
                destination.display()
            )
        })?;

        if let Some(target_parent) = destination.parent() {
            if !target_parent.exists() {
                tokio::fs::create_dir_all(target_parent)
                    .await
                    .with_context(|| {
                        format!(
                            "Failed to create parent directories for destination '{}'",
                            destination.display()
                        )
                    })?;
            }
        }
        let mut destination_file = io::BufWriter::new(
            fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(destination)
                .await
                .with_context(|| {
                    format!(
                        "Failed to open download destination file '{}'",
                        destination.display()
                    )
                })?,
        );

        remote_storage
            .download(&source, &mut destination_file)
            .await?;
        destination_file.flush().await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::{BTreeMap, BTreeSet},
        fs,
        io::Cursor,
    };

    use super::*;
    use crate::{
        remote_storage::local_fs::LocalFs,
        repository::repo_harness::{RepoHarness, TIMELINE_ID},
    };
    use hex_literal::hex;
    use tempfile::tempdir;
    use tokio::io::BufReader;

    const NO_METADATA_TIMELINE_ID: ZTimelineId =
        ZTimelineId::from_array(hex!("3755461d2259a63a80635d760958efd0"));
    const CORRUPT_METADATA_TIMELINE_ID: ZTimelineId =
        ZTimelineId::from_array(hex!("314db9af91fbc02dda586880a3216c61"));

    #[tokio::test]
    async fn upload_new_timeline() -> anyhow::Result<()> {
        let repo_harness = RepoHarness::create("upload_new_timeline")?;
        let storage = LocalFs::new(tempdir()?.path().to_owned(), &repo_harness.conf.workdir)?;
        let mut remote_timelines = HashMap::new();

        assert_timelines_equal(
            HashMap::new(),
            fetch_existing_uploads(&storage).await.unwrap(),
        );

        let upload_metadata = dummy_metadata(Lsn(0x30));
        let upload = create_local_timeline(
            &repo_harness,
            TIMELINE_ID,
            &["a", "b"],
            upload_metadata.clone(),
        )?;
        let expected_layers = upload.layers.clone();
        ensure_correct_timeline_upload(
            &repo_harness,
            &mut remote_timelines,
            &storage,
            TIMELINE_ID,
            upload,
        )
        .await;

        let mut expected_uploads = HashMap::new();
        expected_uploads.insert(
            TimelineSyncId(repo_harness.tenant_id, TIMELINE_ID),
            RemoteTimeline {
                layers: expected_layers,
                metadata: Some(upload_metadata),
            },
        );
        assert_timelines_equal(expected_uploads, fetch_existing_uploads(&storage).await?);

        Ok(())
    }

    #[tokio::test]
    async fn reupload_timeline() -> anyhow::Result<()> {
        let repo_harness = RepoHarness::create("reupload_timeline")?;
        let sync_id = TimelineSyncId(repo_harness.tenant_id, TIMELINE_ID);
        let storage = LocalFs::new(tempdir()?.path().to_owned(), &repo_harness.conf.workdir)?;
        let mut remote_timelines = HashMap::new();

        let first_upload_metadata = dummy_metadata(Lsn(0x30));
        let first_timeline = create_local_timeline(
            &repo_harness,
            TIMELINE_ID,
            &["a", "b"],
            first_upload_metadata.clone(),
        )?;
        let first_paths = first_timeline.layers.clone();
        ensure_correct_timeline_upload(
            &repo_harness,
            &mut remote_timelines,
            &storage,
            TIMELINE_ID,
            first_timeline,
        )
        .await;
        let after_first_uploads = remote_timelines.clone();

        let new_upload_metadata = dummy_metadata(Lsn(0x20));
        assert!(
            new_upload_metadata.disk_consistent_lsn() < first_upload_metadata.disk_consistent_lsn()
        );
        let new_upload =
            create_local_timeline(&repo_harness, TIMELINE_ID, &["b", "c"], new_upload_metadata)?;
        upload_timeline(
            repo_harness.conf,
            &mut remote_timelines,
            &storage,
            sync_id,
            new_upload.clone(),
            0,
        )
        .await;
        assert_timelines_equal(after_first_uploads, remote_timelines.clone());

        let second_upload_metadata = dummy_metadata(Lsn(0x40));
        let second_timeline = create_local_timeline(
            &repo_harness,
            TIMELINE_ID,
            &["b", "c"],
            second_upload_metadata.clone(),
        )?;
        let second_paths = second_timeline.layers.clone();
        assert!(
            first_upload_metadata.disk_consistent_lsn()
                < second_upload_metadata.disk_consistent_lsn()
        );
        ensure_correct_timeline_upload(
            &repo_harness,
            &mut remote_timelines,
            &storage,
            TIMELINE_ID,
            second_timeline,
        )
        .await;

        let mut expected_uploads = HashMap::new();
        let mut expected_layers = first_paths.clone();
        expected_layers.extend(second_paths.clone().into_iter());
        expected_layers.dedup();

        expected_uploads.insert(
            TimelineSyncId(repo_harness.tenant_id, TIMELINE_ID),
            RemoteTimeline {
                layers: expected_layers,
                metadata: Some(second_upload_metadata.clone()),
            },
        );
        assert_timelines_equal(expected_uploads, remote_timelines.clone());

        let third_upload_metadata = dummy_metadata(Lsn(0x50));
        assert!(
            second_upload_metadata.disk_consistent_lsn()
                < third_upload_metadata.disk_consistent_lsn()
        );
        let third_timeline = create_local_timeline(
            &repo_harness,
            TIMELINE_ID,
            &["d", "e"],
            third_upload_metadata.clone(),
        )?;
        let third_paths = third_timeline.layers.clone();
        ensure_correct_timeline_upload(
            &repo_harness,
            &mut remote_timelines,
            &storage,
            TIMELINE_ID,
            third_timeline,
        )
        .await;

        let mut expected_uploads = HashMap::new();
        let mut expected_layers = first_paths;
        expected_layers.extend(second_paths.into_iter());
        expected_layers.extend(third_paths.into_iter());
        expected_layers.dedup();

        expected_uploads.insert(
            TimelineSyncId(repo_harness.tenant_id, TIMELINE_ID),
            RemoteTimeline {
                layers: expected_layers,
                metadata: Some(third_upload_metadata),
            },
        );
        assert_timelines_equal(expected_uploads, remote_timelines);

        Ok(())
    }

    #[tokio::test]
    async fn reupload_missing_metadata() -> anyhow::Result<()> {
        let repo_harness = RepoHarness::create("reupload_missing_metadata")?;
        let sync_id = TimelineSyncId(repo_harness.tenant_id, NO_METADATA_TIMELINE_ID);
        let storage = LocalFs::new(tempdir()?.path().to_owned(), &repo_harness.conf.workdir)?;
        let mut remote_timelines =
            store_timelines_with_incorrect_metadata(&repo_harness, &storage).await?;
        assert_timelines_equal(
            remote_timelines.clone(),
            fetch_existing_uploads(&storage).await?,
        );

        let old_remote_timeline = remote_timelines.get(&sync_id).unwrap().clone();
        let updated_metadata = dummy_metadata(Lsn(0x100));
        create_local_metadata(&repo_harness, NO_METADATA_TIMELINE_ID, &updated_metadata)?;
        ensure_correct_timeline_upload(
            &repo_harness,
            &mut remote_timelines,
            &storage,
            NO_METADATA_TIMELINE_ID,
            NewCheckpoint {
                layers: old_remote_timeline.layers.clone(),
                metadata: updated_metadata.clone(),
            },
        )
        .await;
        let reuploaded_timelines = fetch_existing_uploads(&storage).await?;

        let mut expected_timeline = RemoteTimeline {
            metadata: Some(updated_metadata),
            ..old_remote_timeline
        };
        expected_timeline.layers.sort();
        let mut updated_timeline = reuploaded_timelines.get(&sync_id).unwrap().clone();
        updated_timeline.layers.sort();
        assert_eq!(expected_timeline, updated_timeline);

        Ok(())
    }

    #[tokio::test]
    async fn test_upload_with_errors() -> anyhow::Result<()> {
        let repo_harness = RepoHarness::create("test_upload_with_errors")?;
        let sync_id = TimelineSyncId(repo_harness.tenant_id, TIMELINE_ID);
        let storage = LocalFs::new(tempdir()?.path().to_owned(), &repo_harness.conf.workdir)?;
        let mut remote_timelines = HashMap::new();

        let local_path = repo_harness.timeline_path(&TIMELINE_ID).join("something");
        assert!(!local_path.exists());
        assert!(fetch_existing_uploads(&storage).await?.is_empty());

        let timeline_without_local_files = NewCheckpoint {
            layers: vec![local_path],
            metadata: dummy_metadata(Lsn(0x30)),
        };

        upload_timeline(
            repo_harness.conf,
            &mut remote_timelines,
            &storage,
            sync_id,
            timeline_without_local_files.clone(),
            0,
        )
        .await;

        assert!(fetch_existing_uploads(&storage).await?.is_empty());
        assert!(!repo_harness.timeline_path(&TIMELINE_ID).exists());

        Ok(())
    }

    #[tokio::test]
    async fn test_download_timeline() -> anyhow::Result<()> {
        let repo_harness = RepoHarness::create("test_download_timeline")?;
        let sync_id = TimelineSyncId(repo_harness.tenant_id, TIMELINE_ID);
        let storage = LocalFs::new(tempdir()?.path().to_owned(), &repo_harness.conf.workdir)?;
        let mut remote_timelines =
            store_timelines_with_incorrect_metadata(&repo_harness, &storage).await?;
        fs::remove_dir_all(repo_harness.timeline_path(&NO_METADATA_TIMELINE_ID))?;
        fs::remove_dir_all(repo_harness.timeline_path(&CORRUPT_METADATA_TIMELINE_ID))?;

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
            &storage,
            TIMELINE_ID,
            regular_timeline,
        )
        .await;
        fs::remove_dir_all(&regular_timeline_path)?;
        let remote_regular_timeline = remote_timelines.get(&sync_id).unwrap().clone();

        download_timeline(
            repo_harness.conf,
            &storage,
            sync_id,
            remote_regular_timeline.clone(),
            0,
        )
        .await;
        download_timeline(
            repo_harness.conf,
            &storage,
            sync_id,
            remote_regular_timeline.clone(),
            0,
        )
        .await;
        download_timeline(
            repo_harness.conf,
            &storage,
            sync_id,
            remote_timelines.get(&sync_id).unwrap().clone(),
            0,
        )
        .await;
        download_timeline(
            repo_harness.conf,
            &storage,
            sync_id,
            remote_timelines.get(&sync_id).unwrap().clone(),
            0,
        )
        .await;

        assert_timelines_equal(remote_timelines, fetch_existing_uploads(&storage).await?);
        assert!(!repo_harness
            .timeline_path(&NO_METADATA_TIMELINE_ID)
            .exists());
        assert!(!repo_harness
            .timeline_path(&CORRUPT_METADATA_TIMELINE_ID)
            .exists());
        assert_timeline_files_match(&repo_harness, TIMELINE_ID, remote_regular_timeline);

        Ok(())
    }

    #[tokio::test]
    async fn metadata_file_sync() -> anyhow::Result<()> {
        let repo_harness = RepoHarness::create("metadata_file_sync")?;
        let sync_id = TimelineSyncId(repo_harness.tenant_id, TIMELINE_ID);
        let storage = LocalFs::new(tempdir()?.path().to_owned(), &repo_harness.conf.workdir)?;
        let mut remote_timelines = HashMap::new();

        let uploaded_metadata = dummy_metadata(Lsn(0x30));
        let metadata_local_path =
            metadata_path(repo_harness.conf, TIMELINE_ID, repo_harness.tenant_id);
        let new_upload = create_local_timeline(
            &repo_harness,
            TIMELINE_ID,
            &["a", "b"],
            uploaded_metadata.clone(),
        )?;
        tokio::fs::write(&metadata_local_path, b"incorrect metadata").await?;

        upload_timeline(
            repo_harness.conf,
            &mut remote_timelines,
            &storage,
            sync_id,
            new_upload.clone(),
            0,
        )
        .await;
        assert_timelines_equal(
            remote_timelines.clone(),
            fetch_existing_uploads(&storage).await?,
        );

        let remote_timeline = remote_timelines.get(&sync_id).unwrap().clone();
        assert_eq!(
            remote_timeline.metadata.as_ref(),
            Some(&uploaded_metadata),
            "Local corrputed metadata should be ignored when uploading an image"
        );

        download_timeline(
            repo_harness.conf,
            &storage,
            sync_id,
            remote_timeline.clone(),
            0,
        )
        .await;
        let downloaded_metadata_bytes = tokio::fs::read(&metadata_local_path)
            .await
            .expect("Failed to read metadata file contents after redownload");
        let downloaded_metadata = TimelineMetadata::from_bytes(&downloaded_metadata_bytes)
            .expect("Failed to parse metadata file contents after redownload");
        assert_eq!(
            downloaded_metadata, uploaded_metadata,
            "Should redownload the same metadata that was uploaed"
        );

        Ok(())
    }

    #[track_caller]
    async fn ensure_correct_timeline_upload(
        harness: &RepoHarness,
        remote_timelines: &mut HashMap<TimelineSyncId, RemoteTimeline>,
        remote_storage: &LocalFs,
        timeline_id: ZTimelineId,
        new_upload: NewCheckpoint,
    ) {
        let sync_id = TimelineSyncId(harness.tenant_id, timeline_id);
        upload_timeline(
            harness.conf,
            remote_timelines,
            remote_storage,
            sync_id,
            new_upload.clone(),
            0,
        )
        .await;
        assert_timelines_equal(
            remote_timelines.clone(),
            fetch_existing_uploads(remote_storage).await.unwrap(),
        );

        let new_remote_files = remote_timelines.get(&sync_id).unwrap().clone();
        assert_eq!(
            new_remote_files.metadata,
            Some(new_upload.metadata.clone()),
            "Remote timeline should have an updated metadata with later Lsn after successful reupload"
        );
        let remote_files_after_upload = new_remote_files
            .layers
            .clone()
            .into_iter()
            .collect::<HashSet<_>>();
        for new_uploaded_layer in &new_upload.layers {
            assert!(
                remote_files_after_upload.contains(new_uploaded_layer),
                "Remote files do not contain layer that should be uploaded: '{}'",
                new_uploaded_layer.display()
            );
        }

        assert_timeline_files_match(harness, timeline_id, new_remote_files);
    }

    #[track_caller]
    fn assert_timelines_equal(
        mut expected: HashMap<TimelineSyncId, RemoteTimeline>,
        mut actual: HashMap<TimelineSyncId, RemoteTimeline>,
    ) {
        let expected_sorted = expected
            .iter_mut()
            .map(|(key, remote_timeline)| {
                remote_timeline.layers.sort();
                (key, remote_timeline)
            })
            .collect::<BTreeMap<_, _>>();

        let actual_sorted = actual
            .iter_mut()
            .map(|(key, remote_timeline)| {
                remote_timeline.layers.sort();
                (key, remote_timeline)
            })
            .collect::<BTreeMap<_, _>>();

        assert_eq!(
            expected_sorted, actual_sorted,
            "Different timeline contents"
        );
    }

    fn assert_timeline_files_match(
        harness: &RepoHarness,
        timeline_id: ZTimelineId,
        remote_files: RemoteTimeline,
    ) {
        let local_timeline_dir = harness.timeline_path(&timeline_id);
        let local_paths = fs::read_dir(&local_timeline_dir)
            .unwrap()
            .map(|dir| dir.unwrap().path())
            .collect::<BTreeSet<_>>();
        let mut reported_remote_files = remote_files.layers.into_iter().collect::<BTreeSet<_>>();
        if let Some(remote_metadata) = remote_files.metadata {
            let local_metadata_path = metadata_path(harness.conf, timeline_id, harness.tenant_id);
            let local_metadata = TimelineMetadata::from_bytes(
                &fs::read(&local_metadata_path).expect("Failed to read metadata file when comparing remote and local image files")
            ).expect("Failed to parse metadata file contents when comparing remote and local image files");
            assert_eq!(
                local_metadata, remote_metadata,
                "Timeline remote metadata is different the local one"
            );
            reported_remote_files.insert(local_metadata_path);
        }

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

    async fn store_timelines_with_incorrect_metadata(
        harness: &RepoHarness,
        storage: &LocalFs,
    ) -> anyhow::Result<HashMap<TimelineSyncId, RemoteTimeline>> {
        let mut remote_timelines = HashMap::new();

        ensure_correct_timeline_upload(
            harness,
            &mut remote_timelines,
            storage,
            NO_METADATA_TIMELINE_ID,
            create_local_timeline(
                harness,
                NO_METADATA_TIMELINE_ID,
                &["a1", "b1"],
                dummy_metadata(Lsn(0)),
            )?,
        )
        .await;
        ensure_correct_timeline_upload(
            harness,
            &mut remote_timelines,
            storage,
            CORRUPT_METADATA_TIMELINE_ID,
            create_local_timeline(
                harness,
                CORRUPT_METADATA_TIMELINE_ID,
                &["a2", "b2"],
                dummy_metadata(Lsn(0)),
            )?,
        )
        .await;

        storage
            .delete(&storage.storage_path(&metadata_path(
                harness.conf,
                NO_METADATA_TIMELINE_ID,
                harness.tenant_id,
            ))?)
            .await?;
        storage
            .upload(
                BufReader::new(Cursor::new("corrupt meta".to_string().into_bytes())),
                &storage.storage_path(&metadata_path(
                    harness.conf,
                    CORRUPT_METADATA_TIMELINE_ID,
                    harness.tenant_id,
                ))?,
            )
            .await?;

        for remote_file in remote_timelines.values_mut() {
            remote_file.metadata = None;
        }

        Ok(remote_timelines)
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

        create_local_metadata(harness, timeline_id, &metadata)?;

        Ok(NewCheckpoint { layers, metadata })
    }

    fn create_local_metadata(
        harness: &RepoHarness,
        timeline_id: ZTimelineId,
        metadata: &TimelineMetadata,
    ) -> anyhow::Result<()> {
        fs::write(
            metadata_path(harness.conf, timeline_id, harness.tenant_id),
            metadata.to_bytes()?,
        )?;
        Ok(())
    }

    fn dummy_contents(name: &str) -> String {
        format!("contents for {}", name)
    }

    fn dummy_metadata(disk_consistent_lsn: Lsn) -> TimelineMetadata {
        TimelineMetadata::new(disk_consistent_lsn, None, None, Lsn(0), Lsn(0), Lsn(0))
    }
}

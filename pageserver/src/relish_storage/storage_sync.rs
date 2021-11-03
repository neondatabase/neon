//! A synchronization logic for the [`RelishStorage`] and the state to ensure the correct synchronizations.
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
//! an exclusive write access to the relish storage: new files appear in the storage only after the same
//! pageserver writes them.
//!
//! The list construction is currently the only place where the storage sync can return an [`Err`] to the user.
//! New upload tasks are accepted via [`schedule_timeline_upload`] function disregarding of the corresponding loop startup,
//! it's up to the caller to avoid uploading of the new relishes, if that caller did not enable the loop.
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
//! After the whole is downloaded, [`crate::tenant_mgr::register_relish_download`] function is used to register the image in pageserver.
//!
//! When pageserver signals shutdown, current sync task gets finished and the loop exists.
//!
//! Currently there's no other way to download a remote relish if it was not downloaded after initial remote storage files check.
//! This is a subject to change in the near future, but requires more changes to [`crate::tenant_mgr`] before it can happen.

use std::{
    cmp::Ordering,
    collections::{hash_map, BinaryHeap, HashMap, HashSet},
    path::{Path, PathBuf},
    sync::Mutex,
    thread,
    time::Duration,
};

use anyhow::{ensure, Context};
use futures::stream::{FuturesUnordered, StreamExt};
use lazy_static::lazy_static;
use tokio::{
    fs,
    io::{self, AsyncWriteExt},
    sync::Semaphore,
    time::Instant,
};
use tracing::*;

use super::{RelishStorage, RemoteRelishInfo};
use crate::{
    layered_repository::metadata::{metadata_path, TimelineMetadata},
    tenant_mgr::register_relish_download,
    PageServerConf,
};
use zenith_metrics::{register_histogram_vec, register_int_gauge, HistogramVec, IntGauge};
use zenith_utils::{
    lsn::Lsn,
    zid::{ZTenantId, ZTimelineId},
};

lazy_static! {
    static ref REMAINING_SYNC_ITEMS: IntGauge = register_int_gauge!(
        "pageserver_backup_remaining_sync_items",
        "Number of storage sync items left in the queue"
    )
    .expect("failed to register pageserver backup remaining sync items int gauge");
    static ref IMAGE_SYNC_TIME: HistogramVec = register_histogram_vec!(
        "pageserver_backup_image_sync_time",
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

lazy_static! {
    static ref SYNC_QUEUE: Mutex<BinaryHeap<SyncTask>> = Mutex::new(BinaryHeap::new());
}

/// An image sync task to store in the priority queue.
/// The task priority is defined by its [`PartialOrd`] derive:
/// * lower enum variants are of more priority compared to the higher ones
/// * for the same enum variants, "natural" comparison happens for their data
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone)]
enum SyncTask {
    /// Regular image download, that is not critical for running, but still needed.
    Download(RemoteTimeline),
    /// A checkpoint outcome with possible local file updates that need actualization in the remote storage.
    /// Not necessary more fresh than the one already uploaded.
    Upload(LocalTimeline),
    /// Every image that's not present locally but found remotely during sync loop start.
    /// Treated as "lost state" that pageserver needs to recover fully before it's ready to work.
    UrgentDownload(RemoteTimeline),
}

/// Local timeline files for upload.
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone)]
struct LocalTimeline {
    tenant_id: ZTenantId,
    timeline_id: ZTimelineId,
    /// Relish file paths in the pageserver workdir.
    layers: Vec<PathBuf>,
    metadata: TimelineMetadata,
}

/// Info about the remote image files.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct RemoteTimeline {
    tenant_id: ZTenantId,
    timeline_id: ZTimelineId,
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

/// Adds the new image as an upload sync task to the queue.
/// Ensure that the loop is started otherwise the task is never processed.
pub fn schedule_timeline_upload(
    tenant_id: ZTenantId,
    timeline_id: ZTimelineId,
    layers: Vec<PathBuf>,
    metadata: TimelineMetadata,
) {
    SYNC_QUEUE
        .lock()
        .unwrap()
        .push(SyncTask::Upload(LocalTimeline {
            tenant_id,
            timeline_id,
            layers,
            metadata,
        }))
}

/// Uses a relish storage given to start the storage sync loop.
/// See module docs for loop step description.
pub(super) fn spawn_storage_sync_thread<
    P: std::fmt::Debug,
    S: 'static + RelishStorage<RelishStoragePath = P>,
>(
    config: &'static PageServerConf,
    relish_storage: S,
    max_concurrent_sync: usize,
) -> anyhow::Result<thread::JoinHandle<anyhow::Result<()>>> {
    ensure!(
        max_concurrent_sync > 0,
        "Got 0 as max concurrent synchronizations allowed, cannot initialize a storage sync thread"
    );

    let handle = thread::Builder::new()
        .name("Queue based relish storage sync".to_string())
        .spawn(move || {
            let concurrent_sync_limit = Semaphore::new(max_concurrent_sync);
            let thread_result = storage_sync_loop(config, relish_storage, &concurrent_sync_limit);
            concurrent_sync_limit.close();
            if let Err(e) = &thread_result {
                error!("Failed to run storage sync thread: {:#}", e);
            }
            thread_result
        })?;
    Ok(handle)
}

fn storage_sync_loop<P: std::fmt::Debug, S: 'static + RelishStorage<RelishStoragePath = P>>(
    config: &'static PageServerConf,
    relish_storage: S,
    concurrent_sync_limit: &Semaphore,
) -> anyhow::Result<()> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let mut remote_timelines = runtime
        .block_on(fetch_existing_uploads(&relish_storage))
        .context("Failed to determine previously uploaded timelines")?;

    let urgent_downloads = latest_timelines(&remote_timelines)
        .iter()
        .filter_map(|(&tenant_id, &timeline_id)| remote_timelines.get(&(tenant_id, timeline_id)))
        .filter(|latest_remote_timeline| {
            let tenant_id = latest_remote_timeline.tenant_id;
            let timeline_id = latest_remote_timeline.timeline_id;
            let exists_locally = config.timeline_path(&timeline_id, &tenant_id).exists();
            if exists_locally {
                debug!(
                    "Timeline with tenant id {}, relish id {} exists locally, not downloading",
                    tenant_id, timeline_id
                );
                false
            } else {
                true
            }
        })
        .cloned()
        .map(SyncTask::UrgentDownload)
        .collect::<Vec<_>>();
    info!(
        "Will download {} timelines to restore state",
        urgent_downloads.len()
    );
    let mut accessor = SYNC_QUEUE.lock().unwrap();
    accessor.extend(urgent_downloads.into_iter());
    drop(accessor);

    while !crate::tenant_mgr::shutdown_requested() {
        let mut queue_accessor = SYNC_QUEUE.lock().unwrap();
        let next_task = queue_accessor.pop();
        let remaining_queue_length = queue_accessor.len();
        drop(queue_accessor);

        match next_task {
            Some(task) => {
                debug!(
                    "Processing a new task, more tasks left to process: {}",
                    remaining_queue_length
                );
                REMAINING_SYNC_ITEMS.set(remaining_queue_length as i64);

                runtime.block_on(async {
                    let sync_start = Instant::now();
                    match task {
                        SyncTask::Download(download_data) => {
                            let sync_status = download_timeline(
                                config,
                                concurrent_sync_limit,
                                &relish_storage,
                                download_data,
                                false,
                            )
                            .await;
                            register_sync_status(sync_start, "download", sync_status);
                        }
                        SyncTask::UrgentDownload(download_data) => {
                            let sync_status = download_timeline(
                                config,
                                concurrent_sync_limit,
                                &relish_storage,
                                download_data,
                                true,
                            )
                            .await;
                            register_sync_status(sync_start, "download", sync_status);
                        }
                        SyncTask::Upload(layer_upload) => {
                            let sync_status = upload_timeline(
                                config,
                                concurrent_sync_limit,
                                &mut remote_timelines,
                                &relish_storage,
                                layer_upload,
                            )
                            .await;
                            register_sync_status(sync_start, "upload", sync_status);
                        }
                    }
                })
            }
            None => {
                trace!("No storage sync tasks found");
                thread::sleep(Duration::from_secs(1));
                continue;
            }
        };
    }
    debug!("Queue based relish storage sync thread shut down");
    Ok(())
}

fn add_to_queue(task: SyncTask) {
    SYNC_QUEUE.lock().unwrap().push(task)
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
    remote_timelines: &HashMap<(ZTenantId, ZTimelineId), RemoteTimeline>,
) -> HashMap<ZTenantId, ZTimelineId> {
    let mut latest_timelines_for_tenants = HashMap::with_capacity(remote_timelines.len());
    for (&(remote_tenant_id, remote_timeline_id), remote_timeline_data) in remote_timelines {
        let (latest_timeline_id, timeline_metadata) = latest_timelines_for_tenants
            .entry(remote_tenant_id)
            .or_insert_with(|| (remote_timeline_id, remote_timeline_data.metadata.clone()));
        if latest_timeline_id != &remote_timeline_id
            && timeline_metadata
                .as_ref()
                .map(|metadata| metadata.disk_consistent_lsn())
                < remote_timeline_data.disk_consistent_lsn()
        {
            *latest_timeline_id = remote_timeline_id;
            *timeline_metadata = remote_timeline_data.metadata.clone();
        }
    }

    latest_timelines_for_tenants
        .into_iter()
        .map(|(tenant_id, (timeline_id, _))| (tenant_id, timeline_id))
        .collect()
}

async fn fetch_existing_uploads<
    P: std::fmt::Debug,
    S: 'static + RelishStorage<RelishStoragePath = P>,
>(
    relish_storage: &S,
) -> anyhow::Result<HashMap<(ZTenantId, ZTimelineId), RemoteTimeline>> {
    let uploaded_relishes = relish_storage
        .list_relishes()
        .await
        .context("Failed to list relish uploads")?;

    let mut relish_data_fetches = uploaded_relishes
        .into_iter()
        .map(|remote_path| async {
            (
                remote_relish_info(relish_storage, &remote_path).await,
                remote_path,
            )
        })
        .collect::<FuturesUnordered<_>>();

    let mut fetched = HashMap::new();
    while let Some((fetch_result, remote_path)) = relish_data_fetches.next().await {
        match fetch_result {
            Ok((relish_info, remote_metadata)) => {
                let tenant_id = relish_info.tenant_id;
                let timeline_id = relish_info.timeline_id;
                let remote_timeline =
                    fetched
                        .entry((tenant_id, timeline_id))
                        .or_insert_with(|| RemoteTimeline {
                            tenant_id,
                            timeline_id,
                            layers: Vec::new(),
                            metadata: None,
                        });
                if remote_metadata.is_some() {
                    remote_timeline.metadata = remote_metadata;
                } else {
                    remote_timeline
                        .layers
                        .push(relish_info.download_destination);
                }
            }
            Err(e) => {
                warn!(
                    "Failed to fetch relish info for path {:?}, reason: {:#}",
                    remote_path, e
                );
                continue;
            }
        }
    }

    Ok(fetched)
}

async fn remote_relish_info<P, S: 'static + RelishStorage<RelishStoragePath = P>>(
    relish_storage: &S,
    remote_path: &P,
) -> anyhow::Result<(RemoteRelishInfo, Option<TimelineMetadata>)> {
    let info = relish_storage.info(remote_path)?;
    let metadata = if info.is_metadata {
        let mut metadata_bytes = io::BufWriter::new(std::io::Cursor::new(Vec::new()));
        relish_storage
            .download_relish(remote_path, &mut metadata_bytes)
            .await
            .with_context(|| {
                format!(
                    "Failed to download metadata file contents for tenant {}, timeline {}",
                    info.tenant_id, info.timeline_id
                )
            })?;
        metadata_bytes.flush().await.with_context(|| {
            format!(
                "Failed to download metadata file contents for tenant {}, timeline {}",
                info.tenant_id, info.timeline_id
            )
        })?;
        let metadata_bytes = metadata_bytes.into_inner().into_inner();
        Some(TimelineMetadata::from_bytes(&metadata_bytes)?)
    } else {
        None
    };
    Ok((info, metadata))
}

async fn download_timeline<'a, P, S: 'static + RelishStorage<RelishStoragePath = P>>(
    config: &'static PageServerConf,
    concurrent_sync_limit: &'a Semaphore,
    relish_storage: &'a S,
    remote_timeline: RemoteTimeline,
    urgent: bool,
) -> Option<bool> {
    let timeline_id = remote_timeline.timeline_id;
    let tenant_id = remote_timeline.tenant_id;
    debug!("Downloading layers for timeline {}", timeline_id);

    let new_metadata = if let Some(metadata) = remote_timeline.metadata {
        metadata
    } else {
        warn!("Remote timeline incomplete: no metadata found, aborting the download");
        return None;
    };
    debug!("Downloading {} layers", remote_timeline.layers.len());

    let sync_result = synchronize_layers(
        config,
        concurrent_sync_limit,
        relish_storage,
        remote_timeline.layers.into_iter(),
        SyncOperation::Download,
        &new_metadata,
        tenant_id,
        timeline_id,
    )
    .await;

    match sync_result {
        SyncResult::Success { .. } => {
            register_relish_download(config, tenant_id, timeline_id);
            Some(true)
        }
        SyncResult::MetadataSyncError { .. } => {
            let download = RemoteTimeline {
                layers: Vec::new(),
                metadata: Some(new_metadata),
                tenant_id,
                timeline_id,
            };
            add_to_queue(if urgent {
                SyncTask::UrgentDownload(download)
            } else {
                SyncTask::Download(download)
            });
            Some(false)
        }
        SyncResult::LayerSyncError { not_synced, .. } => {
            let download = RemoteTimeline {
                layers: not_synced,
                metadata: Some(new_metadata),
                tenant_id,
                timeline_id,
            };
            add_to_queue(if urgent {
                SyncTask::UrgentDownload(download)
            } else {
                SyncTask::Download(download)
            });
            Some(false)
        }
    }
}

#[allow(clippy::unnecessary_filter_map)]
async fn upload_timeline<'a, P, S: 'static + RelishStorage<RelishStoragePath = P>>(
    config: &'static PageServerConf,
    concurrent_sync_limit: &'a Semaphore,
    remote_timelines: &'a mut HashMap<(ZTenantId, ZTimelineId), RemoteTimeline>,
    relish_storage: &'a S,
    mut new_upload: LocalTimeline,
) -> Option<bool> {
    let tenant_id = new_upload.tenant_id;
    let timeline_id = new_upload.timeline_id;
    debug!("Uploading layers for timeline {}", timeline_id);

    if let hash_map::Entry::Occupied(o) = remote_timelines.entry((tenant_id, timeline_id)) {
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
                match new_lsn.cmp(&remote_lsn) {
                    Ordering::Equal | Ordering::Less => {
                        warn!(
                        "Received a timeline witn LSN {} that's not later than the one from remote storage {}, not uploading",
                        new_lsn, remote_lsn
                    );
                        return None;
                    }
                    Ordering::Greater => debug!(
                    "Received a timeline with newer LSN {} (storage LSN {}), updating the upload",
                    new_lsn, remote_lsn
                ),
                }
            }
        }
    }

    let LocalTimeline {
        layers: new_layers,
        metadata: new_metadata,
        ..
    } = new_upload;
    let sync_result = synchronize_layers(
        config,
        concurrent_sync_limit,
        relish_storage,
        new_layers.into_iter(),
        SyncOperation::Upload,
        &new_metadata,
        tenant_id,
        timeline_id,
    )
    .await;

    let entry_to_update = remote_timelines
        .entry((tenant_id, timeline_id))
        .or_insert_with(|| RemoteTimeline {
            layers: Vec::new(),
            metadata: Some(new_metadata.clone()),
            tenant_id,
            timeline_id,
        });
    match sync_result {
        SyncResult::Success { synced } => {
            entry_to_update.layers.extend(synced.into_iter());
            entry_to_update.metadata = Some(new_metadata);
            Some(true)
        }
        SyncResult::MetadataSyncError { synced } => {
            entry_to_update.layers.extend(synced.into_iter());
            add_to_queue(SyncTask::Upload(LocalTimeline {
                tenant_id,
                timeline_id,
                layers: Vec::new(),
                metadata: new_metadata,
            }));
            Some(false)
        }
        SyncResult::LayerSyncError { synced, not_synced } => {
            entry_to_update.layers.extend(synced.into_iter());
            add_to_queue(SyncTask::Upload(LocalTimeline {
                tenant_id,
                timeline_id,
                layers: not_synced,
                metadata: new_metadata,
            }));
            Some(false)
        }
    }
}

/// Layer sync operation kind.
///
/// This enum allows to unify the logic for image relish uploads and downloads.
/// When image's layers are synchronized, the only difference
/// between downloads and uploads is the [`RelishStorage`] method we need to call.
#[derive(Debug, Copy, Clone)]
enum SyncOperation {
    Download,
    Upload,
}

/// Image sync result.
#[derive(Debug)]
enum SyncResult {
    /// All relish files are synced (their paths returned).
    /// Metadata file is synced too (path not returned).
    Success { synced: Vec<PathBuf> },
    /// All relish files are synced (their paths returned).
    /// Metadata file is not synced (path not returned).
    MetadataSyncError { synced: Vec<PathBuf> },
    /// Some relish files are not synced, some are (paths returned).
    /// Metadata file is not synced (path not returned).
    LayerSyncError {
        synced: Vec<PathBuf>,
        not_synced: Vec<PathBuf>,
    },
}

/// Synchronizes given layers and metadata contents of a certain image.
/// Relishes are always synced before metadata files are, the latter gets synced only if
/// the rest of the files are successfully processed.
#[allow(clippy::too_many_arguments)]
async fn synchronize_layers<'a, P, S: 'static + RelishStorage<RelishStoragePath = P>>(
    config: &'static PageServerConf,
    concurrent_sync_limit: &'a Semaphore,
    relish_storage: &'a S,
    layers: impl Iterator<Item = PathBuf>,
    sync_operation: SyncOperation,
    new_metadata: &'a TimelineMetadata,
    tenant_id: ZTenantId,
    timeline_id: ZTimelineId,
) -> SyncResult {
    let mut sync_operations = layers
        .into_iter()
        .map(|layer_path| async move {
            let permit = concurrent_sync_limit
                .acquire()
                .await
                .expect("Semaphore should not be closed yet");
            let sync_result = match sync_operation {
                SyncOperation::Download => download(relish_storage, &layer_path).await,
                SyncOperation::Upload => upload(relish_storage, &layer_path).await,
            };
            drop(permit);
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
            relish_storage,
            sync_operation,
            new_metadata,
            tenant_id,
            timeline_id,
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

async fn sync_metadata<'a, P, S: 'static + RelishStorage<RelishStoragePath = P>>(
    config: &'static PageServerConf,
    relish_storage: &'a S,
    sync_operation: SyncOperation,
    new_metadata: &'a TimelineMetadata,
    tenant_id: ZTenantId,
    timeline_id: ZTimelineId,
) -> anyhow::Result<()> {
    debug!("Synchronizing ({:?}) metadata file", sync_operation);

    let local_metadata_path = metadata_path(config, timeline_id, tenant_id);
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
            let remote_path = relish_storage
                .storage_path(&local_metadata_path)
                .with_context(|| {
                    format!(
                        "Failed to get remote storage path for local metadata path '{}'",
                        local_metadata_path.display()
                    )
                })?;
            let mut bytes = io::BufReader::new(new_metadata_bytes.as_slice());
            relish_storage
                .upload_relish(&mut bytes, &remote_path)
                .await?;
        }
    }
    Ok(())
}

async fn upload<P, S: 'static + RelishStorage<RelishStoragePath = P>>(
    relish_storage: &S,
    source: &Path,
) -> anyhow::Result<()> {
    let destination = relish_storage.storage_path(source).with_context(|| {
        format!(
            "Failed to derive storage destination out of upload path {}",
            source.display()
        )
    })?;
    let mut source_file = io::BufReader::new(
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
    relish_storage
        .upload_relish(&mut source_file, &destination)
        .await
}

async fn download<P, S: 'static + RelishStorage<RelishStoragePath = P>>(
    relish_storage: &S,
    destination: &Path,
) -> anyhow::Result<()> {
    if destination.exists() {
        Ok(())
    } else {
        let source = relish_storage.storage_path(destination).with_context(|| {
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

        relish_storage
            .download_relish(&source, &mut destination_file)
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
        relish_storage::local_fs::LocalFs,
        repository::repo_harness::{RepoHarness, TIMELINE_ID},
    };
    use hex_literal::hex;
    use tempfile::tempdir;
    use tokio::io::BufReader;

    const NO_METADATA_TIMELINE_ID: ZTimelineId =
        ZTimelineId::from_array(hex!("3755461d2259a63a80635d760958efd0"));
    const CORRUPT_METADATA_TIMELINE_ID: ZTimelineId =
        ZTimelineId::from_array(hex!("314db9af91fbc02dda586880a3216c61"));

    lazy_static! {
        static ref LIMIT: Semaphore = Semaphore::new(100);
    }

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
        ensure_correct_timeline_upload(&repo_harness, &mut remote_timelines, &storage, upload)
            .await;

        let mut expected_uploads = HashMap::new();
        expected_uploads.insert(
            (repo_harness.tenant_id, TIMELINE_ID),
            RemoteTimeline {
                tenant_id: repo_harness.tenant_id,
                timeline_id: TIMELINE_ID,
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
            &LIMIT,
            &mut remote_timelines,
            &storage,
            new_upload.clone(),
        )
        .await;
        assert_sync_queue_contents(SyncTask::Upload(new_upload), false);
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
            second_timeline,
        )
        .await;

        let mut expected_uploads = HashMap::new();
        let mut expected_layers = first_paths.clone();
        expected_layers.extend(second_paths.clone().into_iter());
        expected_layers.dedup();

        expected_uploads.insert(
            (repo_harness.tenant_id, TIMELINE_ID),
            RemoteTimeline {
                tenant_id: repo_harness.tenant_id,
                timeline_id: TIMELINE_ID,
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
            third_timeline,
        )
        .await;

        let mut expected_uploads = HashMap::new();
        let mut expected_layers = first_paths;
        expected_layers.extend(second_paths.into_iter());
        expected_layers.extend(third_paths.into_iter());
        expected_layers.dedup();

        expected_uploads.insert(
            (repo_harness.tenant_id, TIMELINE_ID),
            RemoteTimeline {
                tenant_id: repo_harness.tenant_id,
                timeline_id: TIMELINE_ID,
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
        let storage = LocalFs::new(tempdir()?.path().to_owned(), &repo_harness.conf.workdir)?;
        let mut remote_timelines =
            store_incorrect_metadata_relishes(&repo_harness, &storage).await?;
        assert_timelines_equal(
            remote_timelines.clone(),
            fetch_existing_uploads(&storage).await?,
        );

        let old_remote_timeline = remote_timelines
            .get(&(repo_harness.tenant_id, NO_METADATA_TIMELINE_ID))
            .unwrap()
            .clone();
        let updated_metadata = dummy_metadata(Lsn(0x100));
        create_local_metadata(&repo_harness, NO_METADATA_TIMELINE_ID, &updated_metadata)?;
        ensure_correct_timeline_upload(
            &repo_harness,
            &mut remote_timelines,
            &storage,
            LocalTimeline {
                tenant_id: repo_harness.tenant_id,
                timeline_id: NO_METADATA_TIMELINE_ID,
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
        let mut updated_timeline = reuploaded_timelines
            .get(&(repo_harness.tenant_id, NO_METADATA_TIMELINE_ID))
            .unwrap()
            .clone();
        updated_timeline.layers.sort();
        assert_eq!(expected_timeline, updated_timeline);

        Ok(())
    }

    #[tokio::test]
    async fn test_upload_with_errors() -> anyhow::Result<()> {
        let repo_harness = RepoHarness::create("test_upload_with_errors")?;
        let storage = LocalFs::new(tempdir()?.path().to_owned(), &repo_harness.conf.workdir)?;
        let mut remote_timelines = HashMap::new();

        let local_path = repo_harness.timeline_path(&TIMELINE_ID).join("something");
        assert!(!local_path.exists());
        assert!(fetch_existing_uploads(&storage).await?.is_empty());

        let timeline_without_local_files = LocalTimeline {
            tenant_id: repo_harness.tenant_id,
            timeline_id: TIMELINE_ID,
            layers: vec![local_path],
            metadata: dummy_metadata(Lsn(0x30)),
        };

        upload_timeline(
            repo_harness.conf,
            &LIMIT,
            &mut remote_timelines,
            &storage,
            timeline_without_local_files.clone(),
        )
        .await;

        assert!(fetch_existing_uploads(&storage).await?.is_empty());
        assert_sync_queue_contents(SyncTask::Upload(timeline_without_local_files), true);
        assert!(!repo_harness.timeline_path(&TIMELINE_ID).exists());

        Ok(())
    }

    #[tokio::test]
    async fn test_download_timeline() -> anyhow::Result<()> {
        let repo_harness = RepoHarness::create("test_download_timeline")?;
        let storage = LocalFs::new(tempdir()?.path().to_owned(), &repo_harness.conf.workdir)?;
        let mut remote_timelines =
            store_incorrect_metadata_relishes(&repo_harness, &storage).await?;
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
            regular_timeline,
        )
        .await;
        fs::remove_dir_all(&regular_timeline_path)?;
        let remote_regular_timeline = remote_timelines
            .get(&(repo_harness.tenant_id, TIMELINE_ID))
            .unwrap()
            .clone();

        download_timeline(
            repo_harness.conf,
            &LIMIT,
            &storage,
            remote_regular_timeline.clone(),
            true,
        )
        .await;
        download_timeline(
            repo_harness.conf,
            &LIMIT,
            &storage,
            remote_regular_timeline.clone(),
            true,
        )
        .await;
        download_timeline(
            repo_harness.conf,
            &LIMIT,
            &storage,
            remote_timelines
                .get(&(repo_harness.tenant_id, NO_METADATA_TIMELINE_ID))
                .unwrap()
                .clone(),
            true,
        )
        .await;
        download_timeline(
            repo_harness.conf,
            &LIMIT,
            &storage,
            remote_timelines
                .get(&(repo_harness.tenant_id, CORRUPT_METADATA_TIMELINE_ID))
                .unwrap()
                .clone(),
            true,
        )
        .await;

        assert_timelines_equal(remote_timelines, fetch_existing_uploads(&storage).await?);
        assert!(!repo_harness
            .timeline_path(&NO_METADATA_TIMELINE_ID)
            .exists());
        assert!(!repo_harness
            .timeline_path(&CORRUPT_METADATA_TIMELINE_ID)
            .exists());
        assert_timeline_files_match(&repo_harness, remote_regular_timeline);

        Ok(())
    }

    #[tokio::test]
    async fn metadata_file_sync() -> anyhow::Result<()> {
        let repo_harness = RepoHarness::create("metadata_file_sync")?;
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
            &LIMIT,
            &mut remote_timelines,
            &storage,
            new_upload.clone(),
        )
        .await;
        assert_timelines_equal(
            remote_timelines.clone(),
            fetch_existing_uploads(&storage).await?,
        );

        let remote_timeline = remote_timelines
            .get(&(repo_harness.tenant_id, TIMELINE_ID))
            .unwrap()
            .clone();
        assert_eq!(
            remote_timeline.metadata.as_ref(),
            Some(&uploaded_metadata),
            "Local corrputed metadata should be ignored when uploading an image"
        );

        download_timeline(
            repo_harness.conf,
            &LIMIT,
            &storage,
            remote_timeline.clone(),
            false,
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

    #[test]
    fn queue_order_test() {
        let repo_harness = RepoHarness::create("queue_order_test").unwrap();

        let tenant_id = repo_harness.tenant_id;
        let timeline_id = TIMELINE_ID;
        let layers = Vec::new();
        let smaller_lsn_metadata = dummy_metadata(Lsn(0x200));
        let bigger_lsn_metadata = dummy_metadata(Lsn(0x300));
        assert!(bigger_lsn_metadata > smaller_lsn_metadata);

        for metadata in [bigger_lsn_metadata.clone(), smaller_lsn_metadata.clone()] {
            add_to_queue(SyncTask::Upload(LocalTimeline {
                tenant_id,
                timeline_id,
                layers: layers.clone(),
                metadata: metadata.clone(),
            }));
            add_to_queue(SyncTask::Download(RemoteTimeline {
                tenant_id,
                timeline_id,
                layers: layers.clone(),
                metadata: Some(metadata.clone()),
            }));
            add_to_queue(SyncTask::UrgentDownload(RemoteTimeline {
                tenant_id,
                timeline_id,
                layers: layers.clone(),
                metadata: Some(metadata),
            }));
        }

        let mut queue_accessor = SYNC_QUEUE.lock().unwrap();
        let mut ordered_tasks = Vec::with_capacity(queue_accessor.len());
        while let Some(task) = queue_accessor.pop() {
            let task_lsn = match &task {
                SyncTask::Upload(LocalTimeline { metadata, .. }) => {
                    Some(metadata.disk_consistent_lsn())
                }
                SyncTask::UrgentDownload(remote_timeline) | SyncTask::Download(remote_timeline) => {
                    remote_timeline.disk_consistent_lsn()
                }
            };

            if let Some(task_lsn) = task_lsn {
                if task_lsn == smaller_lsn_metadata.disk_consistent_lsn()
                    || task_lsn == bigger_lsn_metadata.disk_consistent_lsn()
                {
                    ordered_tasks.push(task);
                }
            }
        }
        drop(queue_accessor);

        let expected_ordered_tasks = vec![
            SyncTask::UrgentDownload(RemoteTimeline {
                tenant_id,
                timeline_id,
                layers: layers.clone(),
                metadata: Some(bigger_lsn_metadata.clone()),
            }),
            SyncTask::UrgentDownload(RemoteTimeline {
                tenant_id,
                timeline_id,
                layers: layers.clone(),
                metadata: Some(smaller_lsn_metadata.clone()),
            }),
            SyncTask::Upload(LocalTimeline {
                tenant_id,
                timeline_id,
                layers: layers.clone(),
                metadata: bigger_lsn_metadata.clone(),
            }),
            SyncTask::Upload(LocalTimeline {
                tenant_id,
                timeline_id,
                layers: layers.clone(),
                metadata: smaller_lsn_metadata.clone(),
            }),
            SyncTask::Download(RemoteTimeline {
                tenant_id,
                timeline_id,
                layers: layers.clone(),
                metadata: Some(bigger_lsn_metadata),
            }),
            SyncTask::Download(RemoteTimeline {
                tenant_id,
                timeline_id,
                layers,
                metadata: Some(smaller_lsn_metadata),
            }),
        ];
        assert_eq!(expected_ordered_tasks, ordered_tasks);
    }

    async fn ensure_correct_timeline_upload<'a>(
        harness: &RepoHarness,
        remote_timelines: &'a mut HashMap<(ZTenantId, ZTimelineId), RemoteTimeline>,
        relish_storage: &'a LocalFs,
        new_upload: LocalTimeline,
    ) {
        upload_timeline(
            harness.conf,
            &LIMIT,
            remote_timelines,
            relish_storage,
            new_upload.clone(),
        )
        .await;
        assert_timelines_equal(
            remote_timelines.clone(),
            fetch_existing_uploads(relish_storage).await.unwrap(),
        );

        let new_remote_files = remote_timelines
            .get(&(new_upload.tenant_id, new_upload.timeline_id))
            .unwrap()
            .clone();
        assert_eq!(new_remote_files.tenant_id, new_upload.tenant_id);
        assert_eq!(new_remote_files.timeline_id, new_upload.timeline_id);
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

        assert_timeline_files_match(harness, new_remote_files);
        assert_sync_queue_contents(SyncTask::Upload(new_upload), false);
    }

    #[track_caller]
    fn assert_timelines_equal(
        mut expected: HashMap<(ZTenantId, ZTimelineId), RemoteTimeline>,
        mut actual: HashMap<(ZTenantId, ZTimelineId), RemoteTimeline>,
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

    #[track_caller]
    fn assert_sync_queue_contents(task: SyncTask, expected_in_queue: bool) {
        let mut queue_accessor = SYNC_QUEUE.lock().unwrap();
        let queue_tasks = queue_accessor.drain().collect::<BTreeSet<_>>();
        drop(queue_accessor);

        if expected_in_queue {
            assert!(
                queue_tasks.contains(&task),
                "Sync queue should contain task {:?}",
                task
            );
        } else {
            assert!(
                !queue_tasks.contains(&task),
                "Sync queue has unexpected task {:?}",
                task
            );
        }
    }

    fn assert_timeline_files_match(harness: &RepoHarness, remote_files: RemoteTimeline) {
        let local_timeline_dir = harness.timeline_path(&remote_files.timeline_id);
        let local_paths = fs::read_dir(&local_timeline_dir)
            .unwrap()
            .map(|dir| dir.unwrap().path())
            .collect::<BTreeSet<_>>();
        let mut reported_remote_files = remote_files.layers.into_iter().collect::<BTreeSet<_>>();
        if let Some(remote_metadata) = remote_files.metadata {
            let local_metadata_path =
                metadata_path(harness.conf, remote_files.timeline_id, harness.tenant_id);
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
                    .expect("Remote relishes are expected to have their timeline dir as parent"),
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

    async fn store_incorrect_metadata_relishes(
        harness: &RepoHarness,
        storage: &LocalFs,
    ) -> anyhow::Result<HashMap<(ZTenantId, ZTimelineId), RemoteTimeline>> {
        let mut remote_timelines = HashMap::new();

        ensure_correct_timeline_upload(
            harness,
            &mut remote_timelines,
            storage,
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
            create_local_timeline(
                harness,
                CORRUPT_METADATA_TIMELINE_ID,
                &["a2", "b2"],
                dummy_metadata(Lsn(0)),
            )?,
        )
        .await;

        storage
            .delete_relish(&storage.storage_path(&metadata_path(
                harness.conf,
                NO_METADATA_TIMELINE_ID,
                harness.tenant_id,
            ))?)
            .await?;
        storage
            .upload_relish(
                &mut BufReader::new(Cursor::new("corrupt meta".to_string().into_bytes())),
                &storage.storage_path(&metadata_path(
                    harness.conf,
                    CORRUPT_METADATA_TIMELINE_ID,
                    harness.tenant_id,
                ))?,
            )
            .await?;

        for remote_relish in remote_timelines.values_mut() {
            remote_relish.metadata = None;
        }

        Ok(remote_timelines)
    }

    fn create_local_timeline(
        harness: &RepoHarness,
        timeline_id: ZTimelineId,
        filenames: &[&str],
        metadata: TimelineMetadata,
    ) -> anyhow::Result<LocalTimeline> {
        let timeline_path = harness.timeline_path(&timeline_id);
        fs::create_dir_all(&timeline_path)?;

        let mut layers = Vec::with_capacity(filenames.len());
        for &file in filenames {
            let file_path = timeline_path.join(file);
            fs::write(&file_path, dummy_contents(file).into_bytes())?;
            layers.push(file_path);
        }

        create_local_metadata(harness, timeline_id, &metadata)?;

        Ok(LocalTimeline {
            tenant_id: harness.tenant_id,
            timeline_id,
            layers,
            metadata,
        })
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
        TimelineMetadata::new(disk_consistent_lsn, None, None, Lsn(0))
    }
}

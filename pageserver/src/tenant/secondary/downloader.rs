use std::{
    collections::{HashMap, HashSet},
    str::FromStr,
    sync::Arc,
    time::{Duration, Instant, SystemTime},
};

use crate::{
    config::PageServerConf,
    metrics::SECONDARY_MODE,
    tenant::{
        remote_timeline_client::{index::LayerFileMetadata, HEATMAP_BASENAME},
        secondary::CommandResponse,
        storage_layer::{Layer, LayerFileName},
        timeline::{DiskUsageEvictionInfo, LocalLayerInfoForDiskUsageEviction},
    },
    METADATA_FILE_NAME,
};

use super::SecondaryTenant;
use crate::tenant::{
    mgr::TenantManager,
    remote_timeline_client::{download::download_layer_file, remote_heatmap_path},
};
use anyhow::Context;

use chrono::format::{DelayedFormat, StrftimeItems};
use remote_storage::GenericRemoteStorage;

use tokio_util::sync::CancellationToken;
use tracing::Instrument;
use utils::{
    completion::Barrier,
    fs_ext,
    id::{TenantId, TimelineId},
    sync::gate::GateGuard,
};

use super::{
    heatmap::{HeatMapTenant, HeatMapTimeline},
    CommandRequest, DownloadCommand,
};

/// Interval between checking if any Secondary tenants have download work to do:
/// note that this is _not_ the frequency with which we actually freshen the tenants,
/// just the frequency with which we wake up to decide whether anyone needs freshening.
///
/// Making this somewhat infrequent reduces the load on mutexes inside TenantManager
/// and SecondaryTenant for reads when checking for work to do.
const DOWNLOAD_CHECK_INTERVAL: Duration = Duration::from_millis(10000);

/// For each tenant, how long must have passed since the last download_tenant call before
/// calling it again.  This is approximately the time by which local data is allowed
/// to fall behind remote data.
///
/// TODO: this should be an upper bound, and tenants that are uploading regularly
/// should adaptively freshen more often (e.g. a tenant writing 1 layer per second
/// should not wait a minute between freshens)
const DOWNLOAD_FRESHEN_INTERVAL: Duration = Duration::from_millis(60000);

#[derive(Debug, Clone)]
pub(super) struct OnDiskState {
    layer: Layer,
    access_time: SystemTime,
}

impl OnDiskState {
    fn new(
        conf: &'static PageServerConf,
        tenant_id: &TenantId,
        timeline_id: &TimelineId,
        name: LayerFileName,
        metadata: LayerFileMetadata,
        access_time: SystemTime,
    ) -> Self {
        Self {
            layer: Layer::for_secondary(conf, tenant_id, timeline_id, name, metadata),
            access_time,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub(super) struct SecondaryDetailTimeline {
    pub(super) on_disk_layers: HashMap<LayerFileName, OnDiskState>,

    /// We remember when layers were evicted, to prevent re-downloading them.
    /// TODO: persist this, so that we don't try and re-download everything on restart.
    pub(super) evicted_at: HashMap<LayerFileName, SystemTime>,
}

/// This state is written by the secondary downloader, it is opaque
/// to TenantManager
#[derive(Default, Debug)]
pub(super) struct SecondaryDetail {
    freshened_at: Option<Instant>,
    pub(super) timelines: HashMap<TimelineId, SecondaryDetailTimeline>,
}

/// Helper for logging SystemTime
fn strftime(t: &'_ SystemTime) -> DelayedFormat<StrftimeItems<'_>> {
    let datetime: chrono::DateTime<chrono::Utc> = (*t).into();
    datetime.format("%d/%m/%Y %T")
}

impl SecondaryDetail {
    pub(super) fn is_uninit(&self) -> bool {
        // FIXME: empty timelines is not synonymous with not initialized, as it is legal for
        // a tenant to exist with no timelines.
        self.timelines.is_empty()
    }

    pub(super) async fn init_detail(
        conf: &'static PageServerConf,
        tenant_id: TenantId,
    ) -> HashMap<TimelineId, SecondaryDetailTimeline> {
        tracing::info!("init_detail");
        // Load heatmap from local storage
        let heatmap_path = conf.tenant_path(&tenant_id).join(HEATMAP_BASENAME);
        let heatmap = match tokio::fs::read(heatmap_path).await {
            Ok(bytes) => serde_json::from_slice::<HeatMapTenant>(&bytes).unwrap(),
            Err(e) => {
                if e.kind() == std::io::ErrorKind::NotFound {
                    return HashMap::new();
                } else {
                    // TODO: use local IO fatal helpers
                    panic!("Unexpected error");
                }
            }
        };

        let mut timelines = HashMap::new();

        for heatmap_timeline in heatmap.timelines {
            // TODO: use fatal IO helpers
            let detail = init_timeline_state(conf, &tenant_id, &heatmap_timeline)
                .await
                .expect("Failed reading local disk");
            timelines.insert(heatmap_timeline.timeline_id, detail);
        }

        timelines
    }

    pub(super) fn get_layers_for_eviction(&self) -> Vec<(TimelineId, DiskUsageEvictionInfo)> {
        let mut result = Vec::new();
        for (timeline_id, timeline_detail) in &self.timelines {
            let layers: Vec<_> = timeline_detail
                .on_disk_layers
                .values()
                .map(|ods| LocalLayerInfoForDiskUsageEviction {
                    layer: ods.layer.clone(),
                    last_activity_ts: ods.access_time,
                })
                .collect();

            let max_layer_size = layers.iter().map(|l| l.layer.metadata().file_size()).max();

            result.push((
                *timeline_id,
                DiskUsageEvictionInfo {
                    resident_layers: layers,
                    max_layer_size,
                },
            ))
        }

        tracing::debug!(
            "Found {} timelines, {} layers",
            self.timelines.len(),
            result.len()
        );

        result
    }
}

/// Keep trying to do downloads until the cancellation token is fired.  Remote storage
/// errors are handled internally: any error returned by this function is an unexpected
/// internal error of some kind.
pub(super) async fn downloader_task(
    conf: &'static PageServerConf,
    tenant_manager: Arc<TenantManager>,
    remote_storage: GenericRemoteStorage,
    mut command_queue: tokio::sync::mpsc::Receiver<CommandRequest<DownloadCommand>>,
    background_jobs_can_start: Barrier,
    cancel: CancellationToken,
) -> anyhow::Result<()> {
    let downloader = SecondaryDownloader {
        conf,
        tenant_manager,
        remote_storage,
        cancel: cancel.clone(),
    };

    tracing::info!("Waiting for background_jobs_can start...");
    background_jobs_can_start.wait().await;
    tracing::info!("background_jobs_can is ready, proceeding.");

    while !cancel.is_cancelled() {
        downloader.iteration().await?;

        tokio::select! {
            _ = cancel.cancelled() => {
                tracing::info!("Heatmap writer terminating");
                break;
            },
            _ = tokio::time::sleep(DOWNLOAD_CHECK_INTERVAL) => {},
            cmd = command_queue.recv() => {
                let cmd = match cmd {
                    Some(c) =>c,
                    None => {
                        // SecondaryController was destroyed, and this has raced with
                        // our CancellationToken
                        tracing::info!("Heatmap writer terminating");
                        break;
                    }
                };

                let CommandRequest{
                    response_tx,
                    payload
                } = cmd;
                let result = downloader.handle_command(payload).await;
                if response_tx.send(CommandResponse{result}).is_err() {
                    // Caller went away, e.g. because an HTTP request timed out
                    tracing::info!("Dropping response to administrative command")
                }
            }
        }
    }

    Ok(())
}
struct SecondaryDownloader {
    conf: &'static PageServerConf,
    tenant_manager: Arc<TenantManager>,
    remote_storage: GenericRemoteStorage,
    cancel: CancellationToken,
}

struct TenantJob {
    tenant_id: TenantId,
    secondary_state: Arc<SecondaryTenant>,

    // This mutex guard conveys the right to write to the tenant's local directory: it must
    // be taken before doing downloads, and TenantManager must ensure it has been released
    // before it considers shutdown complete for the secondary state -- [`SecondaryDownloader`]
    // will thereby never be racing with [`Tenant`] for access to local files.
    _guard: GateGuard,
}

impl SecondaryDownloader {
    async fn iteration(&self) -> anyhow::Result<()> {
        // Step 1: identify some tenants that we may work on
        let mut candidates: Vec<TenantJob> = Vec::new();
        self.tenant_manager
            .foreach_secondary_tenants(|tenant_id, secondary_state| {
                let guard = match secondary_state.gate.enter() {
                    Ok(guard) => guard,
                    // Tenant is shutting down, do no downloads for it
                    Err(_) => return,
                };

                candidates.push(TenantJob {
                    tenant_id: *tenant_id,
                    secondary_state: secondary_state.clone(),
                    _guard: guard,
                });
            });

        // Step 2: prioritized selection of next batch of tenants to freshen
        let now = Instant::now();
        let candidates = candidates.into_iter().filter(|c| {
            let detail = c.secondary_state.detail.lock().unwrap();
            match detail.freshened_at {
                None => true, // Not yet freshened, therefore elegible to run
                Some(t) => {
                    let since = now.duration_since(t);
                    since > DOWNLOAD_FRESHEN_INTERVAL
                }
            }
        });

        // TODO: don't just cut down the list, prioritize it to freshen the stalest tenants first
        // TODO: bounded parallelism

        // Step 3: spawn download_tenant tasks
        for job in candidates {
            if job.secondary_state.cancel.is_cancelled() {
                continue;
            }

            async {
                if let Err(e) = self
                    .download_tenant(&job)
                    .instrument(tracing::info_span!("download_tenant", tenant_id=%job.tenant_id))
                    .await
                {
                    tracing::info!("Failed to freshen secondary content: {e:#}")
                };

                // Update freshened_at even if there was an error: we don't want errored tenants to implicitly
                // take priority to run again.
                let mut detail = job.secondary_state.detail.lock().unwrap();
                detail.freshened_at = Some(Instant::now());
            }
            .instrument(tracing::info_span!(
                "download_tenant",
                tenant_id = %job.tenant_id
            ))
            .await;
        }

        Ok(())
    }

    async fn handle_command(&self, command: DownloadCommand) -> anyhow::Result<()> {
        match command {
            DownloadCommand::Download(req_tenant_id) => {
                let mut candidates: Vec<TenantJob> = Vec::new();
                self.tenant_manager
                    .foreach_secondary_tenants(|tenant_id, secondary_state| {
                        tracing::info!("foreach_secondary: {tenant_id} ({req_tenant_id})");
                        if tenant_id == &req_tenant_id {
                            let guard = match secondary_state.gate.enter() {
                                Ok(guard) => guard,
                                // Shutting down
                                Err(_) => return,
                            };

                            candidates.push(TenantJob {
                                tenant_id: *tenant_id,
                                secondary_state: secondary_state.clone(),
                                _guard: guard,
                            });
                        }
                    });

                let tenant_job = if candidates.len() != 1 {
                    anyhow::bail!("Tenant not found in secondary mode");
                } else {
                    candidates.pop().unwrap()
                };

                self.download_tenant(&tenant_job).await
            }
        }
    }

    async fn download_heatmap(&self, tenant_id: &TenantId) -> anyhow::Result<HeatMapTenant> {
        // TODO: make download conditional on ETag having changed since last download

        let heatmap_path = remote_heatmap_path(tenant_id);
        // TODO: wrap this download in a select! that checks self.cancel
        let mut download = self.remote_storage.download(&heatmap_path).await?;
        let mut heatmap_bytes = Vec::new();
        let _size = tokio::io::copy(&mut download.download_stream, &mut heatmap_bytes)
            .await
            .with_context(|| format!("download heatmap {heatmap_path:?}"))?;

        SECONDARY_MODE.download_heatmap.inc();

        Ok(serde_json::from_slice::<HeatMapTenant>(&heatmap_bytes)?)
    }

    async fn download_timeline(
        &self,
        job: &TenantJob,
        timeline: HeatMapTimeline,
    ) -> anyhow::Result<()> {
        let timeline_path = self
            .conf
            .timeline_path(&job.tenant_id, &timeline.timeline_id);

        // Accumulate updates to the state
        let mut touched = Vec::new();

        // Clone a view of what layers already exist on disk
        let timeline_state = job
            .secondary_state
            .detail
            .lock()
            .unwrap()
            .timelines
            .get(&timeline.timeline_id)
            .cloned();

        let timeline_state = match timeline_state {
            Some(t) => t,
            None => {
                // We have no existing state: need to scan local disk for layers first.
                let timeline_state =
                    init_timeline_state(self.conf, &job.tenant_id, &timeline).await?;

                // Re-acquire detail lock now that we're done with async load from local FS
                job.secondary_state
                    .detail
                    .lock()
                    .unwrap()
                    .timelines
                    .insert(timeline.timeline_id, timeline_state.clone());
                timeline_state
            }
        };

        let layers_in_heatmap = timeline
            .layers
            .iter()
            .map(|l| &l.name)
            .collect::<HashSet<_>>();
        let layers_on_disk = timeline_state
            .on_disk_layers
            .iter()
            .map(|l| l.0)
            .collect::<HashSet<_>>();

        // Remove on-disk layers that are no longer present in heatmap
        for layer in layers_on_disk.difference(&layers_in_heatmap) {
            let local_path = timeline_path.join(layer.to_string());
            tracing::info!("Removing secondary local layer {layer} because it's absent in heatmap",);
            tokio::fs::remove_file(&local_path)
                .await
                .or_else(fs_ext::ignore_not_found)?;
        }

        // Download heatmap layers that are not present on local disk, or update their
        // access time if they are already present.
        for layer in timeline.layers {
            if self.cancel.is_cancelled() {
                return Ok(());
            }

            // Existing on-disk layers: just update their access time.
            if let Some(on_disk) = timeline_state.on_disk_layers.get(&layer.name) {
                tracing::debug!("Layer {} is already on disk", layer.name);
                if on_disk.layer.metadata() != LayerFileMetadata::from(&layer.metadata)
                    || on_disk.access_time != layer.access_time
                {
                    // We already have this layer on disk.  Update its access time.
                    tracing::debug!(
                        "Access time updated for layer {}: {} -> {}",
                        layer.name,
                        strftime(&on_disk.access_time),
                        strftime(&layer.access_time)
                    );
                    touched.push(layer);
                }
                continue;
            } else {
                tracing::debug!("Layer {} not present on disk yet", layer.name);
            }

            // Eviction: if we evicted a layer, then do not re-download it unless it was accessed more
            // recently than it was evicted.
            if let Some(evicted_at) = timeline_state.evicted_at.get(&layer.name) {
                if &layer.access_time > evicted_at {
                    tracing::info!(
                        "Re-downloading evicted layer {}, accessed at {}, evicted at {}",
                        layer.name,
                        strftime(&layer.access_time),
                        strftime(evicted_at)
                    );
                } else {
                    tracing::trace!(
                        "Not re-downloading evicted layer {}, accessed at {}, evicted at {}",
                        layer.name,
                        strftime(&layer.access_time),
                        strftime(evicted_at)
                    );
                    continue;
                }
            }

            match download_layer_file(
                self.conf,
                &self.remote_storage,
                job.tenant_id,
                timeline.timeline_id,
                &layer.name,
                &LayerFileMetadata::from(&layer.metadata),
            )
            .await
            {
                Ok(downloaded_bytes) => {
                    if downloaded_bytes != layer.metadata.file_size {
                        let local_path = timeline_path.join(layer.name.to_string());

                        tracing::error!(
                            "Downloaded layer {} with unexpected size {} != {}",
                            layer.name,
                            downloaded_bytes,
                            layer.metadata.file_size
                        );

                        tokio::fs::remove_file(&local_path)
                            .await
                            .or_else(fs_ext::ignore_not_found)?;
                    }

                    SECONDARY_MODE.download_layer.inc();
                    touched.push(layer)
                }
                Err(e) => {
                    // No retries here: secondary downloads don't have to succeed: if they fail we just proceed and expect
                    // that on some future call to freshen the download will work.
                    // TODO: refine this behavior.
                    tracing::info!("Failed to download layer {}: {}", layer.name, e);
                }
            }
        }

        // Write updates to state to record layers we just downloaded or touched.
        {
            let mut detail = job.secondary_state.detail.lock().unwrap();
            let timeline_detail = detail.timelines.entry(timeline.timeline_id).or_default();

            tracing::info!("Wrote timeline_detail for {} touched layers", touched.len());

            for t in touched {
                use std::collections::hash_map::Entry;
                match timeline_detail.on_disk_layers.entry(t.name.clone()) {
                    Entry::Occupied(mut v) => {
                        v.get_mut().access_time = t.access_time;
                    }
                    Entry::Vacant(e) => {
                        e.insert(OnDiskState::new(
                            self.conf,
                            &job.tenant_id,
                            &timeline.timeline_id,
                            t.name,
                            LayerFileMetadata::from(&t.metadata),
                            t.access_time,
                        ));
                    }
                }
            }
        }

        Ok(())
    }

    async fn download_tenant(&self, job: &TenantJob) -> anyhow::Result<()> {
        tracing::debug!("Downloading heatmap for secondary tenant {}", job.tenant_id);
        // Download the tenant's heatmap
        let heatmap = self.download_heatmap(&job.tenant_id).await?;

        // Save the heatmap: this will be useful on restart, allowing us to reconstruct
        // layer metadata without having to re-download it.
        let heatmap_path = self.conf.tenant_path(&job.tenant_id).join(HEATMAP_BASENAME);
        // TODO: use crashsafe overwrite
        // TODO: use die-on-io-error helpers
        tokio::fs::write(
            &heatmap_path,
            &serde_json::to_vec(&heatmap).expect("We just deserialized this"),
        )
        .await
        .unwrap();

        tracing::debug!("Wrote heatmap to {}", heatmap_path);

        // Download the layers in the heatmap
        for timeline in heatmap.timelines {
            if self.cancel.is_cancelled() {
                return Ok(());
            }

            let timeline_id = timeline.timeline_id;
            self.download_timeline(job, timeline)
                .instrument(tracing::info_span!(
                    "download_timeline",
                    tenant_id=%job.tenant_id,
                    %timeline_id
                ))
                .await?;
        }

        // TODO: remove local disk content for any timelines that don't exist in remote storage

        Ok(())
    }
}

/// Scan local storage and build up Layer objects based on the metadata in a HeatMapTimeline
async fn init_timeline_state(
    conf: &'static PageServerConf,
    tenant_id: &TenantId,
    heatmap: &HeatMapTimeline,
) -> anyhow::Result<SecondaryDetailTimeline> {
    let timeline_path = conf.timeline_path(tenant_id, &heatmap.timeline_id);
    let mut detail = SecondaryDetailTimeline::default();

    let mut dir = match tokio::fs::read_dir(&timeline_path).await {
        Ok(d) => d,
        Err(e) => {
            if e.kind() == std::io::ErrorKind::NotFound {
                tracing::info!("Creating timeline directory {timeline_path}");
                tokio::fs::create_dir_all(&timeline_path).await?;

                // No entries to report: drop out.
                return Ok(detail);
            } else {
                return Err(e.into());
            }
        }
    };

    let heatmap_metadata: HashMap<_, _> = heatmap.layers.iter().map(|l| (&l.name, l)).collect();

    while let Some(dentry) = dir.next_entry().await? {
        let dentry_file_name = dentry.file_name();
        let file_name = dentry_file_name.to_string_lossy();
        let local_meta = dentry.metadata().await?;

        // Secondary mode doesn't use local metadata files, but they might have been left behind by an attached tenant.
        if file_name == METADATA_FILE_NAME {
            continue;
        }

        match LayerFileName::from_str(&file_name) {
            Ok(name) => {
                let remote_meta = heatmap_metadata.get(&name);
                match remote_meta {
                    Some(remote_meta) => {
                        // TODO: checksums for layers (https://github.com/neondatabase/neon/issues/2784)
                        if local_meta.len() != remote_meta.metadata.file_size {
                            // This should not happen, because we do crashsafe write-then-rename when downloading
                            // layers, and layers in remote storage are immutable.  Remove the local file because
                            // we cannot trust it.
                            tracing::warn!(
                                "Removing local layer {name} with unexpected local size {} != {}",
                                local_meta.len(),
                                remote_meta.metadata.file_size
                            );
                        } else {
                            // We expect the access time to be initialized immediately afterwards, when
                            // the latest heatmap is applied to the state.
                            detail.on_disk_layers.insert(
                                name.clone(),
                                OnDiskState::new(
                                    conf,
                                    tenant_id,
                                    &heatmap.timeline_id,
                                    name,
                                    LayerFileMetadata::from(&remote_meta.metadata),
                                    remote_meta.access_time,
                                ),
                            );
                        }
                    }
                    None => {
                        // FIXME: consider some optimization when transitioning from attached to secondary: maybe
                        // wait until we have seen a heatmap that is more recent than the most recent on-disk state?  Otherwise
                        // we will end up deleting any layers which were created+uploaded more recently than the heatmap.
                        tracing::info!(
                            "Removing secondary local layer {} because it's absent in heatmap",
                            name
                        );
                        tokio::fs::remove_file(dentry.path()).await?;
                    }
                }
            }
            Err(_) => {
                // Ignore it.
                tracing::warn!("Unexpected file in timeline directory: {file_name}");
            }
        }
    }

    Ok(detail)
}

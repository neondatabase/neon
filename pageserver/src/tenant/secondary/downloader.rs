use std::{
    collections::{HashMap, HashSet},
    pin::Pin,
    str::FromStr,
    sync::Arc,
    time::{Duration, Instant, SystemTime},
};

use crate::{
    config::PageServerConf,
    metrics::SECONDARY_MODE,
    tenant::{
        config::SecondaryLocationConfig,
        debug_assert_current_span_has_tenant_and_timeline_id,
        remote_timeline_client::index::LayerFileMetadata,
        span::debug_assert_current_span_has_tenant_id,
        storage_layer::{Layer, LayerFileName},
        tasks::{warn_when_period_overrun, BackgroundLoopKind},
        timeline::{DiskUsageEvictionInfo, LocalLayerInfoForDiskUsageEviction},
    },
    virtual_file::{on_fatal_io_error, MaybeFatalIo, VirtualFile},
    METADATA_FILE_NAME, TEMP_FILE_SUFFIX,
};

use super::{
    scheduler::{HasBarrier, JobGenerator, SchedulingResult, TenantBackgroundJobs, TenantScoped},
    SecondaryTenant,
};
use crate::tenant::{
    mgr::TenantManager,
    remote_timeline_client::{download::download_layer_file, remote_heatmap_path},
};
use anyhow::Context;

use chrono::format::{DelayedFormat, StrftimeItems};
use futures::Future;
use pageserver_api::shard::TenantShardId;
use rand::Rng;
use remote_storage::GenericRemoteStorage;

use tokio_util::sync::CancellationToken;
use tracing::{info_span, instrument, Instrument};
use utils::{completion::Barrier, crashsafe::path_with_suffix_extension, fs_ext, id::TimelineId};

use super::{
    heatmap::{HeatMapTenant, HeatMapTimeline},
    CommandRequest, DownloadCommand,
};

/// For each tenant, how long must have passed since the last download_tenant call before
/// calling it again.  This is approximately the time by which local data is allowed
/// to fall behind remote data.
///
/// TODO: this should just be a default, and the actual period should be controlled
/// via the heatmap itself
/// `<ttps://github.com/neondatabase/neon/issues/6200>`
const DOWNLOAD_FRESHEN_INTERVAL: Duration = Duration::from_millis(60000);

#[derive(Debug, Clone)]
pub(super) struct OnDiskState {
    layer: Layer,
    access_time: SystemTime,
}

impl OnDiskState {
    fn new(
        conf: &'static PageServerConf,
        tenant_shard_id: &TenantShardId,
        timeline_id: &TimelineId,
        name: LayerFileName,
        metadata: LayerFileMetadata,
        access_time: SystemTime,
    ) -> Self {
        Self {
            layer: Layer::for_secondary(conf, tenant_shard_id, timeline_id, name, metadata),
            access_time,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub(super) struct SecondaryDetailTimeline {
    pub(super) on_disk_layers: HashMap<LayerFileName, OnDiskState>,

    /// We remember when layers were evicted, to prevent re-downloading them.
    pub(super) evicted_at: HashMap<LayerFileName, SystemTime>,
}

/// This state is written by the secondary downloader, it is opaque
/// to TenantManager
#[derive(Debug)]
pub(super) struct SecondaryDetail {
    pub(super) config: SecondaryLocationConfig,

    last_download: Option<Instant>,
    next_download: Option<Instant>,
    pub(super) timelines: HashMap<TimelineId, SecondaryDetailTimeline>,
}

/// Helper for logging SystemTime
fn strftime(t: &'_ SystemTime) -> DelayedFormat<StrftimeItems<'_>> {
    let datetime: chrono::DateTime<chrono::Utc> = (*t).into();
    datetime.format("%d/%m/%Y %T")
}

impl SecondaryDetail {
    pub(super) fn new(config: SecondaryLocationConfig) -> Self {
        Self {
            config,
            last_download: None,
            next_download: None,
            timelines: HashMap::new(),
        }
    }

    pub(super) fn is_uninit(&self) -> bool {
        // FIXME: empty timelines is not synonymous with not initialized, as it is legal for
        // a tenant to exist with no timelines.
        self.timelines.is_empty()
    }

    pub(super) async fn init_detail(
        conf: &'static PageServerConf,
        tenant_shard_id: TenantShardId,
    ) -> HashMap<TimelineId, SecondaryDetailTimeline> {
        tracing::info!("init_detail");
        // Load heatmap from local storage
        let heatmap_path = conf.tenant_heatmap_path(&tenant_shard_id);
        let heatmap = match tokio::fs::read(&heatmap_path).await {
            Ok(bytes) => serde_json::from_slice::<HeatMapTenant>(&bytes).unwrap(),
            Err(e) => {
                if e.kind() == std::io::ErrorKind::NotFound {
                    return HashMap::new();
                } else {
                    on_fatal_io_error(&e, &format!("Reading heatmap file from {heatmap_path}"))
                }
            }
        };

        let mut timelines = HashMap::new();

        for heatmap_timeline in heatmap.timelines {
            let detail = init_timeline_state(conf, &tenant_shard_id, &heatmap_timeline).await;
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

struct SecondaryDownloader {
    tenant_manager: Arc<TenantManager>,
    remote_storage: GenericRemoteStorage,
}

struct PendingDownload {
    secondary_state: Arc<SecondaryTenant>,
    last_download: Option<Instant>,
    target_time: Option<Instant>,
    period: Option<Duration>,
}

impl TenantScoped for PendingDownload {
    fn get_tenant_shard_id(&self) -> &TenantShardId {
        self.secondary_state.get_tenant_shard_id()
    }
}

struct RunningDownload {
    barrier: Barrier,
}

impl HasBarrier for RunningDownload {
    fn get_barrier(&self) -> Barrier {
        self.barrier.clone()
    }
}

struct CompleteDownload {
    secondary_state: Arc<SecondaryTenant>,
    completed_at: Instant,
}

impl TenantScoped for CompleteDownload {
    fn get_tenant_shard_id(&self) -> &TenantShardId {
        self.secondary_state.get_tenant_shard_id()
    }
}

impl TenantScoped for SecondaryTenant {
    fn get_tenant_shard_id(&self) -> &TenantShardId {
        self.get_tenant_shard_id()
    }
}

type Scheduler = TenantBackgroundJobs<
    SecondaryDownloader,
    PendingDownload,
    RunningDownload,
    CompleteDownload,
    DownloadCommand,
>;

#[async_trait::async_trait]
impl JobGenerator<PendingDownload, RunningDownload, CompleteDownload, DownloadCommand>
    for SecondaryDownloader
{
    #[instrument(skip_all, fields(tenant_id=%completion.get_tenant_shard_id().tenant_id, shard_id=%completion.get_tenant_shard_id().shard_slug()))]
    fn on_completion(&mut self, completion: CompleteDownload) {
        let CompleteDownload {
            secondary_state,
            completed_at: _completed_at,
        } = completion;

        tracing::debug!("Secondary tenant download completed");

        // Update freshened_at even if there was an error: we don't want errored tenants to implicitly
        // take priority to run again.
        let mut detail = secondary_state.detail.lock().unwrap();
        detail.next_download = Some(Instant::now() + DOWNLOAD_FRESHEN_INTERVAL);
    }

    async fn schedule(&mut self) -> SchedulingResult<PendingDownload> {
        let mut result = SchedulingResult {
            jobs: Vec::new(),
            want_interval: None,
        };

        // Step 1: identify some tenants that we may work on
        let mut tenants: Vec<Arc<SecondaryTenant>> = Vec::new();
        self.tenant_manager
            .foreach_secondary_tenants(|_id, secondary_state| {
                tenants.push(secondary_state.clone());
            });

        // Step 2: filter out tenants which are not yet elegible to run
        let now = Instant::now();
        result.jobs = tenants
            .into_iter()
            .filter_map(|secondary_tenant| {
                let (last_download, next_download) = {
                    let mut detail = secondary_tenant.detail.lock().unwrap();

                    if !detail.config.warm {
                        // Downloads are disabled for this tenant
                        detail.next_download = None;
                        return None;
                    }

                    if detail.next_download.is_none() {
                        // Initialize with a jitter: this spreads initial downloads on startup
                        // or mass-attach across our freshen interval.
                        let jittered_period =
                            rand::thread_rng().gen_range(Duration::ZERO..DOWNLOAD_FRESHEN_INTERVAL);
                        detail.next_download = Some(now.checked_add(jittered_period).expect(
                        "Using our constant, which is known to be small compared with clock range",
                    ));
                    }
                    (detail.last_download, detail.next_download.unwrap())
                };

                if now < next_download {
                    Some(PendingDownload {
                        secondary_state: secondary_tenant,
                        last_download,
                        target_time: Some(next_download),
                        period: Some(DOWNLOAD_FRESHEN_INTERVAL),
                    })
                } else {
                    None
                }
            })
            .collect();

        // Step 3: sort by target execution time to run most urgent first.
        result.jobs.sort_by_key(|j| j.target_time);

        result
    }

    fn on_command(&mut self, command: DownloadCommand) -> anyhow::Result<PendingDownload> {
        let tenant_shard_id = command.get_tenant_shard_id();

        let tenant = self
            .tenant_manager
            .get_secondary_tenant_shard(*tenant_shard_id);
        let Some(tenant) = tenant else {
            {
                return Err(anyhow::anyhow!("Not found or not in Secondary mode"));
            }
        };

        Ok(PendingDownload {
            target_time: None,
            period: None,
            last_download: None,
            secondary_state: tenant,
        })
    }

    fn spawn(
        &mut self,
        job: PendingDownload,
    ) -> (
        RunningDownload,
        Pin<Box<dyn Future<Output = CompleteDownload> + Send>>,
    ) {
        let PendingDownload {
            secondary_state,
            last_download,
            target_time,
            period,
        } = job;

        let (completion, barrier) = utils::completion::channel();
        let remote_storage = self.remote_storage.clone();
        let conf = self.tenant_manager.get_conf();
        let tenant_shard_id = *secondary_state.get_tenant_shard_id();
        (RunningDownload { barrier }, Box::pin(async move {
            let _completion = completion;

            if let Err(e) = TenantDownloader::new(conf, &remote_storage, &secondary_state)
                .download()
                .await
            {
                tracing::info!("Failed to freshen secondary content: {e:#}")
            };

            // If the job had a target execution time, we may check our final execution
            // time against that for observability purposes.
            if let (Some(target_time), Some(period)) = (target_time, period) {
                // Only track execution lag if this isn't our first download: otherwise, it is expected
                // that execution will have taken longer than our configured interval, for example
                // when starting up a pageserver and 
                if last_download.is_some() {
                    // Elapsed time includes any scheduling lag as well as the execution of the job
                    let elapsed = Instant::now().duration_since(target_time);

                    warn_when_period_overrun(
                        elapsed,
                        period,
                        BackgroundLoopKind::SecondaryDownload,
                    );
                }
            }

            CompleteDownload {
                    secondary_state,
                    completed_at: Instant::now(),
                }
        }.instrument(info_span!(parent: None, "secondary_download", tenant_id=%tenant_shard_id.tenant_id, shard_id=%tenant_shard_id.shard_slug()))))
    }
}

pub(super) async fn downloader_task(
    tenant_manager: Arc<TenantManager>,
    remote_storage: GenericRemoteStorage,
    command_queue: tokio::sync::mpsc::Receiver<CommandRequest<DownloadCommand>>,
    background_jobs_can_start: Barrier,
    cancel: CancellationToken,
) {
    // TODO: separate config for downloads
    let concurrency = tenant_manager.get_conf().heatmap_upload_concurrency;

    let generator = SecondaryDownloader {
        tenant_manager,
        remote_storage,
    };
    let mut scheduler = Scheduler::new(generator, concurrency);

    scheduler
        .run(command_queue, background_jobs_can_start, cancel)
        .instrument(info_span!("secondary_downloads"))
        .await
}

/// Scan local storage and build up Layer objects based on the metadata in a HeatMapTimeline
async fn init_timeline_state(
    conf: &'static PageServerConf,
    tenant_shard_id: &TenantShardId,
    heatmap: &HeatMapTimeline,
) -> SecondaryDetailTimeline {
    let timeline_path = conf.timeline_path(tenant_shard_id, &heatmap.timeline_id);
    let mut detail = SecondaryDetailTimeline::default();

    let mut dir = match tokio::fs::read_dir(&timeline_path).await {
        Ok(d) => d,
        Err(e) => {
            if e.kind() == std::io::ErrorKind::NotFound {
                let context = format!("Creating timeline directory {timeline_path}");
                tracing::info!("{}", context);
                tokio::fs::create_dir_all(&timeline_path)
                    .await
                    .fatal_err(&context);

                // No entries to report: drop out.
                return detail;
            } else {
                on_fatal_io_error(&e, &format!("Reading timeline dir {timeline_path}"));
            }
        }
    };

    let heatmap_metadata: HashMap<_, _> = heatmap.layers.iter().map(|l| (&l.name, l)).collect();

    while let Some(dentry) = dir
        .next_entry()
        .await
        .fatal_err(&format!("Listing {timeline_path}"))
    {
        let dentry_file_name = dentry.file_name();
        let file_name = dentry_file_name.to_string_lossy();
        let local_meta = dentry.metadata().await.fatal_err(&format!(
            "Read metadata on {}",
            dentry.path().to_string_lossy()
        ));

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
                                    tenant_shard_id,
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
                        tokio::fs::remove_file(&dentry.path())
                            .await
                            .or_else(fs_ext::ignore_not_found)
                            .fatal_err(&format!(
                                "Removing layer {}",
                                dentry.path().to_string_lossy()
                            ));
                    }
                }
            }
            Err(_) => {
                // Ignore it.
                tracing::warn!("Unexpected file in timeline directory: {file_name}");
            }
        }
    }

    detail
}

/// This type is a convenience to group together the various functions involved in
/// freshening a secondary tenant.
struct TenantDownloader<'a> {
    conf: &'static PageServerConf,
    remote_storage: &'a GenericRemoteStorage,
    secondary_state: &'a SecondaryTenant,
}

impl<'a> TenantDownloader<'a> {
    fn new(
        conf: &'static PageServerConf,
        remote_storage: &'a GenericRemoteStorage,
        secondary_state: &'a SecondaryTenant,
    ) -> Self {
        Self {
            conf,
            remote_storage,
            secondary_state,
        }
    }

    async fn download(&self) -> anyhow::Result<()> {
        debug_assert_current_span_has_tenant_id();

        // For the duration of a download, we must hold the SecondaryTenant::gate, to ensure
        // cover our access to local storage.
        let Ok(_guard) = self.secondary_state.gate.enter() else {
            // Shutting down
            return Ok(());
        };

        debug_assert_current_span_has_tenant_id();
        let tenant_shard_id = self.secondary_state.get_tenant_shard_id();
        // Download the tenant's heatmap
        let heatmap_bytes = tokio::select!(
            bytes = self.download_heatmap() => {bytes?},
            _ = self.secondary_state.cancel.cancelled() => return Ok(())
        );

        let heatmap = serde_json::from_slice::<HeatMapTenant>(&heatmap_bytes)?;

        // Save the heatmap: this will be useful on restart, allowing us to reconstruct
        // layer metadata without having to re-download it.
        let heatmap_path = self.conf.tenant_heatmap_path(tenant_shard_id);

        let temp_path = path_with_suffix_extension(&heatmap_path, TEMP_FILE_SUFFIX);
        let context_msg = format!("write tenant {tenant_shard_id} heatmap to {heatmap_path}");
        let heatmap_path_bg = heatmap_path.clone();
        tokio::task::spawn_blocking(move || {
            tokio::runtime::Handle::current().block_on(async move {
                VirtualFile::crashsafe_overwrite(&heatmap_path_bg, &temp_path, &heatmap_bytes).await
            })
        })
        .await?
        .maybe_fatal_err(&context_msg)
        .with_context(|| context_msg)?;

        tracing::debug!("Wrote local heatmap to {}", heatmap_path);

        // Download the layers in the heatmap
        for timeline in heatmap.timelines {
            if self.secondary_state.cancel.is_cancelled() {
                return Ok(());
            }

            let timeline_id = timeline.timeline_id;
            self.download_timeline(timeline)
                .instrument(tracing::info_span!(
                    "secondary_download_timeline",
                    tenant_id=%tenant_shard_id.tenant_id,
                    shard_id=%tenant_shard_id.shard_slug(),
                    %timeline_id
                ))
                .await?;
        }

        Ok(())
    }

    async fn download_heatmap(&self) -> anyhow::Result<Vec<u8>> {
        debug_assert_current_span_has_tenant_id();
        let tenant_shard_id = self.secondary_state.get_tenant_shard_id();
        // TODO: make download conditional on ETag having changed since last download
        // (https://github.com/neondatabase/neon/issues/6199)
        tracing::debug!("Downloading heatmap for secondary tenant",);

        let heatmap_path = remote_heatmap_path(tenant_shard_id);
        let download = self.remote_storage.download(&heatmap_path).await?;
        let mut heatmap_bytes = Vec::new();
        let mut body = tokio_util::io::StreamReader::new(download.download_stream);
        let _size = tokio::io::copy(&mut body, &mut heatmap_bytes)
            .await
            .with_context(|| format!("download heatmap {heatmap_path:?}"))?;

        SECONDARY_MODE.download_heatmap.inc();

        Ok(heatmap_bytes)
    }

    async fn download_timeline(&self, timeline: HeatMapTimeline) -> anyhow::Result<()> {
        debug_assert_current_span_has_tenant_and_timeline_id();
        let tenant_shard_id = self.secondary_state.get_tenant_shard_id();
        let timeline_path = self
            .conf
            .timeline_path(tenant_shard_id, &timeline.timeline_id);

        // Accumulate updates to the state
        let mut touched = Vec::new();

        // Clone a view of what layers already exist on disk
        let timeline_state = self
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
                    init_timeline_state(self.conf, tenant_shard_id, &timeline).await;

                // Re-acquire detail lock now that we're done with async load from local FS
                self.secondary_state
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
            if self.secondary_state.cancel.is_cancelled() {
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
                self.remote_storage,
                *tenant_shard_id,
                timeline.timeline_id,
                &layer.name,
                &LayerFileMetadata::from(&layer.metadata),
                &self.secondary_state.cancel,
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
            let mut detail = self.secondary_state.detail.lock().unwrap();
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
                            tenant_shard_id,
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
}

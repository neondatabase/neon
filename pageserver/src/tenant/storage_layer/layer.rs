use anyhow::Context;
use pageserver_api::models::{
    HistoricLayerInfo, LayerAccessKind, LayerResidenceEventReason, LayerResidenceStatus,
};
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Weak};
use std::time::SystemTime;
use tracing::Instrument;
use utils::lsn::Lsn;
use utils::sync::heavier_once_cell;

use crate::config::PageServerConf;
use crate::context::RequestContext;
use crate::repository::Key;
use crate::tenant::{remote_timeline_client::LayerFileMetadata, RemoteTimelineClient, Timeline};

use super::delta_layer::{self, DeltaEntry};
use super::image_layer;
use super::{
    AsLayerDesc, LayerAccessStats, LayerAccessStatsReset, LayerFileName, PersistentLayerDesc,
    ValueReconstructResult, ValueReconstructState,
};

/// A Layer contains all data in a "rectangle" consisting of a range of keys and
/// range of LSNs.
///
/// There are two kinds of layers, in-memory and on-disk layers. In-memory
/// layers are used to ingest incoming WAL, and provide fast access to the
/// recent page versions. On-disk layers are stored as files on disk, and are
/// immutable. This type represents the on-disk kind while in-memory kind are represented by
/// [`InMemoryLayer`].
///
/// Furthermore, there are two kinds of on-disk layers: delta and image layers.
/// A delta layer contains all modifications within a range of LSNs and keys.
/// An image layer is a snapshot of all the data in a key-range, at a single
/// LSN.
///
/// This type models the on-disk layers, which can be evicted and on-demand downloaded.
///
/// [`InMemoryLayer`]: inmemory_layer::InMemoryLayer
#[derive(Clone)]
pub(crate) struct Layer(Arc<LayerInner>);

impl std::fmt::Display for Layer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.layer_desc().short_id())
    }
}

impl std::fmt::Debug for Layer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self)
    }
}

impl AsLayerDesc for Layer {
    fn layer_desc(&self) -> &PersistentLayerDesc {
        self.0.layer_desc()
    }
}

impl Layer {
    /// Creates a layer value for a file we know not to be downloaded or resident.
    pub(crate) fn for_evicted(
        conf: &'static PageServerConf,
        timeline: &Arc<Timeline>,
        file_name: LayerFileName,
        metadata: LayerFileMetadata,
    ) -> Self {
        let desc = PersistentLayerDesc::from_filename(
            timeline.tenant_id,
            timeline.timeline_id,
            file_name,
            metadata.file_size(),
        );

        let access_stats = LayerAccessStats::for_loading_layer(LayerResidenceStatus::Evicted);

        let owner = Layer(Arc::new(LayerInner::new(
            conf,
            timeline,
            access_stats,
            desc,
            None,
        )));

        debug_assert!(owner.0.needs_download_blocking().unwrap().is_some());

        owner
    }

    /// Creates a Layer value for a file we know to be resident in timeline directory.
    pub(crate) fn for_resident(
        conf: &'static PageServerConf,
        timeline: &Arc<Timeline>,
        file_name: LayerFileName,
        metadata: LayerFileMetadata,
    ) -> ResidentLayer {
        let desc = PersistentLayerDesc::from_filename(
            timeline.tenant_id,
            timeline.timeline_id,
            file_name,
            metadata.file_size(),
        );

        let access_stats = LayerAccessStats::for_loading_layer(LayerResidenceStatus::Resident);

        let mut resident = None;

        let owner = Layer(Arc::new_cyclic(|owner| {
            let inner = Arc::new(DownloadedLayer {
                owner: owner.clone(),
                kind: tokio::sync::OnceCell::default(),
            });
            resident = Some(inner.clone());

            LayerInner::new(conf, timeline, access_stats, desc, Some(inner))
        }));

        debug_assert!(owner.0.needs_download_blocking().unwrap().is_none());

        let downloaded = resident.expect("just initialized");

        ResidentLayer { downloaded, owner }
    }

    /// Creates a Layer value for freshly written out new layer file by renaming it from a
    /// temporary path.
    pub(crate) fn for_written_tempfile(
        conf: &'static PageServerConf,
        timeline: &Arc<Timeline>,
        desc: PersistentLayerDesc,
        temp_path: &Path,
    ) -> anyhow::Result<ResidentLayer> {
        let mut resident = None;

        let owner = Layer(Arc::new_cyclic(|owner| {
            let inner = Arc::new(DownloadedLayer {
                owner: owner.clone(),
                kind: tokio::sync::OnceCell::default(),
            });
            resident = Some(inner.clone());
            let access_stats = LayerAccessStats::empty_will_record_residence_event_later();
            access_stats.record_residence_event(
                LayerResidenceStatus::Resident,
                LayerResidenceEventReason::LayerCreate,
            );
            LayerInner::new(conf, timeline, access_stats, desc, Some(inner))
        }));

        let downloaded = resident.expect("just initialized");

        // if the rename works, the path is as expected
        std::fs::rename(temp_path, owner.local_path())
            .context("rename temporary file as correct path for {owner}")?;

        Ok(ResidentLayer { downloaded, owner })
    }

    /// Requests the layer to be evicted and waits for this to be done.
    ///
    /// If the file is not resident, an [`EvictionError::NotFound`] is returned.
    ///
    /// If for a bad luck or blocking of the executor, we miss the actual eviction and the layer is
    /// re-downloaded, [`EvictionError::Downloaded`] is returned.
    ///
    /// Technically cancellation safe, but cancelling might shift the viewpoint of what generation
    /// of download-evict cycle on retry.
    pub(crate) async fn evict_and_wait(
        &self,
        rtc: &RemoteTimelineClient,
    ) -> Result<(), EvictionError> {
        self.0.evict_and_wait(rtc).await
    }

    /// Delete the layer file when the `self` gets dropped, also try to schedule a remote index upload
    /// then.
    pub(crate) fn garbage_collect_on_drop(&self) {
        self.0.garbage_collect_on_drop();
    }

    /// Return data needed to reconstruct given page at LSN.
    ///
    /// It is up to the caller to collect more data from the previous layer and
    /// perform WAL redo, if necessary.
    pub(crate) async fn get_value_reconstruct_data(
        &self,
        key: Key,
        lsn_range: Range<Lsn>,
        reconstruct_data: &mut ValueReconstructState,
        ctx: &RequestContext,
    ) -> anyhow::Result<ValueReconstructResult> {
        use anyhow::ensure;

        let layer = self.0.get_or_maybe_download(true, Some(ctx)).await?;
        self.0
            .access_stats
            .record_access(LayerAccessKind::GetValueReconstructData, ctx);

        if self.layer_desc().is_delta {
            ensure!(lsn_range.start >= self.layer_desc().lsn_range.start);
            ensure!(self.layer_desc().key_range.contains(&key));
        } else {
            ensure!(self.layer_desc().key_range.contains(&key));
            ensure!(lsn_range.start >= self.layer_desc().image_layer_lsn());
            ensure!(lsn_range.end >= self.layer_desc().image_layer_lsn());
        }

        layer
            .get_value_reconstruct_data(key, lsn_range, reconstruct_data, &self.0)
            .await
    }

    /// Download the layer if evicted.
    ///
    /// Will not error when the layer is already downloaded.
    pub(crate) async fn download(&self) -> anyhow::Result<()> {
        self.0.get_or_maybe_download(true, None).await?;
        Ok(())
    }

    /// Assuming the layer is already downloaded, returns a guard which will prohibit eviction
    /// while the guard exists.
    ///
    /// Returns None if the layer is currently evicted.
    pub(crate) async fn keep_resident(&self) -> anyhow::Result<Option<ResidentLayer>> {
        let downloaded = match self.0.get_or_maybe_download(false, None).await {
            Ok(d) => d,
            // technically there are a lot of possible errors, but in practice it should only be
            // DownloadRequired which is tripped up. could work to improve this situation
            // statically later.
            Err(DownloadError::DownloadRequired) => return Ok(None),
            Err(e) => return Err(e.into()),
        };

        Ok(Some(ResidentLayer {
            downloaded,
            owner: self.clone(),
        }))
    }

    /// Downloads if necessary and creates a guard, which will keep this layer from being evicted.
    pub(crate) async fn download_and_keep_resident(&self) -> anyhow::Result<ResidentLayer> {
        let downloaded = self.0.get_or_maybe_download(true, None).await?;

        Ok(ResidentLayer {
            downloaded,
            owner: self.clone(),
        })
    }

    pub(crate) fn info(&self, reset: LayerAccessStatsReset) -> HistoricLayerInfo {
        self.0.info(reset)
    }

    pub(crate) fn access_stats(&self) -> &LayerAccessStats {
        &self.0.access_stats
    }

    pub(crate) fn local_path(&self) -> &Path {
        &self.0.path
    }

    /// Traditional debug dumping facility
    #[allow(unused)]
    pub(crate) async fn dump(&self, verbose: bool, ctx: &RequestContext) -> anyhow::Result<()> {
        self.0.desc.dump();

        if verbose {
            // for now, unconditionally download everything, even if that might not be wanted.
            let l = self.0.get_or_maybe_download(true, Some(ctx)).await?;
            l.dump(&self.0).await?
        }

        Ok(())
    }
}

/// The download-ness ([`DownloadedLayer`]) can be either resident or wanted evicted.
///
/// However when we want something evicted, we cannot evict it right away as there might be current
/// reads happening on it. For example: it has been searched from [`LayerMap::search`] but not yet
/// read with [`Layer::get_value_reconstruct_data`].
///
/// [`LayerMap::search`]: crate::tenant::layer_map::LayerMap::search
enum ResidentOrWantedEvicted {
    Resident(Arc<DownloadedLayer>),
    WantedEvicted(Weak<DownloadedLayer>),
}

impl ResidentOrWantedEvicted {
    fn get(&self) -> Option<Arc<DownloadedLayer>> {
        match self {
            ResidentOrWantedEvicted::Resident(strong) => Some(strong.clone()),
            ResidentOrWantedEvicted::WantedEvicted(weak) => weak.upgrade(),
        }
    }
    /// When eviction is first requested, drop down to holding a [`Weak`].
    ///
    /// Returns `true` if this was the first time eviction was requested.
    fn downgrade(&mut self) -> &Weak<DownloadedLayer> {
        let _was_first = match self {
            ResidentOrWantedEvicted::Resident(strong) => {
                let weak = Arc::downgrade(strong);
                *self = ResidentOrWantedEvicted::WantedEvicted(weak);
                // returning the weak is not useful, because the drop could had already ran with
                // the replacement above, and that will take care of cleaning the Option we are in
                true
            }
            ResidentOrWantedEvicted::WantedEvicted(_) => false,
        };

        match self {
            ResidentOrWantedEvicted::WantedEvicted(ref weak) => weak,
            _ => unreachable!("just wrote wanted evicted"),
        }
    }
}

struct LayerInner {
    /// Only needed to check ondemand_download_behavior_treat_error_as_warn and creation of
    /// [`Self::path`].
    conf: &'static PageServerConf,

    /// Full path to the file; unclear if this should exist anymore.
    path: PathBuf,

    desc: PersistentLayerDesc,

    /// Timeline access is needed for remote timeline client and metrics.
    timeline: Weak<Timeline>,

    /// Cached knowledge of [`Timeline::remote_client`] being `Some`.
    have_remote_client: bool,

    access_stats: LayerAccessStats,

    /// This custom OnceCell is backed by std mutex, but only held for short time periods.
    /// Initialization and deinitialization are done while holding a permit.
    inner: heavier_once_cell::OnceCell<ResidentOrWantedEvicted>,

    /// Do we want to garbage collect this when `LayerInner` is dropped
    wanted_garbage_collected: AtomicBool,

    /// Do we want to evict this layer as soon as possible? After being set to `true`, all accesses
    /// will try to downgrade [`ResidentOrWantedEvicted`], which will eventually trigger
    /// [`LayerInner::on_drop`].
    wanted_evicted: AtomicBool,

    /// Version is to make sure we will in fact only evict a file if no new guard has been created
    /// for it.
    version: AtomicUsize,

    /// Allow subscribing to when the layer actually gets evicted.
    status: tokio::sync::broadcast::Sender<Status>,

    /// Counter for exponential backoff with the download
    consecutive_failures: AtomicUsize,
}

impl std::fmt::Display for LayerInner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.layer_desc().short_id())
    }
}

impl AsLayerDesc for LayerInner {
    fn layer_desc(&self) -> &PersistentLayerDesc {
        &self.desc
    }
}

#[derive(Debug, Clone, Copy)]
enum Status {
    Evicted,
    Downloaded,
}

impl Drop for LayerInner {
    fn drop(&mut self) {
        if !*self.wanted_garbage_collected.get_mut() {
            // should we try to evict if the last wish was for eviction?
            // feels like there's some hazard of overcrowding near shutdown near by, but we don't
            // run drops during shutdown (yet)
            return;
        }

        let span = tracing::info_span!(parent: None, "layer_gc", tenant_id = %self.layer_desc().tenant_id, timeline_id = %self.layer_desc().timeline_id, layer = %self);

        let path = std::mem::take(&mut self.path);
        let file_name = self.layer_desc().filename();
        let file_size = self.layer_desc().file_size;
        let timeline = self.timeline.clone();

        crate::task_mgr::BACKGROUND_RUNTIME.spawn_blocking(move || {
            let _g = span.entered();

            let mut removed = false;
            match std::fs::remove_file(path) {
                Ok(()) => {
                    removed = true;
                }
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                    // until we no longer do detaches by removing all local files before removing the
                    // tenant from the global map, we will always get these errors even if we knew what
                    // is the latest state.
                    //
                    // we currently do not track the latest state, so we'll also end up here on evicted
                    // layers.
                }
                Err(e) => {
                    tracing::error!("failed to remove garbage collected layer: {e}");
                }
            }

            if let Some(timeline) = timeline.upgrade() {
                if removed {
                    timeline.metrics.resident_physical_size_gauge.sub(file_size);
                }
                if let Some(remote_client) = timeline.remote_client.as_ref() {
                    let res = remote_client.schedule_layer_file_deletion(&[file_name]);

                    if let Err(e) = res {
                        // test_timeline_deletion_with_files_stuck_in_upload_queue is good at
                        // demonstrating this deadlock (without spawn_blocking): stop will drop
                        // queued items, which will have ResidentLayer's, and those drops would try
                        // to re-entrantly lock the RemoteTimelineClient inner state.
                        if !timeline.is_active() {
                            tracing::info!("scheduling deletion on drop failed: {e:#}");
                        } else {
                            tracing::warn!("scheduling deletion on drop failed: {e:#}");
                        }
                    }
                }
            } else {
                // no need to nag that timeline is gone: under normal situation on
                // task_mgr::remove_tenant_from_memory the timeline is gone before we get dropped.
            }
        });
    }
}

impl LayerInner {
    fn new(
        conf: &'static PageServerConf,
        timeline: &Arc<Timeline>,
        access_stats: LayerAccessStats,
        desc: PersistentLayerDesc,
        downloaded: Option<Arc<DownloadedLayer>>,
    ) -> Self {
        let path = conf
            .timeline_path(&timeline.tenant_id, &timeline.timeline_id)
            .join(desc.filename().to_string());

        LayerInner {
            conf,
            path,
            desc,
            timeline: Arc::downgrade(timeline),
            have_remote_client: timeline.remote_client.is_some(),
            access_stats,
            wanted_garbage_collected: AtomicBool::new(false),
            wanted_evicted: AtomicBool::new(false),
            inner: if let Some(inner) = downloaded {
                heavier_once_cell::OnceCell::new(ResidentOrWantedEvicted::Resident(inner))
            } else {
                heavier_once_cell::OnceCell::default()
            },
            version: AtomicUsize::new(0),
            status: tokio::sync::broadcast::channel(1).0,
            consecutive_failures: AtomicUsize::new(0),
        }
    }

    fn garbage_collect_on_drop(&self) {
        self.wanted_garbage_collected.store(true, Ordering::Release);
    }

    pub(crate) async fn evict_and_wait(
        &self,
        _: &RemoteTimelineClient,
    ) -> Result<(), EvictionError> {
        use tokio::sync::broadcast::error::RecvError;

        assert!(self.have_remote_client);

        let mut rx = self.status.subscribe();

        self.wanted_evicted.store(true, Ordering::Release);

        if self.get().is_none() {
            // it was not evictable in the first place
            // our store to the wanted_evicted does not matter; it will be reset by next download
            return Err(EvictionError::NotFound);
        }

        match rx.recv().await {
            Ok(Status::Evicted) => Ok(()),
            Ok(Status::Downloaded) => Err(EvictionError::Downloaded),
            Err(RecvError::Closed) => {
                unreachable!("sender cannot be dropped while we are in &self method")
            }
            Err(RecvError::Lagged(_)) => {
                // this is quite unlikely, but we are blocking a lot in the async context, so
                // we might be missing this because we are stuck on a LIFO slot on a thread
                // which is busy blocking for a 1TB database create_image_layers.
                //
                // use however late (compared to the initial expressing of wanted) as the
                // "outcome" now
                match self.get() {
                    Some(_) => Err(EvictionError::Downloaded),
                    None => Ok(()),
                }
            }
        }
    }

    /// Should be cancellation safe, but cancellation is troublesome together with the spawned
    /// download.
    async fn get_or_maybe_download(
        self: &Arc<Self>,
        allow_download: bool,
        ctx: Option<&RequestContext>,
    ) -> Result<Arc<DownloadedLayer>, DownloadError> {
        let download = move || async move {
            // disable any scheduled but not yet running eviction deletions for this
            self.version.fetch_add(1, Ordering::Relaxed);

            // no need to make the evict_and_wait wait for the actual download to complete
            drop(self.status.send(Status::Downloaded));

            let timeline = self
                .timeline
                .upgrade()
                .ok_or_else(|| DownloadError::TimelineShutdown)?;

            let can_ever_evict = timeline.remote_client.as_ref().is_some();

            // check if we really need to be downloaded; could have been already downloaded by a
            // cancelled previous attempt.
            let needs_download = self
                .needs_download()
                .await
                .map_err(DownloadError::PreStatFailed)?;

            if let Some(reason) = needs_download {
                // only reset this after we've decided we really need to download. otherwise it'd
                // be impossible to mark cancelled downloads for eviction, like one could imagine
                // we would like to do for prefetching which was not needed.
                self.wanted_evicted.store(false, Ordering::Release);

                if !can_ever_evict {
                    return Err(DownloadError::NoRemoteStorage);
                }

                tracing::debug!(%reason, "downloading layer");

                if let Some(ctx) = ctx {
                    self.check_expected_download(ctx)?;
                }

                if !allow_download {
                    // this does look weird, but for LayerInner the "downloading" means also changing
                    // internal once related state ...
                    return Err(DownloadError::DownloadRequired);
                }

                self.spawn_download_and_wait(timeline).await?;
            } else {
                // the file is present locally, probably by a previous but cancelled call to
                // get_or_maybe_download. alternatively we might be running without remote storage.
            }

            let res = Arc::new(DownloadedLayer {
                owner: Arc::downgrade(self),
                kind: tokio::sync::OnceCell::default(),
            });

            self.access_stats.record_residence_event(
                LayerResidenceStatus::Resident,
                LayerResidenceEventReason::ResidenceChange,
            );

            Ok(if self.wanted_evicted.load(Ordering::Acquire) {
                // because we reset wanted_evictness earlier, this most likely means when we were downloading someone
                // wanted to evict this layer.
                ResidentOrWantedEvicted::WantedEvicted(Arc::downgrade(&res))
            } else {
                ResidentOrWantedEvicted::Resident(res.clone())
            })
        };

        let locked = self.inner.get_or_init(download).await?;

        Ok(
            Self::get_or_apply_evictedness(Some(locked), &self.wanted_evicted)
                .expect("It is not none, we just received it"),
        )
    }

    /// Nag or fail per RequestContext policy
    fn check_expected_download(&self, ctx: &RequestContext) -> Result<(), DownloadError> {
        use crate::context::DownloadBehavior::*;
        let b = ctx.download_behavior();
        match b {
            Download => Ok(()),
            Warn | Error => {
                tracing::warn!(
                    "unexpectedly on-demand downloading remote layer {self} for task kind {:?}",
                    ctx.task_kind()
                );
                crate::metrics::UNEXPECTED_ONDEMAND_DOWNLOADS.inc();

                let really_error =
                    matches!(b, Error) && !self.conf.ondemand_download_behavior_treat_error_as_warn;

                if really_error {
                    // this check is only probablistic, seems like flakyness footgun
                    Err(DownloadError::ContextAndConfigReallyDeniesDownloads)
                } else {
                    Ok(())
                }
            }
        }
    }

    /// Actual download, at most one is executed at the time.
    async fn spawn_download_and_wait(
        self: &Arc<Self>,
        timeline: Arc<Timeline>,
    ) -> Result<(), DownloadError> {
        let task_name = format!("download layer {}", self);

        let (tx, rx) = tokio::sync::oneshot::channel();
        // this is sadly needed because of task_mgr::shutdown_tasks, otherwise we cannot
        // block tenant::mgr::remove_tenant_from_memory.

        let this: Arc<Self> = self.clone();
        crate::task_mgr::spawn(
            &tokio::runtime::Handle::current(),
            crate::task_mgr::TaskKind::RemoteDownloadTask,
            Some(self.desc.tenant_id),
            Some(self.desc.timeline_id),
            &task_name,
            false,
            async move {
                let client = timeline
                    .remote_client
                    .as_ref()
                    .expect("checked above with have_remote_client");

                let result = client.download_layer_file(
                    &this.desc.filename(),
                    &LayerFileMetadata::new(
                        this.desc.file_size,
                    ),
                )
                .await;

                let result = match result {
                    Ok(size) => {
                        timeline.metrics.resident_physical_size_gauge.add(size);
                        Ok(())
                    }
                    Err(e) => {
                        Err(e)
                    }
                };

                if let Err(res) = tx.send(result) {
                    match res {
                        Ok(()) => {
                            // our caller is cancellation safe so this is fine; if someone
                            // else requests the layer, they'll find it already downloaded
                            // or redownload.
                            //
                            // however, could be that we should consider marking the layer
                            // for eviction? alas, cannot: because only DownloadedLayer
                            // will handle that.
                        },
                        Err(e) => {
                            // our caller is cancellation safe, but we might be racing with
                            // another attempt to initialize. before we have cancellation
                            // token support: these attempts should converge regardless of
                            // their completion order.
                            tracing::error!("layer file download failed, and additionally failed to communicate this to caller: {e:?}");
                        }
                    }
                }

                Ok(())
            }
            .in_current_span(),
        );
        match rx.await {
            Ok(Ok(())) => {
                if let Some(reason) = self
                    .needs_download()
                    .await
                    .map_err(DownloadError::PostStatFailed)?
                {
                    // this is really a bug in needs_download or remote timeline client
                    panic!("post-condition failed: needs_download returned {reason:?}");
                }

                self.consecutive_failures.store(0, Ordering::Relaxed);

                Ok(())
            }
            Ok(Err(e)) => {
                let consecutive_failures =
                    self.consecutive_failures.fetch_add(1, Ordering::Relaxed);
                tracing::error!(consecutive_failures, "layer file download failed: {e:#}");
                let backoff = utils::backoff::exponential_backoff_duration_seconds(
                    consecutive_failures.min(u32::MAX as usize) as u32,
                    1.5,
                    60.0,
                );
                let backoff = std::time::Duration::from_secs_f64(backoff);

                // unless we get cancelled, we will hold off the init semaphore
                tokio::time::sleep(backoff).await;

                Err(DownloadError::DownloadFailed)
            }
            Err(_gone) => Err(DownloadError::DownloadCancelled),
        }
    }

    /// Access the current state without waiting for the file to be downloaded.
    ///
    /// Requires that we've initialized to state which is respective to the
    /// actual residency state.
    fn get(&self) -> Option<Arc<DownloadedLayer>> {
        let locked = self.inner.get();
        Self::get_or_apply_evictedness(locked, &self.wanted_evicted)
    }

    fn get_or_apply_evictedness(
        guard: Option<heavier_once_cell::Guard<'_, ResidentOrWantedEvicted>>,
        wanted_evicted: &AtomicBool,
    ) -> Option<Arc<DownloadedLayer>> {
        if let Some(mut x) = guard {
            if let Some(won) = x.get() {
                // there are no guarantees that we will always get to observe a concurrent call
                // to evict
                if wanted_evicted.load(Ordering::Acquire) {
                    x.downgrade();
                }
                return Some(won);
            }
        }

        None
    }

    async fn needs_download(&self) -> Result<Option<NeedsDownload>, std::io::Error> {
        match tokio::fs::metadata(&self.path).await {
            Ok(m) => Ok(self.is_file_present_and_good_size(&m).err()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Some(NeedsDownload::NotFound)),
            Err(e) => Err(e),
        }
    }

    fn needs_download_blocking(&self) -> Result<Option<NeedsDownload>, std::io::Error> {
        match self.path.metadata() {
            Ok(m) => Ok(self.is_file_present_and_good_size(&m).err()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Some(NeedsDownload::NotFound)),
            Err(e) => Err(e),
        }
    }

    fn is_file_present_and_good_size(&self, m: &std::fs::Metadata) -> Result<(), NeedsDownload> {
        // in future, this should include sha2-256 of the file.
        if !m.is_file() {
            Err(NeedsDownload::NotFile)
        } else if m.len() != self.desc.file_size {
            Err(NeedsDownload::WrongSize {
                actual: m.len(),
                expected: self.desc.file_size,
            })
        } else {
            Ok(())
        }
    }

    fn info(&self, reset: LayerAccessStatsReset) -> HistoricLayerInfo {
        let layer_file_name = self.desc.filename().file_name();

        let remote = self.get().is_none();

        let access_stats = self.access_stats.as_api_model(reset);

        if self.desc.is_delta {
            let lsn_range = &self.desc.lsn_range;

            HistoricLayerInfo::Delta {
                layer_file_name,
                layer_file_size: self.desc.file_size,
                lsn_start: lsn_range.start,
                lsn_end: lsn_range.end,
                remote,
                access_stats,
            }
        } else {
            let lsn = self.desc.image_layer_lsn();

            HistoricLayerInfo::Image {
                layer_file_name,
                layer_file_size: self.desc.file_size,
                lsn_start: lsn,
                remote,
                access_stats,
            }
        }
    }

    /// `DownloadedLayer` is being dropped, so it calls this method.
    fn on_drop(self: Arc<LayerInner>) {
        let gc = self.wanted_garbage_collected.load(Ordering::Acquire);
        let evict = self.wanted_evicted.load(Ordering::Acquire);
        let can_evict = self.have_remote_client;

        if gc {
            // do nothing now, only when the whole layer is dropped. gc will end up deleting the
            // whole layer, in case there is no reference cycle.
        } else if can_evict && evict {
            let version = self.version.load(Ordering::Relaxed);

            let span = tracing::info_span!(parent: None, "layer_evict", tenant_id = %self.desc.tenant_id, timeline_id = %self.desc.timeline_id, layer=%self);

            // downgrade for the duration of the queue, in case there's a shutdown already ongoing
            // we should not hold it up.
            let this = Arc::downgrade(&self);
            drop(self);

            crate::task_mgr::BACKGROUND_RUNTIME.spawn_blocking(move || {
                let _g = span.entered();

                // if LayerInner is already dropped here, do nothing for the garbage collection has
                // already ran while we were in queue
                let Some(this) = this.upgrade() else { return; };
                this.evict_blocking(version);
            });
        }
    }

    fn evict_blocking(&self, version: usize) {
        // deleted or detached timeline, don't do anything.
        let Some(timeline) = self.timeline.upgrade() else { return; };

        // to avoid starting a new download while we evict, keep holding on to the
        // permit.
        let _permit = {
            let maybe_downloaded = self.inner.get();

            if version != self.version.load(Ordering::Relaxed) {
                // downloadness-state has advanced, we might no longer be the latest eviction
                // work; don't do anything.
                return;
            }

            // free the DownloadedLayer allocation
            match maybe_downloaded.map(|mut g| g.take_and_deinit()) {
                Some((taken, permit)) => {
                    assert!(matches!(taken, ResidentOrWantedEvicted::WantedEvicted(_)));
                    permit
                }
                None => {
                    unreachable!("we do the version checking for this exact reason")
                }
            }
        };

        self.access_stats.record_residence_event(
            LayerResidenceStatus::Evicted,
            LayerResidenceEventReason::ResidenceChange,
        );

        match capture_mtime_and_remove(&self.path) {
            Ok(local_layer_mtime) => {
                let duration = SystemTime::now().duration_since(local_layer_mtime);
                match duration {
                    Ok(elapsed) => {
                        timeline
                            .metrics
                            .evictions_with_low_residence_duration
                            .read()
                            .unwrap()
                            .observe(elapsed);
                        tracing::info!(
                            residence_millis = elapsed.as_millis(),
                            "evicted layer after known residence period"
                        );
                    }
                    Err(_) => {
                        tracing::info!("evicted layer after unknown residence period");
                    }
                }
                timeline
                    .metrics
                    .resident_physical_size_gauge
                    .sub(self.desc.file_size);
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                tracing::info!("failed to evict file from disk, it was already gone");
            }
            Err(e) => {
                tracing::warn!("failed to evict file from disk: {e:#}");
            }
        }

        // we are still holding the permit, so no new spawn_download_and_wait can happen
        drop(self.status.send(Status::Evicted));
    }
}

fn capture_mtime_and_remove(path: &Path) -> Result<SystemTime, std::io::Error> {
    let m = path.metadata()?;
    let local_layer_mtime = m.modified()?;
    std::fs::remove_file(path)?;
    Ok(local_layer_mtime)
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum EvictionError {
    #[error("layer was already evicted")]
    NotFound,

    /// Evictions must always lose to downloads in races, and this time it happened.
    #[error("layer was downloaded instead")]
    Downloaded,
}

/// Error internal to the [`LayerInner::get_or_maybe_download`]
#[derive(Debug, thiserror::Error)]
enum DownloadError {
    #[error("timeline has already shutdown")]
    TimelineShutdown,
    #[error("no remote storage configured")]
    NoRemoteStorage,
    #[error("context denies downloading")]
    ContextAndConfigReallyDeniesDownloads,
    #[error("downloading is really required but not allowed by this method")]
    DownloadRequired,
    /// Why no error here? Because it will be reported by page_service. We should had also done
    /// retries already.
    #[error("downloading evicted layer file failed")]
    DownloadFailed,
    #[error("downloading failed, possibly for shutdown")]
    DownloadCancelled,
    #[error("pre-condition: stat before download failed")]
    PreStatFailed(#[source] std::io::Error),
    #[error("post-condition: stat after download failed")]
    PostStatFailed(#[source] std::io::Error),
}

#[derive(Debug, PartialEq)]
pub(crate) enum NeedsDownload {
    NotFound,
    NotFile,
    WrongSize { actual: u64, expected: u64 },
}

impl std::fmt::Display for NeedsDownload {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            NeedsDownload::NotFound => write!(f, "file was not found"),
            NeedsDownload::NotFile => write!(f, "path is not a file"),
            NeedsDownload::WrongSize { actual, expected } => {
                write!(f, "file size mismatch {actual} vs. {expected}")
            }
        }
    }
}

/// Existence of `DownloadedLayer` means that we have the file locally, and can later evict it.
pub(crate) struct DownloadedLayer {
    owner: Weak<LayerInner>,
    kind: tokio::sync::OnceCell<anyhow::Result<LayerKind>>,
}

impl std::fmt::Debug for DownloadedLayer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DownloadedLayer")
            // FIXME: this is not useful, always "Weak"
            .field("owner", &self.owner)
            .field("kind", &self.kind)
            .finish()
    }
}

impl Drop for DownloadedLayer {
    fn drop(&mut self) {
        if let Some(owner) = self.owner.upgrade() {
            owner.on_drop();
        } else {
            // no need to do anything, we are shutting down
        }
    }
}

impl DownloadedLayer {
    /// Initializes the `DeltaLayerInner` or `ImageLayerInner` within [`LayerKind`], or fails to
    /// initialize it permanently.
    ///
    /// `owner` parameter is a borrow pointing at the same `LayerInner` as the
    /// `DownloadedLayer::owner` would point when upgraded. Given how this method ends up called,
    /// we will however always have the LayerInner on the callstack, so we can just use it.
    async fn get(&self, owner: &LayerInner) -> anyhow::Result<&LayerKind> {
        let init = || async {
            // there is nothing async here, but it should be async
            if owner.desc.is_delta {
                let summary = Some(delta_layer::Summary::expected(
                    owner.desc.tenant_id,
                    owner.desc.timeline_id,
                    owner.desc.key_range.clone(),
                    owner.desc.lsn_range.clone(),
                ));
                delta_layer::DeltaLayerInner::load(&owner.path, summary).map(LayerKind::Delta)
            } else {
                let lsn = owner.desc.image_layer_lsn();
                let summary = Some(image_layer::Summary::expected(
                    owner.desc.tenant_id,
                    owner.desc.timeline_id,
                    owner.desc.key_range.clone(),
                    lsn,
                ));
                image_layer::ImageLayerInner::load(&owner.path, lsn, summary).map(LayerKind::Image)
            }
            // this will be a permanent failure
            .context("load layer")
        };
        self.kind.get_or_init(init).await.as_ref().map_err(|e| {
            // errors are not clonabled, cannot but stringify
            // test_broken_timeline matches this string
            anyhow::anyhow!("layer loading failed: {e:#}")
        })
    }

    async fn get_value_reconstruct_data(
        &self,
        key: Key,
        lsn_range: Range<Lsn>,
        reconstruct_data: &mut ValueReconstructState,
        owner: &LayerInner,
    ) -> anyhow::Result<ValueReconstructResult> {
        use LayerKind::*;

        match self.get(owner).await? {
            Delta(d) => {
                d.get_value_reconstruct_data(key, lsn_range, reconstruct_data)
                    .await
            }
            Image(i) => i.get_value_reconstruct_data(key, reconstruct_data).await,
        }
    }

    async fn dump(&self, owner: &LayerInner) -> anyhow::Result<()> {
        use LayerKind::*;
        match self.get(owner).await? {
            Delta(d) => d.dump().await?,
            Image(i) => i.dump().await?,
        }

        Ok(())
    }
}

/// Wrapper around an actual layer implementation.
#[derive(Debug)]
enum LayerKind {
    Delta(delta_layer::DeltaLayerInner),
    Image(image_layer::ImageLayerInner),
}

/// Guard value for forcing a layer be resident while it exists.
#[derive(Clone)]
pub(crate) struct ResidentLayer {
    owner: Layer,
    downloaded: Arc<DownloadedLayer>,
}

impl std::fmt::Display for ResidentLayer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.owner)
    }
}

impl std::fmt::Debug for ResidentLayer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.owner)
    }
}

impl ResidentLayer {
    /// Release the eviction guard, converting back into a plain [`Layer`].
    pub(crate) fn drop_eviction_guard(self) -> Layer {
        self.into()
    }

    /// Loads all keys stored in the layer. Returns key, lsn and value size.
    pub(crate) async fn load_keys(
        &self,
        ctx: &RequestContext,
    ) -> anyhow::Result<Vec<DeltaEntry<'_>>> {
        use LayerKind::*;

        let owner = &self.owner.0;

        match self.downloaded.get(owner).await? {
            Delta(d) => {
                owner
                    .access_stats
                    .record_access(LayerAccessKind::KeyIter, ctx);

                // this is valid because the DownloadedLayer::kind is a OnceCell, not a
                // Mutex<OnceCell>, so we cannot go and deinitialize the value with OnceCell::take
                // while it's being held.
                d.load_keys().await.context("Layer index is corrupted")
            }
            Image(_) => anyhow::bail!("cannot load_keys on a image layer"),
        }
    }

    pub(crate) fn local_path(&self) -> &Path {
        &self.owner.0.path
    }

    pub(crate) fn access_stats(&self) -> &LayerAccessStats {
        self.owner.access_stats()
    }
}

impl AsLayerDesc for ResidentLayer {
    fn layer_desc(&self) -> &PersistentLayerDesc {
        self.owner.layer_desc()
    }
}

impl AsRef<Layer> for ResidentLayer {
    fn as_ref(&self) -> &Layer {
        &self.owner
    }
}

/// Allow slimming down if we don't want the `2*usize` with eviction candidates?
impl From<ResidentLayer> for Layer {
    fn from(value: ResidentLayer) -> Self {
        value.owner
    }
}

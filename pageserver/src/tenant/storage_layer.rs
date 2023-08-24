//! Common traits and structs for layers

pub mod delta_layer;
mod filename;
mod image_layer;
mod inmemory_layer;
mod layer_desc;

use crate::config::PageServerConf;
use crate::context::{AccessStatsBehavior, RequestContext};
use crate::repository::Key;
use crate::task_mgr::TaskKind;
use crate::walrecord::NeonWalRecord;
use anyhow::Context;
use anyhow::Result;
use bytes::Bytes;
use enum_map::EnumMap;
use enumset::EnumSet;
use once_cell::sync::Lazy;
use pageserver_api::models::LayerAccessKind;
use pageserver_api::models::{
    HistoricLayerInfo, LayerResidenceEvent, LayerResidenceEventReason, LayerResidenceStatus,
};
use std::ops::Range;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tracing::warn;
use tracing::Instrument;
use utils::history_buffer::HistoryBufferWithDropCounter;
use utils::rate_limit::RateLimit;

use utils::{
    id::{TenantId, TimelineId},
    lsn::Lsn,
};

pub use delta_layer::{DeltaLayer, DeltaLayerWriter, ValueRef};
pub use filename::{DeltaFileName, ImageFileName, LayerFileName};
pub use image_layer::{ImageLayer, ImageLayerWriter};
pub use inmemory_layer::InMemoryLayer;
pub use layer_desc::{PersistentLayerDesc, PersistentLayerKey};

use self::delta_layer::DeltaEntry;
use super::remote_timeline_client::index::LayerFileMetadata;
use super::remote_timeline_client::RemoteTimelineClient;
use super::Timeline;
use utils::sync::heavier_once_cell;

pub fn range_overlaps<T>(a: &Range<T>, b: &Range<T>) -> bool
where
    T: PartialOrd<T>,
{
    if a.start < b.start {
        a.end > b.start
    } else {
        b.end > a.start
    }
}

/// Struct used to communicate across calls to 'get_value_reconstruct_data'.
///
/// Before first call, you can fill in 'page_img' if you have an older cached
/// version of the page available. That can save work in
/// 'get_value_reconstruct_data', as it can stop searching for page versions
/// when all the WAL records going back to the cached image have been collected.
///
/// When get_value_reconstruct_data returns Complete, 'img' is set to an image
/// of the page, or the oldest WAL record in 'records' is a will_init-type
/// record that initializes the page without requiring a previous image.
///
/// If 'get_page_reconstruct_data' returns Continue, some 'records' may have
/// been collected, but there are more records outside the current layer. Pass
/// the same ValueReconstructState struct in the next 'get_value_reconstruct_data'
/// call, to collect more records.
///
#[derive(Debug)]
pub struct ValueReconstructState {
    pub records: Vec<(Lsn, NeonWalRecord)>,
    pub img: Option<(Lsn, Bytes)>,
}

/// Return value from [`LayerE::get_value_reconstruct_data`]
#[derive(Clone, Copy, Debug)]
pub enum ValueReconstructResult {
    /// Got all the data needed to reconstruct the requested page
    Complete,
    /// This layer didn't contain all the required data, the caller should look up
    /// the predecessor layer at the returned LSN and collect more data from there.
    Continue,

    /// This layer didn't contain data needed to reconstruct the page version at
    /// the returned LSN. This is usually considered an error, but might be OK
    /// in some circumstances.
    Missing,
}

#[derive(Debug)]
pub struct LayerAccessStats(Mutex<LayerAccessStatsLocked>);

/// This struct holds two instances of [`LayerAccessStatsInner`].
/// Accesses are recorded to both instances.
/// The `for_scraping_api`instance can be reset from the management API via [`LayerAccessStatsReset`].
/// The `for_eviction_policy` is never reset.
#[derive(Debug, Default, Clone)]
struct LayerAccessStatsLocked {
    for_scraping_api: LayerAccessStatsInner,
    for_eviction_policy: LayerAccessStatsInner,
}

impl LayerAccessStatsLocked {
    fn iter_mut(&mut self) -> impl Iterator<Item = &mut LayerAccessStatsInner> {
        [&mut self.for_scraping_api, &mut self.for_eviction_policy].into_iter()
    }
}

#[derive(Debug, Default, Clone)]
struct LayerAccessStatsInner {
    first_access: Option<LayerAccessStatFullDetails>,
    count_by_access_kind: EnumMap<LayerAccessKind, u64>,
    task_kind_flag: EnumSet<TaskKind>,
    last_accesses: HistoryBufferWithDropCounter<LayerAccessStatFullDetails, 16>,
    last_residence_changes: HistoryBufferWithDropCounter<LayerResidenceEvent, 16>,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct LayerAccessStatFullDetails {
    pub(crate) when: SystemTime,
    pub(crate) task_kind: TaskKind,
    pub(crate) access_kind: LayerAccessKind,
}

#[derive(Clone, Copy, strum_macros::EnumString)]
pub enum LayerAccessStatsReset {
    NoReset,
    JustTaskKindFlags,
    AllStats,
}

fn system_time_to_millis_since_epoch(ts: &SystemTime) -> u64 {
    ts.duration_since(UNIX_EPOCH)
        .expect("better to die in this unlikely case than report false stats")
        .as_millis()
        .try_into()
        .expect("64 bits is enough for few more years")
}

impl LayerAccessStatFullDetails {
    fn as_api_model(&self) -> pageserver_api::models::LayerAccessStatFullDetails {
        let Self {
            when,
            task_kind,
            access_kind,
        } = self;
        pageserver_api::models::LayerAccessStatFullDetails {
            when_millis_since_epoch: system_time_to_millis_since_epoch(when),
            task_kind: task_kind.into(), // into static str, powered by strum_macros
            access_kind: *access_kind,
        }
    }
}

impl LayerAccessStats {
    /// Create an empty stats object.
    ///
    /// The caller is responsible for recording a residence event
    /// using [`record_residence_event`] before calling `latest_activity`.
    /// If they don't, [`latest_activity`] will return `None`.
    ///
    /// [`record_residence_event`]: Self::record_residence_event
    /// [`latest_activity`]: Self::latest_activity
    pub(crate) fn empty_will_record_residence_event_later() -> Self {
        LayerAccessStats(Mutex::default())
    }

    /// Create an empty stats object and record a [`LayerLoad`] event with the given residence status.
    ///
    /// See [`record_residence_event`] for why you need to do this while holding the layer map lock.
    ///
    /// [`LayerLoad`]: LayerResidenceEventReason::LayerLoad
    /// [`record_residence_event`]: Self::record_residence_event
    pub(crate) fn for_loading_layer(status: LayerResidenceStatus) -> Self {
        let new = LayerAccessStats(Mutex::new(LayerAccessStatsLocked::default()));
        new.record_residence_event(status, LayerResidenceEventReason::LayerLoad);
        new
    }

    /// Record a change in layer residency.
    ///
    /// Recording the event must happen while holding the layer map lock to
    /// ensure that latest-activity-threshold-based layer eviction (eviction_task.rs)
    /// can do an "imitate access" to this layer, before it observes `now-latest_activity() > threshold`.
    ///
    /// If we instead recorded the residence event with a timestamp from before grabbing the layer map lock,
    /// the following race could happen:
    ///
    /// - Compact: Write out an L1 layer from several L0 layers. This records residence event LayerCreate with the current timestamp.
    /// - Eviction: imitate access logical size calculation. This accesses the L0 layers because the L1 layer is not yet in the layer map.
    /// - Compact: Grab layer map lock, add the new L1 to layer map and remove the L0s, release layer map lock.
    /// - Eviction: observes the new L1 layer whose only activity timestamp is the LayerCreate event.
    ///
    pub(crate) fn record_residence_event(
        &self,
        status: LayerResidenceStatus,
        reason: LayerResidenceEventReason,
    ) {
        let mut locked = self.0.lock().unwrap();
        locked.iter_mut().for_each(|inner| {
            inner
                .last_residence_changes
                .write(LayerResidenceEvent::new(status, reason))
        });
    }

    fn record_access(&self, access_kind: LayerAccessKind, ctx: &RequestContext) {
        if ctx.access_stats_behavior() == AccessStatsBehavior::Skip {
            return;
        }

        let this_access = LayerAccessStatFullDetails {
            when: SystemTime::now(),
            task_kind: ctx.task_kind(),
            access_kind,
        };

        let mut locked = self.0.lock().unwrap();
        locked.iter_mut().for_each(|inner| {
            inner.first_access.get_or_insert(this_access);
            inner.count_by_access_kind[access_kind] += 1;
            inner.task_kind_flag |= ctx.task_kind();
            inner.last_accesses.write(this_access);
        })
    }

    fn as_api_model(
        &self,
        reset: LayerAccessStatsReset,
    ) -> pageserver_api::models::LayerAccessStats {
        let mut locked = self.0.lock().unwrap();
        let inner = &mut locked.for_scraping_api;
        let LayerAccessStatsInner {
            first_access,
            count_by_access_kind,
            task_kind_flag,
            last_accesses,
            last_residence_changes,
        } = inner;
        let ret = pageserver_api::models::LayerAccessStats {
            access_count_by_access_kind: count_by_access_kind
                .iter()
                .map(|(kind, count)| (kind, *count))
                .collect(),
            task_kind_access_flag: task_kind_flag
                .iter()
                .map(|task_kind| task_kind.into()) // into static str, powered by strum_macros
                .collect(),
            first: first_access.as_ref().map(|a| a.as_api_model()),
            accesses_history: last_accesses.map(|m| m.as_api_model()),
            residence_events_history: last_residence_changes.clone(),
        };
        match reset {
            LayerAccessStatsReset::NoReset => (),
            LayerAccessStatsReset::JustTaskKindFlags => {
                inner.task_kind_flag.clear();
            }
            LayerAccessStatsReset::AllStats => {
                *inner = LayerAccessStatsInner::default();
            }
        }
        ret
    }

    /// Get the latest access timestamp, falling back to latest residence event.
    ///
    /// This function can only return `None` if there has not yet been a call to the
    /// [`record_residence_event`] method. That would generally be considered an
    /// implementation error. This function logs a rate-limited warning in that case.
    ///
    /// TODO: use type system to avoid the need for `fallback`.
    /// The approach in <https://github.com/neondatabase/neon/pull/3775>
    /// could be used to enforce that a residence event is recorded
    /// before a layer is added to the layer map. We could also have
    /// a layer wrapper type that holds the LayerAccessStats, and ensure
    /// that that type can only be produced by inserting into the layer map.
    ///
    /// [`record_residence_event`]: Self::record_residence_event
    pub(crate) fn latest_activity(&self) -> Option<SystemTime> {
        let locked = self.0.lock().unwrap();
        let inner = &locked.for_eviction_policy;
        match inner.last_accesses.recent() {
            Some(a) => Some(a.when),
            None => match inner.last_residence_changes.recent() {
                Some(e) => Some(e.timestamp),
                None => {
                    static WARN_RATE_LIMIT: Lazy<Mutex<(usize, RateLimit)>> =
                        Lazy::new(|| Mutex::new((0, RateLimit::new(Duration::from_secs(10)))));
                    let mut guard = WARN_RATE_LIMIT.lock().unwrap();
                    guard.0 += 1;
                    let occurences = guard.0;
                    guard.1.call(move || {
                        warn!(parent: None, occurences, "latest_activity not available, this is an implementation bug, using fallback value");
                    });
                    None
                }
            },
        }
    }
}

/// The download-ness ([`DownloadedLayer`]) can be either resident or wanted evicted.
///
/// However when we want something evicted, we cannot evict it right away as there might be current
/// reads happening on it. It has been for example searched from [`LayerMap`] but not yet
/// [`LayerE::get_value_reconstruct_data`].
///
/// [`LayerMap`]: crate::tenant::layer_map::LayerMap
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

/// A Layer contains all data in a "rectangle" consisting of a range of keys and
/// range of LSNs.
///
/// There are two kinds of layers, in-memory and on-disk layers. In-memory
/// layers are used to ingest incoming WAL, and provide fast access to the
/// recent page versions. On-disk layers are stored as files on disk, and are
/// immutable. This trait presents the common functionality of in-memory and
/// on-disk layers.
///
/// Furthermore, there are two kinds of on-disk layers: delta and image layers.
/// A delta layer contains all modifications within a range of LSNs and keys.
/// An image layer is a snapshot of all the data in a key-range, at a single
/// LSN.
///
/// This type models the on-disk layers, which can be evicted and on-demand downloaded.
// TODO:
// - internal arc, because I've now worked away majority of external wrapping
// - load time api which checks that files are present, fixmes in load time, remote timeline
//  client tests
pub(crate) struct LayerE {
    // do we really need this?
    conf: &'static PageServerConf,
    path: PathBuf,

    desc: PersistentLayerDesc,

    timeline: Weak<Timeline>,

    access_stats: LayerAccessStats,

    /// This custom OnceCell is backed by std mutex, but only held for short time periods.
    /// Initialization and deinitialization is done while holding a permit.
    inner: heavier_once_cell::OnceCell<ResidentOrWantedEvicted>,

    /// Do we want to garbage collect this when `LayerE` is dropped, where garbage collection
    /// means:
    /// - schedule remote deletion
    /// - instant local deletion
    wanted_garbage_collected: AtomicBool,

    /// Accessed using `Ordering::Acquire` or `Ordering::Release` to have happens before together
    /// to allow wait-less `evict`
    ///
    /// FIXME: this is likely bogus assumption, there is still time for us to set the flag in
    /// `evict` after the task holding the lock has made the check and is dropping the mutex guard.
    ///
    /// However eviction will try to evict this again, so maybe it's fine?
    wanted_evicted: AtomicBool,

    /// Version is to make sure we will in fact only evict a file if no new guard has been created
    /// for it.
    version: AtomicUsize,

    have_remote_client: bool,

    /// Allow subscribing to when the layer actually gets evicted.
    ///
    /// This might never come unless eviction called periodically.
    status: tokio::sync::broadcast::Sender<Status>,
}

#[derive(Debug, Clone, Copy)]
enum Status {
    Evicted,
    Downloaded,
}

impl std::fmt::Display for LayerE {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.layer_desc().short_id())
    }
}

impl std::fmt::Debug for LayerE {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self)
    }
}

impl AsLayerDesc for LayerE {
    fn layer_desc(&self) -> &PersistentLayerDesc {
        &self.desc
    }
}

impl Drop for LayerE {
    fn drop(&mut self) {
        if !*self.wanted_garbage_collected.get_mut() {
            // should we try to evict if the last wish was for eviction?
            // feels like there's some hazard of overcrowding near shutdown near by, but we don't
            // run drops during shutdown (yet)
            return;
        }

        // TODO: spawn_blocking?
        let span = tracing::info_span!(parent: None, "layer_drop", tenant_id = %self.layer_desc().tenant_id, timeline_id = %self.layer_desc().timeline_id, layer = %self);

        // SEMITODO: yes, this is sync, could spawn as well..
        let _g = span.entered();

        let mut removed = false;
        match std::fs::remove_file(&self.path) {
            Ok(()) => {
                removed = true;
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                // FIXME: unsure how to handle this. there are no deleted by mistake here, but it
                // feels like the downloadedness state tracking, and so knowing here if the file
                // should be present or not, requires load_layer_map fixing.
            }
            Err(e) => {
                // FIXME: it is possible, that we've just evicted the layer or it was always remote
                tracing::error!(layer = %self, "failed to remove garbage collected layer: {e}");
            }
        }

        if let Some(timeline) = self.timeline.upgrade() {
            if removed {
                timeline
                    .metrics
                    .resident_physical_size_gauge
                    .sub(self.layer_desc().file_size);
            }
            if let Some(remote_client) = timeline.remote_client.as_ref() {
                let res =
                    remote_client.schedule_layer_file_deletion(&[self.layer_desc().filename()]);

                if let Err(e) = res {
                    if !timeline.is_active() {
                        // downgrade the warning to info maybe?
                    }
                    tracing::warn!(layer=%self, "scheduling deletion on drop failed: {e:#}");
                }
            }
        } else {
            // no need to nag that timeline is gone
        }
    }
}

impl LayerE {
    pub(crate) fn new(
        conf: &'static PageServerConf,
        timeline: &Arc<Timeline>,
        filename: &LayerFileName,
        file_size: u64,
        access_stats: LayerAccessStats,
    ) -> LayerE {
        let desc = PersistentLayerDesc::from_filename(
            timeline.tenant_id,
            timeline.timeline_id,
            filename.clone(),
            file_size,
        );
        let path = conf
            .timeline_path(&desc.tenant_id, &desc.timeline_id)
            .join(desc.filename().to_string());
        LayerE {
            conf,
            path,
            desc,
            timeline: Arc::downgrade(timeline),
            have_remote_client: timeline.remote_client.is_some(),
            access_stats,
            wanted_garbage_collected: AtomicBool::new(false),
            wanted_evicted: AtomicBool::new(false),
            inner: Default::default(),
            version: AtomicUsize::new(0),
            status: tokio::sync::broadcast::channel(1).0,
        }
    }

    pub(crate) fn for_evicted(
        conf: &'static PageServerConf,
        timeline: &Arc<Timeline>,
        file_name: LayerFileName,
        metadata: LayerFileMetadata,
    ) -> Arc<LayerE> {
        let path = conf
            .timeline_path(&timeline.tenant_id, &timeline.timeline_id)
            .join(file_name.file_name());

        let desc = PersistentLayerDesc::from_filename(
            timeline.tenant_id,
            timeline.timeline_id,
            file_name,
            metadata.file_size(),
        );

        let access_stats = LayerAccessStats::for_loading_layer(LayerResidenceStatus::Evicted);

        let outer = Arc::new(LayerE {
            conf,
            path,
            desc,
            timeline: Arc::downgrade(timeline),
            access_stats,
            inner: heavier_once_cell::OnceCell::default(),
            wanted_garbage_collected: AtomicBool::default(),
            wanted_evicted: AtomicBool::default(),
            version: AtomicUsize::default(),
            have_remote_client: timeline.remote_client.is_some(),
            status: tokio::sync::broadcast::channel(1).0,
        });

        debug_assert!(outer.needs_download_blocking().unwrap().is_some());

        outer
    }

    pub(crate) fn for_resident(
        conf: &'static PageServerConf,
        timeline: &Arc<Timeline>,
        file_name: LayerFileName,
        metadata: LayerFileMetadata,
    ) -> ResidentLayer {
        let path = conf
            .timeline_path(&timeline.tenant_id, &timeline.timeline_id)
            .join(file_name.file_name());

        let desc = PersistentLayerDesc::from_filename(
            timeline.tenant_id,
            timeline.timeline_id,
            file_name,
            metadata.file_size(),
        );

        let access_stats = LayerAccessStats::for_loading_layer(LayerResidenceStatus::Resident);

        let mut resident = None;

        let outer = Arc::new_cyclic(|owner| {
            let inner = Arc::new(DownloadedLayer {
                owner: owner.clone(),
                kind: tokio::sync::OnceCell::default(),
            });
            resident = Some(inner.clone());
            LayerE {
                conf,
                path,
                desc,
                timeline: Arc::downgrade(timeline),
                have_remote_client: timeline.remote_client.is_some(),
                access_stats,
                wanted_garbage_collected: AtomicBool::new(false),
                wanted_evicted: AtomicBool::new(false),
                inner: heavier_once_cell::OnceCell::new(ResidentOrWantedEvicted::Resident(inner)),
                version: AtomicUsize::new(0),
                status: tokio::sync::broadcast::channel(1).0,
            }
        });

        debug_assert!(outer.needs_download_blocking().unwrap().is_none());

        let downloaded = resident.expect("just initialized");

        ResidentLayer {
            downloaded,
            owner: outer,
        }
    }

    pub(crate) fn for_written(
        conf: &'static PageServerConf,
        timeline: &Arc<Timeline>,
        desc: PersistentLayerDesc,
    ) -> anyhow::Result<ResidentLayer> {
        let path = conf
            .timeline_path(&desc.tenant_id, &desc.timeline_id)
            .join(desc.filename().to_string());

        let mut resident = None;

        let outer = Arc::new_cyclic(|owner| {
            let inner = Arc::new(DownloadedLayer {
                owner: owner.clone(),
                kind: tokio::sync::OnceCell::default(),
            });
            resident = Some(inner.clone());
            LayerE {
                conf,
                path,
                desc,
                timeline: Arc::downgrade(timeline),
                have_remote_client: timeline.remote_client.is_some(),
                access_stats: LayerAccessStats::empty_will_record_residence_event_later(),
                wanted_garbage_collected: AtomicBool::new(false),
                wanted_evicted: AtomicBool::new(false),
                inner: heavier_once_cell::OnceCell::new(ResidentOrWantedEvicted::Resident(inner)),
                version: AtomicUsize::new(0),
                status: tokio::sync::broadcast::channel(1).0,
            }
        });

        // FIXME: ugly, but if we don't do this check here, any error will pop up at read time
        // but we cannot check it because DeltaLayerWriter and ImageLayerWriter create the
        // instances *before* renaming the file to final destination
        // anyhow::ensure!(
        //     outer.needs_download_blocking()?.is_none(),
        //     "should not need downloading if it was just written"
        // );

        // FIXME: because we can now do garbage collection on drop, should we mark these files as
        // garbage collected until they get really get added to LayerMap? consider that files are
        // written out to disk, fsynced, renamed by `{Delta,Image}LayerWriter`, then waiting for
        // remaining files to be generated (compaction, create_image_layers) before being added to
        // LayerMap. We could panic or just error out during that time, even for unrelated reasons,
        // but the files would be left.

        Ok(ResidentLayer {
            downloaded: resident.expect("just wrote Some"),
            owner: outer,
        })
    }

    pub(crate) async fn evict_and_wait(
        self: &Arc<Self>,
        _: &RemoteTimelineClient,
    ) -> Result<(), super::timeline::EvictionError> {
        use tokio::sync::broadcast::error::RecvError;

        assert!(self.have_remote_client);

        self.wanted_evicted.store(true, Ordering::Release);

        let mut rx = self.status.subscribe();

        // why call get instead of looking at the watch? because get will downgrade any
        // Arc<_> it finds, because we set the wanted_evicted
        if self.get().is_none() {
            // it was not evictable in the first place
            // our store to the wanted_evicted does not matter; it will be reset by next download
            return Err(super::timeline::EvictionError::NotFound);
        }

        match rx.recv().await {
            Ok(Status::Evicted) => Ok(()),
            Ok(Status::Downloaded) => Err(super::timeline::EvictionError::Downloaded),
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
                    Some(_) => Err(super::timeline::EvictionError::Downloaded),
                    None => Ok(()),
                }
            }
        }
    }

    /// Access the current state without waiting for the file to be downloaded.
    ///
    /// Used by eviction only. Requires that we've initialized to state which is respective to the
    /// actual residency state.
    fn get(&self) -> Option<Arc<DownloadedLayer>> {
        let locked = self.inner.get();
        Self::get_or_apply_evictedness(locked, &self.wanted_evicted)
    }

    /// Delete the layer file when the `self` gets dropped, also schedule a remote index upload
    /// then perhaps.
    pub(crate) fn garbage_collect(&self) {
        self.wanted_garbage_collected.store(true, Ordering::Release);
    }

    /// Return data needed to reconstruct given page at LSN.
    ///
    /// It is up to the caller to collect more data from previous layer and
    /// perform WAL redo, if necessary.
    ///
    /// See PageReconstructResult for possible return values. The collected data
    /// is appended to reconstruct_data; the caller should pass an empty struct
    /// on first call, or a struct with a cached older image of the page if one
    /// is available. If this returns ValueReconstructResult::Continue, look up
    /// the predecessor layer and call again with the same 'reconstruct_data' to
    /// collect more data.
    pub(crate) async fn get_value_reconstruct_data(
        self: &Arc<Self>,
        key: Key,
        lsn_range: Range<Lsn>,
        reconstruct_data: &mut ValueReconstructState,
        ctx: &RequestContext,
    ) -> anyhow::Result<ValueReconstructResult> {
        use anyhow::ensure;

        let layer = self.get_or_maybe_download(true, Some(ctx)).await?;
        self.access_stats
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
            .get_value_reconstruct_data(key, lsn_range, reconstruct_data, self)
            .await
    }

    /// Creates a guard object which prohibit evicting this layer as long as the value is kept
    /// around.
    pub(crate) async fn guard_against_eviction(
        self: &Arc<Self>,
        allow_download: bool,
    ) -> anyhow::Result<ResidentLayer> {
        let downloaded = self.get_or_maybe_download(allow_download, None).await?;

        Ok(ResidentLayer {
            downloaded,
            owner: self.clone(),
        })
    }

    pub(crate) async fn get_or_download(self: &Arc<Self>) -> anyhow::Result<()> {
        self.get_or_maybe_download(true, None).await?;
        Ok(())
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

    /// Cancellation safe.
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

            // drop the old one, it at most held the weak or had not been initialized ever
            // locked.take();

            // technically the mutex could be dropped here.
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
                // what to do if we have a concurrent eviction request when we are downloading? eviction
                // api's use ResidentLayer, so evict could be moved there, or we just reset the state here.
                //
                // only reset this after we've decided we really need to download. otherwise it'd
                // be impossible to mark cancelled downloads for eviction, like one could imagine
                // we would like to do for prefetching which was not needed.
                self.wanted_evicted.store(false, Ordering::Release);

                if !can_ever_evict {
                    return Err(DownloadError::NoRemoteStorage);
                }

                if self.wanted_garbage_collected.load(Ordering::Acquire) {
                    // it will fail because we should had already scheduled a delete and an
                    // index update
                    tracing::info!(%reason, "downloading a wanted garbage collected layer, this might fail");
                    // FIXME: we probably do not gc delete until the file goes away...? unsure
                } else {
                    tracing::debug!(%reason, "downloading layer");
                }

                if let Some(ctx) = ctx {
                    use crate::context::DownloadBehavior::*;
                    let b = ctx.download_behavior();
                    match b {
                        Download => {}
                        Warn | Error => {
                            warn!(
                                "unexpectedly on-demand downloading remote layer {self} for task kind {:?}",
                                ctx.task_kind()
                            );
                            crate::metrics::UNEXPECTED_ONDEMAND_DOWNLOADS.inc();

                            let really_error = matches!(b, Error)
                                && !self.conf.ondemand_download_behavior_treat_error_as_warn;

                            if really_error {
                                // originally this returned
                                // return Err(PageReconstructError::NeedsDownload(
                                //     TenantTimelineId::new(self.tenant_id, self.timeline_id),
                                //     remote_layer.filename(),
                                // ))
                                //
                                // this check is only probablistic, seems like flakyness footgun
                                return Err(DownloadError::ContextAndConfigReallyDeniesDownloads);
                            }
                        }
                    }
                }

                if !allow_download {
                    // this does look weird, but for LayerE the "downloading" means also changing
                    // internal once related state ...
                    return Err(DownloadError::DownloadRequired);
                }

                let task_name = format!("download layer {}", self);

                let (tx, rx) = tokio::sync::oneshot::channel();
                // this is sadly needed because of task_mgr::shutdown_tasks, otherwise we cannot
                // block tenant::mgr::remove_tenant_from_memory.

                let this = self.clone();
                crate::task_mgr::spawn(
                    &tokio::runtime::Handle::current(),
                    TaskKind::RemoteDownloadTask,
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
                            &crate::tenant::remote_timeline_client::index::LayerFileMetadata::new(
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
                                    // for eviction?
                                },
                                Err(e) => {
                                    // our caller is cancellation safe, but we might be racing with
                                    // another attempt to reinitialize. before we have cancellation
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
                    }
                    Ok(Err(e)) => {
                        tracing::error!("layer file download failed: {e:#}");
                        return Err(DownloadError::DownloadFailed);
                        // FIXME: we need backoff here so never spiral to download loop, maybe,
                        // because remote timeline client already retries
                    }
                    Err(_gone) => {
                        return Err(DownloadError::DownloadCancelled);
                    }
                }
            } else {
                // the file is present locally and we could even be running without remote
                // storage
            }

            // the assumption is that we own the layer residentness, no operator should go in
            // and delete random files. this would be evident when trying to access the file
            // Nth time (N>1) while having the VirtualFile evicted in between.
            //
            // we could support this by looping on NotFound from the layer access methods, but
            // it's difficult to implement this so that the operator does not delete
            // not-yet-uploaded files.

            let res = Arc::new(DownloadedLayer {
                owner: Arc::downgrade(self),
                kind: tokio::sync::OnceCell::default(),
            });

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

    pub(crate) fn local_path(&self) -> &std::path::Path {
        // maybe it does make sense to have this or maybe not
        &self.path
    }

    async fn needs_download(&self) -> Result<Option<NeedsDownload>, std::io::Error> {
        match tokio::fs::metadata(self.local_path()).await {
            Ok(m) => Ok(self.is_file_present_and_good_size(&m)),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Some(NeedsDownload::NotFound)),
            Err(e) => Err(e),
        }
    }

    pub(crate) fn needs_download_blocking(&self) -> Result<Option<NeedsDownload>, std::io::Error> {
        match self.local_path().metadata() {
            Ok(m) => Ok(self.is_file_present_and_good_size(&m)),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Some(NeedsDownload::NotFound)),
            Err(e) => Err(e),
        }
    }

    fn is_file_present_and_good_size(&self, m: &std::fs::Metadata) -> Option<NeedsDownload> {
        // in future, this should include sha2-256 the file, hopefully rarely, because info uses
        // this as well
        if !m.is_file() {
            Some(NeedsDownload::NotFile)
        } else if m.len() != self.desc.file_size {
            Some(NeedsDownload::WrongSize {
                actual: m.len(),
                expected: self.desc.file_size,
            })
        } else {
            None
        }
    }

    pub(crate) fn info(&self, reset: LayerAccessStatsReset) -> HistoricLayerInfo {
        let layer_file_name = self.desc.filename().file_name();

        let remote = self
            .needs_download_blocking()
            .map(|maybe| maybe.is_some())
            .unwrap_or(false);
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

    pub(crate) fn access_stats(&self) -> &LayerAccessStats {
        &self.access_stats
    }

    /// Our resident layer has been dropped, we might hold the lock elsewhere.
    fn on_drop(self: Arc<LayerE>) {
        let gc = self.wanted_garbage_collected.load(Ordering::Acquire);
        let evict = self.wanted_evicted.load(Ordering::Acquire);
        let can_evict = self.have_remote_client;

        if gc {
            // do nothing now, only when the whole layer is dropped. gc will end up deleting the
            // whole layer, in case there is no reference cycle.
        } else if can_evict && evict {
            // we can remove this right now, but ... we really should not block or do anything.
            // spawn a task which first does a version check, and that version is also incremented
            // on get_or_download, so we will not collide?
            let version = self.version.load(Ordering::Relaxed);

            let span = tracing::info_span!(parent: None, "layer_evict", tenant_id = %self.desc.tenant_id, timeline_id = %self.desc.timeline_id, layer=%self);

            // downgrade in case there's a queue backing up, or we are just tearing stuff down, and
            // would soon delete anyways.
            let this = Arc::downgrade(&self);
            drop(self);

            let eviction = {
                let span = tracing::info_span!(parent: span.clone(), "blocking");
                async move {
                    // the layer is already gone, don't do anything. LayerE drop has already ran.
                    let Some(this) = this.upgrade() else { return; };

                    // deleted or detached timeline, don't do anything.
                    let Some(timeline) = this.timeline.upgrade() else { return; };

                    // to avoid starting a new download while we evict, keep holding on to the
                    // permit. note that we will not close the semaphore when done, because it will
                    // be used by the re-download.
                    let _permit = {
                        let maybe_downloaded = this.inner.get();
                        // relaxed ordering: we dont have any other atomics pending
                        if version != this.version.load(Ordering::Relaxed) {
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

                    if !this.wanted_evicted.load(Ordering::Acquire) {
                        // if there's already interest, should we just early exit? this is not
                        // currently *cleared* on interest, maybe it shouldn't?
                        // FIXME: wanted_evicted cannot be unset right now
                        //
                        // NOTE: us holding the permit prevents a new round of download happening
                        // right now
                        return;
                    }

                    let path = this.path.to_owned();

                    let capture_mtime_and_delete = tokio::task::spawn_blocking({
                        let span = span.clone();
                        move || {
                            let _e = span.entered();
                            // FIXME: we can now initialize the mtime during first get_or_download,
                            // and track that in-memory for the following? does that help?
                            let m = path.metadata()?;
                            let local_layer_mtime = m.modified()?;
                            std::fs::remove_file(&path)?;
                            Ok::<_, std::io::Error>(local_layer_mtime)
                        }
                    });

                    let res = capture_mtime_and_delete.await;

                    drop(this.status.send(Status::Evicted));

                    match res {
                        Ok(Ok(local_layer_mtime)) => {
                            let duration =
                                std::time::SystemTime::now().duration_since(local_layer_mtime);
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
                                .sub(this.desc.file_size);
                        }
                        Ok(Err(e)) if e.kind() == std::io::ErrorKind::NotFound => {
                            tracing::info!("failed to evict file from disk, it was already gone");
                        }
                        Ok(Err(e)) => {
                            tracing::warn!("failed to evict file from disk: {e:#}");
                        }
                        Err(je) if je.is_cancelled() => unreachable!("unsupported"),
                        Err(je) if je.is_panic() => { /* already logged */ }
                        Err(je) => {
                            tracing::warn!(error = ?je, "unexpected join_error while evicting the file")
                        }
                    }
                }
            }
            .instrument(span);

            crate::task_mgr::BACKGROUND_RUNTIME.spawn(eviction);
        }
    }
}

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

impl NeedsDownload {
    pub(crate) fn is_not_found(&self) -> bool {
        matches!(self, NeedsDownload::NotFound)
    }

    pub(crate) fn actual_size(&self) -> Option<u64> {
        match self {
            NeedsDownload::WrongSize { actual, .. } => Some(*actual),
            _ => None,
        }
    }
}

/// Holds both Arc requriring that both components stay resident while holding this alive and no evictions
/// or garbage collection happens.
#[derive(Clone)]
pub(crate) struct ResidentLayer {
    owner: Arc<LayerE>,
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
    pub(crate) fn drop_eviction_guard(self) -> Arc<LayerE> {
        self.into()
    }

    /// Loads all keys stored in the layer. Returns key, lsn and value size.
    pub(crate) async fn load_keys(
        &self,
        ctx: &RequestContext,
    ) -> anyhow::Result<Vec<DeltaEntry<'_>>> {
        use LayerKind::*;

        match self.downloaded.get(&self.owner).await? {
            Delta(d) => {
                self.owner
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
}

impl AsLayerDesc for ResidentLayer {
    fn layer_desc(&self) -> &PersistentLayerDesc {
        self.owner.layer_desc()
    }
}

impl AsRef<Arc<LayerE>> for ResidentLayer {
    fn as_ref(&self) -> &Arc<LayerE> {
        &self.owner
    }
}

/// Allow slimming down if we don't want the `2*usize` with eviction candidates?
impl From<ResidentLayer> for Arc<LayerE> {
    fn from(value: ResidentLayer) -> Self {
        value.owner
    }
}

impl std::ops::Deref for ResidentLayer {
    type Target = LayerE;

    fn deref(&self) -> &Self::Target {
        &self.owner
    }
}

#[derive(Debug, thiserror::Error)]
#[error("Layer has been removed from LayerMap already")]
pub(crate) struct RemovedFromLayerMap;

/// Holds the actual downloaded layer, and handles evicting the file on drop.
pub(crate) struct DownloadedLayer {
    owner: Weak<LayerE>,
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
    async fn get(&self, owner: &LayerE) -> anyhow::Result<&LayerKind> {
        // the owner is required so that we don't have to upgrade the self.owner, which will only
        // be used on drop. this way, initializing a DownloadedLayer without an owner is statically
        // impossible, so we can just not worry about it.
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
        owner: &LayerE,
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
}

/// Wrapper around an actual layer implementation.
#[derive(Debug)]
enum LayerKind {
    Delta(delta_layer::DeltaLayerInner),
    Image(image_layer::ImageLayerInner),
}

/// Get a layer descriptor from a layer.
pub trait AsLayerDesc {
    /// Get the layer descriptor.
    fn layer_desc(&self) -> &PersistentLayerDesc;
}

pub mod tests {
    use super::*;

    impl From<DeltaFileName> for PersistentLayerDesc {
        fn from(value: DeltaFileName) -> Self {
            PersistentLayerDesc::new_delta(
                TenantId::from_array([0; 16]),
                TimelineId::from_array([0; 16]),
                value.key_range,
                value.lsn_range,
                233,
            )
        }
    }

    impl From<ImageFileName> for PersistentLayerDesc {
        fn from(value: ImageFileName) -> Self {
            PersistentLayerDesc::new_img(
                TenantId::from_array([0; 16]),
                TimelineId::from_array([0; 16]),
                value.key_range,
                value.lsn,
                233,
            )
        }
    }

    impl From<LayerFileName> for PersistentLayerDesc {
        fn from(value: LayerFileName) -> Self {
            match value {
                LayerFileName::Delta(d) => Self::from(d),
                LayerFileName::Image(i) => Self::from(i),
            }
        }
    }
}

/// Range wrapping newtype, which uses display to render Debug.
///
/// Useful with `Key`, which has too verbose `{:?}` for printing multiple layers.
struct RangeDisplayDebug<'a, T: std::fmt::Display>(&'a Range<T>);

impl<'a, T: std::fmt::Display> std::fmt::Debug for RangeDisplayDebug<'a, T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}..{}", self.0.start, self.0.end)
    }
}

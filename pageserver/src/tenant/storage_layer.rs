//! Common traits and structs for layers

pub mod delta_layer;
pub mod image_layer;
pub(crate) mod inmemory_layer;
pub(crate) mod layer;
mod layer_desc;
mod layer_name;

use crate::context::{AccessStatsBehavior, RequestContext};
use crate::repository::Value;
use crate::task_mgr::TaskKind;
use crate::walrecord::NeonWalRecord;
use bytes::Bytes;
use enum_map::EnumMap;
use enumset::EnumSet;
use once_cell::sync::Lazy;
use pageserver_api::key::Key;
use pageserver_api::keyspace::{KeySpace, KeySpaceRandomAccum};
use pageserver_api::models::{
    LayerAccessKind, LayerResidenceEvent, LayerResidenceEventReason, LayerResidenceStatus,
};
use std::borrow::Cow;
use std::cmp::{Ordering, Reverse};
use std::collections::hash_map::Entry;
use std::collections::{BinaryHeap, HashMap};
use std::ops::Range;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tracing::warn;
use utils::history_buffer::HistoryBufferWithDropCounter;
use utils::rate_limit::RateLimit;

use utils::{id::TimelineId, lsn::Lsn};

pub use delta_layer::{DeltaLayer, DeltaLayerWriter, ValueRef};
pub use image_layer::{ImageLayer, ImageLayerWriter};
pub use inmemory_layer::InMemoryLayer;
pub use layer_desc::{PersistentLayerDesc, PersistentLayerKey};
pub use layer_name::{DeltaLayerName, ImageLayerName, LayerName};

pub(crate) use layer::{EvictionError, Layer, ResidentLayer};

use self::inmemory_layer::InMemoryLayerFileId;

use super::timeline::GetVectoredError;
use super::PageReconstructError;

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
#[derive(Debug, Default)]
pub struct ValueReconstructState {
    pub records: Vec<(Lsn, NeonWalRecord)>,
    pub img: Option<(Lsn, Bytes)>,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) enum ValueReconstructSituation {
    Complete,
    #[default]
    Continue,
}

/// Reconstruct data accumulated for a single key during a vectored get
#[derive(Debug, Default, Clone)]
pub(crate) struct VectoredValueReconstructState {
    pub(crate) records: Vec<(Lsn, NeonWalRecord)>,
    pub(crate) img: Option<(Lsn, Bytes)>,

    situation: ValueReconstructSituation,
}

impl VectoredValueReconstructState {
    fn get_cached_lsn(&self) -> Option<Lsn> {
        self.img.as_ref().map(|img| img.0)
    }
}

impl From<VectoredValueReconstructState> for ValueReconstructState {
    fn from(mut state: VectoredValueReconstructState) -> Self {
        // walredo expects the records to be descending in terms of Lsn
        state.records.sort_by_key(|(lsn, _)| Reverse(*lsn));

        ValueReconstructState {
            records: state.records,
            img: state.img,
        }
    }
}

/// Bag of data accumulated during a vectored get..
pub(crate) struct ValuesReconstructState {
    /// The keys will be removed after `get_vectored` completes. The caller outside `Timeline`
    /// should not expect to get anything from this hashmap.
    pub(crate) keys: HashMap<Key, Result<VectoredValueReconstructState, PageReconstructError>>,
    /// The keys which are already retrieved
    keys_done: KeySpaceRandomAccum,

    /// The keys covered by the image layers
    keys_with_image_coverage: Option<Range<Key>>,

    // Statistics that are still accessible as a caller of `get_vectored_impl`.
    layers_visited: u32,
    delta_layers_visited: u32,
}

impl ValuesReconstructState {
    pub(crate) fn new() -> Self {
        Self {
            keys: HashMap::new(),
            keys_done: KeySpaceRandomAccum::new(),
            keys_with_image_coverage: None,
            layers_visited: 0,
            delta_layers_visited: 0,
        }
    }

    /// Associate a key with the error which it encountered and mark it as done
    pub(crate) fn on_key_error(&mut self, key: Key, err: PageReconstructError) {
        let previous = self.keys.insert(key, Err(err));
        if let Some(Ok(state)) = previous {
            if state.situation == ValueReconstructSituation::Continue {
                self.keys_done.add_key(key);
            }
        }
    }

    pub(crate) fn on_layer_visited(&mut self, layer: &ReadableLayer) {
        self.layers_visited += 1;
        if let ReadableLayer::PersistentLayer(layer) = layer {
            if layer.layer_desc().is_delta() {
                self.delta_layers_visited += 1;
            }
        }
    }

    pub(crate) fn get_delta_layers_visited(&self) -> u32 {
        self.delta_layers_visited
    }

    pub(crate) fn get_layers_visited(&self) -> u32 {
        self.layers_visited
    }

    /// This function is called after reading a keyspace from a layer.
    /// It checks if the read path has now moved past the cached Lsn for any keys.
    ///
    /// Implementation note: We intentionally iterate over the keys for which we've
    /// already collected some reconstruct data. This avoids scaling complexity with
    /// the size of the search space.
    pub(crate) fn on_lsn_advanced(&mut self, keyspace: &KeySpace, advanced_to: Lsn) {
        for (key, value) in self.keys.iter_mut() {
            if !keyspace.contains(key) {
                continue;
            }

            if let Ok(state) = value {
                if state.situation != ValueReconstructSituation::Complete
                    && state.get_cached_lsn() >= Some(advanced_to)
                {
                    state.situation = ValueReconstructSituation::Complete;
                    self.keys_done.add_key(*key);
                }
            }
        }
    }

    /// On hitting image layer, we can mark all keys in this range as done, because
    /// if the image layer does not contain a key, it is deleted/never added.
    pub(crate) fn on_image_layer_visited(&mut self, key_range: &Range<Key>) {
        let prev_val = self.keys_with_image_coverage.replace(key_range.clone());
        assert_eq!(
            prev_val, None,
            "should consume the keyspace before the next iteration"
        );
    }

    /// Update the state collected for a given key.
    /// Returns true if this was the last value needed for the key and false otherwise.
    ///
    /// If the key is done after the update, mark it as such.
    pub(crate) fn update_key(
        &mut self,
        key: &Key,
        lsn: Lsn,
        value: Value,
    ) -> ValueReconstructSituation {
        let state = self
            .keys
            .entry(*key)
            .or_insert(Ok(VectoredValueReconstructState::default()));

        if let Ok(state) = state {
            let key_done = match state.situation {
                ValueReconstructSituation::Complete => unreachable!(),
                ValueReconstructSituation::Continue => match value {
                    Value::Image(img) => {
                        state.img = Some((lsn, img));
                        true
                    }
                    Value::WalRecord(rec) => {
                        debug_assert!(
                            Some(lsn) > state.get_cached_lsn(),
                            "Attempt to collect a record below cached LSN for walredo: {} < {}",
                            lsn,
                            state
                                .get_cached_lsn()
                                .expect("Assertion can only fire if a cached lsn is present")
                        );

                        let will_init = rec.will_init();
                        state.records.push((lsn, rec));
                        will_init
                    }
                },
            };

            if key_done && state.situation == ValueReconstructSituation::Continue {
                state.situation = ValueReconstructSituation::Complete;
                self.keys_done.add_key(*key);
            }

            state.situation
        } else {
            ValueReconstructSituation::Complete
        }
    }

    /// Returns the Lsn at which this key is cached if one exists.
    /// The read path should go no further than this Lsn for the given key.
    pub(crate) fn get_cached_lsn(&self, key: &Key) -> Option<Lsn> {
        self.keys
            .get(key)
            .and_then(|k| k.as_ref().ok())
            .and_then(|state| state.get_cached_lsn())
    }

    /// Returns the key space describing the keys that have
    /// been marked as completed since the last call to this function.
    /// Returns individual keys done, and the image layer coverage.
    pub(crate) fn consume_done_keys(&mut self) -> (KeySpace, Option<Range<Key>>) {
        (
            self.keys_done.consume_keyspace(),
            self.keys_with_image_coverage.take(),
        )
    }
}

impl Default for ValuesReconstructState {
    fn default() -> Self {
        Self::new()
    }
}

/// A key that uniquely identifies a layer in a timeline
#[derive(Debug, PartialEq, Eq, Clone, Hash)]
pub(crate) enum LayerId {
    PersitentLayerId(PersistentLayerKey),
    InMemoryLayerId(InMemoryLayerFileId),
}

/// Layer wrapper for the read path. Note that it is valid
/// to use these layers even after external operations have
/// been performed on them (compaction, freeze, etc.).
#[derive(Debug)]
pub(crate) enum ReadableLayer {
    PersistentLayer(Layer),
    InMemoryLayer(Arc<InMemoryLayer>),
}

/// A partial description of a read to be done.
#[derive(Debug, Clone)]
struct ReadDesc {
    /// An id used to resolve the readable layer within the fringe
    layer_id: LayerId,
    /// Lsn range for the read, used for selecting the next read
    lsn_range: Range<Lsn>,
}

/// Data structure which maintains a fringe of layers for the
/// read path. The fringe is the set of layers which intersects
/// the current keyspace that the search is descending on.
/// Each layer tracks the keyspace that intersects it.
///
/// The fringe must appear sorted by Lsn. Hence, it uses
/// a two layer indexing scheme.
#[derive(Debug)]
pub(crate) struct LayerFringe {
    planned_reads_by_lsn: BinaryHeap<ReadDesc>,
    layers: HashMap<LayerId, LayerKeyspace>,
}

#[derive(Debug)]
struct LayerKeyspace {
    layer: ReadableLayer,
    target_keyspace: KeySpace,
}

impl LayerFringe {
    pub(crate) fn new() -> Self {
        LayerFringe {
            planned_reads_by_lsn: BinaryHeap::new(),
            layers: HashMap::new(),
        }
    }

    pub(crate) fn next_layer(&mut self) -> Option<(ReadableLayer, KeySpace, Range<Lsn>)> {
        let read_desc = match self.planned_reads_by_lsn.pop() {
            Some(desc) => desc,
            None => return None,
        };

        let removed = self.layers.remove_entry(&read_desc.layer_id);
        match removed {
            Some((
                _,
                LayerKeyspace {
                    layer,
                    target_keyspace,
                },
            )) => Some((layer, target_keyspace, read_desc.lsn_range)),
            None => unreachable!("fringe internals are always consistent"),
        }
    }

    pub(crate) fn update(
        &mut self,
        layer: ReadableLayer,
        keyspace: KeySpace,
        lsn_range: Range<Lsn>,
    ) {
        let layer_id = layer.id();
        let entry = self.layers.entry(layer_id.clone());
        match entry {
            Entry::Occupied(mut entry) => {
                entry.get_mut().target_keyspace.merge(&keyspace);
            }
            Entry::Vacant(entry) => {
                self.planned_reads_by_lsn.push(ReadDesc {
                    lsn_range,
                    layer_id: layer_id.clone(),
                });
                entry.insert(LayerKeyspace {
                    layer,
                    target_keyspace: keyspace,
                });
            }
        }
    }
}

impl Default for LayerFringe {
    fn default() -> Self {
        Self::new()
    }
}

impl Ord for ReadDesc {
    fn cmp(&self, other: &Self) -> Ordering {
        let ord = self.lsn_range.end.cmp(&other.lsn_range.end);
        if ord == std::cmp::Ordering::Equal {
            self.lsn_range.start.cmp(&other.lsn_range.start).reverse()
        } else {
            ord
        }
    }
}

impl PartialOrd for ReadDesc {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for ReadDesc {
    fn eq(&self, other: &Self) -> bool {
        self.lsn_range == other.lsn_range
    }
}

impl Eq for ReadDesc {}

impl ReadableLayer {
    pub(crate) fn id(&self) -> LayerId {
        match self {
            Self::PersistentLayer(layer) => LayerId::PersitentLayerId(layer.layer_desc().key()),
            Self::InMemoryLayer(layer) => LayerId::InMemoryLayerId(layer.file_id()),
        }
    }

    pub(crate) async fn get_values_reconstruct_data(
        &self,
        keyspace: KeySpace,
        lsn_range: Range<Lsn>,
        reconstruct_state: &mut ValuesReconstructState,
        ctx: &RequestContext,
    ) -> Result<(), GetVectoredError> {
        match self {
            ReadableLayer::PersistentLayer(layer) => {
                layer
                    .get_values_reconstruct_data(keyspace, lsn_range, reconstruct_state, ctx)
                    .await
            }
            ReadableLayer::InMemoryLayer(layer) => {
                layer
                    .get_values_reconstruct_data(keyspace, lsn_range.end, reconstruct_state, ctx)
                    .await
            }
        }
    }
}

/// Return value from [`Layer::get_value_reconstruct_data`]
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
            task_kind: Cow::Borrowed(task_kind.into()), // into static str, powered by strum_macros
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
                .map(|task_kind| Cow::Borrowed(task_kind.into())) // into static str, powered by strum_macros
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

    /// Get the latest access timestamp, falling back to latest residence event, further falling
    /// back to `SystemTime::now` for a usable timestamp for eviction.
    pub(crate) fn latest_activity_or_now(&self) -> SystemTime {
        self.latest_activity().unwrap_or_else(SystemTime::now)
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
    fn latest_activity(&self) -> Option<SystemTime> {
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

/// Get a layer descriptor from a layer.
pub trait AsLayerDesc {
    /// Get the layer descriptor.
    fn layer_desc(&self) -> &PersistentLayerDesc;
}

pub mod tests {
    use pageserver_api::shard::TenantShardId;

    use super::*;

    impl From<DeltaLayerName> for PersistentLayerDesc {
        fn from(value: DeltaLayerName) -> Self {
            PersistentLayerDesc::new_delta(
                TenantShardId::from([0; 18]),
                TimelineId::from_array([0; 16]),
                value.key_range,
                value.lsn_range,
                233,
            )
        }
    }

    impl From<ImageLayerName> for PersistentLayerDesc {
        fn from(value: ImageLayerName) -> Self {
            PersistentLayerDesc::new_img(
                TenantShardId::from([0; 18]),
                TimelineId::from_array([0; 16]),
                value.key_range,
                value.lsn,
                233,
            )
        }
    }

    impl From<LayerName> for PersistentLayerDesc {
        fn from(value: LayerName) -> Self {
            match value {
                LayerName::Delta(d) => Self::from(d),
                LayerName::Image(i) => Self::from(i),
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

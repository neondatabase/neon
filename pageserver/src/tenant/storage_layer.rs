//! Common traits and structs for layers

mod delta_layer;
mod filename;
mod image_layer;
mod inmemory_layer;
mod remote_layer;

use crate::task_mgr::TaskKind;
use crate::walrecord::NeonWalRecord;
use crate::{context::RequestContext, repository::Value};
use anyhow::Result;
use bytes::Bytes;
use enum_map::EnumMap;
use enumset::EnumSet;
use heapless::HistoryBuffer;
use pageserver_api::models::LayerAccessKind;
use pageserver_api::models::{HistoricLayerInfo, Key};
use std::ops::Range;
use std::path::PathBuf;
use std::sync::atomic::{self, AtomicBool};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use utils::{
    id::{TenantId, TimelineId},
    lsn::Lsn,
};

pub use delta_layer::{DeltaLayer, DeltaLayerWriter};
pub use filename::{DeltaFileName, ImageFileName, LayerFileName, PathOrConf};
pub use image_layer::{ImageLayer, ImageLayerWriter};
pub use inmemory_layer::InMemoryLayer;
pub use remote_layer::RemoteLayer;

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

pub fn range_eq<T>(a: &Range<T>, b: &Range<T>) -> bool
where
    T: PartialEq<T>,
{
    a.start == b.start && a.end == b.end
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

/// Return value from Layer::get_page_reconstruct_data
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
pub struct LayerAccessStats(Mutex<LayerAccessStatsInner>);

#[derive(Debug, Clone)]
struct LayerAccessStatsInner {
    first_access: Option<LayerAccessStatFullDetails>,
    count_by_access_kind: EnumMap<LayerAccessKind, u64>,
    task_kind_flag: EnumSet<TaskKind>,
    last_accesses: HistoryBuffer<LayerAccessStatFullDetails, 16>,
    last_residence_changes: HistoryBuffer<LayerResidenceStatus, 16>,
}

#[derive(Debug, Clone)]
struct LayerAccessStatFullDetails {
    when: SystemTime,
    task_kind: TaskKind,
    access_kind: LayerAccessKind,
}

#[derive(Debug, Clone)]
pub enum LayerResidenceStatus {
    Resident {
        timestamp: SystemTime,
        /// If `true`, then this resident status marks the birth time of the layer.
        created: bool,
    },
    Evicted {
        timestamp: SystemTime,
    },
}

#[derive(Clone, Copy, strum_macros::EnumString)]
pub enum LayerAccessStatsReset {
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
    fn to_api_model(&self) -> pageserver_api::models::LayerAccessStatFullDetails {
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

impl LayerResidenceStatus {
    /// Residence status for a layer file that only exists on the remote.
    pub fn evicted() -> Self {
        LayerResidenceStatus::Evicted {
            timestamp: SystemTime::now(),
        }
    }

    /// Residence status for a layer file that exists locally.
    /// It may also exist on the remote, we don't care here.
    /// NB: use this for existing layer files, e.g., during timeline load.
    /// For newly written layer files, use [`created`].
    pub fn resident() -> Self {
        LayerResidenceStatus::Resident {
            timestamp: SystemTime::now(),
            created: false,
        }
    }

    /// Residence status for a local layer file that we just wrote.
    /// Example: a layer file created by compaction.
    /// Private, because callers are supposed to use [`LayerAccessStats::new_for_new_layer_file`].
    fn created() -> Self {
        LayerResidenceStatus::Resident {
            timestamp: SystemTime::now(),
            created: true,
        }
    }

    fn to_api_model(&self) -> pageserver_api::models::LayerResidenceStatus {
        match self {
            LayerResidenceStatus::Resident { timestamp, created } => {
                pageserver_api::models::LayerResidenceStatus::Resident {
                    timestamp_millis_since_epoch: system_time_to_millis_since_epoch(timestamp),
                    created: *created,
                }
            }
            LayerResidenceStatus::Evicted { timestamp } => {
                pageserver_api::models::LayerResidenceStatus::Evicted {
                    timestamp_millis_since_epoch: system_time_to_millis_since_epoch(timestamp),
                }
            }
        }
    }
}

pub static LAYER_ACCESS_STATS_KILLSWITCH: AtomicBool = AtomicBool::new(false);

impl Default for LayerAccessStatsInner {
    fn default() -> Self {
        LayerAccessStatsInner {
            first_access: None,
            count_by_access_kind: EnumMap::default(),
            task_kind_flag: EnumSet::default(),
            last_accesses: HistoryBuffer::default(),
            last_residence_changes: HistoryBuffer::default(),
        }
    }
}

impl LayerAccessStats {
    pub(crate) fn for_loading_layer(residence_status: LayerResidenceStatus) -> Self {
        let new = LayerAccessStats(Mutex::new(LayerAccessStatsInner::default()));
        new.record_residence_change(residence_status);
        new
    }

    pub(crate) fn for_new_layer_file() -> Self {
        let new = LayerAccessStats(Mutex::new(LayerAccessStatsInner::default()));
        new.record_residence_change(LayerResidenceStatus::created());
        new
    }

    /// Creates a clone of `self` and records `new_status` in the clone.
    /// The `new_status` is not recorded in `self`
    pub(crate) fn clone_for_residence_change(
        &self,
        new_status: LayerResidenceStatus,
    ) -> LayerAccessStats {
        let clone = {
            let inner = self.0.lock().unwrap();
            inner.clone()
        };
        let new = LayerAccessStats(Mutex::new(clone));
        new.record_residence_change(new_status);
        new
    }

    fn record_residence_change(&self, new_status: LayerResidenceStatus) {
        if LAYER_ACCESS_STATS_KILLSWITCH.load(atomic::Ordering::SeqCst) {
            return;
        }
        let mut inner = self.0.lock().unwrap();
        inner.last_residence_changes.write(new_status);
    }

    fn record_access(&self, access_kind: LayerAccessKind, task_kind: TaskKind) {
        if LAYER_ACCESS_STATS_KILLSWITCH.load(atomic::Ordering::SeqCst) {
            return;
        }
        let mut inner = self.0.lock().unwrap();
        let this_access = LayerAccessStatFullDetails {
            when: SystemTime::now(),
            task_kind,
            access_kind,
        };
        inner
            .first_access
            .get_or_insert_with(|| this_access.clone());
        inner.count_by_access_kind[access_kind] += 1;
        inner.task_kind_flag |= task_kind;
        inner.last_accesses.write(this_access);
    }
    fn reset(&self, what: LayerAccessStatsReset) {
        let mut inner = self.0.lock().unwrap();
        match what {
            LayerAccessStatsReset::JustTaskKindFlags => {
                inner.task_kind_flag.clear();
            }
            LayerAccessStatsReset::AllStats => {
                *inner = LayerAccessStatsInner::default();
            }
        }
    }
    fn to_api_model(&self) -> pageserver_api::models::LayerAccessStats {
        let inner = self.0.lock().unwrap();
        let LayerAccessStatsInner {
            first_access,
            count_by_access_kind,
            task_kind_flag,
            last_accesses,
            last_residence_changes,
        } = &*inner;
        pageserver_api::models::LayerAccessStats {
            access_count_by_access_kind: count_by_access_kind
                .iter()
                .map(|(kind, count)| (kind, *count))
                .collect(),
            task_kind_access_flag: task_kind_flag
                .iter()
                .map(|task_kind| task_kind.into()) // into static str, powered by strum_macros
                .collect(),
            first: first_access.as_ref().map(|a| a.to_api_model()),
            most_recent: last_accesses.iter().map(|a| a.to_api_model()).collect(),
            most_recent_residence_changes: last_residence_changes
                .iter()
                .map(|s| s.to_api_model())
                .collect(),
        }
    }
}

/// Supertrait of the [`Layer`] trait that captures the bare minimum interface
/// required by [`LayerMap`].
pub trait Layer: Send + Sync {
    /// Range of keys that this layer covers
    fn get_key_range(&self) -> Range<Key>;

    /// Inclusive start bound of the LSN range that this layer holds
    /// Exclusive end bound of the LSN range that this layer holds.
    ///
    /// - For an open in-memory layer, this is MAX_LSN.
    /// - For a frozen in-memory layer or a delta layer, this is a valid end bound.
    /// - An image layer represents snapshot at one LSN, so end_lsn is always the snapshot LSN + 1
    fn get_lsn_range(&self) -> Range<Lsn>;

    /// Does this layer only contain some data for the key-range (incremental),
    /// or does it contain a version of every page? This is important to know
    /// for garbage collecting old layers: an incremental layer depends on
    /// the previous non-incremental layer.
    fn is_incremental(&self) -> bool;

    ///
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
    fn get_value_reconstruct_data(
        &self,
        key: Key,
        lsn_range: Range<Lsn>,
        reconstruct_data: &mut ValueReconstructState,
        ctx: &RequestContext,
    ) -> Result<ValueReconstructResult>;

    /// A short ID string that uniquely identifies the given layer within a [`LayerMap`].
    fn short_id(&self) -> String;

    /// Dump summary of the contents of the layer to stdout
    fn dump(&self, verbose: bool, ctx: &RequestContext) -> Result<()>;
}

/// Returned by [`Layer::iter`]
pub type LayerIter<'i> = Box<dyn Iterator<Item = Result<(Key, Lsn, Value)>> + 'i>;

/// Returned by [`Layer::key_iter`]
pub type LayerKeyIter<'i> = Box<dyn Iterator<Item = (Key, Lsn, u64)> + 'i>;

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
/// LSN
///
pub trait PersistentLayer: Layer {
    fn get_tenant_id(&self) -> TenantId;

    /// Identify the timeline this layer belongs to
    fn get_timeline_id(&self) -> TimelineId;

    /// File name used for this layer, both in the pageserver's local filesystem
    /// state as well as in the remote storage.
    fn filename(&self) -> LayerFileName;

    // Path to the layer file in the local filesystem.
    // `None` for `RemoteLayer`.
    fn local_path(&self) -> Option<PathBuf>;

    /// Iterate through all keys and values stored in the layer
    fn iter(&self, ctx: &RequestContext) -> Result<LayerIter<'_>>;

    /// Iterate through all keys stored in the layer. Returns key, lsn and value size
    /// It is used only for compaction and so is currently implemented only for DeltaLayer
    fn key_iter(&self, _ctx: &RequestContext) -> Result<LayerKeyIter<'_>> {
        panic!("Not implemented")
    }

    /// Permanently remove this layer from disk.
    fn delete(&self) -> Result<()>;

    fn downcast_remote_layer(self: Arc<Self>) -> Option<std::sync::Arc<RemoteLayer>> {
        None
    }

    fn is_remote_layer(&self) -> bool {
        false
    }

    /// Returns None if the layer file size is not known.
    ///
    /// Should not change over the lifetime of the layer object because
    /// current_physical_size is computed as the som of this value.
    fn file_size(&self) -> Option<u64>;

    fn info(&self, reset: Option<LayerAccessStatsReset>) -> HistoricLayerInfo;

    fn access_stats(&self) -> &LayerAccessStats;
}

pub fn downcast_remote_layer(
    layer: &Arc<dyn PersistentLayer>,
) -> Option<std::sync::Arc<RemoteLayer>> {
    if layer.is_remote_layer() {
        Arc::clone(layer).downcast_remote_layer()
    } else {
        None
    }
}

impl std::fmt::Debug for dyn Layer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Layer")
            .field("short_id", &self.short_id())
            .finish()
    }
}

/// Holds metadata about a layer without any content. Used mostly for testing.
pub struct LayerDescriptor {
    pub key: Range<Key>,
    pub lsn: Range<Lsn>,
    pub is_incremental: bool,
    pub short_id: String,
}

impl Layer for LayerDescriptor {
    fn get_key_range(&self) -> Range<Key> {
        self.key.clone()
    }

    fn get_lsn_range(&self) -> Range<Lsn> {
        self.lsn.clone()
    }

    fn is_incremental(&self) -> bool {
        self.is_incremental
    }

    fn get_value_reconstruct_data(
        &self,
        _key: Key,
        _lsn_range: Range<Lsn>,
        _reconstruct_data: &mut ValueReconstructState,
        _ctx: &RequestContext,
    ) -> Result<ValueReconstructResult> {
        todo!("This method shouldn't be part of the Layer trait")
    }

    fn short_id(&self) -> String {
        self.short_id.clone()
    }

    fn dump(&self, _verbose: bool, _ctx: &RequestContext) -> Result<()> {
        todo!()
    }
}

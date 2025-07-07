pub(crate) mod analysis;
pub(crate) mod compaction;
pub mod delete;
pub(crate) mod detach_ancestor;
mod eviction_task;
pub(crate) mod handle;
mod heatmap_layers_downloader;
pub(crate) mod import_pgdata;
mod init;
pub mod layer_manager;
pub(crate) mod logical_size;
pub mod offload;
pub mod span;
pub mod uninit;
mod walreceiver;

use hashlink::LruCache;
use std::array;
use std::cmp::{max, min};
use std::collections::btree_map::Entry;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::ops::{ControlFlow, Deref, Range};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering as AtomicOrdering};
use std::sync::{Arc, Mutex, OnceLock, RwLock, Weak};
use std::time::{Duration, Instant, SystemTime};

use anyhow::{Context, Result, anyhow, bail, ensure};
use arc_swap::{ArcSwap, ArcSwapOption};
use bytes::Bytes;
use camino::Utf8Path;
use chrono::{DateTime, Utc};
use compaction::{CompactionOutcome, GcCompactionCombinedSettings};
use enumset::EnumSet;
use fail::fail_point;
use futures::stream::FuturesUnordered;
use futures::{FutureExt, StreamExt};
use handle::ShardTimelineId;
use layer_manager::{
    LayerManagerLockHolder, LayerManagerReadGuard, LayerManagerWriteGuard, LockedLayerManager,
    Shutdown,
};

use once_cell::sync::Lazy;
use pageserver_api::config::tenant_conf_defaults::DEFAULT_PITR_INTERVAL;
use pageserver_api::key::{
    KEY_SIZE, Key, METADATA_KEY_BEGIN_PREFIX, METADATA_KEY_END_PREFIX, NON_INHERITED_RANGE,
    SPARSE_RANGE,
};
use pageserver_api::keyspace::{KeySpaceAccum, KeySpaceRandomAccum, SparseKeyPartitioning};
use pageserver_api::models::{
    CompactKeyRange, CompactLsnRange, CompactionAlgorithm, CompactionAlgorithmSettings,
    DetachBehavior, DownloadRemoteLayersTaskInfo, DownloadRemoteLayersTaskSpawnRequest,
    EvictionPolicy, InMemoryLayerInfo, LayerMapInfo, LsnLease, PageTraceEvent, RelSizeMigration,
    TimelineState,
};
use pageserver_api::reltag::{BlockNumber, RelTag};
use pageserver_api::shard::{ShardIdentity, ShardIndex, ShardNumber, TenantShardId};
use postgres_connection::PgConnectionConfig;
use postgres_ffi::v14::xlog_utils;
use postgres_ffi::{PgMajorVersion, WAL_SEGMENT_SIZE, to_pg_timestamp};
use rand::Rng;
use remote_storage::DownloadError;
use serde_with::serde_as;
use storage_broker::BrokerClientChannel;
use tokio::runtime::Handle;
use tokio::sync::mpsc::Sender;
use tokio::sync::{Notify, oneshot, watch};
use tokio_util::sync::CancellationToken;
use tracing::*;
use utils::generation::Generation;
use utils::guard_arc_swap::GuardArcSwap;
use utils::id::TimelineId;
use utils::logging::{MonitorSlowFutureCallback, monitor_slow_future};
use utils::lsn::{AtomicLsn, Lsn, RecordLsn};
use utils::postgres_client::PostgresClientProtocol;
use utils::rate_limit::RateLimit;
use utils::seqwait::SeqWait;
use utils::simple_rcu::{Rcu, RcuReadGuard};
use utils::sync::gate::{Gate, GateGuard};
use utils::{completion, critical_timeline, fs_ext, pausable_failpoint};
#[cfg(test)]
use wal_decoder::models::value::Value;
use wal_decoder::serialized_batch::{SerializedValueBatch, ValueMeta};

use self::delete::DeleteTimelineFlow;
pub(super) use self::eviction_task::EvictionTaskTenantState;
use self::eviction_task::EvictionTaskTimelineState;
use self::logical_size::LogicalSize;
use self::walreceiver::{WalReceiver, WalReceiverConf};
use super::remote_timeline_client::RemoteTimelineClient;
use super::remote_timeline_client::index::{GcCompactionState, IndexPart};
use super::secondary::heatmap::HeatMapLayer;
use super::storage_layer::{LayerFringe, LayerVisibilityHint, ReadableLayer};
use super::tasks::log_compaction_error;
use super::upload_queue::NotInitialized;
use super::{
    AttachedTenantConf, GcError, HeatMapTimeline, MaybeOffloaded,
    debug_assert_current_span_has_tenant_and_timeline_id,
};
use crate::PERF_TRACE_TARGET;
use crate::aux_file::AuxFileSizeEstimator;
use crate::basebackup_cache::BasebackupCache;
use crate::config::PageServerConf;
use crate::context::{
    DownloadBehavior, PerfInstrumentFutureExt, RequestContext, RequestContextBuilder,
};
use crate::disk_usage_eviction_task::{DiskUsageEvictionInfo, EvictionCandidate, finite_f32};
use crate::feature_resolver::TenantFeatureResolver;
use crate::keyspace::{KeyPartitioning, KeySpace};
use crate::l0_flush::{self, L0FlushGlobalState};
use crate::metrics::{
    DELTAS_PER_READ_GLOBAL, LAYERS_PER_READ_AMORTIZED_GLOBAL, LAYERS_PER_READ_BATCH_GLOBAL,
    LAYERS_PER_READ_GLOBAL, ScanLatencyOngoingRecording, TimelineMetrics,
};
use crate::page_service::TenantManagerTypes;
use crate::pgdatadir_mapping::{
    CalculateLogicalSizeError, CollectKeySpaceError, DirectoryKind, LsnForTimestamp,
    MAX_AUX_FILE_V2_DELTAS, MetricsUpdate,
};
use crate::task_mgr::TaskKind;
use crate::tenant::gc_result::GcResult;
use crate::tenant::layer_map::LayerMap;
use crate::tenant::metadata::TimelineMetadata;
use crate::tenant::storage_layer::delta_layer::DeltaEntry;
use crate::tenant::storage_layer::inmemory_layer::IndexEntry;
use crate::tenant::storage_layer::{
    AsLayerDesc, BatchLayerWriter, DeltaLayerWriter, EvictionError, ImageLayerName,
    ImageLayerWriter, InMemoryLayer, IoConcurrency, Layer, LayerAccessStatsReset, LayerName,
    PersistentLayerDesc, PersistentLayerKey, ResidentLayer, ValueReconstructSituation,
    ValueReconstructState, ValuesReconstructState,
};
use crate::tenant::tasks::BackgroundLoopKind;
use crate::tenant::timeline::logical_size::CurrentLogicalSize;
use crate::virtual_file::{MaybeFatalIo, VirtualFile};
use crate::walingest::WalLagCooldown;
use crate::walredo::RedoAttemptType;
use crate::{ZERO_PAGE, task_mgr, walredo};

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub(crate) enum FlushLoopState {
    NotStarted,
    Running {
        #[cfg(test)]
        expect_initdb_optimization: bool,
        #[cfg(test)]
        initdb_optimization_count: usize,
    },
    Exited,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum ImageLayerCreationMode {
    /// Try to create image layers based on `time_for_new_image_layer`. Used in compaction code path.
    Try,
    /// Force creating the image layers if possible. For now, no image layers will be created
    /// for metadata keys. Used in compaction code path with force flag enabled.
    Force,
    /// Initial ingestion of the data, and no data should be dropped in this function. This
    /// means that no metadata keys should be included in the partitions. Used in flush frozen layer
    /// code path.
    Initial,
}

#[derive(Clone, Debug, Default)]
pub enum LastImageLayerCreationStatus {
    Incomplete {
        /// The last key of the partition (exclusive) that was processed in the last
        /// image layer creation attempt. We will continue from this key in the next
        /// attempt.
        last_key: Key,
    },
    Complete,
    #[default]
    Initial,
}

impl std::fmt::Display for ImageLayerCreationMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}

/// Temporary function for immutable storage state refactor, ensures we are dropping mutex guard instead of other things.
/// Can be removed after all refactors are done.
fn drop_layer_manager_rlock(rlock: LayerManagerReadGuard<'_>) {
    drop(rlock)
}

/// Temporary function for immutable storage state refactor, ensures we are dropping mutex guard instead of other things.
/// Can be removed after all refactors are done.
fn drop_layer_manager_wlock(rlock: LayerManagerWriteGuard<'_>) {
    drop(rlock)
}

/// The outward-facing resources required to build a Timeline
pub struct TimelineResources {
    pub remote_client: RemoteTimelineClient,
    pub pagestream_throttle: Arc<crate::tenant::throttle::Throttle>,
    pub pagestream_throttle_metrics: Arc<crate::metrics::tenant_throttling::Pagestream>,
    pub l0_compaction_trigger: Arc<Notify>,
    pub l0_flush_global_state: l0_flush::L0FlushGlobalState,
    pub basebackup_cache: Arc<BasebackupCache>,
    pub feature_resolver: Arc<TenantFeatureResolver>,
}

pub struct Timeline {
    pub(crate) conf: &'static PageServerConf,
    tenant_conf: Arc<ArcSwap<AttachedTenantConf>>,

    myself: Weak<Self>,

    pub(crate) tenant_shard_id: TenantShardId,
    pub timeline_id: TimelineId,

    /// The generation of the tenant that instantiated us: this is used for safety when writing remote objects.
    /// Never changes for the lifetime of this [`Timeline`] object.
    ///
    /// This duplicates the generation stored in LocationConf, but that structure is mutable:
    /// this copy enforces the invariant that generatio doesn't change during a Tenant's lifetime.
    pub(crate) generation: Generation,

    /// The detailed sharding information from our parent Tenant.  This enables us to map keys
    /// to shards, and is constant through the lifetime of this Timeline.
    shard_identity: ShardIdentity,

    pub pg_version: PgMajorVersion,

    /// The tuple has two elements.
    /// 1. `LayerFileManager` keeps track of the various physical representations of the layer files (inmem, local, remote).
    /// 2. `LayerMap`, the acceleration data structure for `get_reconstruct_data`.
    ///
    /// `LayerMap` maps out the `(PAGE,LSN) / (KEY,LSN)` space, which is composed of `(KeyRange, LsnRange)` rectangles.
    /// We describe these rectangles through the `PersistentLayerDesc` struct.
    ///
    /// When we want to reconstruct a page, we first find the `PersistentLayerDesc`'s that we need for page reconstruction,
    /// using `LayerMap`. Then, we use `LayerFileManager` to get the `PersistentLayer`'s that correspond to the
    /// `PersistentLayerDesc`'s.
    ///
    /// Hence, it's important to keep things coherent. The `LayerFileManager` must always have an entry for all
    /// `PersistentLayerDesc`'s in the `LayerMap`. If it doesn't, `LayerFileManager::get_from_desc` will panic at
    /// runtime, e.g., during page reconstruction.
    ///
    /// In the future, we'll be able to split up the tuple of LayerMap and `LayerFileManager`,
    /// so that e.g. on-demand-download/eviction, and layer spreading, can operate just on `LayerFileManager`.
    pub(crate) layers: LockedLayerManager,

    last_freeze_at: AtomicLsn,
    // Atomic would be more appropriate here.
    last_freeze_ts: RwLock<Instant>,

    pub(crate) standby_horizon: AtomicLsn,

    // WAL redo manager. `None` only for broken tenants.
    walredo_mgr: Option<Arc<super::WalRedoManager>>,

    /// Remote storage client.
    /// See [`remote_timeline_client`](super::remote_timeline_client) module comment for details.
    pub(crate) remote_client: Arc<RemoteTimelineClient>,

    // What page versions do we hold in the repository? If we get a
    // request > last_record_lsn, we need to wait until we receive all
    // the WAL up to the request. The SeqWait provides functions for
    // that. TODO: If we get a request for an old LSN, such that the
    // versions have already been garbage collected away, we should
    // throw an error, but we don't track that currently.
    //
    // last_record_lsn.load().last points to the end of last processed WAL record.
    //
    // We also remember the starting point of the previous record in
    // 'last_record_lsn.load().prev'. It's used to set the xl_prev pointer of the
    // first WAL record when the node is started up. But here, we just
    // keep track of it.
    last_record_lsn: SeqWait<RecordLsn, Lsn>,

    // All WAL records have been processed and stored durably on files on
    // local disk, up to this LSN. On crash and restart, we need to re-process
    // the WAL starting from this point.
    //
    // Some later WAL records might have been processed and also flushed to disk
    // already, so don't be surprised to see some, but there's no guarantee on
    // them yet.
    disk_consistent_lsn: AtomicLsn,

    // Parent timeline that this timeline was branched from, and the LSN
    // of the branch point.
    ancestor_timeline: Option<Arc<Timeline>>,
    ancestor_lsn: Lsn,

    // The LSN of gc-compaction that was last applied to this timeline.
    gc_compaction_state: ArcSwap<Option<GcCompactionState>>,

    pub(crate) metrics: Arc<TimelineMetrics>,

    // `Timeline` doesn't write these metrics itself, but it manages the lifetime.  Code
    // in `crate::page_service` writes these metrics.
    pub(crate) query_metrics: crate::metrics::SmgrQueryTimePerTimeline,

    directory_metrics_inited: [AtomicBool; DirectoryKind::KINDS_NUM],
    directory_metrics: [AtomicU64; DirectoryKind::KINDS_NUM],

    /// Ensures layers aren't frozen by checkpointer between
    /// [`Timeline::get_layer_for_write`] and layer reads.
    /// Locked automatically by [`TimelineWriter`] and checkpointer.
    /// Must always be acquired before the layer map/individual layer lock
    /// to avoid deadlock.
    ///
    /// The state is cleared upon freezing.
    write_lock: tokio::sync::Mutex<Option<TimelineWriterState>>,

    /// Used to avoid multiple `flush_loop` tasks running
    pub(super) flush_loop_state: Mutex<FlushLoopState>,

    /// layer_flush_start_tx can be used to wake up the layer-flushing task.
    /// - The u64 value is a counter, incremented every time a new flush cycle is requested.
    ///   The flush cycle counter is sent back on the layer_flush_done channel when
    ///   the flush finishes. You can use that to wait for the flush to finish.
    /// - The LSN is updated to max() of its current value and the latest disk_consistent_lsn
    ///   read by whoever sends an update
    layer_flush_start_tx: tokio::sync::watch::Sender<(u64, Lsn)>,
    /// to be notified when layer flushing has finished, subscribe to the layer_flush_done channel
    layer_flush_done_tx: tokio::sync::watch::Sender<(u64, Result<(), FlushLayerError>)>,

    // The LSN at which we have executed GC: whereas [`Self::gc_info`] records the LSN at which
    // we _intend_ to GC (i.e. the PITR cutoff), this LSN records where we actually last did it.
    // Because PITR interval is mutable, it's possible for this LSN to be earlier or later than
    // the planned GC cutoff.
    pub applied_gc_cutoff_lsn: Rcu<Lsn>,

    pub(crate) gc_compaction_layer_update_lock: tokio::sync::RwLock<()>,

    // List of child timelines and their branch points. This is needed to avoid
    // garbage collecting data that is still needed by the child timelines.
    pub(crate) gc_info: std::sync::RwLock<GcInfo>,

    pub(crate) last_image_layer_creation_status: ArcSwap<LastImageLayerCreationStatus>,

    // It may change across major versions so for simplicity
    // keep it after running initdb for a timeline.
    // It is needed in checks when we want to error on some operations
    // when they are requested for pre-initdb lsn.
    // It can be unified with latest_gc_cutoff_lsn under some "first_valid_lsn",
    // though let's keep them both for better error visibility.
    pub initdb_lsn: Lsn,

    /// The repartitioning result. Allows a single writer and multiple readers.
    pub(crate) partitioning: GuardArcSwap<((KeyPartitioning, SparseKeyPartitioning), Lsn)>,

    /// Configuration: how often should the partitioning be recalculated.
    repartition_threshold: u64,

    last_image_layer_creation_check_at: AtomicLsn,
    last_image_layer_creation_check_instant: std::sync::Mutex<Option<Instant>>,

    /// Current logical size of the "datadir", at the last LSN.
    current_logical_size: LogicalSize,

    /// Information about the last processed message by the WAL receiver,
    /// or None if WAL receiver has not received anything for this timeline
    /// yet.
    pub last_received_wal: Mutex<Option<WalReceiverInfo>>,
    pub walreceiver: Mutex<Option<WalReceiver>>,

    /// Relation size cache
    pub(crate) rel_size_latest_cache: RwLock<HashMap<RelTag, (Lsn, BlockNumber)>>,
    pub(crate) rel_size_snapshot_cache: Mutex<LruCache<(Lsn, RelTag), BlockNumber>>,

    download_all_remote_layers_task_info: RwLock<Option<DownloadRemoteLayersTaskInfo>>,

    state: watch::Sender<TimelineState>,

    /// Prevent two tasks from deleting the timeline at the same time. If held, the
    /// timeline is being deleted. If 'true', the timeline has already been deleted.
    pub delete_progress: TimelineDeleteProgress,

    eviction_task_timeline_state: tokio::sync::Mutex<EvictionTaskTimelineState>,

    /// Load or creation time information about the disk_consistent_lsn and when the loading
    /// happened. Used for consumption metrics.
    pub(crate) loaded_at: (Lsn, SystemTime),

    /// Gate to prevent shutdown completing while I/O is still happening to this timeline's data
    pub(crate) gate: Gate,

    /// Cancellation token scoped to this timeline: anything doing long-running work relating
    /// to the timeline should drop out when this token fires.
    pub(crate) cancel: CancellationToken,

    /// Make sure we only have one running compaction at a time in tests.
    ///
    /// Must only be taken in two places:
    /// - [`Timeline::compact`] (this file)
    /// - [`delete::delete_local_timeline_directory`]
    ///
    /// Timeline deletion will acquire both compaction and gc locks in whatever order.
    compaction_lock: tokio::sync::Mutex<()>,

    /// If true, the last compaction failed.
    compaction_failed: AtomicBool,

    /// Notifies the tenant compaction loop that there is pending L0 compaction work.
    l0_compaction_trigger: Arc<Notify>,

    /// Make sure we only have one running gc at a time.
    ///
    /// Must only be taken in two places:
    /// - [`Timeline::gc`] (this file)
    /// - [`delete::delete_local_timeline_directory`]
    ///
    /// Timeline deletion will acquire both compaction and gc locks in whatever order.
    gc_lock: tokio::sync::Mutex<()>,

    /// Cloned from [`super::TenantShard::pagestream_throttle`] on construction.
    pub(crate) pagestream_throttle: Arc<crate::tenant::throttle::Throttle>,

    /// Size estimator for aux file v2
    pub(crate) aux_file_size_estimator: AuxFileSizeEstimator,

    /// Some test cases directly place keys into the timeline without actually modifying the directory
    /// keys (i.e., DB_DIR). The test cases creating such keys will put the keyspaces here, so that
    /// these keys won't get garbage-collected during compaction/GC. This field only modifies the dense
    /// keyspace return value of `collect_keyspace`. For sparse keyspaces, use AUX keys for testing, and
    /// in the future, add `extra_test_sparse_keyspace` if necessary.
    #[cfg(test)]
    pub(crate) extra_test_dense_keyspace: ArcSwap<KeySpace>,

    pub(crate) l0_flush_global_state: L0FlushGlobalState,

    pub(crate) handles: handle::PerTimelineState<TenantManagerTypes>,

    pub(crate) attach_wal_lag_cooldown: Arc<OnceLock<WalLagCooldown>>,

    /// Cf. [`crate::tenant::CreateTimelineIdempotency`].
    pub(crate) create_idempotency: crate::tenant::CreateTimelineIdempotency,

    /// If Some, collects GetPage metadata for an ongoing PageTrace.
    pub(crate) page_trace: ArcSwapOption<Sender<PageTraceEvent>>,

    pub(super) previous_heatmap: ArcSwapOption<PreviousHeatmap>,

    /// May host a background Tokio task which downloads all the layers from the current
    /// heatmap on demand.
    heatmap_layers_downloader: Mutex<Option<heatmap_layers_downloader::HeatmapLayersDownloader>>,

    pub(crate) rel_size_v2_status: ArcSwapOption<RelSizeMigration>,

    wait_lsn_log_slow: tokio::sync::Semaphore,

    /// A channel to send async requests to prepare a basebackup for the basebackup cache.
    basebackup_cache: Arc<BasebackupCache>,

    feature_resolver: Arc<TenantFeatureResolver>,
}

pub(crate) enum PreviousHeatmap {
    Active {
        heatmap: HeatMapTimeline,
        read_at: std::time::Instant,
        // End LSN covered by the heatmap if known
        end_lsn: Option<Lsn>,
    },
    Obsolete,
}

pub type TimelineDeleteProgress = Arc<tokio::sync::Mutex<DeleteTimelineFlow>>;

pub struct WalReceiverInfo {
    pub wal_source_connconf: PgConnectionConfig,
    pub last_received_msg_lsn: Lsn,
    pub last_received_msg_ts: u128,
}

/// Information about how much history needs to be retained, needed by
/// Garbage Collection.
#[derive(Default)]
pub(crate) struct GcInfo {
    /// Specific LSNs that are needed.
    ///
    /// Currently, this includes all points where child branches have
    /// been forked off from. In the future, could also include
    /// explicit user-defined snapshot points.
    pub(crate) retain_lsns: Vec<(Lsn, TimelineId, MaybeOffloaded)>,

    /// The cutoff coordinates, which are combined by selecting the minimum.
    pub(crate) cutoffs: GcCutoffs,

    /// Leases granted to particular LSNs.
    pub(crate) leases: BTreeMap<Lsn, LsnLease>,

    /// Whether our branch point is within our ancestor's PITR interval (for cost estimation)
    pub(crate) within_ancestor_pitr: bool,
}

impl GcInfo {
    pub(crate) fn min_cutoff(&self) -> Lsn {
        self.cutoffs.select_min()
    }

    pub(super) fn insert_child(
        &mut self,
        child_id: TimelineId,
        child_lsn: Lsn,
        is_offloaded: MaybeOffloaded,
    ) {
        self.retain_lsns.push((child_lsn, child_id, is_offloaded));
        self.retain_lsns.sort_by_key(|i| i.0);
    }

    pub(super) fn remove_child_maybe_offloaded(
        &mut self,
        child_id: TimelineId,
        maybe_offloaded: MaybeOffloaded,
    ) -> bool {
        // Remove at most one element. Needed for correctness if there is two live `Timeline` objects referencing
        // the same timeline. Shouldn't but maybe can occur when Arc's live longer than intended.
        let mut removed = false;
        self.retain_lsns.retain(|i| {
            if removed {
                return true;
            }
            let remove = i.1 == child_id && i.2 == maybe_offloaded;
            removed |= remove;
            !remove
        });
        removed
    }

    pub(super) fn remove_child_not_offloaded(&mut self, child_id: TimelineId) -> bool {
        self.remove_child_maybe_offloaded(child_id, MaybeOffloaded::No)
    }

    pub(super) fn remove_child_offloaded(&mut self, child_id: TimelineId) -> bool {
        self.remove_child_maybe_offloaded(child_id, MaybeOffloaded::Yes)
    }
    pub(crate) fn lsn_covered_by_lease(&self, lsn: Lsn) -> bool {
        self.leases.contains_key(&lsn)
    }
}

/// The `GcInfo` component describing which Lsns need to be retained.  Functionally, this
/// is a single number (the oldest LSN which we must retain), but it internally distinguishes
/// between time-based and space-based retention for observability and consumption metrics purposes.
#[derive(Clone, Debug, Default)]
pub(crate) struct GcCutoffs {
    /// Calculated from the [`pageserver_api::models::TenantConfig::gc_horizon`], this LSN indicates how much
    /// history we must keep to retain a specified number of bytes of WAL.
    pub(crate) space: Lsn,

    /// Calculated from [`pageserver_api::models::TenantConfig::pitr_interval`], this LSN indicates
    /// how much history we must keep to enable reading back at least the PITR interval duration.
    ///
    /// None indicates that the PITR cutoff has not been computed. A PITR interval of 0 will yield
    /// Some(last_record_lsn).
    pub(crate) time: Option<Lsn>,
}

impl GcCutoffs {
    fn select_min(&self) -> Lsn {
        // NB: if we haven't computed the PITR cutoff yet, we can't GC anything.
        self.space.min(self.time.unwrap_or_default())
    }
}

pub(crate) struct TimelineVisitOutcome {
    completed_keyspace: KeySpace,
    image_covered_keyspace: KeySpace,
}

/// An error happened in a get() operation.
#[derive(thiserror::Error, Debug)]
pub(crate) enum PageReconstructError {
    #[error(transparent)]
    Other(anyhow::Error),

    #[error("Ancestor LSN wait error: {0}")]
    AncestorLsnTimeout(WaitLsnError),

    #[error("timeline shutting down")]
    Cancelled,

    /// An error happened replaying WAL records
    #[error(transparent)]
    WalRedo(anyhow::Error),

    #[error("{0}")]
    MissingKey(Box<MissingKeyError>),
}

impl From<anyhow::Error> for PageReconstructError {
    fn from(value: anyhow::Error) -> Self {
        // with walingest.rs many PageReconstructError are wrapped in as anyhow::Error
        match value.downcast::<PageReconstructError>() {
            Ok(pre) => pre,
            Err(other) => PageReconstructError::Other(other),
        }
    }
}

impl From<utils::bin_ser::DeserializeError> for PageReconstructError {
    fn from(value: utils::bin_ser::DeserializeError) -> Self {
        PageReconstructError::Other(anyhow::Error::new(value).context("deserialization failure"))
    }
}

impl From<layer_manager::Shutdown> for PageReconstructError {
    fn from(_: layer_manager::Shutdown) -> Self {
        PageReconstructError::Cancelled
    }
}

impl GetVectoredError {
    #[cfg(test)]
    pub(crate) fn is_missing_key_error(&self) -> bool {
        matches!(self, Self::MissingKey(_))
    }
}

impl From<layer_manager::Shutdown> for GetVectoredError {
    fn from(_: layer_manager::Shutdown) -> Self {
        GetVectoredError::Cancelled
    }
}

/// A layer identifier when used in the [`ReadPath`] structure. This enum is for observability purposes
/// only and not used by the "real read path".
pub enum ReadPathLayerId {
    PersistentLayer(PersistentLayerKey),
    InMemoryLayer(Range<Lsn>),
}

impl std::fmt::Display for ReadPathLayerId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ReadPathLayerId::PersistentLayer(key) => write!(f, "{key}"),
            ReadPathLayerId::InMemoryLayer(range) => {
                write!(f, "in-mem {}..{}", range.start, range.end)
            }
        }
    }
}
pub struct ReadPath {
    keyspace: KeySpace,
    lsn: Lsn,
    path: Vec<(ReadPathLayerId, KeySpace, Range<Lsn>)>,
}

impl ReadPath {
    pub fn new(keyspace: KeySpace, lsn: Lsn) -> Self {
        Self {
            keyspace,
            lsn,
            path: Vec::new(),
        }
    }

    pub fn record_layer_visit(
        &mut self,
        layer_to_read: &ReadableLayer,
        keyspace_to_read: &KeySpace,
        lsn_range: &Range<Lsn>,
    ) {
        let id = match layer_to_read {
            ReadableLayer::PersistentLayer(layer) => {
                ReadPathLayerId::PersistentLayer(layer.layer_desc().key())
            }
            ReadableLayer::InMemoryLayer(layer) => {
                ReadPathLayerId::InMemoryLayer(layer.get_lsn_range())
            }
        };
        self.path
            .push((id, keyspace_to_read.clone(), lsn_range.clone()));
    }
}

impl std::fmt::Display for ReadPath {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "Read path for {} at lsn {}:", self.keyspace, self.lsn)?;
        for (idx, (layer_id, keyspace, lsn_range)) in self.path.iter().enumerate() {
            writeln!(
                f,
                "{}: {} {}..{} {}",
                idx, layer_id, lsn_range.start, lsn_range.end, keyspace
            )?;
        }
        Ok(())
    }
}

#[derive(thiserror::Error)]
pub struct MissingKeyError {
    keyspace: KeySpace,
    shard: ShardNumber,
    query: Option<VersionedKeySpaceQuery>,
    // This is largest request LSN from the get page request batch
    original_hwm_lsn: Lsn,
    ancestor_lsn: Option<Lsn>,
    /// Debug information about the read path if there's an error
    read_path: Option<ReadPath>,
    backtrace: Option<std::backtrace::Backtrace>,
}

impl MissingKeyError {
    fn enrich(&mut self, query: VersionedKeySpaceQuery) {
        self.query = Some(query);
    }
}

impl std::fmt::Debug for MissingKeyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self}")
    }
}

impl std::fmt::Display for MissingKeyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "could not find data for key {} (shard {:?}), original HWM LSN {}",
            self.keyspace, self.shard, self.original_hwm_lsn
        )?;

        if let Some(ref ancestor_lsn) = self.ancestor_lsn {
            write!(f, ", ancestor {ancestor_lsn}")?;
        }

        if let Some(ref query) = self.query {
            write!(f, ", query {query}")?;
        }

        if let Some(ref read_path) = self.read_path {
            write!(f, "\n{read_path}")?;
        }

        if let Some(ref backtrace) = self.backtrace {
            write!(f, "\n{backtrace}")?;
        }

        Ok(())
    }
}

impl PageReconstructError {
    /// Returns true if this error indicates a tenant/timeline shutdown alike situation
    pub(crate) fn is_stopping(&self) -> bool {
        use PageReconstructError::*;
        match self {
            Cancelled => true,
            Other(_) | AncestorLsnTimeout(_) | WalRedo(_) | MissingKey(_) => false,
        }
    }
}

#[derive(thiserror::Error, Debug)]
pub(crate) enum CreateImageLayersError {
    #[error("timeline shutting down")]
    Cancelled,

    #[error("read failed")]
    GetVectoredError(#[source] GetVectoredError),

    #[error("reconstruction failed")]
    PageReconstructError(#[source] PageReconstructError),

    #[error(transparent)]
    Other(anyhow::Error),
}

impl From<layer_manager::Shutdown> for CreateImageLayersError {
    fn from(_: layer_manager::Shutdown) -> Self {
        CreateImageLayersError::Cancelled
    }
}

#[derive(thiserror::Error, Debug, Clone)]
pub(crate) enum FlushLayerError {
    /// Timeline cancellation token was cancelled
    #[error("timeline shutting down")]
    Cancelled,

    /// We tried to flush a layer while the Timeline is in an unexpected state
    #[error("cannot flush frozen layers when flush_loop is not running, state is {0:?}")]
    NotRunning(FlushLoopState),

    // Arc<> the following non-clonable error types: we must be Clone-able because the flush error is propagated from the flush
    // loop via a watch channel, where we can only borrow it.
    #[error("create image layers (shared)")]
    CreateImageLayersError(Arc<CreateImageLayersError>),

    #[error("other (shared)")]
    Other(#[from] Arc<anyhow::Error>),
}

impl FlushLayerError {
    // When crossing from generic anyhow errors to this error type, we explicitly check
    // for timeline cancellation to avoid logging inoffensive shutdown errors as warn/err.
    fn from_anyhow(timeline: &Timeline, err: anyhow::Error) -> Self {
        let cancelled = timeline.cancel.is_cancelled()
            // The upload queue might have been shut down before the official cancellation of the timeline.
            || err
                .downcast_ref::<NotInitialized>()
                .map(NotInitialized::is_stopping)
                .unwrap_or_default();
        if cancelled {
            Self::Cancelled
        } else {
            Self::Other(Arc::new(err))
        }
    }
}

impl From<layer_manager::Shutdown> for FlushLayerError {
    fn from(_: layer_manager::Shutdown) -> Self {
        FlushLayerError::Cancelled
    }
}

#[derive(thiserror::Error, Debug)]
pub enum GetVectoredError {
    #[error("timeline shutting down")]
    Cancelled,

    #[error("requested too many keys: {0} > {1}")]
    Oversized(u64, u64),

    #[error("requested at invalid LSN: {0}")]
    InvalidLsn(Lsn),

    #[error("requested key not found: {0}")]
    MissingKey(Box<MissingKeyError>),

    #[error("ancestry walk")]
    GetReadyAncestorError(#[source] GetReadyAncestorError),

    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

impl From<GetReadyAncestorError> for GetVectoredError {
    fn from(value: GetReadyAncestorError) -> Self {
        use GetReadyAncestorError::*;
        match value {
            Cancelled => GetVectoredError::Cancelled,
            AncestorLsnTimeout(_) | BadState { .. } => {
                GetVectoredError::GetReadyAncestorError(value)
            }
        }
    }
}

#[derive(thiserror::Error, Debug)]
pub enum GetReadyAncestorError {
    #[error("ancestor LSN wait error")]
    AncestorLsnTimeout(#[from] WaitLsnError),

    #[error("bad state on timeline {timeline_id}: {state:?}")]
    BadState {
        timeline_id: TimelineId,
        state: TimelineState,
    },

    #[error("cancelled")]
    Cancelled,
}

#[derive(Clone, Copy)]
pub enum LogicalSizeCalculationCause {
    Initial,
    ConsumptionMetricsSyntheticSize,
    EvictionTaskImitation,
    TenantSizeHandler,
}

pub enum GetLogicalSizePriority {
    User,
    Background,
}

#[derive(Debug, enumset::EnumSetType)]
pub(crate) enum CompactFlags {
    ForceRepartition,
    ForceImageLayerCreation,
    ForceL0Compaction,
    OnlyL0Compaction,
    EnhancedGcBottomMostCompaction,
    DryRun,
    /// Makes image compaction yield if there's pending L0 compaction. This should always be used in
    /// the background compaction task, since we want to aggressively compact down L0 to bound
    /// read amplification.
    ///
    /// It only makes sense to use this when `compaction_l0_first` is enabled (such that we yield to
    /// an L0 compaction pass), and without `OnlyL0Compaction` (L0 compaction shouldn't yield for L0
    /// compaction).
    YieldForL0,
}

#[serde_with::serde_as]
#[derive(Debug, Clone, serde::Deserialize)]
pub(crate) struct CompactRequest {
    pub compact_key_range: Option<CompactKeyRange>,
    pub compact_lsn_range: Option<CompactLsnRange>,
    /// Whether the compaction job should be scheduled.
    #[serde(default)]
    pub scheduled: bool,
    /// Whether the compaction job should be split across key ranges.
    #[serde(default)]
    pub sub_compaction: bool,
    /// Max job size for each subcompaction job.
    pub sub_compaction_max_job_size_mb: Option<u64>,
}

#[derive(Debug, Clone, serde::Deserialize)]
pub(crate) struct MarkInvisibleRequest {
    #[serde(default)]
    pub is_visible: Option<bool>,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct CompactOptions {
    pub flags: EnumSet<CompactFlags>,
    /// If set, the compaction will only compact the key range specified by this option.
    /// This option is only used by GC compaction. For the full explanation, see [`compaction::GcCompactJob`].
    pub compact_key_range: Option<CompactKeyRange>,
    /// If set, the compaction will only compact the LSN within this value.
    /// This option is only used by GC compaction. For the full explanation, see [`compaction::GcCompactJob`].
    pub compact_lsn_range: Option<CompactLsnRange>,
    /// Enable sub-compaction (split compaction job across key ranges).
    /// This option is only used by GC compaction.
    pub sub_compaction: bool,
    /// Set job size for the GC compaction.
    /// This option is only used by GC compaction.
    pub sub_compaction_max_job_size_mb: Option<u64>,
}

impl std::fmt::Debug for Timeline {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "Timeline<{}>", self.timeline_id)
    }
}

#[derive(thiserror::Error, Debug, Clone)]
pub enum WaitLsnError {
    // Called on a timeline which is shutting down
    #[error("Shutdown")]
    Shutdown,

    // Called on an timeline not in active state or shutting down
    #[error("Bad timeline state: {0:?}")]
    BadState(TimelineState),

    // Timeout expired while waiting for LSN to catch up with goal.
    #[error("{0}")]
    Timeout(String),
}

impl From<WaitLsnError> for tonic::Status {
    fn from(err: WaitLsnError) -> Self {
        use tonic::Code;
        let code = match &err {
            WaitLsnError::Timeout(_) => Code::Internal,
            WaitLsnError::BadState(_) => Code::Internal,
            WaitLsnError::Shutdown => Code::Unavailable,
        };
        tonic::Status::new(code, err.to_string())
    }
}

// The impls below achieve cancellation mapping for errors.
// Perhaps there's a way of achieving this with less cruft.

impl From<CreateImageLayersError> for CompactionError {
    fn from(e: CreateImageLayersError) -> Self {
        match e {
            CreateImageLayersError::Cancelled => CompactionError::ShuttingDown,
            CreateImageLayersError::Other(e) => {
                CompactionError::Other(e.context("create image layers"))
            }
            _ => CompactionError::Other(e.into()),
        }
    }
}

impl From<CreateImageLayersError> for FlushLayerError {
    fn from(e: CreateImageLayersError) -> Self {
        match e {
            CreateImageLayersError::Cancelled => FlushLayerError::Cancelled,
            any => FlushLayerError::CreateImageLayersError(Arc::new(any)),
        }
    }
}

impl From<PageReconstructError> for CreateImageLayersError {
    fn from(e: PageReconstructError) -> Self {
        match e {
            PageReconstructError::Cancelled => CreateImageLayersError::Cancelled,
            _ => CreateImageLayersError::PageReconstructError(e),
        }
    }
}

impl From<super::storage_layer::errors::PutError> for CreateImageLayersError {
    fn from(e: super::storage_layer::errors::PutError) -> Self {
        if e.is_cancel() {
            CreateImageLayersError::Cancelled
        } else {
            CreateImageLayersError::Other(e.into_anyhow())
        }
    }
}

impl From<GetVectoredError> for CreateImageLayersError {
    fn from(e: GetVectoredError) -> Self {
        match e {
            GetVectoredError::Cancelled => CreateImageLayersError::Cancelled,
            _ => CreateImageLayersError::GetVectoredError(e),
        }
    }
}

impl From<GetVectoredError> for PageReconstructError {
    fn from(e: GetVectoredError) -> Self {
        match e {
            GetVectoredError::Cancelled => PageReconstructError::Cancelled,
            GetVectoredError::InvalidLsn(_) => PageReconstructError::Other(anyhow!("Invalid LSN")),
            err @ GetVectoredError::Oversized(_, _) => PageReconstructError::Other(err.into()),
            GetVectoredError::MissingKey(err) => PageReconstructError::MissingKey(err),
            GetVectoredError::GetReadyAncestorError(err) => PageReconstructError::from(err),
            GetVectoredError::Other(err) => PageReconstructError::Other(err),
        }
    }
}

impl From<GetReadyAncestorError> for PageReconstructError {
    fn from(e: GetReadyAncestorError) -> Self {
        use GetReadyAncestorError::*;
        match e {
            AncestorLsnTimeout(wait_err) => PageReconstructError::AncestorLsnTimeout(wait_err),
            bad_state @ BadState { .. } => PageReconstructError::Other(anyhow::anyhow!(bad_state)),
            Cancelled => PageReconstructError::Cancelled,
        }
    }
}

pub(crate) enum WaitLsnTimeout {
    Custom(Duration),
    // Use the [`PageServerConf::wait_lsn_timeout`] default
    Default,
}

pub(crate) enum WaitLsnWaiter<'a> {
    Timeline(&'a Timeline),
    Tenant,
    PageService,
    HttpEndpoint,
    BaseBackupCache,
}

/// Argument to [`Timeline::shutdown`].
#[derive(Debug, Clone, Copy)]
pub(crate) enum ShutdownMode {
    /// Graceful shutdown, may do a lot of I/O as we flush any open layers to disk. This method can
    /// take multiple seconds for a busy timeline.
    ///
    /// While we are flushing, we continue to accept read I/O for LSNs ingested before
    /// the call to [`Timeline::shutdown`].
    FreezeAndFlush,
    /// Only flush the layers to the remote storage without freezing any open layers. Flush the deletion
    /// queue. This is the mode used by ancestor detach and any other operations that reloads a tenant
    /// but not increasing the generation number. Note that this mode cannot be used at tenant shutdown,
    /// as flushing the deletion queue at that time will cause shutdown-in-progress errors.
    Reload,
    /// Shut down immediately, without waiting for any open layers to flush.
    Hard,
}

#[allow(clippy::large_enum_variant, reason = "TODO")]
enum ImageLayerCreationOutcome {
    /// We generated an image layer
    Generated {
        unfinished_image_layer: ImageLayerWriter,
    },
    /// The key range is empty
    Empty,
    /// (Only used in metadata image layer creation), after reading the metadata keys, we decide to skip
    /// the image layer creation.
    Skip,
}

/// Public interface functions
impl Timeline {
    /// Get the LSN where this branch was created
    pub(crate) fn get_ancestor_lsn(&self) -> Lsn {
        self.ancestor_lsn
    }

    /// Get the ancestor's timeline id
    pub(crate) fn get_ancestor_timeline_id(&self) -> Option<TimelineId> {
        self.ancestor_timeline
            .as_ref()
            .map(|ancestor| ancestor.timeline_id)
    }

    /// Get the ancestor timeline
    pub(crate) fn ancestor_timeline(&self) -> Option<&Arc<Timeline>> {
        self.ancestor_timeline.as_ref()
    }

    /// Get the bytes written since the PITR cutoff on this branch, and
    /// whether this branch's ancestor_lsn is within its parent's PITR.
    pub(crate) fn get_pitr_history_stats(&self) -> (u64, bool) {
        // TODO: for backwards compatibility, we return the full history back to 0 when the PITR
        // cutoff has not yet been initialized. This should return None instead, but this is exposed
        // in external HTTP APIs and callers may not handle a null value.
        let gc_info = self.gc_info.read().unwrap();
        let history = self
            .get_last_record_lsn()
            .checked_sub(gc_info.cutoffs.time.unwrap_or_default())
            .unwrap_or_default()
            .0;
        (history, gc_info.within_ancestor_pitr)
    }

    /// Read timeline's GC cutoff: this is the LSN at which GC has started to happen
    pub(crate) fn get_applied_gc_cutoff_lsn(&self) -> RcuReadGuard<Lsn> {
        self.applied_gc_cutoff_lsn.read()
    }

    /// Read timeline's planned GC cutoff: this is the logical end of history that users are allowed
    /// to read (based on configured PITR), even if physically we have more history. Returns None
    /// if the PITR cutoff has not yet been initialized.
    pub(crate) fn get_gc_cutoff_lsn(&self) -> Option<Lsn> {
        self.gc_info.read().unwrap().cutoffs.time
    }

    /// Look up given page version.
    ///
    /// If a remote layer file is needed, it is downloaded as part of this
    /// call.
    ///
    /// This method enforces [`Self::pagestream_throttle`] internally.
    ///
    /// NOTE: It is considered an error to 'get' a key that doesn't exist. The
    /// abstraction above this needs to store suitable metadata to track what
    /// data exists with what keys, in separate metadata entries. If a
    /// non-existent key is requested, we may incorrectly return a value from
    /// an ancestor branch, for example, or waste a lot of cycles chasing the
    /// non-existing key.
    ///
    /// # Cancel-Safety
    ///
    /// This method is cancellation-safe.
    #[inline(always)]
    pub(crate) async fn get(
        &self,
        key: Key,
        lsn: Lsn,
        ctx: &RequestContext,
    ) -> Result<Bytes, PageReconstructError> {
        if !lsn.is_valid() {
            return Err(PageReconstructError::Other(anyhow::anyhow!("Invalid LSN")));
        }

        // This check is debug-only because of the cost of hashing, and because it's a double-check: we
        // already checked the key against the shard_identity when looking up the Timeline from
        // page_service.
        debug_assert!(!self.shard_identity.is_key_disposable(&key));

        let mut reconstruct_state = ValuesReconstructState::new(IoConcurrency::sequential());

        let query = VersionedKeySpaceQuery::uniform(KeySpace::single(key..key.next()), lsn);

        let vectored_res = self
            .get_vectored_impl(query, &mut reconstruct_state, ctx)
            .await;

        let key_value = vectored_res?.pop_first();
        match key_value {
            Some((got_key, value)) => {
                if got_key != key {
                    error!(
                        "Expected {}, but singular vectored get returned {}",
                        key, got_key
                    );
                    Err(PageReconstructError::Other(anyhow!(
                        "Singular vectored get returned wrong key"
                    )))
                } else {
                    value
                }
            }
            None => Err(PageReconstructError::MissingKey(Box::new(
                MissingKeyError {
                    keyspace: KeySpace::single(key..key.next()),
                    shard: self.shard_identity.get_shard_number(&key),
                    original_hwm_lsn: lsn,
                    ancestor_lsn: None,
                    backtrace: None,
                    read_path: None,
                    query: None,
                },
            ))),
        }
    }

    pub(crate) const LAYERS_VISITED_WARN_THRESHOLD: u32 = 100;

    /// Look up multiple page versions at a given LSN
    ///
    /// This naive implementation will be replaced with a more efficient one
    /// which actually vectorizes the read path.
    pub(crate) async fn get_vectored(
        &self,
        query: VersionedKeySpaceQuery,
        io_concurrency: super::storage_layer::IoConcurrency,
        ctx: &RequestContext,
    ) -> Result<BTreeMap<Key, Result<Bytes, PageReconstructError>>, GetVectoredError> {
        let total_keyspace = query.total_keyspace();

        let key_count = total_keyspace.total_raw_size();
        if key_count > self.conf.max_get_vectored_keys.get() {
            return Err(GetVectoredError::Oversized(
                key_count as u64,
                self.conf.max_get_vectored_keys.get() as u64,
            ));
        }

        for range in &total_keyspace.ranges {
            let mut key = range.start;
            while key != range.end {
                assert!(!self.shard_identity.is_key_disposable(&key));
                key = key.next();
            }
        }

        trace!(
            "get vectored query {} from task kind {:?}",
            query,
            ctx.task_kind(),
        );

        let start = crate::metrics::GET_VECTORED_LATENCY
            .for_task_kind(ctx.task_kind())
            .map(|metric| (metric, Instant::now()));

        let res = self
            .get_vectored_impl(query, &mut ValuesReconstructState::new(io_concurrency), ctx)
            .await;

        if let Some((metric, start)) = start {
            let elapsed = start.elapsed();
            metric.observe(elapsed.as_secs_f64());
        }

        res
    }

    /// Scan the keyspace and return all existing key-values in the keyspace. This currently uses vectored
    /// get underlying. Normal vectored get would throw an error when a key in the keyspace is not found
    /// during the search, but for the scan interface, it returns all existing key-value pairs, and does
    /// not expect each single key in the key space will be found. The semantics is closer to the RocksDB
    /// scan iterator interface. We could optimize this interface later to avoid some checks in the vectored
    /// get path to maintain and split the probing and to-be-probe keyspace. We also need to ensure that
    /// the scan operation will not cause OOM in the future.
    pub(crate) async fn scan(
        &self,
        keyspace: KeySpace,
        lsn: Lsn,
        ctx: &RequestContext,
        io_concurrency: super::storage_layer::IoConcurrency,
    ) -> Result<BTreeMap<Key, Result<Bytes, PageReconstructError>>, GetVectoredError> {
        if !lsn.is_valid() {
            return Err(GetVectoredError::InvalidLsn(lsn));
        }

        trace!(
            "key-value scan request for {:?}@{} from task kind {:?}",
            keyspace,
            lsn,
            ctx.task_kind()
        );

        // We should generalize this into Keyspace::contains in the future.
        for range in &keyspace.ranges {
            if range.start.field1 < METADATA_KEY_BEGIN_PREFIX
                || range.end.field1 > METADATA_KEY_END_PREFIX
            {
                return Err(GetVectoredError::Other(anyhow::anyhow!(
                    "only metadata keyspace can be scanned"
                )));
            }
        }

        let start = crate::metrics::SCAN_LATENCY
            .for_task_kind(ctx.task_kind())
            .map(ScanLatencyOngoingRecording::start_recording);

        let query = VersionedKeySpaceQuery::uniform(keyspace, lsn);

        let vectored_res = self
            .get_vectored_impl(query, &mut ValuesReconstructState::new(io_concurrency), ctx)
            .await;

        if let Some(recording) = start {
            recording.observe();
        }

        vectored_res
    }

    pub(super) async fn get_vectored_impl(
        &self,
        query: VersionedKeySpaceQuery,
        reconstruct_state: &mut ValuesReconstructState,
        ctx: &RequestContext,
    ) -> Result<BTreeMap<Key, Result<Bytes, PageReconstructError>>, GetVectoredError> {
        if query.is_empty() {
            return Ok(BTreeMap::default());
        }

        let read_path = if self.conf.enable_read_path_debugging || ctx.read_path_debug() {
            Some(ReadPath::new(
                query.total_keyspace(),
                query.high_watermark_lsn()?,
            ))
        } else {
            None
        };

        reconstruct_state.read_path = read_path;

        let redo_attempt_type = if ctx.task_kind() == TaskKind::Compaction {
            RedoAttemptType::LegacyCompaction
        } else {
            RedoAttemptType::ReadPage
        };

        let traversal_res: Result<(), _> = {
            let ctx = RequestContextBuilder::from(ctx)
                .perf_span(|crnt_perf_span| {
                    info_span!(
                        target: PERF_TRACE_TARGET,
                        parent: crnt_perf_span,
                        "PLAN_IO",
                    )
                })
                .attached_child();

            self.get_vectored_reconstruct_data(query.clone(), reconstruct_state, &ctx)
                .maybe_perf_instrument(&ctx, |crnt_perf_span| crnt_perf_span.clone())
                .await
        };

        if let Err(err) = traversal_res {
            // Wait for all the spawned IOs to complete.
            // See comments on `spawn_io` inside `storage_layer` for more details.
            let mut collect_futs = std::mem::take(&mut reconstruct_state.keys)
                .into_values()
                .map(|state| state.collect_pending_ios())
                .collect::<FuturesUnordered<_>>();
            while collect_futs.next().await.is_some() {}

            // Enrich the missing key error with the original query.
            if let GetVectoredError::MissingKey(mut missing_err) = err {
                missing_err.enrich(query.clone());
                return Err(GetVectoredError::MissingKey(missing_err));
            }

            return Err(err);
        };

        let layers_visited = reconstruct_state.get_layers_visited();

        let ctx = RequestContextBuilder::from(ctx)
            .perf_span(|crnt_perf_span| {
                info_span!(
                    target: PERF_TRACE_TARGET,
                    parent: crnt_perf_span,
                    "RECONSTRUCT",
                )
            })
            .attached_child();

        let futs = FuturesUnordered::new();
        for (key, state) in std::mem::take(&mut reconstruct_state.keys) {
            let req_lsn_for_key = query.map_key_to_lsn(&key);

            futs.push({
                let walredo_self = self.myself.upgrade().expect("&self method holds the arc");
                let ctx = RequestContextBuilder::from(&ctx)
                    .perf_span(|crnt_perf_span| {
                        info_span!(
                            target: PERF_TRACE_TARGET,
                            parent: crnt_perf_span,
                            "RECONSTRUCT_KEY",
                            key = %key,
                        )
                    })
                    .attached_child();

                async move {
                    assert_eq!(state.situation, ValueReconstructSituation::Complete);

                    let res = state
                        .collect_pending_ios()
                        .maybe_perf_instrument(&ctx, |crnt_perf_span| {
                            info_span!(
                                target: PERF_TRACE_TARGET,
                                parent: crnt_perf_span,
                                "WAIT_FOR_IO_COMPLETIONS",
                            )
                        })
                        .await;

                    let converted = match res {
                        Ok(ok) => ok,
                        Err(err) => {
                            return (key, Err(err));
                        }
                    };
                    DELTAS_PER_READ_GLOBAL.observe(converted.num_deltas() as f64);

                    // The walredo module expects the records to be descending in terms of Lsn.
                    // And we submit the IOs in that order, so, there shuold be no need to sort here.
                    debug_assert!(
                        converted
                            .records
                            .is_sorted_by_key(|(lsn, _)| std::cmp::Reverse(*lsn)),
                        "{converted:?}"
                    );

                    let walredo_deltas = converted.num_deltas();
                    let walredo_res = walredo_self
                        .reconstruct_value(key, req_lsn_for_key, converted, redo_attempt_type)
                        .maybe_perf_instrument(&ctx, |crnt_perf_span| {
                            info_span!(
                                target: PERF_TRACE_TARGET,
                                parent: crnt_perf_span,
                                "WALREDO",
                                deltas = %walredo_deltas,
                            )
                        })
                        .await;

                    (key, walredo_res)
                }
            });
        }

        let results = futs
            .collect::<BTreeMap<Key, Result<Bytes, PageReconstructError>>>()
            .maybe_perf_instrument(&ctx, |crnt_perf_span| crnt_perf_span.clone())
            .await;

        // For aux file keys (v1 or v2) the vectored read path does not return an error
        // when they're missing. Instead they are omitted from the resulting btree
        // (this is a requirement, not a bug). Skip updating the metric in these cases
        // to avoid infinite results.
        if !results.is_empty() {
            if layers_visited >= Self::LAYERS_VISITED_WARN_THRESHOLD {
                let total_keyspace = query.total_keyspace();
                let max_request_lsn = query.high_watermark_lsn().expect("Validated previously");

                static LOG_PACER: Lazy<Mutex<RateLimit>> =
                    Lazy::new(|| Mutex::new(RateLimit::new(Duration::from_secs(60))));
                LOG_PACER.lock().unwrap().call(|| {
                    let num_keys = total_keyspace.total_raw_size();
                    let num_pages = results.len();
                    tracing::info!(
                      shard_id = %self.tenant_shard_id.shard_slug(),
                      lsn = %max_request_lsn,
                      "Vectored read for {total_keyspace} visited {layers_visited} layers. Returned {num_pages}/{num_keys} pages.",
                    );
                });
            }

            // Records the number of layers visited in a few different ways:
            //
            // * LAYERS_PER_READ: all layers count towards every read in the batch, because each
            //   layer directly affects its observed latency.
            //
            // * LAYERS_PER_READ_BATCH: all layers count towards each batch, to get the per-batch
            //   layer visits and access cost.
            //
            // * LAYERS_PER_READ_AMORTIZED: the average layer count per read, to get the amortized
            //   read amplification after batching.
            let layers_visited = layers_visited as f64;
            let avg_layers_visited = layers_visited / results.len() as f64;
            LAYERS_PER_READ_BATCH_GLOBAL.observe(layers_visited);
            for _ in &results {
                self.metrics.layers_per_read.observe(layers_visited);
                LAYERS_PER_READ_GLOBAL.observe(layers_visited);
                LAYERS_PER_READ_AMORTIZED_GLOBAL.observe(avg_layers_visited);
            }
        }

        Ok(results)
    }

    /// Get last or prev record separately. Same as get_last_record_rlsn().last/prev.
    pub(crate) fn get_last_record_lsn(&self) -> Lsn {
        self.last_record_lsn.load().last
    }

    pub(crate) fn get_prev_record_lsn(&self) -> Lsn {
        self.last_record_lsn.load().prev
    }

    /// Atomically get both last and prev.
    pub(crate) fn get_last_record_rlsn(&self) -> RecordLsn {
        self.last_record_lsn.load()
    }

    /// Subscribe to callers of wait_lsn(). The value of the channel is None if there are no
    /// wait_lsn() calls in progress, and Some(Lsn) if there is an active waiter for wait_lsn().
    pub(crate) fn subscribe_for_wait_lsn_updates(&self) -> watch::Receiver<Option<Lsn>> {
        self.last_record_lsn.status_receiver()
    }

    pub(crate) fn get_disk_consistent_lsn(&self) -> Lsn {
        self.disk_consistent_lsn.load()
    }

    /// remote_consistent_lsn from the perspective of the tenant's current generation,
    /// not validated with control plane yet.
    /// See [`Self::get_remote_consistent_lsn_visible`].
    pub(crate) fn get_remote_consistent_lsn_projected(&self) -> Option<Lsn> {
        self.remote_client.remote_consistent_lsn_projected()
    }

    /// remote_consistent_lsn which the tenant is guaranteed not to go backward from,
    /// i.e. a value of remote_consistent_lsn_projected which has undergone
    /// generation validation in the deletion queue.
    pub(crate) fn get_remote_consistent_lsn_visible(&self) -> Option<Lsn> {
        self.remote_client.remote_consistent_lsn_visible()
    }

    /// The sum of the file size of all historic layers in the layer map.
    /// This method makes no distinction between local and remote layers.
    /// Hence, the result **does not represent local filesystem usage**.
    pub(crate) async fn layer_size_sum(&self) -> u64 {
        let guard = self
            .layers
            .read(LayerManagerLockHolder::GetLayerMapInfo)
            .await;
        guard.layer_size_sum()
    }

    pub(crate) fn resident_physical_size(&self) -> u64 {
        self.metrics.resident_physical_size_get()
    }

    pub(crate) fn get_directory_metrics(&self) -> [u64; DirectoryKind::KINDS_NUM] {
        array::from_fn(|idx| self.directory_metrics[idx].load(AtomicOrdering::Relaxed))
    }

    ///
    /// Wait until WAL has been received and processed up to this LSN.
    ///
    /// You should call this before any of the other get_* or list_* functions. Calling
    /// those functions with an LSN that has been processed yet is an error.
    ///
    pub(crate) async fn wait_lsn(
        &self,
        lsn: Lsn,
        who_is_waiting: WaitLsnWaiter<'_>,
        timeout: WaitLsnTimeout,
        ctx: &RequestContext, /* Prepare for use by cancellation */
    ) -> Result<(), WaitLsnError> {
        let state = self.current_state();
        if self.cancel.is_cancelled() || matches!(state, TimelineState::Stopping) {
            return Err(WaitLsnError::Shutdown);
        } else if !matches!(state, TimelineState::Active) {
            return Err(WaitLsnError::BadState(state));
        }

        if cfg!(debug_assertions) {
            match ctx.task_kind() {
                TaskKind::WalReceiverManager
                | TaskKind::WalReceiverConnectionHandler
                | TaskKind::WalReceiverConnectionPoller => {
                    let is_myself = match who_is_waiting {
                        WaitLsnWaiter::Timeline(waiter) => {
                            Weak::ptr_eq(&waiter.myself, &self.myself)
                        }
                        WaitLsnWaiter::Tenant
                        | WaitLsnWaiter::PageService
                        | WaitLsnWaiter::HttpEndpoint
                        | WaitLsnWaiter::BaseBackupCache => unreachable!(
                            "tenant or page_service context are not expected to have task kind {:?}",
                            ctx.task_kind()
                        ),
                    };
                    if is_myself {
                        if let Err(current) = self.last_record_lsn.would_wait_for(lsn) {
                            // walingest is the only one that can advance last_record_lsn; it should make sure to never reach here
                            panic!(
                                "this timeline's walingest task is calling wait_lsn({lsn}) but we only have last_record_lsn={current}; would deadlock"
                            );
                        }
                    } else {
                        // if another  timeline's  is waiting for us, there's no deadlock risk because
                        // our walreceiver task can make progress independent of theirs
                    }
                }
                _ => {}
            }
        }

        let timeout = match timeout {
            WaitLsnTimeout::Custom(t) => t,
            WaitLsnTimeout::Default => self.conf.wait_lsn_timeout,
        };

        let timer = crate::metrics::WAIT_LSN_TIME.start_timer();
        let start_finish_counterpair_guard = self.metrics.wait_lsn_start_finish_counterpair.guard();

        let wait_for_timeout = self.last_record_lsn.wait_for_timeout(lsn, timeout);
        let wait_for_timeout = std::pin::pin!(wait_for_timeout);
        // Use threshold of 1 because even 1 second of wait for ingest is very much abnormal.
        let log_slow_threshold = Duration::from_secs(1);
        // Use period of 10 to avoid flooding logs during an outage that affects all timelines.
        let log_slow_period = Duration::from_secs(10);
        let mut logging_permit = None;
        let wait_for_timeout = monitor_slow_future(
            log_slow_threshold,
            log_slow_period,
            wait_for_timeout,
            |MonitorSlowFutureCallback {
                 ready,
                 is_slow,
                 elapsed_total,
                 elapsed_since_last_callback,
             }| {
                self.metrics
                    .wait_lsn_in_progress_micros
                    .inc_by(u64::try_from(elapsed_since_last_callback.as_micros()).unwrap());
                if !is_slow {
                    return;
                }
                // It's slow, see if we should log it.
                // (We limit the logging to one per invocation per timeline to avoid excessive
                // logging during an extended broker / networking outage that affects all timelines.)
                if logging_permit.is_none() {
                    logging_permit = self.wait_lsn_log_slow.try_acquire().ok();
                }
                if logging_permit.is_none() {
                    return;
                }
                // We log it.
                if ready {
                    info!(
                        "slow wait_lsn completed after {:.3}s",
                        elapsed_total.as_secs_f64()
                    );
                } else {
                    info!(
                        "slow wait_lsn still running for {:.3}s",
                        elapsed_total.as_secs_f64()
                    );
                }
            },
        );
        let res = wait_for_timeout.await;
        // don't count the time spent waiting for lock below, and also in walreceiver.status(), towards the wait_lsn_time_histo
        drop(logging_permit);
        drop(start_finish_counterpair_guard);
        drop(timer);
        match res {
            Ok(()) => Ok(()),
            Err(e) => {
                use utils::seqwait::SeqWaitError::*;
                match e {
                    Shutdown => Err(WaitLsnError::Shutdown),
                    Timeout => {
                        let walreceiver_status = self.walreceiver_status();
                        Err(WaitLsnError::Timeout(format!(
                            "Timed out while waiting for WAL record at LSN {} to arrive, last_record_lsn {} disk consistent LSN={}, WalReceiver status: {}",
                            lsn,
                            self.get_last_record_lsn(),
                            self.get_disk_consistent_lsn(),
                            walreceiver_status,
                        )))
                    }
                }
            }
        }
    }

    pub(crate) fn walreceiver_status(&self) -> String {
        match &*self.walreceiver.lock().unwrap() {
            None => "stopping or stopped".to_string(),
            Some(walreceiver) => match walreceiver.status() {
                Some(status) => status.to_human_readable_string(),
                None => "Not active".to_string(),
            },
        }
    }

    /// Check that it is valid to request operations with that lsn.
    pub(crate) fn check_lsn_is_in_scope(
        &self,
        lsn: Lsn,
        latest_gc_cutoff_lsn: &RcuReadGuard<Lsn>,
    ) -> anyhow::Result<()> {
        ensure!(
            lsn >= **latest_gc_cutoff_lsn,
            "LSN {} is earlier than latest GC cutoff {} (we might've already garbage collected needed data)",
            lsn,
            **latest_gc_cutoff_lsn,
        );
        Ok(())
    }

    /// Initializes an LSN lease. The function will return an error if the requested LSN is less than the `latest_gc_cutoff_lsn`.
    pub(crate) fn init_lsn_lease(
        &self,
        lsn: Lsn,
        length: Duration,
        ctx: &RequestContext,
    ) -> anyhow::Result<LsnLease> {
        self.make_lsn_lease(lsn, length, true, ctx)
    }

    /// Renews a lease at a particular LSN. The requested LSN is not validated against the `latest_gc_cutoff_lsn` when we are in the grace period.
    pub(crate) fn renew_lsn_lease(
        &self,
        lsn: Lsn,
        length: Duration,
        ctx: &RequestContext,
    ) -> anyhow::Result<LsnLease> {
        self.make_lsn_lease(lsn, length, false, ctx)
    }

    /// Obtains a temporary lease blocking garbage collection for the given LSN.
    ///
    /// If we are in `AttachedSingle` mode and is not blocked by the lsn lease deadline, this function will error
    /// if the requesting LSN is less than the `latest_gc_cutoff_lsn` and there is no existing request present.
    ///
    /// If there is an existing lease in the map, the lease will be renewed only if the request extends the lease.
    /// The returned lease is therefore the maximum between the existing lease and the requesting lease.
    fn make_lsn_lease(
        &self,
        lsn: Lsn,
        length: Duration,
        init: bool,
        _ctx: &RequestContext,
    ) -> anyhow::Result<LsnLease> {
        let lease = {
            // Normalize the requested LSN to be aligned, and move to the first record
            // if it points to the beginning of the page (header).
            let lsn = xlog_utils::normalize_lsn(lsn, WAL_SEGMENT_SIZE);

            let mut gc_info = self.gc_info.write().unwrap();
            let planned_cutoff = gc_info.min_cutoff();

            let valid_until = SystemTime::now() + length;

            let entry = gc_info.leases.entry(lsn);

            match entry {
                Entry::Occupied(mut occupied) => {
                    let existing_lease = occupied.get_mut();
                    if valid_until > existing_lease.valid_until {
                        existing_lease.valid_until = valid_until;
                        let dt: DateTime<Utc> = valid_until.into();
                        info!("lease extended to {}", dt);
                    } else {
                        let dt: DateTime<Utc> = existing_lease.valid_until.into();
                        info!("existing lease covers greater length, valid until {}", dt);
                    }

                    existing_lease.clone()
                }
                Entry::Vacant(vacant) => {
                    // Never allow a lease to be requested for an LSN below the applied GC cutoff. The data could have been deleted.
                    let latest_gc_cutoff_lsn = self.get_applied_gc_cutoff_lsn();
                    if lsn < *latest_gc_cutoff_lsn {
                        bail!(
                            "tried to request an lsn lease for an lsn below the latest gc cutoff. requested at {} gc cutoff {}",
                            lsn,
                            *latest_gc_cutoff_lsn
                        );
                    }

                    // We allow create lease for those below the planned gc cutoff if we are still within the grace period
                    // of GC blocking.
                    let validate = {
                        let conf = self.tenant_conf.load();
                        !conf.is_gc_blocked_by_lsn_lease_deadline()
                    };

                    // Do not allow initial lease creation to be below the planned gc cutoff. The client (compute_ctl) determines
                    // whether it is a initial lease creation or a renewal.
                    if (init || validate) && lsn < planned_cutoff {
                        bail!(
                            "tried to request an lsn lease for an lsn below the planned gc cutoff. requested at {} planned gc cutoff {}",
                            lsn,
                            planned_cutoff
                        );
                    }

                    let dt: DateTime<Utc> = valid_until.into();
                    info!("lease created, valid until {}", dt);
                    vacant.insert(LsnLease { valid_until }).clone()
                }
            }
        };

        Ok(lease)
    }

    /// Freeze the current open in-memory layer. It will be written to disk on next iteration.
    /// Returns the flush request ID which can be awaited with wait_flush_completion().
    #[instrument(skip(self), fields(tenant_id=%self.tenant_shard_id.tenant_id, shard_id=%self.tenant_shard_id.shard_slug(), timeline_id=%self.timeline_id))]
    pub(crate) async fn freeze(&self) -> Result<u64, FlushLayerError> {
        self.freeze0().await
    }

    /// Freeze and flush the open in-memory layer, waiting for it to be written to disk.
    #[instrument(skip(self), fields(tenant_id=%self.tenant_shard_id.tenant_id, shard_id=%self.tenant_shard_id.shard_slug(), timeline_id=%self.timeline_id))]
    pub(crate) async fn freeze_and_flush(&self) -> Result<(), FlushLayerError> {
        self.freeze_and_flush0().await
    }

    /// Freeze the current open in-memory layer. It will be written to disk on next iteration.
    /// Returns the flush request ID which can be awaited with wait_flush_completion().
    pub(crate) async fn freeze0(&self) -> Result<u64, FlushLayerError> {
        let mut g = self.write_lock.lock().await;
        let to_lsn = self.get_last_record_lsn();
        self.freeze_inmem_layer_at(to_lsn, &mut g).await
    }

    // This exists to provide a non-span creating version of `freeze_and_flush` we can call without
    // polluting the span hierarchy.
    pub(crate) async fn freeze_and_flush0(&self) -> Result<(), FlushLayerError> {
        let token = self.freeze0().await?;
        self.wait_flush_completion(token).await
    }

    // Check if an open ephemeral layer should be closed: this provides
    // background enforcement of checkpoint interval if there is no active WAL receiver, to avoid keeping
    // an ephemeral layer open forever when idle.  It also freezes layers if the global limit on
    // ephemeral layer bytes has been breached.
    pub(super) async fn maybe_freeze_ephemeral_layer(&self) {
        let Ok(mut write_guard) = self.write_lock.try_lock() else {
            // If the write lock is held, there is an active wal receiver: rolling open layers
            // is their responsibility while they hold this lock.
            return;
        };

        // FIXME: why not early exit? because before #7927 the state would had been cleared every
        // time, and this was missed.
        // if write_guard.is_none() { return; }

        let Ok(layers_guard) = self.layers.try_read(LayerManagerLockHolder::TryFreezeLayer) else {
            // Don't block if the layer lock is busy
            return;
        };

        let Ok(lm) = layers_guard.layer_map() else {
            return;
        };

        let Some(open_layer) = &lm.open_layer else {
            // If there is no open layer, we have no layer freezing to do.  However, we might need to generate
            // some updates to disk_consistent_lsn and remote_consistent_lsn, in case we ingested some WAL regions
            // that didn't result in writes to this shard.

            // Must not hold the layers lock while waiting for a flush.
            drop(layers_guard);

            let last_record_lsn = self.get_last_record_lsn();
            let disk_consistent_lsn = self.get_disk_consistent_lsn();
            if last_record_lsn > disk_consistent_lsn {
                // We have no open layer, but disk_consistent_lsn is behind the last record: this indicates
                // we are a sharded tenant and have skipped some WAL
                let last_freeze_ts = *self.last_freeze_ts.read().unwrap();
                if last_freeze_ts.elapsed() >= self.get_checkpoint_timeout() {
                    // Only do this if have been layer-less longer than get_checkpoint_timeout, so that a shard
                    // without any data ingested (yet) doesn't write a remote index as soon as it
                    // sees its LSN advance: we only do this if we've been layer-less
                    // for some time.
                    tracing::debug!(
                        "Advancing disk_consistent_lsn past WAL ingest gap {} -> {}",
                        disk_consistent_lsn,
                        last_record_lsn
                    );

                    // The flush loop will update remote consistent LSN as well as disk consistent LSN.
                    // We know there is no open layer, so we can request freezing without actually
                    // freezing anything. This is true even if we have dropped the layers_guard, we
                    // still hold the write_guard.
                    let _ = async {
                        let token = self
                            .freeze_inmem_layer_at(last_record_lsn, &mut write_guard)
                            .await?;
                        self.wait_flush_completion(token).await
                    }
                    .await;
                }
            }

            return;
        };

        let current_size = open_layer.len();

        let current_lsn = self.get_last_record_lsn();

        let checkpoint_distance_override = open_layer.tick();

        if let Some(size_override) = checkpoint_distance_override {
            if current_size > size_override {
                // This is not harmful, but it only happens in relatively rare cases where
                // time-based checkpoints are not happening fast enough to keep the amount of
                // ephemeral data within configured limits.  It's a sign of stress on the system.
                tracing::info!(
                    "Early-rolling open layer at size {current_size} (limit {size_override}) due to dirty data pressure"
                );
            }
        }

        let checkpoint_distance =
            checkpoint_distance_override.unwrap_or(self.get_checkpoint_distance());

        if self.should_roll(
            current_size,
            current_size,
            checkpoint_distance,
            self.get_last_record_lsn(),
            self.last_freeze_at.load(),
            open_layer.get_opened_at(),
        ) {
            match open_layer.info() {
                InMemoryLayerInfo::Frozen { lsn_start, lsn_end } => {
                    // We may reach this point if the layer was already frozen by not yet flushed: flushing
                    // happens asynchronously in the background.
                    tracing::debug!(
                        "Not freezing open layer, it's already frozen ({lsn_start}..{lsn_end})"
                    );
                }
                InMemoryLayerInfo::Open { .. } => {
                    // Upgrade to a write lock and freeze the layer
                    drop(layers_guard);
                    let res = self
                        .freeze_inmem_layer_at(current_lsn, &mut write_guard)
                        .await;

                    if let Err(e) = res {
                        tracing::info!(
                            "failed to flush frozen layer after background freeze: {e:#}"
                        );
                    }
                }
            }
        }
    }

    /// Checks if the internal state of the timeline is consistent with it being able to be offloaded.
    ///
    /// This is neccessary but not sufficient for offloading of the timeline as it might have
    /// child timelines that are not offloaded yet.
    pub(crate) fn can_offload(&self) -> (bool, &'static str) {
        if self.remote_client.is_archived() != Some(true) {
            return (false, "the timeline is not archived");
        }
        if !self.remote_client.no_pending_work() {
            // if the remote client is still processing some work, we can't offload
            return (false, "the upload queue is not drained yet");
        }

        (true, "ok")
    }

    /// Outermost timeline compaction operation; downloads needed layers. Returns whether we have pending
    /// compaction tasks.
    pub(crate) async fn compact(
        self: &Arc<Self>,
        cancel: &CancellationToken,
        flags: EnumSet<CompactFlags>,
        ctx: &RequestContext,
    ) -> Result<CompactionOutcome, CompactionError> {
        let res = self
            .compact_with_options(
                cancel,
                CompactOptions {
                    flags,
                    compact_key_range: None,
                    compact_lsn_range: None,
                    sub_compaction: false,
                    sub_compaction_max_job_size_mb: None,
                },
                ctx,
            )
            .await;
        if let Err(err) = &res {
            log_compaction_error(err, None, cancel.is_cancelled(), false);
        }
        res
    }

    /// Outermost timeline compaction operation; downloads needed layers.
    ///
    /// NB: the cancellation token is usually from a background task, but can also come from a
    /// request task.
    pub(crate) async fn compact_with_options(
        self: &Arc<Self>,
        cancel: &CancellationToken,
        options: CompactOptions,
        ctx: &RequestContext,
    ) -> Result<CompactionOutcome, CompactionError> {
        // Acquire the compaction lock and task semaphore.
        //
        // L0-only compaction uses a separate semaphore (if enabled) to make sure it isn't starved
        // out by other background tasks (including image compaction). We request this via
        // `BackgroundLoopKind::L0Compaction`.
        //
        // Yield for pending L0 compaction while waiting for the semaphore.
        let is_l0_only = options.flags.contains(CompactFlags::OnlyL0Compaction);
        let semaphore_kind = match is_l0_only && self.get_compaction_l0_semaphore() {
            true => BackgroundLoopKind::L0Compaction,
            false => BackgroundLoopKind::Compaction,
        };
        let yield_for_l0 = options.flags.contains(CompactFlags::YieldForL0);
        if yield_for_l0 {
            // If this is an L0 pass, it doesn't make sense to yield for L0.
            debug_assert!(!is_l0_only, "YieldForL0 during L0 pass");
            // If `compaction_l0_first` is disabled, there's no point yielding.
            debug_assert!(self.get_compaction_l0_first(), "YieldForL0 without L0 pass");
        }

        let acquire = async move {
            let guard = self.compaction_lock.lock().await;
            let permit = super::tasks::acquire_concurrency_permit(semaphore_kind, ctx).await;
            (guard, permit)
        };

        let (_guard, _permit) = tokio::select! {
            (guard, permit) = acquire => (guard, permit),
            _ = self.l0_compaction_trigger.notified(), if yield_for_l0 => {
                return Ok(CompactionOutcome::YieldForL0);
            }
            _ = self.cancel.cancelled() => return Ok(CompactionOutcome::Skipped),
            _ = cancel.cancelled() => return Ok(CompactionOutcome::Skipped),
        };

        let last_record_lsn = self.get_last_record_lsn();

        // Last record Lsn could be zero in case the timeline was just created
        if !last_record_lsn.is_valid() {
            warn!(
                "Skipping compaction for potentially just initialized timeline, it has invalid last record lsn: {last_record_lsn}"
            );
            return Ok(CompactionOutcome::Skipped);
        }

        let result = match self.get_compaction_algorithm_settings().kind {
            CompactionAlgorithm::Tiered => {
                self.compact_tiered(cancel, ctx).await?;
                Ok(CompactionOutcome::Done)
            }
            CompactionAlgorithm::Legacy => self.compact_legacy(cancel, options, ctx).await,
        };

        // Signal compaction failure to avoid L0 flush stalls when it's broken.
        match &result {
            Ok(_) => self.compaction_failed.store(false, AtomicOrdering::Relaxed),
            Err(e) if e.is_cancel() => {}
            Err(CompactionError::ShuttingDown) => {
                // Covered by the `Err(e) if e.is_cancel()` branch.
            }
            Err(CompactionError::Other(_)) => {
                self.compaction_failed.store(true, AtomicOrdering::Relaxed)
            }
            Err(CompactionError::CollectKeySpaceError(_)) => {
                // Cancelled errors are covered by the `Err(e) if e.is_cancel()` branch.
                self.compaction_failed.store(true, AtomicOrdering::Relaxed)
            }
        };

        result
    }

    /// Mutate the timeline with a [`TimelineWriter`].
    pub(crate) async fn writer(&self) -> TimelineWriter<'_> {
        TimelineWriter {
            tl: self,
            write_guard: self.write_lock.lock().await,
        }
    }

    pub(crate) fn activate(
        self: &Arc<Self>,
        parent: Arc<crate::tenant::TenantShard>,
        broker_client: BrokerClientChannel,
        background_jobs_can_start: Option<&completion::Barrier>,
        ctx: &RequestContext,
    ) {
        if self.tenant_shard_id.is_shard_zero() {
            // Logical size is only maintained accurately on shard zero.
            self.spawn_initial_logical_size_computation_task(ctx);
        }
        self.launch_wal_receiver(ctx, broker_client);
        self.set_state(TimelineState::Active);
        self.launch_eviction_task(parent, background_jobs_can_start);
    }

    /// After this function returns, there are no timeline-scoped tasks are left running.
    ///
    /// The preferred pattern for is:
    /// - in any spawned tasks, keep Timeline::guard open + Timeline::cancel / child token
    /// - if early shutdown (not just cancellation) of a sub-tree of tasks is required,
    ///   go the extra mile and keep track of JoinHandles
    /// - Keep track of JoinHandles using a passed-down `Arc<Mutex<Option<JoinSet>>>` or similar,
    ///   instead of spawning directly on a runtime. It is a more composable / testable pattern.
    ///
    /// For legacy reasons, we still have multiple tasks spawned using
    /// `task_mgr::spawn(X, Some(tenant_id), Some(timeline_id))`.
    /// We refer to these as "timeline-scoped task_mgr tasks".
    /// Some of these tasks are already sensitive to Timeline::cancel while others are
    /// not sensitive to Timeline::cancel and instead respect [`task_mgr::shutdown_token`]
    /// or [`task_mgr::shutdown_watcher`].
    /// We want to gradually convert the code base away from these.
    ///
    /// Here is an inventory of timeline-scoped task_mgr tasks that are still sensitive to
    /// `task_mgr::shutdown_{token,watcher}` (there are also tenant-scoped and global-scoped
    /// ones that aren't mentioned here):
    /// - [`TaskKind::TimelineDeletionWorker`]
    ///    - NB: also used for tenant deletion
    /// - [`TaskKind::RemoteUploadTask`]`
    /// - [`TaskKind::InitialLogicalSizeCalculation`]
    /// - [`TaskKind::DownloadAllRemoteLayers`] (can we get rid of it?)
    // Inventory of timeline-scoped task_mgr tasks that use spawn but aren't sensitive:
    /// - [`TaskKind::Eviction`]
    /// - [`TaskKind::LayerFlushTask`]
    /// - [`TaskKind::OndemandLogicalSizeCalculation`]
    /// - [`TaskKind::GarbageCollector`] (immediate_gc is timeline-scoped)
    pub(crate) async fn shutdown(&self, mode: ShutdownMode) {
        debug_assert_current_span_has_tenant_and_timeline_id();

        // Regardless of whether we're going to try_freeze_and_flush
        // cancel walreceiver to stop ingesting more data asap.
        //
        // Note that we're accepting a race condition here where we may
        // do the final flush below, before walreceiver observes the
        // cancellation and exits.
        // This means we may open a new InMemoryLayer after the final flush below.
        // Flush loop is also still running for a short while, so, in theory, it
        // could also make its way into the upload queue.
        //
        // If we wait for the shutdown of the walreceiver before moving on to the
        // flush, then that would be avoided. But we don't do it because the
        // walreceiver entertains reads internally, which means that it possibly
        // depends on the download of layers. Layer download is only sensitive to
        // the cancellation of the entire timeline, so cancelling the walreceiver
        // will have no effect on the individual get requests.
        // This would cause problems when there is a lot of ongoing downloads or
        // there is S3 unavailabilities, i.e. detach, deletion, etc would hang,
        // and we can't deallocate resources of the timeline, etc.
        let walreceiver = self.walreceiver.lock().unwrap().take();
        tracing::debug!(
            is_some = walreceiver.is_some(),
            "Waiting for WalReceiverManager..."
        );
        if let Some(walreceiver) = walreceiver {
            walreceiver.cancel().await;
        }
        // ... and inform any waiters for newer LSNs that there won't be any.
        self.last_record_lsn.shutdown();

        if let ShutdownMode::FreezeAndFlush = mode {
            let do_flush = if let Some((open, frozen)) = self
                .layers
                .read(LayerManagerLockHolder::Shutdown)
                .await
                .layer_map()
                .map(|lm| (lm.open_layer.is_some(), lm.frozen_layers.len()))
                .ok()
                .filter(|(open, frozen)| *open || *frozen > 0)
            {
                if self.remote_client.is_archived() == Some(true) {
                    // No point flushing on shutdown for an archived timeline: it is not important
                    // to have it nice and fresh after our restart, and trying to flush here might
                    // race with trying to offload it (which also stops the flush loop)
                    false
                } else {
                    tracing::info!(?open, frozen, "flushing and freezing on shutdown");
                    true
                }
            } else {
                // this is double-shutdown, it'll be a no-op
                true
            };

            // we shut down walreceiver above, so, we won't add anything more
            // to the InMemoryLayer; freeze it and wait for all frozen layers
            // to reach the disk & upload queue, then shut the upload queue and
            // wait for it to drain.
            if do_flush {
                match self.freeze_and_flush().await {
                    Ok(_) => {
                        // drain the upload queue
                        // if we did not wait for completion here, it might be our shutdown process
                        // didn't wait for remote uploads to complete at all, as new tasks can forever
                        // be spawned.
                        //
                        // what is problematic is the shutting down of RemoteTimelineClient, because
                        // obviously it does not make sense to stop while we wait for it, but what
                        // about corner cases like s3 suddenly hanging up?
                        self.remote_client.shutdown().await;
                    }
                    Err(FlushLayerError::Cancelled) => {
                        // this is likely the second shutdown, ignore silently.
                        // TODO: this can be removed once https://github.com/neondatabase/neon/issues/5080
                        debug_assert!(self.cancel.is_cancelled());
                    }
                    Err(e) => {
                        // Non-fatal.  Shutdown is infallible.  Failures to flush just mean that
                        // we have some extra WAL replay to do next time the timeline starts.
                        warn!("failed to freeze and flush: {e:#}");
                    }
                }

                // `self.remote_client.shutdown().await` above should have already flushed everything from the queue, but
                // we also do a final check here to ensure that the queue is empty.
                if !self.remote_client.no_pending_work() {
                    warn!(
                        "still have pending work in remote upload queue, but continuing shutting down anyways"
                    );
                }
            }
        }

        if let ShutdownMode::Reload = mode {
            // drain the upload queue
            self.remote_client.shutdown().await;
            if !self.remote_client.no_pending_work() {
                warn!(
                    "still have pending work in remote upload queue, but continuing shutting down anyways"
                );
            }
        }

        // Signal any subscribers to our cancellation token to drop out
        tracing::debug!("Cancelling CancellationToken");
        self.cancel.cancel();

        // If we have a background task downloading heatmap layers stop it.
        // The background downloads are sensitive to timeline cancellation (done above),
        // so the drain will be immediate.
        self.stop_and_drain_heatmap_layers_download().await;

        // Ensure Prevent new page service requests from starting.
        self.handles.shutdown();

        // Transition the remote_client into a state where it's only useful for timeline deletion.
        // (The deletion use case is why we can't just hook up remote_client to Self::cancel).)
        self.remote_client.stop();

        // As documented in remote_client.stop()'s doc comment, it's our responsibility
        // to shut down the upload queue tasks.
        // TODO: fix that, task management should be encapsulated inside remote_client.
        task_mgr::shutdown_tasks(
            Some(TaskKind::RemoteUploadTask),
            Some(self.tenant_shard_id),
            Some(self.timeline_id),
        )
        .await;

        // TODO: work toward making this a no-op. See this function's doc comment for more context.
        tracing::debug!("Waiting for tasks...");
        task_mgr::shutdown_tasks(None, Some(self.tenant_shard_id), Some(self.timeline_id)).await;

        {
            // Allow any remaining in-memory layers to do cleanup -- until that, they hold the gate
            // open.
            let mut write_guard = self.write_lock.lock().await;
            self.layers
                .write(LayerManagerLockHolder::Shutdown)
                .await
                .shutdown(&mut write_guard);
        }

        // Finally wait until any gate-holders are complete.
        //
        // TODO: once above shutdown_tasks is a no-op, we can close the gate before calling shutdown_tasks
        // and use a TBD variant of shutdown_tasks that asserts that there were no tasks left.
        self.gate.close().await;

        self.metrics.shutdown();
    }

    pub(crate) fn set_state(&self, new_state: TimelineState) {
        match (self.current_state(), new_state) {
            (equal_state_1, equal_state_2) if equal_state_1 == equal_state_2 => {
                info!("Ignoring new state, equal to the existing one: {equal_state_2:?}");
            }
            (st, TimelineState::Loading) => {
                error!("ignoring transition from {st:?} into Loading state");
            }
            (TimelineState::Broken { .. }, new_state) => {
                error!("Ignoring state update {new_state:?} for broken timeline");
            }
            (TimelineState::Stopping, TimelineState::Active) => {
                error!("Not activating a Stopping timeline");
            }
            (_, new_state) => {
                self.state.send_replace(new_state);
            }
        }
    }

    pub(crate) fn set_broken(&self, reason: String) {
        let backtrace_str: String = format!("{}", std::backtrace::Backtrace::force_capture());
        let broken_state = TimelineState::Broken {
            reason,
            backtrace: backtrace_str,
        };
        self.set_state(broken_state);

        // Although the Broken state is not equivalent to shutdown() (shutdown will be called
        // later when this tenant is detach or the process shuts down), firing the cancellation token
        // here avoids the need for other tasks to watch for the Broken state explicitly.
        self.cancel.cancel();
    }

    pub(crate) fn current_state(&self) -> TimelineState {
        self.state.borrow().clone()
    }

    pub(crate) fn is_broken(&self) -> bool {
        matches!(&*self.state.borrow(), TimelineState::Broken { .. })
    }

    pub(crate) fn is_active(&self) -> bool {
        self.current_state() == TimelineState::Active
    }

    pub(crate) fn is_archived(&self) -> Option<bool> {
        self.remote_client.is_archived()
    }

    pub(crate) fn is_invisible(&self) -> Option<bool> {
        self.remote_client.is_invisible()
    }

    pub(crate) fn is_stopping(&self) -> bool {
        self.current_state() == TimelineState::Stopping
    }

    pub(crate) fn subscribe_for_state_updates(&self) -> watch::Receiver<TimelineState> {
        self.state.subscribe()
    }

    pub(crate) async fn wait_to_become_active(
        &self,
        _ctx: &RequestContext, // Prepare for use by cancellation
    ) -> Result<(), TimelineState> {
        let mut receiver = self.state.subscribe();
        loop {
            let current_state = receiver.borrow().clone();
            match current_state {
                TimelineState::Loading => {
                    receiver
                        .changed()
                        .await
                        .expect("holding a reference to self");
                }
                TimelineState::Active => {
                    return Ok(());
                }
                TimelineState::Broken { .. } | TimelineState::Stopping => {
                    // There's no chance the timeline can transition back into ::Active
                    return Err(current_state);
                }
            }
        }
    }

    pub(crate) async fn layer_map_info(
        &self,
        reset: LayerAccessStatsReset,
    ) -> Result<LayerMapInfo, layer_manager::Shutdown> {
        let guard = self
            .layers
            .read(LayerManagerLockHolder::GetLayerMapInfo)
            .await;
        let layer_map = guard.layer_map()?;
        let mut in_memory_layers = Vec::with_capacity(layer_map.frozen_layers.len() + 1);
        if let Some(open_layer) = &layer_map.open_layer {
            in_memory_layers.push(open_layer.info());
        }
        for frozen_layer in &layer_map.frozen_layers {
            in_memory_layers.push(frozen_layer.info());
        }

        let historic_layers = layer_map
            .iter_historic_layers()
            .map(|desc| guard.get_from_desc(&desc).info(reset))
            .collect();

        Ok(LayerMapInfo {
            in_memory_layers,
            historic_layers,
        })
    }

    #[instrument(skip_all, fields(tenant_id = %self.tenant_shard_id.tenant_id, shard_id = %self.tenant_shard_id.shard_slug(), timeline_id = %self.timeline_id))]
    pub(crate) async fn download_layer(
        &self,
        layer_file_name: &LayerName,
        ctx: &RequestContext,
    ) -> Result<Option<bool>, super::storage_layer::layer::DownloadError> {
        let Some(layer) = self
            .find_layer(layer_file_name)
            .await
            .map_err(|e| match e {
                layer_manager::Shutdown => {
                    super::storage_layer::layer::DownloadError::TimelineShutdown
                }
            })?
        else {
            return Ok(None);
        };

        layer.download(ctx).await?;

        Ok(Some(true))
    }

    /// Evict just one layer.
    ///
    /// Returns `Ok(None)` in the case where the layer could not be found by its `layer_file_name`.
    pub(crate) async fn evict_layer(
        &self,
        layer_file_name: &LayerName,
    ) -> anyhow::Result<Option<bool>> {
        let _gate = self
            .gate
            .enter()
            .map_err(|_| anyhow::anyhow!("Shutting down"))?;

        let Some(local_layer) = self.find_layer(layer_file_name).await? else {
            return Ok(None);
        };

        // curl has this by default
        let timeout = std::time::Duration::from_secs(120);

        match local_layer.evict_and_wait(timeout).await {
            Ok(()) => Ok(Some(true)),
            Err(EvictionError::NotFound) => Ok(Some(false)),
            Err(EvictionError::Downloaded) => Ok(Some(false)),
            Err(EvictionError::Timeout) => Ok(Some(false)),
        }
    }

    fn should_roll(
        &self,
        layer_size: u64,
        projected_layer_size: u64,
        checkpoint_distance: u64,
        projected_lsn: Lsn,
        last_freeze_at: Lsn,
        opened_at: Instant,
    ) -> bool {
        let distance = projected_lsn.widening_sub(last_freeze_at);

        // Rolling the open layer can be triggered by:
        // 1. The distance from the last LSN we rolled at. This bounds the amount of WAL that
        //    the safekeepers need to store.  For sharded tenants, we multiply by shard count to
        //    account for how writes are distributed across shards: we expect each node to consume
        //    1/count of the LSN on average.
        // 2. The size of the currently open layer.
        // 3. The time since the last roll. It helps safekeepers to regard pageserver as caught
        //    up and suspend activity.
        if distance >= checkpoint_distance as i128 * self.shard_identity.count.count() as i128 {
            info!(
                "Will roll layer at {} with layer size {} due to LSN distance ({})",
                projected_lsn, layer_size, distance
            );

            true
        } else if projected_layer_size >= checkpoint_distance {
            // NB: this check is relied upon by:
            let _ = IndexEntry::validate_checkpoint_distance;
            info!(
                "Will roll layer at {} with layer size {} due to layer size ({})",
                projected_lsn, layer_size, projected_layer_size
            );

            true
        } else if distance > 0 && opened_at.elapsed() >= self.get_checkpoint_timeout() {
            info!(
                "Will roll layer at {} with layer size {} due to time since first write to the layer ({:?})",
                projected_lsn,
                layer_size,
                opened_at.elapsed()
            );

            true
        } else {
            false
        }
    }

    pub(crate) fn is_basebackup_cache_enabled(&self) -> bool {
        let tenant_conf = self.tenant_conf.load();
        tenant_conf
            .tenant_conf
            .basebackup_cache_enabled
            .unwrap_or(self.conf.default_tenant_conf.basebackup_cache_enabled)
    }

    /// Try to get a basebackup from the on-disk cache.
    pub(crate) async fn get_cached_basebackup(&self, lsn: Lsn) -> Option<tokio::fs::File> {
        self.basebackup_cache
            .get(self.tenant_shard_id.tenant_id, self.timeline_id, lsn)
            .await
    }

    /// Convenience method to attempt fetching a basebackup for the timeline if enabled and safe for
    /// the given request parameters.
    ///
    /// TODO: consider moving this onto GrpcPageServiceHandler once the libpq handler is gone.
    pub async fn get_cached_basebackup_if_enabled(
        &self,
        lsn: Option<Lsn>,
        prev_lsn: Option<Lsn>,
        full: bool,
        replica: bool,
        gzip: bool,
    ) -> Option<tokio::fs::File> {
        if !self.is_basebackup_cache_enabled() || !self.basebackup_cache.is_enabled() {
            return None;
        }
        // We have to know which LSN to fetch the basebackup for.
        let lsn = lsn?;
        // We only cache gzipped, non-full basebackups for primary computes with automatic prev_lsn.
        if prev_lsn.is_some() || full || replica || !gzip {
            return None;
        }
        self.get_cached_basebackup(lsn).await
    }

    /// Prepare basebackup for the given LSN and store it in the basebackup cache.
    /// The method is asynchronous and returns immediately.
    /// The actual basebackup preparation is performed in the background
    /// by the basebackup cache on a best-effort basis.
    pub(crate) fn prepare_basebackup(&self, lsn: Lsn) {
        if !self.is_basebackup_cache_enabled() {
            return;
        }
        if !self.tenant_shard_id.is_shard_zero() {
            // In theory we should never get here, but just in case check it.
            // Preparing basebackup doesn't make sense for shards other than shard zero.
            return;
        }
        if !self.is_active() {
            // May happen during initial timeline creation.
            // Such timeline is not in the global timeline map yet,
            // so basebackup cache will not be able to find it.
            // TODO(diko): We can prepare such timelines in finish_creation().
            return;
        }

        self.basebackup_cache
            .send_prepare(self.tenant_shard_id, self.timeline_id, lsn);
    }
}

/// Number of times we will compute partition within a checkpoint distance.
const REPARTITION_FREQ_IN_CHECKPOINT_DISTANCE: u64 = 10;

// Private functions
impl Timeline {
    pub(crate) fn get_lsn_lease_length(&self) -> Duration {
        let tenant_conf = self.tenant_conf.load();
        tenant_conf
            .tenant_conf
            .lsn_lease_length
            .unwrap_or(self.conf.default_tenant_conf.lsn_lease_length)
    }

    pub(crate) fn get_lsn_lease_length_for_ts(&self) -> Duration {
        let tenant_conf = self.tenant_conf.load();
        tenant_conf
            .tenant_conf
            .lsn_lease_length_for_ts
            .unwrap_or(self.conf.default_tenant_conf.lsn_lease_length_for_ts)
    }

    pub(crate) fn is_gc_blocked_by_lsn_lease_deadline(&self) -> bool {
        let tenant_conf = self.tenant_conf.load();
        tenant_conf.is_gc_blocked_by_lsn_lease_deadline()
    }

    pub(crate) fn get_lazy_slru_download(&self) -> bool {
        let tenant_conf = self.tenant_conf.load();
        tenant_conf
            .tenant_conf
            .lazy_slru_download
            .unwrap_or(self.conf.default_tenant_conf.lazy_slru_download)
    }

    /// Checks if a get page request should get perf tracing
    ///
    /// The configuration priority is: tenant config override, default tenant config,
    /// pageserver config.
    pub(crate) fn is_get_page_request_sampled(&self) -> bool {
        let tenant_conf = self.tenant_conf.load();
        let ratio = tenant_conf
            .tenant_conf
            .sampling_ratio
            .flatten()
            .or(self.conf.default_tenant_conf.sampling_ratio)
            .or(self.conf.tracing.as_ref().map(|t| t.sampling_ratio));

        match ratio {
            Some(r) => {
                if r.numerator == 0 {
                    false
                } else {
                    rand::thread_rng().gen_range(0..r.denominator) < r.numerator
                }
            }
            None => false,
        }
    }

    fn get_checkpoint_distance(&self) -> u64 {
        let tenant_conf = self.tenant_conf.load();
        tenant_conf
            .tenant_conf
            .checkpoint_distance
            .unwrap_or(self.conf.default_tenant_conf.checkpoint_distance)
    }

    fn get_checkpoint_timeout(&self) -> Duration {
        let tenant_conf = self.tenant_conf.load();
        tenant_conf
            .tenant_conf
            .checkpoint_timeout
            .unwrap_or(self.conf.default_tenant_conf.checkpoint_timeout)
    }

    pub(crate) fn get_pitr_interval(&self) -> Duration {
        let tenant_conf = &self.tenant_conf.load().tenant_conf;
        tenant_conf
            .pitr_interval
            .unwrap_or(self.conf.default_tenant_conf.pitr_interval)
    }

    fn get_compaction_period(&self) -> Duration {
        let tenant_conf = self.tenant_conf.load().tenant_conf.clone();
        tenant_conf
            .compaction_period
            .unwrap_or(self.conf.default_tenant_conf.compaction_period)
    }

    fn get_compaction_target_size(&self) -> u64 {
        let tenant_conf = self.tenant_conf.load();
        tenant_conf
            .tenant_conf
            .compaction_target_size
            .unwrap_or(self.conf.default_tenant_conf.compaction_target_size)
    }

    fn get_compaction_threshold(&self) -> usize {
        let tenant_conf = self.tenant_conf.load();
        tenant_conf
            .tenant_conf
            .compaction_threshold
            .unwrap_or(self.conf.default_tenant_conf.compaction_threshold)
    }

    /// Returns `true` if the rel_size_v2 config is enabled. NOTE: the write path and read path
    /// should look at `get_rel_size_v2_status()` to get the actual status of the timeline. It is
    /// possible that the index part persists the state while the config doesn't get persisted.
    pub(crate) fn get_rel_size_v2_enabled(&self) -> bool {
        let tenant_conf = self.tenant_conf.load();
        tenant_conf
            .tenant_conf
            .rel_size_v2_enabled
            .unwrap_or(self.conf.default_tenant_conf.rel_size_v2_enabled)
    }

    pub(crate) fn get_rel_size_v2_status(&self) -> RelSizeMigration {
        self.rel_size_v2_status
            .load()
            .as_ref()
            .map(|s| s.as_ref().clone())
            .unwrap_or(RelSizeMigration::Legacy)
    }

    fn get_compaction_upper_limit(&self) -> usize {
        let tenant_conf = self.tenant_conf.load();
        tenant_conf
            .tenant_conf
            .compaction_upper_limit
            .unwrap_or(self.conf.default_tenant_conf.compaction_upper_limit)
    }

    pub fn get_compaction_l0_first(&self) -> bool {
        let tenant_conf = self.tenant_conf.load().tenant_conf.clone();
        tenant_conf
            .compaction_l0_first
            .unwrap_or(self.conf.default_tenant_conf.compaction_l0_first)
    }

    pub fn get_compaction_l0_semaphore(&self) -> bool {
        let tenant_conf = self.tenant_conf.load().tenant_conf.clone();
        tenant_conf
            .compaction_l0_semaphore
            .unwrap_or(self.conf.default_tenant_conf.compaction_l0_semaphore)
    }

    fn get_l0_flush_delay_threshold(&self) -> Option<usize> {
        // By default, delay L0 flushes at 3x the compaction threshold. The compaction threshold
        // defaults to 10, and L0 compaction is generally able to keep L0 counts below 30.
        const DEFAULT_L0_FLUSH_DELAY_FACTOR: usize = 3;

        // If compaction is disabled, don't delay.
        if self.get_compaction_period() == Duration::ZERO {
            return None;
        }

        let compaction_threshold = self.get_compaction_threshold();
        let tenant_conf = self.tenant_conf.load();
        let l0_flush_delay_threshold = tenant_conf
            .tenant_conf
            .l0_flush_delay_threshold
            .or(self.conf.default_tenant_conf.l0_flush_delay_threshold)
            .unwrap_or(DEFAULT_L0_FLUSH_DELAY_FACTOR * compaction_threshold);

        // 0 disables backpressure.
        if l0_flush_delay_threshold == 0 {
            return None;
        }

        // Clamp the flush delay threshold to the compaction threshold; it doesn't make sense to
        // backpressure flushes below this.
        // TODO: the tenant config should have validation to prevent this instead.
        debug_assert!(l0_flush_delay_threshold >= compaction_threshold);
        Some(max(l0_flush_delay_threshold, compaction_threshold))
    }

    fn get_l0_flush_stall_threshold(&self) -> Option<usize> {
        // Disable L0 stalls by default. Stalling can cause unavailability if L0 compaction isn't
        // responsive, and it can e.g. block on other compaction via the compaction semaphore or
        // sibling timelines. We need more confidence before enabling this.
        const DEFAULT_L0_FLUSH_STALL_FACTOR: usize = 0; // TODO: default to e.g. 5

        // If compaction is disabled, don't stall.
        if self.get_compaction_period() == Duration::ZERO {
            return None;
        }

        // If compaction is failing, don't stall and try to keep the tenant alive. This may not be a
        // good idea: read amp can grow unbounded, leading to terrible performance, and we may take
        // on unbounded compaction debt that can take a long time to fix once compaction comes back
        // online. At least we'll delay flushes, slowing down the growth and buying some time.
        if self.compaction_failed.load(AtomicOrdering::Relaxed) {
            return None;
        }

        let compaction_threshold = self.get_compaction_threshold();
        let tenant_conf = self.tenant_conf.load();
        let l0_flush_stall_threshold = tenant_conf
            .tenant_conf
            .l0_flush_stall_threshold
            .or(self.conf.default_tenant_conf.l0_flush_stall_threshold);

        // Tests sometimes set compaction_threshold=1 to generate lots of layer files, and don't
        // handle the 20-second compaction delay. Some (e.g. `test_backward_compatibility`) can't
        // easily adjust the L0 backpressure settings, so just disable stalls in this case.
        if cfg!(feature = "testing")
            && compaction_threshold == 1
            && l0_flush_stall_threshold.is_none()
        {
            return None;
        }

        let l0_flush_stall_threshold = l0_flush_stall_threshold
            .unwrap_or(DEFAULT_L0_FLUSH_STALL_FACTOR * compaction_threshold);

        // 0 disables backpressure.
        if l0_flush_stall_threshold == 0 {
            return None;
        }

        // Clamp the flush stall threshold to the compaction threshold; it doesn't make sense to
        // backpressure flushes below this.
        // TODO: the tenant config should have validation to prevent this instead.
        debug_assert!(l0_flush_stall_threshold >= compaction_threshold);
        Some(max(l0_flush_stall_threshold, compaction_threshold))
    }

    fn get_image_creation_threshold(&self) -> usize {
        let tenant_conf = self.tenant_conf.load();
        tenant_conf
            .tenant_conf
            .image_creation_threshold
            .unwrap_or(self.conf.default_tenant_conf.image_creation_threshold)
    }

    fn get_compaction_algorithm_settings(&self) -> CompactionAlgorithmSettings {
        let tenant_conf = &self.tenant_conf.load();
        tenant_conf
            .tenant_conf
            .compaction_algorithm
            .as_ref()
            .unwrap_or(&self.conf.default_tenant_conf.compaction_algorithm)
            .clone()
    }

    pub fn get_compaction_shard_ancestor(&self) -> bool {
        let tenant_conf = self.tenant_conf.load();
        tenant_conf
            .tenant_conf
            .compaction_shard_ancestor
            .unwrap_or(self.conf.default_tenant_conf.compaction_shard_ancestor)
    }

    fn get_eviction_policy(&self) -> EvictionPolicy {
        let tenant_conf = self.tenant_conf.load();
        tenant_conf
            .tenant_conf
            .eviction_policy
            .unwrap_or(self.conf.default_tenant_conf.eviction_policy)
    }

    fn get_evictions_low_residence_duration_metric_threshold(
        tenant_conf: &pageserver_api::models::TenantConfig,
        default_tenant_conf: &pageserver_api::config::TenantConfigToml,
    ) -> Duration {
        tenant_conf
            .evictions_low_residence_duration_metric_threshold
            .unwrap_or(default_tenant_conf.evictions_low_residence_duration_metric_threshold)
    }

    fn get_image_layer_creation_check_threshold(&self) -> u8 {
        let tenant_conf = self.tenant_conf.load();
        tenant_conf
            .tenant_conf
            .image_layer_creation_check_threshold
            .unwrap_or(
                self.conf
                    .default_tenant_conf
                    .image_layer_creation_check_threshold,
            )
    }

    fn get_gc_compaction_settings(&self) -> GcCompactionCombinedSettings {
        let tenant_conf = &self.tenant_conf.load();
        let gc_compaction_enabled = tenant_conf
            .tenant_conf
            .gc_compaction_enabled
            .unwrap_or(self.conf.default_tenant_conf.gc_compaction_enabled);
        let gc_compaction_verification = tenant_conf
            .tenant_conf
            .gc_compaction_verification
            .unwrap_or(self.conf.default_tenant_conf.gc_compaction_verification);
        let gc_compaction_initial_threshold_kb = tenant_conf
            .tenant_conf
            .gc_compaction_initial_threshold_kb
            .unwrap_or(
                self.conf
                    .default_tenant_conf
                    .gc_compaction_initial_threshold_kb,
            );
        let gc_compaction_ratio_percent = tenant_conf
            .tenant_conf
            .gc_compaction_ratio_percent
            .unwrap_or(self.conf.default_tenant_conf.gc_compaction_ratio_percent);
        GcCompactionCombinedSettings {
            gc_compaction_enabled,
            gc_compaction_verification,
            gc_compaction_initial_threshold_kb,
            gc_compaction_ratio_percent,
        }
    }

    fn get_image_creation_preempt_threshold(&self) -> usize {
        let tenant_conf = self.tenant_conf.load();
        tenant_conf
            .tenant_conf
            .image_creation_preempt_threshold
            .unwrap_or(
                self.conf
                    .default_tenant_conf
                    .image_creation_preempt_threshold,
            )
    }

    pub(super) fn tenant_conf_updated(&self, new_conf: &AttachedTenantConf) {
        // NB: Most tenant conf options are read by background loops, so,
        // changes will automatically be picked up.

        // The threshold is embedded in the metric. So, we need to update it.
        {
            let new_threshold = Self::get_evictions_low_residence_duration_metric_threshold(
                &new_conf.tenant_conf,
                &self.conf.default_tenant_conf,
            );

            let tenant_id_str = self.tenant_shard_id.tenant_id.to_string();
            let shard_id_str = format!("{}", self.tenant_shard_id.shard_slug());

            let timeline_id_str = self.timeline_id.to_string();

            self.remote_client.update_config(&new_conf.location);

            let mut rel_size_cache = self.rel_size_snapshot_cache.lock().unwrap();
            if let Some(new_capacity) = new_conf.tenant_conf.relsize_snapshot_cache_capacity {
                if new_capacity != rel_size_cache.capacity() {
                    rel_size_cache.set_capacity(new_capacity);
                }
            }

            self.metrics
                .evictions_with_low_residence_duration
                .write()
                .unwrap()
                .change_threshold(
                    &tenant_id_str,
                    &shard_id_str,
                    &timeline_id_str,
                    new_threshold,
                );
        }
    }

    /// Open a Timeline handle.
    ///
    /// Loads the metadata for the timeline into memory, but not the layer map.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn new(
        conf: &'static PageServerConf,
        tenant_conf: Arc<ArcSwap<AttachedTenantConf>>,
        metadata: &TimelineMetadata,
        previous_heatmap: Option<PreviousHeatmap>,
        ancestor: Option<Arc<Timeline>>,
        timeline_id: TimelineId,
        tenant_shard_id: TenantShardId,
        generation: Generation,
        shard_identity: ShardIdentity,
        walredo_mgr: Option<Arc<super::WalRedoManager>>,
        resources: TimelineResources,
        pg_version: PgMajorVersion,
        state: TimelineState,
        attach_wal_lag_cooldown: Arc<OnceLock<WalLagCooldown>>,
        create_idempotency: crate::tenant::CreateTimelineIdempotency,
        gc_compaction_state: Option<GcCompactionState>,
        rel_size_v2_status: Option<RelSizeMigration>,
        cancel: CancellationToken,
    ) -> Arc<Self> {
        let disk_consistent_lsn = metadata.disk_consistent_lsn();
        let (state, _) = watch::channel(state);

        let (layer_flush_start_tx, _) = tokio::sync::watch::channel((0, disk_consistent_lsn));
        let (layer_flush_done_tx, _) = tokio::sync::watch::channel((0, Ok(())));

        let evictions_low_residence_duration_metric_threshold = {
            let loaded_tenant_conf = tenant_conf.load();
            Self::get_evictions_low_residence_duration_metric_threshold(
                &loaded_tenant_conf.tenant_conf,
                &conf.default_tenant_conf,
            )
        };

        if let Some(ancestor) = &ancestor {
            let mut ancestor_gc_info = ancestor.gc_info.write().unwrap();
            // If we construct an explicit timeline object, it's obviously not offloaded
            let is_offloaded = MaybeOffloaded::No;
            ancestor_gc_info.insert_child(timeline_id, metadata.ancestor_lsn(), is_offloaded);
        }

        let relsize_snapshot_cache_capacity = {
            let loaded_tenant_conf = tenant_conf.load();
            loaded_tenant_conf
                .tenant_conf
                .relsize_snapshot_cache_capacity
                .unwrap_or(conf.default_tenant_conf.relsize_snapshot_cache_capacity)
        };

        Arc::new_cyclic(|myself| {
            let metrics = Arc::new(TimelineMetrics::new(
                &tenant_shard_id,
                &timeline_id,
                crate::metrics::EvictionsWithLowResidenceDurationBuilder::new(
                    "mtime",
                    evictions_low_residence_duration_metric_threshold,
                ),
            ));
            let aux_file_metrics = metrics.aux_file_size_gauge.clone();

            let mut result = Timeline {
                conf,
                tenant_conf,
                myself: myself.clone(),
                timeline_id,
                tenant_shard_id,
                generation,
                shard_identity,
                pg_version,
                layers: Default::default(),
                gc_compaction_layer_update_lock: tokio::sync::RwLock::new(()),

                walredo_mgr,
                walreceiver: Mutex::new(None),

                remote_client: Arc::new(resources.remote_client),

                // initialize in-memory 'last_record_lsn' from 'disk_consistent_lsn'.
                last_record_lsn: SeqWait::new(RecordLsn {
                    last: disk_consistent_lsn,
                    prev: metadata.prev_record_lsn().unwrap_or(Lsn(0)),
                }),
                disk_consistent_lsn: AtomicLsn::new(disk_consistent_lsn.0),

                gc_compaction_state: ArcSwap::new(Arc::new(gc_compaction_state)),

                last_freeze_at: AtomicLsn::new(disk_consistent_lsn.0),
                last_freeze_ts: RwLock::new(Instant::now()),

                loaded_at: (disk_consistent_lsn, SystemTime::now()),

                ancestor_timeline: ancestor,
                ancestor_lsn: metadata.ancestor_lsn(),

                metrics,

                query_metrics: crate::metrics::SmgrQueryTimePerTimeline::new(
                    &tenant_shard_id,
                    &timeline_id,
                    resources.pagestream_throttle_metrics,
                ),

                directory_metrics: array::from_fn(|_| AtomicU64::new(0)),
                directory_metrics_inited: array::from_fn(|_| AtomicBool::new(false)),

                flush_loop_state: Mutex::new(FlushLoopState::NotStarted),

                layer_flush_start_tx,
                layer_flush_done_tx,

                write_lock: tokio::sync::Mutex::new(None),

                gc_info: std::sync::RwLock::new(GcInfo::default()),

                last_image_layer_creation_status: ArcSwap::new(Arc::new(
                    LastImageLayerCreationStatus::default(),
                )),

                applied_gc_cutoff_lsn: Rcu::new(metadata.latest_gc_cutoff_lsn()),
                initdb_lsn: metadata.initdb_lsn(),

                current_logical_size: if disk_consistent_lsn.is_valid() {
                    // we're creating timeline data with some layer files existing locally,
                    // need to recalculate timeline's logical size based on data in the layers.
                    LogicalSize::deferred_initial(disk_consistent_lsn)
                } else {
                    // we're creating timeline data without any layers existing locally,
                    // initial logical size is 0.
                    LogicalSize::empty_initial()
                },

                partitioning: GuardArcSwap::new((
                    (KeyPartitioning::new(), KeyPartitioning::new().into_sparse()),
                    Lsn(0),
                )),
                repartition_threshold: 0,
                last_image_layer_creation_check_at: AtomicLsn::new(0),
                last_image_layer_creation_check_instant: Mutex::new(None),

                last_received_wal: Mutex::new(None),
                rel_size_latest_cache: RwLock::new(HashMap::new()),
                rel_size_snapshot_cache: Mutex::new(LruCache::new(relsize_snapshot_cache_capacity)),

                download_all_remote_layers_task_info: RwLock::new(None),

                state,

                eviction_task_timeline_state: tokio::sync::Mutex::new(
                    EvictionTaskTimelineState::default(),
                ),
                delete_progress: TimelineDeleteProgress::default(),

                cancel,
                gate: Gate::default(),

                compaction_lock: tokio::sync::Mutex::default(),
                compaction_failed: AtomicBool::default(),
                l0_compaction_trigger: resources.l0_compaction_trigger,
                gc_lock: tokio::sync::Mutex::default(),

                standby_horizon: AtomicLsn::new(0),

                pagestream_throttle: resources.pagestream_throttle,

                aux_file_size_estimator: AuxFileSizeEstimator::new(aux_file_metrics),

                #[cfg(test)]
                extra_test_dense_keyspace: ArcSwap::new(Arc::new(KeySpace::default())),

                l0_flush_global_state: resources.l0_flush_global_state,

                handles: Default::default(),

                attach_wal_lag_cooldown,

                create_idempotency,

                page_trace: Default::default(),

                previous_heatmap: ArcSwapOption::from_pointee(previous_heatmap),

                heatmap_layers_downloader: Mutex::new(None),

                rel_size_v2_status: ArcSwapOption::from_pointee(rel_size_v2_status),

                wait_lsn_log_slow: tokio::sync::Semaphore::new(1),

                basebackup_cache: resources.basebackup_cache,

                feature_resolver: resources.feature_resolver.clone(),
            };

            result.repartition_threshold =
                result.get_checkpoint_distance() / REPARTITION_FREQ_IN_CHECKPOINT_DISTANCE;

            result
                .metrics
                .last_record_lsn_gauge
                .set(disk_consistent_lsn.0 as i64);
            result
        })
    }

    pub(super) fn maybe_spawn_flush_loop(self: &Arc<Self>) {
        let Ok(guard) = self.gate.enter() else {
            info!("cannot start flush loop when the timeline gate has already been closed");
            return;
        };
        let mut flush_loop_state = self.flush_loop_state.lock().unwrap();
        match *flush_loop_state {
            FlushLoopState::NotStarted => (),
            FlushLoopState::Running { .. } => {
                info!(
                    "skipping attempt to start flush_loop twice {}/{}",
                    self.tenant_shard_id, self.timeline_id
                );
                return;
            }
            FlushLoopState::Exited => {
                info!(
                    "ignoring attempt to restart exited flush_loop {}/{}",
                    self.tenant_shard_id, self.timeline_id
                );
                return;
            }
        }

        let layer_flush_start_rx = self.layer_flush_start_tx.subscribe();
        let self_clone = Arc::clone(self);

        debug!("spawning flush loop");
        *flush_loop_state = FlushLoopState::Running {
            #[cfg(test)]
            expect_initdb_optimization: false,
            #[cfg(test)]
            initdb_optimization_count: 0,
        };
        task_mgr::spawn(
            task_mgr::BACKGROUND_RUNTIME.handle(),
            task_mgr::TaskKind::LayerFlushTask,
            self.tenant_shard_id,
            Some(self.timeline_id),
            "layer flush task",
            async move {
                let _guard = guard;
                let background_ctx = RequestContext::todo_child(TaskKind::LayerFlushTask, DownloadBehavior::Error).with_scope_timeline(&self_clone);
                self_clone.flush_loop(layer_flush_start_rx, &background_ctx).await;
                let mut flush_loop_state = self_clone.flush_loop_state.lock().unwrap();
                assert!(matches!(*flush_loop_state, FlushLoopState::Running{..}));
                *flush_loop_state  = FlushLoopState::Exited;
                Ok(())
            }
            .instrument(info_span!(parent: None, "layer flush task", tenant_id = %self.tenant_shard_id.tenant_id, shard_id = %self.tenant_shard_id.shard_slug(), timeline_id = %self.timeline_id))
        );
    }

    pub(crate) fn update_gc_compaction_state(
        &self,
        gc_compaction_state: GcCompactionState,
    ) -> anyhow::Result<()> {
        self.gc_compaction_state
            .store(Arc::new(Some(gc_compaction_state.clone())));
        self.remote_client
            .schedule_index_upload_for_gc_compaction_state_update(gc_compaction_state)
    }

    pub(crate) fn update_rel_size_v2_status(
        &self,
        rel_size_v2_status: RelSizeMigration,
    ) -> anyhow::Result<()> {
        self.rel_size_v2_status
            .store(Some(Arc::new(rel_size_v2_status.clone())));
        self.remote_client
            .schedule_index_upload_for_rel_size_v2_status_update(rel_size_v2_status)
    }

    pub(crate) fn get_gc_compaction_state(&self) -> Option<GcCompactionState> {
        self.gc_compaction_state.load_full().as_ref().clone()
    }

    /// Creates and starts the wal receiver.
    ///
    /// This function is expected to be called at most once per Timeline's lifecycle
    /// when the timeline is activated.
    fn launch_wal_receiver(
        self: &Arc<Self>,
        ctx: &RequestContext,
        broker_client: BrokerClientChannel,
    ) {
        info!(
            "launching WAL receiver for timeline {} of tenant {}",
            self.timeline_id, self.tenant_shard_id
        );

        let tenant_conf = self.tenant_conf.load();
        let wal_connect_timeout = tenant_conf
            .tenant_conf
            .walreceiver_connect_timeout
            .unwrap_or(self.conf.default_tenant_conf.walreceiver_connect_timeout);
        let lagging_wal_timeout = tenant_conf
            .tenant_conf
            .lagging_wal_timeout
            .unwrap_or(self.conf.default_tenant_conf.lagging_wal_timeout);
        let max_lsn_wal_lag = tenant_conf
            .tenant_conf
            .max_lsn_wal_lag
            .unwrap_or(self.conf.default_tenant_conf.max_lsn_wal_lag);

        let mut guard = self.walreceiver.lock().unwrap();
        assert!(
            guard.is_none(),
            "multiple launches / re-launches of WAL receiver are not supported"
        );

        let protocol = PostgresClientProtocol::Interpreted {
            format: utils::postgres_client::InterpretedFormat::Protobuf,
            compression: Some(utils::postgres_client::Compression::Zstd { level: 1 }),
        };

        *guard = Some(WalReceiver::start(
            Arc::clone(self),
            WalReceiverConf {
                protocol,
                wal_connect_timeout,
                lagging_wal_timeout,
                max_lsn_wal_lag,
                auth_token: crate::config::SAFEKEEPER_AUTH_TOKEN.get().cloned(),
                availability_zone: self.conf.availability_zone.clone(),
                ingest_batch_size: self.conf.ingest_batch_size,
                validate_wal_contiguity: self.conf.validate_wal_contiguity,
            },
            broker_client,
            ctx,
        ));
    }

    /// Initialize with an empty layer map. Used when creating a new timeline.
    pub(super) fn init_empty_layer_map(&self, start_lsn: Lsn) {
        let mut layers = self.layers.try_write(LayerManagerLockHolder::Init).expect(
            "in the context where we call this function, no other task has access to the object",
        );
        layers
            .open_mut()
            .expect("in this context the LayerManager must still be open")
            .initialize_empty(Lsn(start_lsn.0));
    }

    /// Scan the timeline directory, cleanup, populate the layer map, and schedule uploads for local-only
    /// files.
    pub(super) async fn load_layer_map(
        &self,
        disk_consistent_lsn: Lsn,
        index_part: IndexPart,
    ) -> anyhow::Result<()> {
        use LayerName::*;
        use init::Decision::*;
        use init::{Discovered, DismissedLayer};

        let mut guard = self
            .layers
            .write(LayerManagerLockHolder::LoadLayerMap)
            .await;

        let timer = self.metrics.load_layer_map_histo.start_timer();

        // Scan timeline directory and create ImageLayerName and DeltaFilename
        // structs representing all files on disk
        let timeline_path = self
            .conf
            .timeline_path(&self.tenant_shard_id, &self.timeline_id);
        let conf = self.conf;
        let span = tracing::Span::current();

        // Copy to move into the task we're about to spawn
        let this = self.myself.upgrade().expect("&self method holds the arc");

        let (loaded_layers, needs_cleanup, total_physical_size) = tokio::task::spawn_blocking({
            move || {
                let _g = span.entered();
                let discovered = init::scan_timeline_dir(&timeline_path)?;
                let mut discovered_layers = Vec::with_capacity(discovered.len());
                let mut unrecognized_files = Vec::new();

                let mut path = timeline_path;

                for discovered in discovered {
                    let (name, kind) = match discovered {
                        Discovered::Layer(layer_file_name, local_metadata) => {
                            discovered_layers.push((layer_file_name, local_metadata));
                            continue;
                        }
                        Discovered::IgnoredBackup(path) => {
                            std::fs::remove_file(path)
                                .or_else(fs_ext::ignore_not_found)
                                .fatal_err("Removing .old file");
                            continue;
                        }
                        Discovered::Unknown(file_name) => {
                            // we will later error if there are any
                            unrecognized_files.push(file_name);
                            continue;
                        }
                        Discovered::Ephemeral(name) => (name, "old ephemeral file"),
                        Discovered::Temporary(name) => (name, "temporary timeline file"),
                        Discovered::TemporaryDownload(name) => (name, "temporary download"),
                    };
                    path.push(Utf8Path::new(&name));
                    init::cleanup(&path, kind)?;
                    path.pop();
                }

                if !unrecognized_files.is_empty() {
                    // assume that if there are any there are many many.
                    let n = unrecognized_files.len();
                    let first = &unrecognized_files[..n.min(10)];
                    anyhow::bail!(
                        "unrecognized files in timeline dir (total {n}), first 10: {first:?}"
                    );
                }

                let decided = init::reconcile(discovered_layers, &index_part, disk_consistent_lsn);

                let mut loaded_layers = Vec::new();
                let mut needs_cleanup = Vec::new();
                let mut total_physical_size = 0;

                for (name, decision) in decided {
                    let decision = match decision {
                        Ok(decision) => decision,
                        Err(DismissedLayer::Future { local }) => {
                            if let Some(local) = local {
                                init::cleanup_future_layer(
                                    &local.local_path,
                                    &name,
                                    disk_consistent_lsn,
                                )?;
                            }
                            needs_cleanup.push(name);
                            continue;
                        }
                        Err(DismissedLayer::LocalOnly(local)) => {
                            init::cleanup_local_only_file(&name, &local)?;
                            // this file never existed remotely, we will have to do rework
                            continue;
                        }
                        Err(DismissedLayer::BadMetadata(local)) => {
                            init::cleanup_local_file_for_remote(&local)?;
                            // this file never existed remotely, we will have to do rework
                            continue;
                        }
                    };

                    match &name {
                        Delta(d) => assert!(d.lsn_range.end <= disk_consistent_lsn + 1),
                        Image(i) => assert!(i.lsn <= disk_consistent_lsn),
                    }

                    tracing::debug!(layer=%name, ?decision, "applied");

                    let layer = match decision {
                        Resident { local, remote } => {
                            total_physical_size += local.file_size;
                            Layer::for_resident(conf, &this, local.local_path, name, remote)
                                .drop_eviction_guard()
                        }
                        Evicted(remote) => Layer::for_evicted(conf, &this, name, remote),
                    };

                    loaded_layers.push(layer);
                }
                Ok((loaded_layers, needs_cleanup, total_physical_size))
            }
        })
        .await
        .map_err(anyhow::Error::new)
        .and_then(|x| x)?;

        let num_layers = loaded_layers.len();

        guard
            .open_mut()
            .expect("layermanager must be open during init")
            .initialize_local_layers(loaded_layers, disk_consistent_lsn + 1);

        self.remote_client
            .schedule_layer_file_deletion(&needs_cleanup)?;
        self.remote_client
            .schedule_index_upload_for_file_changes()?;
        // This barrier orders above DELETEs before any later operations.
        // This is critical because code executing after the barrier might
        // create again objects with the same key that we just scheduled for deletion.
        // For example, if we just scheduled deletion of an image layer "from the future",
        // later compaction might run again and re-create the same image layer.
        // "from the future" here means an image layer whose LSN is > IndexPart::disk_consistent_lsn.
        // "same" here means same key range and LSN.
        //
        // Without a barrier between above DELETEs and the re-creation's PUTs,
        // the upload queue may execute the PUT first, then the DELETE.
        // In our example, we will end up with an IndexPart referencing a non-existent object.
        //
        // 1. a future image layer is created and uploaded
        // 2. ps restart
        // 3. the future layer from (1) is deleted during load layer map
        // 4. image layer is re-created and uploaded
        // 5. deletion queue would like to delete (1) but actually deletes (4)
        // 6. delete by name works as expected, but it now deletes the wrong (later) version
        //
        // See https://github.com/neondatabase/neon/issues/5878
        //
        // NB: generation numbers naturally protect against this because they disambiguate
        //     (1) and (4)
        // TODO: this is basically a no-op now, should we remove it?
        self.remote_client.schedule_barrier()?;
        // TenantShard::create_timeline will wait for these uploads to happen before returning, or
        // on retry.

        info!(
            "loaded layer map with {} layers at {}, total physical size: {}",
            num_layers, disk_consistent_lsn, total_physical_size
        );

        timer.stop_and_record();
        Ok(())
    }

    /// Retrieve current logical size of the timeline.
    ///
    /// The size could be lagging behind the actual number, in case
    /// the initial size calculation has not been run (gets triggered on the first size access).
    ///
    /// return size and boolean flag that shows if the size is exact
    pub(crate) fn get_current_logical_size(
        self: &Arc<Self>,
        priority: GetLogicalSizePriority,
        ctx: &RequestContext,
    ) -> logical_size::CurrentLogicalSize {
        if !self.tenant_shard_id.is_shard_zero() {
            // Logical size is only accurately maintained on shard zero: when called elsewhere, for example
            // when HTTP API is serving a GET for timeline zero, return zero
            return logical_size::CurrentLogicalSize::Approximate(logical_size::Approximate::zero());
        }

        let current_size = self.current_logical_size.current_size();
        debug!("Current size: {current_size:?}");

        match (current_size.accuracy(), priority) {
            (logical_size::Accuracy::Exact, _) => (), // nothing to do
            (logical_size::Accuracy::Approximate, GetLogicalSizePriority::Background) => {
                // background task will eventually deliver an exact value, we're in no rush
            }
            (logical_size::Accuracy::Approximate, GetLogicalSizePriority::User) => {
                // background task is not ready, but user is asking for it now;
                // => make the background task skip the line
                // (The alternative would be to calculate the size here, but,
                //  it can actually take a long time if the user has a lot of rels.
                //  And we'll inevitable need it again; So, let the background task do the work.)
                match self
                    .current_logical_size
                    .cancel_wait_for_background_loop_concurrency_limit_semaphore
                    .get()
                {
                    Some(cancel) => cancel.cancel(),
                    None => {
                        match self.current_state() {
                            TimelineState::Broken { .. } | TimelineState::Stopping => {
                                // Can happen when timeline detail endpoint is used when deletion is ongoing (or its broken).
                                // Don't make noise.
                            }
                            TimelineState::Loading => {
                                // Import does not return an activated timeline.
                                info!(
                                    "discarding priority boost for logical size calculation because timeline is not yet active"
                                );
                            }
                            TimelineState::Active => {
                                // activation should be setting the once cell
                                warn!(
                                    "unexpected: cancel_wait_for_background_loop_concurrency_limit_semaphore not set, priority-boosting of logical size calculation will not work"
                                );
                                debug_assert!(false);
                            }
                        }
                    }
                }
            }
        }

        if let CurrentLogicalSize::Approximate(_) = &current_size {
            if ctx.task_kind() == TaskKind::WalReceiverConnectionHandler {
                let first = self
                    .current_logical_size
                    .did_return_approximate_to_walreceiver
                    .compare_exchange(
                        false,
                        true,
                        AtomicOrdering::Relaxed,
                        AtomicOrdering::Relaxed,
                    )
                    .is_ok();
                if first {
                    crate::metrics::initial_logical_size::TIMELINES_WHERE_WALRECEIVER_GOT_APPROXIMATE_SIZE.inc();
                }
            }
        }

        current_size
    }

    fn spawn_initial_logical_size_computation_task(self: &Arc<Self>, ctx: &RequestContext) {
        let Some(initial_part_end) = self.current_logical_size.initial_part_end else {
            // nothing to do for freshly created timelines;
            assert_eq!(
                self.current_logical_size.current_size().accuracy(),
                logical_size::Accuracy::Exact,
            );
            self.current_logical_size.initialized.add_permits(1);
            return;
        };

        let cancel_wait_for_background_loop_concurrency_limit_semaphore = CancellationToken::new();
        let token = cancel_wait_for_background_loop_concurrency_limit_semaphore.clone();
        self.current_logical_size
            .cancel_wait_for_background_loop_concurrency_limit_semaphore.set(token)
            .expect("initial logical size calculation task must be spawned exactly once per Timeline object");

        let self_clone = Arc::clone(self);
        let background_ctx = ctx.detached_child(
            TaskKind::InitialLogicalSizeCalculation,
            DownloadBehavior::Download,
        );
        task_mgr::spawn(
            task_mgr::BACKGROUND_RUNTIME.handle(),
            task_mgr::TaskKind::InitialLogicalSizeCalculation,
            self.tenant_shard_id,
            Some(self.timeline_id),
            "initial size calculation",
            // NB: don't log errors here, task_mgr will do that.
            async move {
                self_clone
                    .initial_logical_size_calculation_task(
                        initial_part_end,
                        cancel_wait_for_background_loop_concurrency_limit_semaphore,
                        background_ctx,
                    )
                    .await;
                Ok(())
            }
            .instrument(info_span!(parent: None, "initial_size_calculation", tenant_id=%self.tenant_shard_id.tenant_id, shard_id=%self.tenant_shard_id.shard_slug(), timeline_id=%self.timeline_id)),
        );
    }

    /// # Cancellation
    ///
    /// This method is sensitive to `Timeline::cancel`.
    ///
    /// It is _not_ sensitive to task_mgr::shutdown_token().
    ///
    /// # Cancel-Safety
    ///
    /// It does Timeline IO, hence this should be polled to completion because
    /// we could be leaving in-flight IOs behind, which is safe, but annoying
    /// to reason about.
    async fn initial_logical_size_calculation_task(
        self: Arc<Self>,
        initial_part_end: Lsn,
        skip_concurrency_limiter: CancellationToken,
        background_ctx: RequestContext,
    ) {
        scopeguard::defer! {
            // Irrespective of the outcome of this operation, we should unblock anyone waiting for it.
            self.current_logical_size.initialized.add_permits(1);
        }

        let try_once = |attempt: usize| {
            let background_ctx = &background_ctx;
            let self_ref = &self;
            let skip_concurrency_limiter = &skip_concurrency_limiter;
            async move {
                let wait_for_permit = super::tasks::acquire_concurrency_permit(
                    BackgroundLoopKind::InitialLogicalSizeCalculation,
                    background_ctx,
                );

                use crate::metrics::initial_logical_size::StartCircumstances;
                let (_maybe_permit, circumstances) = tokio::select! {
                    permit = wait_for_permit => {
                        (Some(permit), StartCircumstances::AfterBackgroundTasksRateLimit)
                    }
                    _ = self_ref.cancel.cancelled() => {
                        return Err(CalculateLogicalSizeError::Cancelled);
                    }
                    () = skip_concurrency_limiter.cancelled() => {
                        // Some action that is part of a end user interaction requested logical size
                        // => break out of the rate limit
                        // TODO: ideally we'd not run on BackgroundRuntime but the requester's runtime;
                        // but then again what happens if they cancel; also, we should just be using
                        // one runtime across the entire process, so, let's leave this for now.
                        (None, StartCircumstances::SkippedConcurrencyLimiter)
                    }
                };

                let metrics_guard = if attempt == 1 {
                    crate::metrics::initial_logical_size::START_CALCULATION.first(circumstances)
                } else {
                    crate::metrics::initial_logical_size::START_CALCULATION.retry(circumstances)
                };

                let io_concurrency = IoConcurrency::spawn_from_conf(
                    self_ref.conf.get_vectored_concurrent_io,
                    self_ref
                        .gate
                        .enter()
                        .map_err(|_| CalculateLogicalSizeError::Cancelled)?,
                );

                let calculated_size = self_ref
                    .logical_size_calculation_task(
                        initial_part_end,
                        LogicalSizeCalculationCause::Initial,
                        background_ctx,
                    )
                    .await?;

                self_ref
                    .trigger_aux_file_size_computation(
                        initial_part_end,
                        background_ctx,
                        io_concurrency,
                    )
                    .await?;

                // TODO: add aux file size to logical size

                Ok((calculated_size, metrics_guard))
            }
        };

        let retrying = async {
            let mut attempt = 0;
            loop {
                attempt += 1;

                match try_once(attempt).await {
                    Ok(res) => return ControlFlow::Continue(res),
                    Err(CalculateLogicalSizeError::Cancelled) => return ControlFlow::Break(()),
                    Err(
                        e @ (CalculateLogicalSizeError::Decode(_)
                        | CalculateLogicalSizeError::PageRead(_)),
                    ) => {
                        warn!(attempt, "initial size calculation failed: {e:?}");
                        // exponential back-off doesn't make sense at these long intervals;
                        // use fixed retry interval with generous jitter instead
                        let sleep_duration = Duration::from_secs(
                            u64::try_from(
                                // 1hour base
                                (60_i64 * 60_i64)
                                    // 10min jitter
                                    + rand::thread_rng().gen_range(-10 * 60..10 * 60),
                            )
                            .expect("10min < 1hour"),
                        );
                        tokio::select! {
                            _ = tokio::time::sleep(sleep_duration) => {}
                            _ = self.cancel.cancelled() => return ControlFlow::Break(()),
                        }
                    }
                }
            }
        };

        let (calculated_size, metrics_guard) = match retrying.await {
            ControlFlow::Continue(calculated_size) => calculated_size,
            ControlFlow::Break(()) => return,
        };

        // we cannot query current_logical_size.current_size() to know the current
        // *negative* value, only truncated to u64.
        let added = self
            .current_logical_size
            .size_added_after_initial
            .load(AtomicOrdering::Relaxed);

        let sum = calculated_size.saturating_add_signed(added);

        // set the gauge value before it can be set in `update_current_logical_size`.
        self.metrics.current_logical_size_gauge.set(sum);

        self.current_logical_size
            .initial_logical_size
            .set((calculated_size, metrics_guard.calculation_result_saved()))
            .ok()
            .expect("only this task sets it");
    }

    pub(crate) fn spawn_ondemand_logical_size_calculation(
        self: &Arc<Self>,
        lsn: Lsn,
        cause: LogicalSizeCalculationCause,
        ctx: RequestContext,
    ) -> oneshot::Receiver<Result<u64, CalculateLogicalSizeError>> {
        let (sender, receiver) = oneshot::channel();
        let self_clone = Arc::clone(self);
        // XXX if our caller loses interest, i.e., ctx is cancelled,
        // we should stop the size calculation work and return an error.
        // That would require restructuring this function's API to
        // return the result directly, instead of a Receiver for the result.
        let ctx = ctx.detached_child(
            TaskKind::OndemandLogicalSizeCalculation,
            DownloadBehavior::Download,
        );
        task_mgr::spawn(
            task_mgr::BACKGROUND_RUNTIME.handle(),
            task_mgr::TaskKind::OndemandLogicalSizeCalculation,
            self.tenant_shard_id,
            Some(self.timeline_id),
            "ondemand logical size calculation",
            async move {
                let res = self_clone
                    .logical_size_calculation_task(lsn, cause, &ctx)
                    .await;
                let _ = sender.send(res).ok();
                Ok(()) // Receiver is responsible for handling errors
            }
            .in_current_span(),
        );
        receiver
    }

    #[instrument(skip_all)]
    async fn logical_size_calculation_task(
        self: &Arc<Self>,
        lsn: Lsn,
        cause: LogicalSizeCalculationCause,
        ctx: &RequestContext,
    ) -> Result<u64, CalculateLogicalSizeError> {
        crate::span::debug_assert_current_span_has_tenant_and_timeline_id();
        // We should never be calculating logical sizes on shard !=0, because these shards do not have
        // accurate relation sizes, and they do not emit consumption metrics.
        debug_assert!(self.tenant_shard_id.is_shard_zero());

        let guard = self
            .gate
            .enter()
            .map_err(|_| CalculateLogicalSizeError::Cancelled)?;

        self.calculate_logical_size(lsn, cause, &guard, ctx).await
    }

    /// Calculate the logical size of the database at the latest LSN.
    ///
    /// NOTE: counted incrementally, includes ancestors. This can be a slow operation,
    /// especially if we need to download remote layers.
    async fn calculate_logical_size(
        &self,
        up_to_lsn: Lsn,
        cause: LogicalSizeCalculationCause,
        _guard: &GateGuard,
        ctx: &RequestContext,
    ) -> Result<u64, CalculateLogicalSizeError> {
        info!(
            "Calculating logical size for timeline {} at {}",
            self.timeline_id, up_to_lsn
        );

        if let Err(()) = pausable_failpoint!("timeline-calculate-logical-size-pause", &self.cancel)
        {
            return Err(CalculateLogicalSizeError::Cancelled);
        }

        // See if we've already done the work for initial size calculation.
        // This is a short-cut for timelines that are mostly unused.
        if let Some(size) = self.current_logical_size.initialized_size(up_to_lsn) {
            return Ok(size);
        }
        let storage_time_metrics = match cause {
            LogicalSizeCalculationCause::Initial
            | LogicalSizeCalculationCause::ConsumptionMetricsSyntheticSize
            | LogicalSizeCalculationCause::TenantSizeHandler => &self.metrics.logical_size_histo,
            LogicalSizeCalculationCause::EvictionTaskImitation => {
                &self.metrics.imitate_logical_size_histo
            }
        };
        let timer = storage_time_metrics.start_timer();
        let logical_size = self
            .get_current_logical_size_non_incremental(up_to_lsn, ctx)
            .await?;
        debug!("calculated logical size: {logical_size}");
        timer.stop_and_record();
        Ok(logical_size)
    }

    /// Update current logical size, adding `delta' to the old value.
    fn update_current_logical_size(&self, delta: i64) {
        let logical_size = &self.current_logical_size;
        logical_size.increment_size(delta);

        // Also set the value in the prometheus gauge. Note that
        // there is a race condition here: if this is is called by two
        // threads concurrently, the prometheus gauge might be set to
        // one value while current_logical_size is set to the
        // other.
        match logical_size.current_size() {
            CurrentLogicalSize::Exact(ref new_current_size) => self
                .metrics
                .current_logical_size_gauge
                .set(new_current_size.into()),
            CurrentLogicalSize::Approximate(_) => {
                // don't update the gauge yet, this allows us not to update the gauge back and
                // forth between the initial size calculation task.
            }
        }
    }

    pub(crate) fn update_directory_entries_count(&self, kind: DirectoryKind, count: MetricsUpdate) {
        // TODO: this directory metrics is not correct -- we could have multiple reldirs in the system
        // for each of the database, but we only store one value, and therefore each pgdirmodification
        // would overwrite the previous value if they modify different databases.

        match count {
            MetricsUpdate::Set(count) => {
                self.directory_metrics[kind.offset()].store(count, AtomicOrdering::Relaxed);
                self.directory_metrics_inited[kind.offset()].store(true, AtomicOrdering::Relaxed);
            }
            MetricsUpdate::Add(count) => {
                // TODO: these operations are not atomic; but we only have one writer to the metrics, so
                // it's fine.
                if self.directory_metrics_inited[kind.offset()].load(AtomicOrdering::Relaxed) {
                    // The metrics has been initialized with `MetricsUpdate::Set` before, so we can add/sub
                    // the value reliably.
                    self.directory_metrics[kind.offset()].fetch_add(count, AtomicOrdering::Relaxed);
                }
                // Otherwise, ignore this update
            }
            MetricsUpdate::Sub(count) => {
                // TODO: these operations are not atomic; but we only have one writer to the metrics, so
                // it's fine.
                if self.directory_metrics_inited[kind.offset()].load(AtomicOrdering::Relaxed) {
                    // The metrics has been initialized with `MetricsUpdate::Set` before.
                    // The operation could overflow so we need to normalize the value.
                    let prev_val =
                        self.directory_metrics[kind.offset()].load(AtomicOrdering::Relaxed);
                    let res = prev_val.saturating_sub(count);
                    self.directory_metrics[kind.offset()].store(res, AtomicOrdering::Relaxed);
                }
                // Otherwise, ignore this update
            }
        };

        // TODO: remove this, there's no place in the code that updates this aux metrics.
        let aux_metric =
            self.directory_metrics[DirectoryKind::AuxFiles.offset()].load(AtomicOrdering::Relaxed);

        let sum_of_entries = self
            .directory_metrics
            .iter()
            .map(|v| v.load(AtomicOrdering::Relaxed))
            .sum();
        // Set a high general threshold and a lower threshold for the auxiliary files,
        // as we can have large numbers of relations in the db directory.
        const SUM_THRESHOLD: u64 = 5000;
        const AUX_THRESHOLD: u64 = 1000;
        if sum_of_entries >= SUM_THRESHOLD || aux_metric >= AUX_THRESHOLD {
            self.metrics
                .directory_entries_count_gauge
                .set(sum_of_entries);
        } else if let Some(metric) = Lazy::get(&self.metrics.directory_entries_count_gauge) {
            metric.set(sum_of_entries);
        }
    }

    async fn find_layer(
        &self,
        layer_name: &LayerName,
    ) -> Result<Option<Layer>, layer_manager::Shutdown> {
        let guard = self
            .layers
            .read(LayerManagerLockHolder::GetLayerMapInfo)
            .await;
        let layer = guard
            .layer_map()?
            .iter_historic_layers()
            .find(|l| &l.layer_name() == layer_name)
            .map(|found| guard.get_from_desc(&found));
        Ok(layer)
    }

    pub(super) fn should_keep_previous_heatmap(&self, new_heatmap_end_lsn: Lsn) -> bool {
        let crnt = self.previous_heatmap.load();
        match crnt.as_deref() {
            Some(PreviousHeatmap::Active { end_lsn, .. }) => match end_lsn {
                Some(crnt_end_lsn) => *crnt_end_lsn > new_heatmap_end_lsn,
                None => true,
            },
            Some(PreviousHeatmap::Obsolete) => false,
            None => false,
        }
    }

    /// The timeline heatmap is a hint to secondary locations from the primary location,
    /// indicating which layers are currently on-disk on the primary.
    ///
    /// None is returned if the Timeline is in a state where uploading a heatmap
    /// doesn't make sense, such as shutting down or initializing.  The caller
    /// should treat this as a cue to simply skip doing any heatmap uploading
    /// for this timeline.
    pub(crate) async fn generate_heatmap(&self) -> Option<HeatMapTimeline> {
        if !self.is_active() {
            return None;
        }

        let guard = self
            .layers
            .read(LayerManagerLockHolder::GenerateHeatmap)
            .await;

        // Firstly, if there's any heatmap left over from when this location
        // was a secondary, take that into account. Keep layers that are:
        // * present in the layer map
        // * visible
        // * non-resident
        // * not evicted since we read the heatmap
        //
        // Without this, a new cold, attached location would clobber the previous
        // heatamp.
        let previous_heatmap = self.previous_heatmap.load();
        let visible_non_resident = match previous_heatmap.as_deref() {
            Some(PreviousHeatmap::Active {
                heatmap, read_at, ..
            }) => Some(heatmap.all_layers().filter_map(|hl| {
                let desc: PersistentLayerDesc = hl.name.clone().into();
                let layer = guard.try_get_from_key(&desc.key())?;

                if layer.visibility() == LayerVisibilityHint::Covered {
                    return None;
                }

                if layer.is_likely_resident() {
                    return None;
                }

                if layer.last_evicted_at().happened_after(*read_at) {
                    return None;
                }

                Some((desc, hl.metadata.clone(), hl.access_time, hl.cold))
            })),
            Some(PreviousHeatmap::Obsolete) => None,
            None => None,
        };

        // Secondly, all currently visible, resident layers are included.
        let resident = guard.likely_resident_layers().filter_map(|layer| {
            match layer.visibility() {
                LayerVisibilityHint::Visible => {
                    // Layer is visible to one or more read LSNs: elegible for inclusion in layer map
                    let last_activity_ts = layer.latest_activity();
                    Some((
                        layer.layer_desc().clone(),
                        layer.metadata(),
                        last_activity_ts,
                        false, // these layers are not cold
                    ))
                }
                LayerVisibilityHint::Covered => {
                    // Layer is resident but unlikely to be read: not elegible for inclusion in heatmap.
                    None
                }
            }
        });

        let mut layers = match visible_non_resident {
            Some(non_resident) => {
                let mut non_resident = non_resident.peekable();
                if non_resident.peek().is_none() {
                    tracing::info!(timeline_id=%self.timeline_id, "Previous heatmap now obsolete");
                    self.previous_heatmap
                        .store(Some(PreviousHeatmap::Obsolete.into()));
                }

                non_resident.chain(resident).collect::<Vec<_>>()
            }
            None => resident.collect::<Vec<_>>(),
        };

        // Sort layers in order of which to download first.  For a large set of layers to download, we
        // want to prioritize those layers which are most likely to still be in the resident many minutes
        // or hours later:
        // - Cold layers go last for convenience when a human inspects the heatmap.
        // - Download L0s last, because they churn the fastest: L0s on a fast-writing tenant might
        //   only exist for a few minutes before being compacted into L1s.
        // - For L1 & image layers, download most recent LSNs first: the older the LSN, the sooner
        //   the layer is likely to be covered by an image layer during compaction.
        layers.sort_by_key(|(desc, _meta, _atime, cold)| {
            std::cmp::Reverse((
                *cold,
                !LayerMap::is_l0(&desc.key_range, desc.is_delta),
                desc.lsn_range.end,
            ))
        });

        let layers = layers
            .into_iter()
            .map(|(desc, meta, atime, cold)| {
                HeatMapLayer::new(desc.layer_name(), meta, atime, cold)
            })
            .collect();

        Some(HeatMapTimeline::new(self.timeline_id, layers))
    }

    pub(super) async fn generate_unarchival_heatmap(&self, end_lsn: Lsn) -> PreviousHeatmap {
        let guard = self
            .layers
            .read(LayerManagerLockHolder::GenerateHeatmap)
            .await;

        let now = SystemTime::now();
        let mut heatmap_layers = Vec::default();
        for vl in guard.visible_layers() {
            if vl.layer_desc().get_lsn_range().start >= end_lsn {
                continue;
            }

            let hl = HeatMapLayer {
                name: vl.layer_desc().layer_name(),
                metadata: vl.metadata(),
                access_time: now,
                cold: true,
            };
            heatmap_layers.push(hl);
        }

        tracing::info!(
            "Generating unarchival heatmap with {} layers",
            heatmap_layers.len()
        );

        let heatmap = HeatMapTimeline::new(self.timeline_id, heatmap_layers);
        PreviousHeatmap::Active {
            heatmap,
            read_at: Instant::now(),
            end_lsn: Some(end_lsn),
        }
    }

    /// Returns true if the given lsn is or was an ancestor branchpoint.
    pub(crate) fn is_ancestor_lsn(&self, lsn: Lsn) -> bool {
        // upon timeline detach, we set the ancestor_lsn to Lsn::INVALID and the store the original
        // branchpoint in the value in IndexPart::lineage
        self.ancestor_lsn == lsn
            || (self.ancestor_lsn == Lsn::INVALID
                && self.remote_client.is_previous_ancestor_lsn(lsn))
    }
}

#[derive(Clone)]
/// Type representing a query in the ([`Lsn`], [`Key`]) space.
/// In other words, a set of segments in a 2D space.
///
/// This representation has the advatange of avoiding hash map
/// allocations for uniform queries.
pub(crate) enum VersionedKeySpaceQuery {
    /// Variant for queries at a single [`Lsn`]
    Uniform { keyspace: KeySpace, lsn: Lsn },
    /// Variant for queries at multiple [`Lsn`]s
    Scattered {
        keyspaces_at_lsn: Vec<(Lsn, KeySpace)>,
    },
}

impl VersionedKeySpaceQuery {
    pub(crate) fn uniform(keyspace: KeySpace, lsn: Lsn) -> Self {
        Self::Uniform { keyspace, lsn }
    }

    pub(crate) fn scattered(keyspaces_at_lsn: Vec<(Lsn, KeySpace)>) -> Self {
        Self::Scattered { keyspaces_at_lsn }
    }

    /// Returns the most recent (largest) LSN included in the query.
    /// If any of the LSNs included in the query are invalid, returns
    /// an error instead.
    fn high_watermark_lsn(&self) -> Result<Lsn, GetVectoredError> {
        match self {
            Self::Uniform { lsn, .. } => {
                if !lsn.is_valid() {
                    return Err(GetVectoredError::InvalidLsn(*lsn));
                }

                Ok(*lsn)
            }
            Self::Scattered { keyspaces_at_lsn } => {
                let mut max_lsn = None;
                for (lsn, _keyspace) in keyspaces_at_lsn.iter() {
                    if !lsn.is_valid() {
                        return Err(GetVectoredError::InvalidLsn(*lsn));
                    }
                    max_lsn = std::cmp::max(max_lsn, Some(lsn));
                }

                if let Some(computed) = max_lsn {
                    Ok(*computed)
                } else {
                    Err(GetVectoredError::Other(anyhow!("empty input")))
                }
            }
        }
    }

    /// Returns the total keyspace being queried: the result of projecting
    /// everything in the key dimensions onto the key axis.
    fn total_keyspace(&self) -> KeySpace {
        match self {
            Self::Uniform { keyspace, .. } => keyspace.clone(),
            Self::Scattered { keyspaces_at_lsn } => keyspaces_at_lsn
                .iter()
                .map(|(_lsn, keyspace)| keyspace)
                .fold(KeySpace::default(), |mut acc, v| {
                    acc.merge(v);
                    acc
                }),
        }
    }

    /// Returns LSN for a specific key.
    ///
    /// Invariant: requested key must be part of [`Self::total_keyspace`]
    pub(super) fn map_key_to_lsn(&self, key: &Key) -> Lsn {
        match self {
            Self::Uniform { lsn, .. } => *lsn,
            Self::Scattered { keyspaces_at_lsn } => {
                keyspaces_at_lsn
                    .iter()
                    .find(|(_lsn, keyspace)| keyspace.contains(key))
                    .expect("Returned key was requested")
                    .0
            }
        }
    }

    /// Remove any parts of the query (segments) which overlap with the provided
    /// key space (also segments).
    fn remove_overlapping_with(&mut self, to_remove: &KeySpace) -> KeySpace {
        match self {
            Self::Uniform { keyspace, .. } => keyspace.remove_overlapping_with(to_remove),
            Self::Scattered { keyspaces_at_lsn } => {
                let mut removed_accum = KeySpaceRandomAccum::new();
                keyspaces_at_lsn.iter_mut().for_each(|(_lsn, keyspace)| {
                    let removed = keyspace.remove_overlapping_with(to_remove);
                    removed_accum.add_keyspace(removed);
                });

                removed_accum.to_keyspace()
            }
        }
    }

    fn is_empty(&self) -> bool {
        match self {
            Self::Uniform { keyspace, .. } => keyspace.is_empty(),
            Self::Scattered { keyspaces_at_lsn } => keyspaces_at_lsn
                .iter()
                .all(|(_lsn, keyspace)| keyspace.is_empty()),
        }
    }

    /// "Lower" the query on the LSN dimension
    fn lower(&mut self, to: Lsn) {
        match self {
            Self::Uniform { lsn, .. } => {
                // If the originally requested LSN is smaller than the starting
                // LSN of the ancestor we are descending into, we need to respect that.
                // Hence the min.
                *lsn = std::cmp::min(*lsn, to);
            }
            Self::Scattered { keyspaces_at_lsn } => {
                keyspaces_at_lsn.iter_mut().for_each(|(lsn, _keyspace)| {
                    *lsn = std::cmp::min(*lsn, to);
                });
            }
        }
    }
}

impl std::fmt::Display for VersionedKeySpaceQuery {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[")?;

        match self {
            VersionedKeySpaceQuery::Uniform { keyspace, lsn } => {
                write!(f, "{keyspace} @ {lsn}")?;
            }
            VersionedKeySpaceQuery::Scattered { keyspaces_at_lsn } => {
                for (lsn, keyspace) in keyspaces_at_lsn.iter() {
                    write!(f, "{keyspace} @ {lsn},")?;
                }
            }
        }

        write!(f, "]")
    }
}

impl Timeline {
    #[allow(clippy::doc_lazy_continuation)]
    /// Get the data needed to reconstruct all keys in the provided keyspace
    ///
    /// The algorithm is as follows:
    /// 1.   While some keys are still not done and there's a timeline to visit:
    /// 2.   Visit the timeline (see [`Timeline::get_vectored_reconstruct_data_timeline`]:
    /// 2.1: Build the fringe for the current keyspace
    /// 2.2  Visit the newest layer from the fringe to collect all values for the range it
    ///      intersects
    /// 2.3. Pop the timeline from the fringe
    /// 2.4. If the fringe is empty, go back to 1
    async fn get_vectored_reconstruct_data(
        &self,
        mut query: VersionedKeySpaceQuery,
        reconstruct_state: &mut ValuesReconstructState,
        ctx: &RequestContext,
    ) -> Result<(), GetVectoredError> {
        let original_hwm_lsn = query.high_watermark_lsn().unwrap();

        let mut timeline_owned: Arc<Timeline>;
        let mut timeline = self;

        let missing_keyspace = loop {
            if self.cancel.is_cancelled() {
                return Err(GetVectoredError::Cancelled);
            }

            let TimelineVisitOutcome {
                completed_keyspace: completed,
                image_covered_keyspace,
            } = {
                let ctx = RequestContextBuilder::from(ctx)
                    .perf_span(|crnt_perf_span| {
                        info_span!(
                            target: PERF_TRACE_TARGET,
                            parent: crnt_perf_span,
                            "PLAN_IO_TIMELINE",
                            timeline = %timeline.timeline_id,
                            high_watermark_lsn = %query.high_watermark_lsn().unwrap(),
                        )
                    })
                    .attached_child();

                Self::get_vectored_reconstruct_data_timeline(
                    timeline,
                    &query,
                    reconstruct_state,
                    &self.cancel,
                    &ctx,
                )
                .maybe_perf_instrument(&ctx, |crnt_perf_span| crnt_perf_span.clone())
                .await?
            };

            query.remove_overlapping_with(&completed);

            // Do not descend into the ancestor timeline for aux files.
            // We don't return a blanket [`GetVectoredError::MissingKey`] to avoid
            // stalling compaction.
            query.remove_overlapping_with(&KeySpace {
                ranges: vec![NON_INHERITED_RANGE, Key::sparse_non_inherited_keyspace()],
            });

            // Keyspace is fully retrieved
            if query.is_empty() {
                break None;
            }

            let Some(ancestor_timeline) = timeline.ancestor_timeline.as_ref() else {
                // Not fully retrieved but no ancestor timeline.
                break Some(query.total_keyspace());
            };

            // Now we see if there are keys covered by the image layer but does not exist in the
            // image layer, which means that the key does not exist.

            // The block below will stop the vectored search if any of the keys encountered an image layer
            // which did not contain a snapshot for said key. Since we have already removed all completed
            // keys from `keyspace`, we expect there to be no overlap between it and the image covered key
            // space. If that's not the case, we had at least one key encounter a gap in the image layer
            // and stop the search as a result of that.
            let mut removed = query.remove_overlapping_with(&image_covered_keyspace);
            // Do not fire missing key error and end early for sparse keys. Note that we hava already removed
            // non-inherited keyspaces before, so we can safely do a full `SPARSE_RANGE` remove instead of
            // figuring out what is the inherited key range and do a fine-grained pruning.
            removed.remove_overlapping_with(&KeySpace {
                ranges: vec![SPARSE_RANGE],
            });
            if !removed.is_empty() {
                break Some(removed);
            }

            // Each key range in the original query is at some point in the LSN space.
            // When descending into the ancestor, lower all ranges in the LSN space
            // such that new changes on the parent timeline are not visible.
            query.lower(timeline.ancestor_lsn);

            let ctx = RequestContextBuilder::from(ctx)
                .perf_span(|crnt_perf_span| {
                    info_span!(
                        target: PERF_TRACE_TARGET,
                        parent: crnt_perf_span,
                        "GET_ANCESTOR",
                        timeline = %timeline.timeline_id,
                        ancestor = %ancestor_timeline.timeline_id,
                        ancestor_lsn = %timeline.ancestor_lsn
                    )
                })
                .attached_child();

            timeline_owned = timeline
                .get_ready_ancestor_timeline(ancestor_timeline, &ctx)
                .maybe_perf_instrument(&ctx, |crnt_perf_span| crnt_perf_span.clone())
                .await?;
            timeline = &*timeline_owned;
        };

        // Remove sparse keys from the keyspace so that it doesn't fire errors.
        let missing_keyspace = if let Some(missing_keyspace) = missing_keyspace {
            let mut missing_keyspace = missing_keyspace;
            missing_keyspace.remove_overlapping_with(&KeySpace {
                ranges: vec![SPARSE_RANGE],
            });
            if missing_keyspace.is_empty() {
                None
            } else {
                Some(missing_keyspace)
            }
        } else {
            None
        };

        if let Some(missing_keyspace) = missing_keyspace {
            return Err(GetVectoredError::MissingKey(Box::new(MissingKeyError {
                keyspace: missing_keyspace, /* better if we can store the full keyspace */
                shard: self.shard_identity.number,
                original_hwm_lsn,
                ancestor_lsn: Some(timeline.ancestor_lsn),
                backtrace: None,
                read_path: std::mem::take(&mut reconstruct_state.read_path),
                query: None,
            })));
        }

        Ok(())
    }

    async fn get_vectored_init_fringe(
        &self,
        query: &VersionedKeySpaceQuery,
    ) -> Result<LayerFringe, GetVectoredError> {
        let mut fringe = LayerFringe::new();
        let guard = self.layers.read(LayerManagerLockHolder::GetPage).await;

        match query {
            VersionedKeySpaceQuery::Uniform { keyspace, lsn } => {
                // LSNs requested by the compute or determined by the pageserver
                // are inclusive. Queries to the layer map use exclusive LSNs.
                // Hence, bump the value before the query - same in the other
                // match arm.
                let cont_lsn = Lsn(lsn.0 + 1);
                guard.update_search_fringe(keyspace, cont_lsn, &mut fringe)?;
            }
            VersionedKeySpaceQuery::Scattered { keyspaces_at_lsn } => {
                for (lsn, keyspace) in keyspaces_at_lsn.iter() {
                    let cont_lsn_for_keyspace = Lsn(lsn.0 + 1);
                    guard.update_search_fringe(keyspace, cont_lsn_for_keyspace, &mut fringe)?;
                }
            }
        }

        Ok(fringe)
    }

    /// Collect the reconstruct data for a keyspace from the specified timeline.
    ///
    /// Maintain a fringe [`LayerFringe`] which tracks all the layers that intersect
    /// the current keyspace. The current keyspace of the search at any given timeline
    /// is the original keyspace minus all the keys that have been completed minus
    /// any keys for which we couldn't find an intersecting layer. It's not tracked explicitly,
    /// but if you merge all the keyspaces in the fringe, you get the "current keyspace".
    ///
    /// This is basically a depth-first search visitor implementation where a vertex
    /// is the (layer, lsn range, key space) tuple. The fringe acts as the stack.
    ///
    /// At each iteration pop the top of the fringe (the layer with the highest Lsn)
    /// and get all the required reconstruct data from the layer in one go.
    ///
    /// Returns the completed keyspace and the keyspaces with image coverage. The caller
    /// decides how to deal with these two keyspaces.
    async fn get_vectored_reconstruct_data_timeline(
        timeline: &Timeline,
        query: &VersionedKeySpaceQuery,
        reconstruct_state: &mut ValuesReconstructState,
        cancel: &CancellationToken,
        ctx: &RequestContext,
    ) -> Result<TimelineVisitOutcome, GetVectoredError> {
        // Prevent GC from progressing while visiting the current timeline.
        // If we are GC-ing because a new image layer was added while traversing
        // the timeline, then it will remove layers that are required for fulfilling
        // the current get request (read-path cannot "look back" and notice the new
        // image layer).
        let _gc_cutoff_holder = timeline.get_applied_gc_cutoff_lsn();

        // See `compaction::compact_with_gc` for why we need this.
        let _guard = timeline.gc_compaction_layer_update_lock.read().await;

        // Initialize the fringe
        let mut fringe = timeline.get_vectored_init_fringe(query).await?;

        let mut completed_keyspace = KeySpace::default();
        let mut image_covered_keyspace = KeySpaceRandomAccum::new();

        while let Some((layer_to_read, keyspace_to_read, lsn_range)) = fringe.next_layer() {
            if cancel.is_cancelled() {
                return Err(GetVectoredError::Cancelled);
            }

            if let Some(ref mut read_path) = reconstruct_state.read_path {
                read_path.record_layer_visit(&layer_to_read, &keyspace_to_read, &lsn_range);
            }

            // Visit the layer and plan IOs for it
            let next_cont_lsn = lsn_range.start;
            layer_to_read
                .get_values_reconstruct_data(
                    keyspace_to_read.clone(),
                    lsn_range,
                    reconstruct_state,
                    ctx,
                )
                .await?;

            let mut unmapped_keyspace = keyspace_to_read;
            let cont_lsn = next_cont_lsn;

            reconstruct_state.on_layer_visited(&layer_to_read);

            let (keys_done_last_step, keys_with_image_coverage) =
                reconstruct_state.consume_done_keys();
            unmapped_keyspace.remove_overlapping_with(&keys_done_last_step);
            completed_keyspace.merge(&keys_done_last_step);
            if let Some(keys_with_image_coverage) = keys_with_image_coverage {
                unmapped_keyspace
                    .remove_overlapping_with(&KeySpace::single(keys_with_image_coverage.clone()));
                image_covered_keyspace.add_range(keys_with_image_coverage);
            }

            // Query the layer map for the next layers to read.
            //
            // Do not descent any further if the last layer we visited
            // completed all keys in the keyspace it inspected. This is not
            // required for correctness, but avoids visiting extra layers
            // which turns out to be a perf bottleneck in some cases.
            if !unmapped_keyspace.is_empty() {
                let guard = timeline.layers.read(LayerManagerLockHolder::GetPage).await;
                guard.update_search_fringe(&unmapped_keyspace, cont_lsn, &mut fringe)?;

                // It's safe to drop the layer map lock after planning the next round of reads.
                // The fringe keeps readable handles for the layers which are safe to read even
                // if layers were compacted or flushed.
                //
                // The more interesting consideration is: "Why is the read algorithm still correct
                // if the layer map changes while it is operating?". Doing a vectored read on a
                // timeline boils down to pushing an imaginary lsn boundary downwards for each range
                // covered by the read. The layer map tells us how to move the lsn downwards for a
                // range at *a particular point in time*. It is fine for the answer to be different
                // at two different time points.
                drop(guard);
            }
        }

        Ok(TimelineVisitOutcome {
            completed_keyspace,
            image_covered_keyspace: image_covered_keyspace.consume_keyspace(),
        })
    }

    async fn get_ready_ancestor_timeline(
        &self,
        ancestor: &Arc<Timeline>,
        ctx: &RequestContext,
    ) -> Result<Arc<Timeline>, GetReadyAncestorError> {
        // It's possible that the ancestor timeline isn't active yet, or
        // is active but hasn't yet caught up to the branch point. Wait
        // for it.
        //
        // This cannot happen while the pageserver is running normally,
        // because you cannot create a branch from a point that isn't
        // present in the pageserver yet. However, we don't wait for the
        // branch point to be uploaded to cloud storage before creating
        // a branch. I.e., the branch LSN need not be remote consistent
        // for the branching operation to succeed.
        //
        // Hence, if we try to load a tenant in such a state where
        // 1. the existence of the branch was persisted (in IndexPart and/or locally)
        // 2. but the ancestor state is behind branch_lsn because it was not yet persisted
        // then we will need to wait for the ancestor timeline to
        // re-stream WAL up to branch_lsn before we access it.
        //
        // How can a tenant get in such a state?
        // - ungraceful pageserver process exit
        // - detach+attach => this is a bug, https://github.com/neondatabase/neon/issues/4219
        //
        // NB: this could be avoided by requiring
        //   branch_lsn >= remote_consistent_lsn
        // during branch creation.
        match ancestor.wait_to_become_active(ctx).await {
            Ok(()) => {}
            Err(TimelineState::Stopping) => {
                // If an ancestor is stopping, it means the tenant is stopping: handle this the same as if this timeline was stopping.
                return Err(GetReadyAncestorError::Cancelled);
            }
            Err(state) => {
                return Err(GetReadyAncestorError::BadState {
                    timeline_id: ancestor.timeline_id,
                    state,
                });
            }
        }
        ancestor
            .wait_lsn(
                self.ancestor_lsn,
                WaitLsnWaiter::Timeline(self),
                WaitLsnTimeout::Default,
                ctx,
            )
            .await
            .map_err(|e| match e {
                e @ WaitLsnError::Timeout(_) => GetReadyAncestorError::AncestorLsnTimeout(e),
                WaitLsnError::Shutdown => GetReadyAncestorError::Cancelled,
                WaitLsnError::BadState(state) => GetReadyAncestorError::BadState {
                    timeline_id: ancestor.timeline_id,
                    state,
                },
            })?;

        Ok(ancestor.clone())
    }

    pub(crate) fn get_shard_identity(&self) -> &ShardIdentity {
        &self.shard_identity
    }

    #[inline(always)]
    pub(crate) fn shard_timeline_id(&self) -> ShardTimelineId {
        ShardTimelineId {
            shard_index: ShardIndex {
                shard_number: self.shard_identity.number,
                shard_count: self.shard_identity.count,
            },
            timeline_id: self.timeline_id,
        }
    }

    /// Returns a non-frozen open in-memory layer for ingestion.
    ///
    /// Takes a witness of timeline writer state lock being held, because it makes no sense to call
    /// this function without holding the mutex.
    async fn get_layer_for_write(
        &self,
        lsn: Lsn,
        _guard: &tokio::sync::MutexGuard<'_, Option<TimelineWriterState>>,
        ctx: &RequestContext,
    ) -> anyhow::Result<Arc<InMemoryLayer>> {
        let mut guard = self
            .layers
            .write(LayerManagerLockHolder::GetLayerForWrite)
            .await;

        let last_record_lsn = self.get_last_record_lsn();
        ensure!(
            lsn > last_record_lsn,
            "cannot modify relation after advancing last_record_lsn (incoming_lsn={}, last_record_lsn={})",
            lsn,
            last_record_lsn,
        );

        let layer = guard
            .open_mut()?
            .get_layer_for_write(
                lsn,
                self.conf,
                self.timeline_id,
                self.tenant_shard_id,
                &self.gate,
                &self.cancel,
                ctx,
            )
            .await?;
        Ok(layer)
    }

    pub(crate) fn finish_write(&self, new_lsn: Lsn) {
        assert!(new_lsn.is_aligned());

        self.metrics.last_record_lsn_gauge.set(new_lsn.0 as i64);
        self.last_record_lsn.advance(new_lsn);
    }

    /// Freeze any existing open in-memory layer and unconditionally notify the flush loop.
    ///
    /// Unconditional flush loop notification is given because in sharded cases we will want to
    /// leave an Lsn gap. Unsharded tenants do not have Lsn gaps.
    async fn freeze_inmem_layer_at(
        &self,
        at: Lsn,
        write_lock: &mut tokio::sync::MutexGuard<'_, Option<TimelineWriterState>>,
    ) -> Result<u64, FlushLayerError> {
        let frozen = {
            let mut guard = self
                .layers
                .write(LayerManagerLockHolder::TryFreezeLayer)
                .await;
            guard
                .open_mut()?
                .try_freeze_in_memory_layer(at, &self.last_freeze_at, write_lock, &self.metrics)
                .await
        };

        if frozen {
            let now = Instant::now();
            *(self.last_freeze_ts.write().unwrap()) = now;
        }

        // Increment the flush cycle counter and wake up the flush task.
        // Remember the new value, so that when we listen for the flush
        // to finish, we know when the flush that we initiated has
        // finished, instead of some other flush that was started earlier.
        let mut my_flush_request = 0;

        let flush_loop_state = { *self.flush_loop_state.lock().unwrap() };
        if !matches!(flush_loop_state, FlushLoopState::Running { .. }) {
            return Err(FlushLayerError::NotRunning(flush_loop_state));
        }

        self.layer_flush_start_tx.send_modify(|(counter, lsn)| {
            my_flush_request = *counter + 1;
            *counter = my_flush_request;
            *lsn = std::cmp::max(at, *lsn);
        });

        assert_ne!(my_flush_request, 0);

        Ok(my_flush_request)
    }

    /// Layer flusher task's main loop.
    async fn flush_loop(
        self: &Arc<Self>,
        mut layer_flush_start_rx: tokio::sync::watch::Receiver<(u64, Lsn)>,
        ctx: &RequestContext,
    ) {
        // Always notify waiters about the flush loop exiting since the loop might stop
        // when the timeline hasn't been cancelled.
        let scopeguard_rx = layer_flush_start_rx.clone();
        scopeguard::defer! {
            let (flush_counter, _) = *scopeguard_rx.borrow();
            let _ = self
                .layer_flush_done_tx
                .send_replace((flush_counter, Err(FlushLayerError::Cancelled)));
        }

        // Subscribe to L0 delta layer updates, for compaction backpressure.
        let mut watch_l0 = match self
            .layers
            .read(LayerManagerLockHolder::FlushLoop)
            .await
            .layer_map()
        {
            Ok(lm) => lm.watch_level0_deltas(),
            Err(Shutdown) => return,
        };

        info!("started flush loop");
        loop {
            tokio::select! {
                _ = self.cancel.cancelled() => {
                    info!("shutting down layer flush task due to Timeline::cancel");
                    break;
                },
                _ = layer_flush_start_rx.changed() => {}
            }
            trace!("waking up");
            let (flush_counter, frozen_to_lsn) = *layer_flush_start_rx.borrow();

            // The highest LSN to which we flushed in the loop over frozen layers
            let mut flushed_to_lsn = Lsn(0);

            let result = loop {
                if self.cancel.is_cancelled() {
                    info!("dropping out of flush loop for timeline shutdown");
                    return;
                }

                // Break to notify potential waiters as soon as we've flushed the requested LSN. If
                // more requests have arrived in the meanwhile, we'll resume flushing afterwards.
                if flushed_to_lsn >= frozen_to_lsn {
                    break Ok(());
                }

                // Fetch the next layer to flush, if any.
                let (layer, l0_count, frozen_count, frozen_size, open_layer_size) = {
                    let layers = self.layers.read(LayerManagerLockHolder::FlushLoop).await;
                    let Ok(lm) = layers.layer_map() else {
                        info!("dropping out of flush loop for timeline shutdown");
                        return;
                    };
                    let l0_count = lm.level0_deltas().len();
                    let frozen_count = lm.frozen_layers.len();
                    let frozen_size: u64 = lm
                        .frozen_layers
                        .iter()
                        .map(|l| l.estimated_in_mem_size())
                        .sum();
                    let open_layer_size: u64 = lm
                        .open_layer
                        .as_ref()
                        .map(|l| l.estimated_in_mem_size())
                        .unwrap_or(0);
                    let layer = lm.frozen_layers.front().cloned();
                    (layer, l0_count, frozen_count, frozen_size, open_layer_size)
                    // drop 'layers' lock
                };
                let Some(layer) = layer else {
                    break Ok(());
                };

                // Stall flushes to backpressure if compaction can't keep up. This is propagated up
                // to WAL ingestion by having ephemeral layer rolls wait for flushes.
                if let Some(stall_threshold) = self.get_l0_flush_stall_threshold() {
                    if l0_count >= stall_threshold {
                        warn!(
                            "stalling layer flushes for compaction backpressure at {l0_count} \
                            L0 layers ({frozen_count} frozen layers with {frozen_size} bytes, {open_layer_size} bytes in open layer)"
                        );
                        let stall_timer = self
                            .metrics
                            .flush_delay_histo
                            .start_timer()
                            .record_on_drop();
                        tokio::select! {
                            result = watch_l0.wait_for(|l0| *l0 < stall_threshold) => {
                                if let Ok(l0) = result.as_deref() {
                                    let delay = stall_timer.elapsed().as_secs_f64();
                                    info!("resuming layer flushes at {l0} L0 layers after {delay:.3}s");
                                }
                            },
                            _ = self.cancel.cancelled() => {},
                        }
                        continue; // check again
                    }
                }

                // Flush the layer.
                let flush_timer = self.metrics.flush_time_histo.start_timer();
                match self.flush_frozen_layer(layer, ctx).await {
                    Ok(layer_lsn) => flushed_to_lsn = max(flushed_to_lsn, layer_lsn),
                    Err(FlushLayerError::Cancelled) => {
                        info!("dropping out of flush loop for timeline shutdown");
                        return;
                    }
                    err @ Err(
                        FlushLayerError::NotRunning(_)
                        | FlushLayerError::Other(_)
                        | FlushLayerError::CreateImageLayersError(_),
                    ) => {
                        error!("could not flush frozen layer: {err:?}");
                        break err.map(|_| ());
                    }
                }
                let flush_duration = flush_timer.stop_and_record();

                // Notify the tenant compaction loop if L0 compaction is needed.
                let l0_count = *watch_l0.borrow();
                if l0_count >= self.get_compaction_threshold() {
                    self.l0_compaction_trigger.notify_one();
                }

                // Delay the next flush to backpressure if compaction can't keep up. We delay by the
                // flush duration such that the flush takes 2x as long. This is propagated up to WAL
                // ingestion by having ephemeral layer rolls wait for flushes.
                if let Some(delay_threshold) = self.get_l0_flush_delay_threshold() {
                    if l0_count >= delay_threshold {
                        let delay = flush_duration.as_secs_f64();
                        info!(
                            "delaying layer flush by {delay:.3}s for compaction backpressure at \
                            {l0_count} L0 layers ({frozen_count} frozen layers with {frozen_size} bytes, {open_layer_size} bytes in open layer)"
                        );
                        let _delay_timer = self
                            .metrics
                            .flush_delay_histo
                            .start_timer()
                            .record_on_drop();
                        tokio::select! {
                            _ = tokio::time::sleep(flush_duration) => {},
                            _ = watch_l0.wait_for(|l0| *l0 < delay_threshold) => {},
                            _ = self.cancel.cancelled() => {},
                        }
                    }
                }
            };

            // Unsharded tenants should never advance their LSN beyond the end of the
            // highest layer they write: such gaps between layer data and the frozen LSN
            // are only legal on sharded tenants.
            debug_assert!(
                self.shard_identity.count.count() > 1
                    || flushed_to_lsn >= frozen_to_lsn
                    || !flushed_to_lsn.is_valid()
            );

            if flushed_to_lsn < frozen_to_lsn
                && self.shard_identity.count.count() > 1
                && result.is_ok()
            {
                // If our layer flushes didn't carry disk_consistent_lsn up to the `to_lsn` advertised
                // to us via layer_flush_start_rx, then advance it here.
                //
                // This path is only taken for tenants with multiple shards: single sharded tenants should
                // never encounter a gap in the wal.
                let old_disk_consistent_lsn = self.disk_consistent_lsn.load();
                tracing::debug!(
                    "Advancing disk_consistent_lsn across layer gap {old_disk_consistent_lsn}->{frozen_to_lsn}"
                );
                if self.set_disk_consistent_lsn(frozen_to_lsn) {
                    if let Err(e) = self.schedule_uploads(frozen_to_lsn, vec![]) {
                        tracing::warn!(
                            "Failed to schedule metadata upload after updating disk_consistent_lsn: {e}"
                        );
                    }
                }
            }

            // Notify any listeners that we're done
            let _ = self
                .layer_flush_done_tx
                .send_replace((flush_counter, result));
        }
    }

    /// Waits any flush request created by [`Self::freeze_inmem_layer_at`] to complete.
    async fn wait_flush_completion(&self, request: u64) -> Result<(), FlushLayerError> {
        let mut rx = self.layer_flush_done_tx.subscribe();
        loop {
            {
                let (last_result_counter, last_result) = &*rx.borrow();
                if *last_result_counter >= request {
                    if let Err(err) = last_result {
                        // We already logged the original error in
                        // flush_loop. We cannot propagate it to the caller
                        // here, because it might not be Cloneable
                        return Err(err.clone());
                    } else {
                        return Ok(());
                    }
                }
            }
            trace!("waiting for flush to complete");
            tokio::select! {
                rx_e = rx.changed() => {
                    rx_e.map_err(|_| FlushLayerError::NotRunning(*self.flush_loop_state.lock().unwrap()))?;
                },
                // Cancellation safety: we are not leaving an I/O in-flight for the flush, we're just ignoring
                // the notification from [`flush_loop`] that it completed.
                _ = self.cancel.cancelled() => {
                    tracing::info!("Cancelled layer flush due on timeline shutdown");
                    return Ok(())
                }
            };
            trace!("done")
        }
    }

    /// Flush one frozen in-memory layer to disk, as a new delta layer.
    ///
    /// Return value is the last lsn (inclusive) of the layer that was frozen.
    #[instrument(skip_all, fields(layer=%frozen_layer))]
    async fn flush_frozen_layer(
        self: &Arc<Self>,
        frozen_layer: Arc<InMemoryLayer>,
        ctx: &RequestContext,
    ) -> Result<Lsn, FlushLayerError> {
        debug_assert_current_span_has_tenant_and_timeline_id();

        // As a special case, when we have just imported an image into the repository,
        // instead of writing out a L0 delta layer, we directly write out image layer
        // files instead. This is possible as long as *all* the data imported into the
        // repository have the same LSN.
        let lsn_range = frozen_layer.get_lsn_range();

        // Whether to directly create image layers for this flush, or flush them as delta layers
        let create_image_layer =
            lsn_range.start == self.initdb_lsn && lsn_range.end == Lsn(self.initdb_lsn.0 + 1);

        #[cfg(test)]
        {
            match &mut *self.flush_loop_state.lock().unwrap() {
                FlushLoopState::NotStarted | FlushLoopState::Exited => {
                    panic!("flush loop not running")
                }
                FlushLoopState::Running {
                    expect_initdb_optimization,
                    initdb_optimization_count,
                    ..
                } => {
                    if create_image_layer {
                        *initdb_optimization_count += 1;
                    } else {
                        assert!(!*expect_initdb_optimization, "expected initdb optimization");
                    }
                }
            }
        }

        let (layers_to_upload, delta_layer_to_add) = if create_image_layer {
            // Note: The 'ctx' in use here has DownloadBehavior::Error. We should not
            // require downloading anything during initial import.
            let ((rel_partition, metadata_partition), _lsn) = self
                .repartition(
                    self.initdb_lsn,
                    self.get_compaction_target_size(),
                    EnumSet::empty(),
                    ctx,
                )
                .await
                .map_err(|e| FlushLayerError::from_anyhow(self, e.into()))?;

            if self.cancel.is_cancelled() {
                return Err(FlushLayerError::Cancelled);
            }

            // Ensure that we have a single call to `create_image_layers` with a combined dense keyspace.
            // So that the key ranges don't overlap.
            let mut partitions = KeyPartitioning::default();
            partitions.parts.extend(rel_partition.parts);
            if !metadata_partition.parts.is_empty() {
                assert_eq!(
                    metadata_partition.parts.len(),
                    1,
                    "currently sparse keyspace should only contain a single metadata keyspace"
                );
                // Safety: create_image_layers treat sparse keyspaces differently that it does not scan
                // every single key within the keyspace, and therefore, it's safe to force converting it
                // into a dense keyspace before calling this function.
                partitions
                    .parts
                    .extend(metadata_partition.into_dense().parts);
            }

            let mut layers_to_upload = Vec::new();
            let (generated_image_layers, is_complete) = self
                .create_image_layers(
                    &partitions,
                    self.initdb_lsn,
                    ImageLayerCreationMode::Initial,
                    ctx,
                    LastImageLayerCreationStatus::Initial,
                    false, // don't yield for L0, we're flushing L0
                )
                .instrument(info_span!("create_image_layers", mode = %ImageLayerCreationMode::Initial, partition_mode = "initial", lsn = %self.initdb_lsn))
                .await?;
            debug_assert!(
                matches!(is_complete, LastImageLayerCreationStatus::Complete),
                "init image generation mode must fully cover the keyspace"
            );
            layers_to_upload.extend(generated_image_layers);

            (layers_to_upload, None)
        } else {
            // Normal case, write out a L0 delta layer file.
            // `create_delta_layer` will not modify the layer map.
            // We will remove frozen layer and add delta layer in one atomic operation later.
            let Some(layer) = self
                .create_delta_layer(&frozen_layer, None, ctx)
                .await
                .map_err(|e| FlushLayerError::from_anyhow(self, e))?
            else {
                panic!("delta layer cannot be empty if no filter is applied");
            };
            (
                // FIXME: even though we have a single image and single delta layer assumption
                // we push them to vec
                vec![layer.clone()],
                Some(layer),
            )
        };

        pausable_failpoint!("flush-layer-cancel-after-writing-layer-out-pausable");

        if self.cancel.is_cancelled() {
            return Err(FlushLayerError::Cancelled);
        }

        fail_point!("flush-layer-before-update-remote-consistent-lsn", |_| {
            Err(FlushLayerError::Other(anyhow!("failpoint").into()))
        });

        let disk_consistent_lsn = Lsn(lsn_range.end.0 - 1);

        // The new on-disk layers are now in the layer map. We can remove the
        // in-memory layer from the map now. The flushed layer is stored in
        // the mapping in `create_delta_layer`.
        {
            let mut guard = self
                .layers
                .write(LayerManagerLockHolder::FlushFrozenLayer)
                .await;

            guard.open_mut()?.finish_flush_l0_layer(
                delta_layer_to_add.as_ref(),
                &frozen_layer,
                &self.metrics,
            );

            if self.set_disk_consistent_lsn(disk_consistent_lsn) {
                // Schedule remote uploads that will reflect our new disk_consistent_lsn
                self.schedule_uploads(disk_consistent_lsn, layers_to_upload)
                    .map_err(|e| FlushLayerError::from_anyhow(self, e))?;
            }
            // release lock on 'layers'
        };

        // FIXME: between create_delta_layer and the scheduling of the upload in `update_metadata_file`,
        // a compaction can delete the file and then it won't be available for uploads any more.
        // We still schedule the upload, resulting in an error, but ideally we'd somehow avoid this
        // race situation.
        // See https://github.com/neondatabase/neon/issues/4526
        pausable_failpoint!("flush-frozen-pausable");

        // This failpoint is used by another test case `test_pageserver_recovery`.
        fail_point!("flush-frozen-exit");

        Ok(Lsn(lsn_range.end.0 - 1))
    }

    /// Return true if the value changed
    ///
    /// This function must only be used from the layer flush task.
    fn set_disk_consistent_lsn(&self, new_value: Lsn) -> bool {
        let old_value = self.disk_consistent_lsn.fetch_max(new_value);
        assert!(
            new_value >= old_value,
            "disk_consistent_lsn must be growing monotonously at runtime; current {old_value}, offered {new_value}"
        );

        self.metrics
            .disk_consistent_lsn_gauge
            .set(new_value.0 as i64);
        new_value != old_value
    }

    /// Update metadata file
    fn schedule_uploads(
        &self,
        disk_consistent_lsn: Lsn,
        layers_to_upload: impl IntoIterator<Item = ResidentLayer>,
    ) -> anyhow::Result<()> {
        // We can only save a valid 'prev_record_lsn' value on disk if we
        // flushed *all* in-memory changes to disk. We only track
        // 'prev_record_lsn' in memory for the latest processed record, so we
        // don't remember what the correct value that corresponds to some old
        // LSN is. But if we flush everything, then the value corresponding
        // current 'last_record_lsn' is correct and we can store it on disk.
        let RecordLsn {
            last: last_record_lsn,
            prev: prev_record_lsn,
        } = self.last_record_lsn.load();
        let ondisk_prev_record_lsn = if disk_consistent_lsn == last_record_lsn {
            Some(prev_record_lsn)
        } else {
            None
        };

        let update = crate::tenant::metadata::MetadataUpdate::new(
            disk_consistent_lsn,
            ondisk_prev_record_lsn,
            *self.applied_gc_cutoff_lsn.read(),
        );

        fail_point!("checkpoint-before-saving-metadata", |x| bail!(
            "{}",
            x.unwrap()
        ));

        for layer in layers_to_upload {
            self.remote_client.schedule_layer_file_upload(layer)?;
        }
        self.remote_client
            .schedule_index_upload_for_metadata_update(&update)?;

        Ok(())
    }

    pub(crate) async fn preserve_initdb_archive(&self) -> anyhow::Result<()> {
        self.remote_client
            .preserve_initdb_archive(
                &self.tenant_shard_id.tenant_id,
                &self.timeline_id,
                &self.cancel,
            )
            .await
    }

    // Write out the given frozen in-memory layer as a new L0 delta file. This L0 file will not be tracked
    // in layer map immediately. The caller is responsible to put it into the layer map.
    async fn create_delta_layer(
        self: &Arc<Self>,
        frozen_layer: &Arc<InMemoryLayer>,
        key_range: Option<Range<Key>>,
        ctx: &RequestContext,
    ) -> anyhow::Result<Option<ResidentLayer>> {
        let self_clone = Arc::clone(self);
        let frozen_layer = Arc::clone(frozen_layer);
        let ctx = ctx.attached_child();
        let work = async move {
            let Some((desc, path)) = frozen_layer
                .write_to_disk(
                    &ctx,
                    key_range,
                    self_clone.l0_flush_global_state.inner(),
                    &self_clone.gate,
                    self_clone.cancel.clone(),
                )
                .await?
            else {
                return Ok(None);
            };
            let new_delta = Layer::finish_creating(self_clone.conf, &self_clone, desc, &path)?;

            // The write_to_disk() above calls writer.finish() which already did the fsync of the inodes.
            // We just need to fsync the directory in which these inodes are linked,
            // which we know to be the timeline directory.
            //
            // We use fatal_err() below because the after write_to_disk returns with success,
            // the in-memory state of the filesystem already has the layer file in its final place,
            // and subsequent pageserver code could think it's durable while it really isn't.
            let timeline_dir = VirtualFile::open(
                &self_clone
                    .conf
                    .timeline_path(&self_clone.tenant_shard_id, &self_clone.timeline_id),
                &ctx,
            )
            .await
            .fatal_err("VirtualFile::open for timeline dir fsync");
            timeline_dir
                .sync_all()
                .await
                .fatal_err("VirtualFile::sync_all timeline dir");
            anyhow::Ok(Some(new_delta))
        };
        // Before tokio-epoll-uring, we ran write_to_disk & the sync_all inside spawn_blocking.
        // Preserve that behavior to maintain the same behavior for `virtual_file_io_engine=std-fs`.
        use crate::virtual_file::io_engine::IoEngine;
        match crate::virtual_file::io_engine::get() {
            IoEngine::NotSet => panic!("io engine not set"),
            IoEngine::StdFs => {
                let span = tracing::info_span!("blocking");
                tokio::task::spawn_blocking({
                    move || Handle::current().block_on(work.instrument(span))
                })
                .await
                .context("spawn_blocking")
                .and_then(|x| x)
            }
            #[cfg(target_os = "linux")]
            IoEngine::TokioEpollUring => work.await,
        }
    }

    async fn repartition(
        &self,
        lsn: Lsn,
        partition_size: u64,
        flags: EnumSet<CompactFlags>,
        ctx: &RequestContext,
    ) -> Result<((KeyPartitioning, SparseKeyPartitioning), Lsn), CompactionError> {
        let Ok(mut guard) = self.partitioning.try_write_guard() else {
            // NB: there are two callers, one is the compaction task, of which there is only one per struct Tenant and hence Timeline.
            // The other is the initdb optimization in flush_frozen_layer, used by `boostrap_timeline`, which runs before `.activate()`
            // and hence before the compaction task starts.
            return Err(CompactionError::Other(anyhow!(
                "repartition() called concurrently"
            )));
        };
        let ((dense_partition, sparse_partition), partition_lsn) = &*guard.read();
        if lsn < *partition_lsn {
            return Err(CompactionError::Other(anyhow!(
                "repartition() called with LSN going backwards, this should not happen"
            )));
        }

        let distance = lsn.0 - partition_lsn.0;
        if *partition_lsn != Lsn(0)
            && distance <= self.repartition_threshold
            && !flags.contains(CompactFlags::ForceRepartition)
        {
            debug!(
                distance,
                threshold = self.repartition_threshold,
                "no repartitioning needed"
            );
            return Ok((
                (dense_partition.clone(), sparse_partition.clone()),
                *partition_lsn,
            ));
        }

        let (dense_ks, sparse_ks) = self.collect_keyspace(lsn, ctx).await?;
        let dense_partitioning = dense_ks.partition(
            &self.shard_identity,
            partition_size,
            postgres_ffi::BLCKSZ as u64,
        );
        let sparse_partitioning = SparseKeyPartitioning {
            parts: vec![sparse_ks],
        }; // no partitioning for metadata keys for now
        let result = ((dense_partitioning, sparse_partitioning), lsn);
        guard.write(result.clone());
        Ok(result)
    }

    // Is it time to create a new image layer for the given partition? True if we want to generate.
    async fn time_for_new_image_layer(&self, partition: &KeySpace, lsn: Lsn) -> bool {
        let threshold = self.get_image_creation_threshold();

        let guard = self.layers.read(LayerManagerLockHolder::Compaction).await;
        let Ok(layers) = guard.layer_map() else {
            return false;
        };

        let mut max_deltas = 0;
        for part_range in &partition.ranges {
            let image_coverage = layers.image_coverage(part_range, lsn);
            for (img_range, last_img) in image_coverage {
                let img_lsn = if let Some(last_img) = last_img {
                    last_img.get_lsn_range().end
                } else {
                    Lsn(0)
                };
                // Let's consider an example:
                //
                // delta layer with LSN range 71-81
                // delta layer with LSN range 81-91
                // delta layer with LSN range 91-101
                // image layer at LSN 100
                //
                // If 'lsn' is still 100, i.e. no new WAL has been processed since the last image layer,
                // there's no need to create a new one. We check this case explicitly, to avoid passing
                // a bogus range to count_deltas below, with start > end. It's even possible that there
                // are some delta layers *later* than current 'lsn', if more WAL was processed and flushed
                // after we read last_record_lsn, which is passed here in the 'lsn' argument.
                if img_lsn < lsn {
                    let num_deltas =
                        layers.count_deltas(&img_range, &(img_lsn..lsn), Some(threshold));

                    max_deltas = max_deltas.max(num_deltas);
                    if num_deltas >= threshold {
                        debug!(
                            "key range {}-{}, has {} deltas on this timeline in LSN range {}..{}",
                            img_range.start, img_range.end, num_deltas, img_lsn, lsn
                        );
                        return true;
                    }
                }
            }
        }

        debug!(
            max_deltas,
            "none of the partitioned ranges had >= {threshold} deltas"
        );
        false
    }

    /// Create image layers for Postgres data. Assumes the caller passes a partition that is not too large,
    /// so that at most one image layer will be produced from this function.
    #[allow(clippy::too_many_arguments)]
    async fn create_image_layer_for_rel_blocks(
        self: &Arc<Self>,
        partition: &KeySpace,
        mut image_layer_writer: ImageLayerWriter,
        lsn: Lsn,
        ctx: &RequestContext,
        img_range: Range<Key>,
        io_concurrency: IoConcurrency,
        progress: Option<(usize, usize)>,
    ) -> Result<ImageLayerCreationOutcome, CreateImageLayersError> {
        let mut wrote_keys = false;

        let mut key_request_accum = KeySpaceAccum::new();
        for range in &partition.ranges {
            let mut key = range.start;
            while key < range.end {
                // Decide whether to retain this key: usually we do, but sharded tenants may
                // need to drop keys that don't belong to them.  If we retain the key, add it
                // to `key_request_accum` for later issuing a vectored get
                if self.shard_identity.is_key_disposable(&key) {
                    debug!(
                        "Dropping key {} during compaction (it belongs on shard {:?})",
                        key,
                        self.shard_identity.get_shard_number(&key)
                    );
                } else {
                    key_request_accum.add_key(key);
                }

                let last_key_in_range = key.next() == range.end;
                key = key.next();

                // Maybe flush `key_rest_accum`
                if key_request_accum.raw_size() >= self.conf.max_get_vectored_keys.get() as u64
                    || (last_key_in_range && key_request_accum.raw_size() > 0)
                {
                    let query =
                        VersionedKeySpaceQuery::uniform(key_request_accum.consume_keyspace(), lsn);

                    let results = self
                        .get_vectored(query, io_concurrency.clone(), ctx)
                        .await?;

                    if self.cancel.is_cancelled() {
                        return Err(CreateImageLayersError::Cancelled);
                    }

                    for (img_key, img) in results {
                        let img = match img {
                            Ok(img) => img,
                            Err(err) => {
                                // If we fail to reconstruct a VM or FSM page, we can zero the
                                // page without losing any actual user data. That seems better
                                // than failing repeatedly and getting stuck.
                                //
                                // We had a bug at one point, where we truncated the FSM and VM
                                // in the pageserver, but the Postgres didn't know about that
                                // and continued to generate incremental WAL records for pages
                                // that didn't exist in the pageserver. Trying to replay those
                                // WAL records failed to find the previous image of the page.
                                // This special case allows us to recover from that situation.
                                // See https://github.com/neondatabase/neon/issues/2601.
                                //
                                // Unfortunately we cannot do this for the main fork, or for
                                // any metadata keys, keys, as that would lead to actual data
                                // loss.
                                if img_key.is_rel_fsm_block_key() || img_key.is_rel_vm_block_key() {
                                    warn!(
                                        "could not reconstruct FSM or VM key {img_key}, filling with zeros: {err:?}"
                                    );
                                    ZERO_PAGE.clone()
                                } else {
                                    return Err(CreateImageLayersError::from(err));
                                }
                            }
                        };

                        // Write all the keys we just read into our new image layer.
                        image_layer_writer.put_image(img_key, img, ctx).await?;
                        wrote_keys = true;
                    }
                }
            }
        }

        let progress_report = progress
            .map(|(idx, total)| format!("({idx}/{total}) "))
            .unwrap_or_default();
        if wrote_keys {
            // Normal path: we have written some data into the new image layer for this
            // partition, so flush it to disk.
            info!(
                "{} produced image layer for rel {}",
                progress_report,
                ImageLayerName {
                    key_range: img_range.clone(),
                    lsn
                },
            );
            Ok(ImageLayerCreationOutcome::Generated {
                unfinished_image_layer: image_layer_writer,
            })
        } else {
            tracing::debug!(
                "{} no data in range {}-{}",
                progress_report,
                img_range.start,
                img_range.end
            );
            Ok(ImageLayerCreationOutcome::Empty)
        }
    }

    /// Create an image layer for metadata keys. This function produces one image layer for all metadata
    /// keys for now. Because metadata keys cannot exceed basebackup size limit, the image layer for it
    /// would not be too large to fit in a single image layer.
    ///
    /// Creating image layers for metadata keys are different from relational keys. Firstly, instead of
    /// iterating each key and get an image for each of them, we do a `vectored_get` scan over the sparse
    /// keyspace to get all images in one run. Secondly, we use a different image layer generation metrics
    /// for metadata keys than relational keys, which is the number of delta files visited during the scan.
    #[allow(clippy::too_many_arguments)]
    async fn create_image_layer_for_metadata_keys(
        self: &Arc<Self>,
        partition: &KeySpace,
        mut image_layer_writer: ImageLayerWriter,
        lsn: Lsn,
        ctx: &RequestContext,
        img_range: Range<Key>,
        mode: ImageLayerCreationMode,
        io_concurrency: IoConcurrency,
    ) -> Result<ImageLayerCreationOutcome, CreateImageLayersError> {
        // Metadata keys image layer creation.
        let mut reconstruct_state = ValuesReconstructState::new(io_concurrency);
        let begin = Instant::now();
        // Directly use `get_vectored_impl` to skip the max_vectored_read_key limit check. Note that the keyspace should
        // not contain too many keys, otherwise this takes a lot of memory.
        let data = self
            .get_vectored_impl(
                VersionedKeySpaceQuery::uniform(partition.clone(), lsn),
                &mut reconstruct_state,
                ctx,
            )
            .await?;
        let (data, total_kb_retrieved, total_keys_retrieved) = {
            let mut new_data = BTreeMap::new();
            let mut total_kb_retrieved = 0;
            let mut total_keys_retrieved = 0;
            for (k, v) in data {
                let v = v?;
                total_kb_retrieved += KEY_SIZE + v.len();
                total_keys_retrieved += 1;
                new_data.insert(k, v);
            }
            (new_data, total_kb_retrieved / 1024, total_keys_retrieved)
        };
        let delta_files_accessed = reconstruct_state.get_delta_layers_visited();
        let elapsed = begin.elapsed();

        let trigger_generation = delta_files_accessed as usize >= MAX_AUX_FILE_V2_DELTAS;
        info!(
            "metadata key compaction: trigger_generation={trigger_generation}, delta_files_accessed={delta_files_accessed}, total_kb_retrieved={total_kb_retrieved}, total_keys_retrieved={total_keys_retrieved}, read_time={}s",
            elapsed.as_secs_f64()
        );

        if !trigger_generation && mode == ImageLayerCreationMode::Try {
            return Ok(ImageLayerCreationOutcome::Skip);
        }
        if self.cancel.is_cancelled() {
            return Err(CreateImageLayersError::Cancelled);
        }
        let mut wrote_any_image = false;
        for (k, v) in data {
            if v.is_empty() {
                // the key has been deleted, it does not need an image
                // in metadata keyspace, an empty image == tombstone
                continue;
            }
            wrote_any_image = true;

            // No need to handle sharding b/c metadata keys are always on the 0-th shard.

            // TODO: split image layers to avoid too large layer files. Too large image files are not handled
            // on the normal data path either.
            image_layer_writer.put_image(k, v, ctx).await?;
        }

        if wrote_any_image {
            // Normal path: we have written some data into the new image layer for this
            // partition, so flush it to disk.
            info!(
                "created image layer for metadata {}",
                ImageLayerName {
                    key_range: img_range.clone(),
                    lsn
                }
            );
            Ok(ImageLayerCreationOutcome::Generated {
                unfinished_image_layer: image_layer_writer,
            })
        } else {
            tracing::debug!("no data in range {}-{}", img_range.start, img_range.end);
            Ok(ImageLayerCreationOutcome::Empty)
        }
    }

    /// Predicate function which indicates whether we should check if new image layers
    /// are required. Since checking if new image layers are required is expensive in
    /// terms of CPU, we only do it in the following cases:
    /// 1. If the timeline has ingested sufficient WAL to justify the cost
    /// 2. If enough time has passed since the last check:
    ///     1. For large tenants, we wish to perform the check more often since they
    ///        suffer from the lack of image layers
    ///     2. For small tenants (that can mostly fit in RAM), we use a much longer interval
    fn should_check_if_image_layers_required(self: &Arc<Timeline>, lsn: Lsn) -> bool {
        const LARGE_TENANT_THRESHOLD: u64 = 2 * 1024 * 1024 * 1024;

        let last_checks_at = self.last_image_layer_creation_check_at.load();
        let distance = lsn
            .checked_sub(last_checks_at)
            .expect("Attempt to compact with LSN going backwards");
        let min_distance =
            self.get_image_layer_creation_check_threshold() as u64 * self.get_checkpoint_distance();

        let distance_based_decision = distance.0 >= min_distance;

        let mut time_based_decision = false;
        let mut last_check_instant = self.last_image_layer_creation_check_instant.lock().unwrap();
        if let CurrentLogicalSize::Exact(logical_size) = self.current_logical_size.current_size() {
            let check_required_after = if Into::<u64>::into(&logical_size) >= LARGE_TENANT_THRESHOLD
            {
                self.get_checkpoint_timeout()
            } else {
                Duration::from_secs(3600 * 48)
            };

            time_based_decision = match *last_check_instant {
                Some(last_check) => {
                    let elapsed = last_check.elapsed();
                    elapsed >= check_required_after
                }
                None => true,
            };
        }

        // Do the expensive delta layer counting only if this timeline has ingested sufficient
        // WAL since the last check or a checkpoint timeout interval has elapsed since the last
        // check.
        let decision = distance_based_decision || time_based_decision;

        if decision {
            self.last_image_layer_creation_check_at.store(lsn);
            *last_check_instant = Some(Instant::now());
        }

        decision
    }

    /// Returns the image layers generated and an enum indicating whether the process is fully completed.
    /// true = we have generate all image layers, false = we preempt the process for L0 compaction.
    ///
    /// `partition_mode` is only for logging purpose and is not used anywhere in this function.
    async fn create_image_layers(
        self: &Arc<Timeline>,
        partitioning: &KeyPartitioning,
        lsn: Lsn,
        mode: ImageLayerCreationMode,
        ctx: &RequestContext,
        last_status: LastImageLayerCreationStatus,
        yield_for_l0: bool,
    ) -> Result<(Vec<ResidentLayer>, LastImageLayerCreationStatus), CreateImageLayersError> {
        let timer = self.metrics.create_images_time_histo.start_timer();

        if partitioning.parts.is_empty() {
            warn!("no partitions to create image layers for");
            return Ok((vec![], LastImageLayerCreationStatus::Complete));
        }

        // We need to avoid holes between generated image layers.
        // Otherwise LayerMap::image_layer_exists will return false if key range of some layer is covered by more than one
        // image layer with hole between them. In this case such layer can not be utilized by GC.
        //
        // How such hole between partitions can appear?
        // if we have relation with relid=1 and size 100 and relation with relid=2 with size 200 then result of
        // KeySpace::partition may contain partitions <100000000..100000099> and <200000000..200000199>.
        // If there is delta layer <100000000..300000000> then it never be garbage collected because
        // image layers  <100000000..100000099> and <200000000..200000199> are not completely covering it.
        let mut start = Key::MIN;

        let check_for_image_layers =
            if let LastImageLayerCreationStatus::Incomplete { last_key } = last_status {
                info!(
                    "resuming image layer creation: last_status=incomplete, continue from {}",
                    last_key
                );
                true
            } else {
                self.should_check_if_image_layers_required(lsn)
            };

        let mut batch_image_writer = BatchLayerWriter::new(self.conf);

        let mut all_generated = true;

        let mut partition_processed = 0;
        let mut total_partitions = partitioning.parts.len();
        let mut last_partition_processed = None;
        let mut partition_parts = partitioning.parts.clone();

        if let LastImageLayerCreationStatus::Incomplete { last_key } = last_status {
            // We need to skip the partitions that have already been processed.
            let mut found = false;
            for (i, partition) in partition_parts.iter().enumerate() {
                if last_key <= partition.end().unwrap() {
                    // ```plain
                    // |------|--------|----------|------|
                    //              ^last_key
                    //                    ^start from this partition
                    // ```
                    // Why `i+1` instead of `i`?
                    // It is possible that the user did some writes after the previous image layer creation attempt so that
                    // a relation grows in size, and the last_key is now in the middle of the partition. In this case, we
                    // still want to skip this partition, so that we can make progress and avoid generating image layers over
                    // the same partition. Doing a mod to ensure we don't end up with an empty vec.
                    if i + 1 >= total_partitions {
                        // In general, this case should not happen -- if last_key is on the last partition, the previous
                        // iteration of image layer creation should return a complete status.
                        break; // with found=false
                    }
                    partition_parts = partition_parts.split_off(i + 1); // Remove the first i + 1 elements
                    total_partitions = partition_parts.len();
                    // Update the start key to the partition start.
                    start = partition_parts[0].start().unwrap();
                    found = true;
                    break;
                }
            }
            if !found {
                // Last key is within the last partition, or larger than all partitions.
                return Ok((vec![], LastImageLayerCreationStatus::Complete));
            }
        }

        let total = partition_parts.len();
        for (idx, partition) in partition_parts.iter().enumerate() {
            if self.cancel.is_cancelled() {
                return Err(CreateImageLayersError::Cancelled);
            }
            partition_processed += 1;
            let img_range = start..partition.ranges.last().unwrap().end;
            let compact_metadata = partition.overlaps(&Key::metadata_key_range());
            if compact_metadata {
                for range in &partition.ranges {
                    assert!(
                        range.start.field1 >= METADATA_KEY_BEGIN_PREFIX
                            && range.end.field1 <= METADATA_KEY_END_PREFIX,
                        "metadata keys must be partitioned separately"
                    );
                }
                if mode == ImageLayerCreationMode::Try && !check_for_image_layers {
                    // Skip compaction if there are not enough updates. Metadata compaction will do a scan and
                    // might mess up with evictions.
                    start = img_range.end;
                    continue;
                }
                // For initial and force modes, we always generate image layers for metadata keys.
            } else if let ImageLayerCreationMode::Try = mode {
                // check_for_image_layers = false -> skip
                // check_for_image_layers = true -> check time_for_new_image_layer -> skip/generate
                if !check_for_image_layers || !self.time_for_new_image_layer(partition, lsn).await {
                    start = img_range.end;
                    continue;
                }
            }
            if let ImageLayerCreationMode::Force = mode {
                // When forced to create image layers, we might try and create them where they already
                // exist.  This mode is only used in tests/debug.
                let layers = self.layers.read(LayerManagerLockHolder::Compaction).await;
                if layers.contains_key(&PersistentLayerKey {
                    key_range: img_range.clone(),
                    lsn_range: PersistentLayerDesc::image_layer_lsn_range(lsn),
                    is_delta: false,
                }) {
                    // TODO: this can be processed with the BatchLayerWriter::finish_with_discard
                    // in the future.
                    tracing::info!(
                        "Skipping image layer at {lsn} {}..{}, already exists",
                        img_range.start,
                        img_range.end
                    );
                    start = img_range.end;
                    continue;
                }
            }

            let image_layer_writer = ImageLayerWriter::new(
                self.conf,
                self.timeline_id,
                self.tenant_shard_id,
                &img_range,
                lsn,
                &self.gate,
                self.cancel.clone(),
                ctx,
            )
            .await
            .map_err(CreateImageLayersError::Other)?;

            fail_point!("image-layer-writer-fail-before-finish", |_| {
                Err(CreateImageLayersError::Other(anyhow::anyhow!(
                    "failpoint image-layer-writer-fail-before-finish"
                )))
            });

            let io_concurrency = IoConcurrency::spawn_from_conf(
                self.conf.get_vectored_concurrent_io,
                self.gate
                    .enter()
                    .map_err(|_| CreateImageLayersError::Cancelled)?,
            );

            let outcome = if !compact_metadata {
                self.create_image_layer_for_rel_blocks(
                    partition,
                    image_layer_writer,
                    lsn,
                    ctx,
                    img_range.clone(),
                    io_concurrency,
                    Some((idx, total)),
                )
                .await?
            } else {
                self.create_image_layer_for_metadata_keys(
                    partition,
                    image_layer_writer,
                    lsn,
                    ctx,
                    img_range.clone(),
                    mode,
                    io_concurrency,
                )
                .await?
            };
            match outcome {
                ImageLayerCreationOutcome::Empty => {
                    // No data in this partition, so we don't need to create an image layer (for now).
                    // The next image layer should cover this key range, so we don't advance the `start`
                    // key.
                }
                ImageLayerCreationOutcome::Generated {
                    unfinished_image_layer,
                } => {
                    batch_image_writer.add_unfinished_image_writer(
                        unfinished_image_layer,
                        img_range.clone(),
                        lsn,
                    );
                    // The next image layer should be generated right after this one.
                    start = img_range.end;
                }
                ImageLayerCreationOutcome::Skip => {
                    // We don't need to create an image layer for this partition.
                    // The next image layer should NOT cover this range, otherwise
                    // the keyspace becomes empty (reads don't go past image layers).
                    start = img_range.end;
                }
            }

            if let ImageLayerCreationMode::Try = mode {
                // We have at least made some progress
                if yield_for_l0 && batch_image_writer.pending_layer_num() >= 1 {
                    // The `Try` mode is currently only used on the compaction path. We want to avoid
                    // image layer generation taking too long time and blocking L0 compaction. So in this
                    // mode, we also inspect the current number of L0 layers and skip image layer generation
                    // if there are too many of them.
                    let image_preempt_threshold = self.get_image_creation_preempt_threshold()
                        * self.get_compaction_threshold();
                    // TODO: currently we do not respect `get_image_creation_preempt_threshold` and always yield
                    // when there is a single timeline with more than L0 threshold L0 layers. As long as the
                    // `get_image_creation_preempt_threshold` is set to a value greater than 0, we will yield for L0 compaction.
                    if image_preempt_threshold != 0 {
                        let should_yield = self
                            .l0_compaction_trigger
                            .notified()
                            .now_or_never()
                            .is_some();
                        if should_yield {
                            tracing::info!(
                                "preempt image layer generation at {lsn} when processing partition {}..{}: too many L0 layers",
                                partition.start().unwrap(),
                                partition.end().unwrap()
                            );
                            last_partition_processed = Some(partition.clone());
                            all_generated = false;
                            break;
                        }
                    }
                }
            }
        }

        let image_layers = batch_image_writer
            .finish(self, ctx)
            .await
            .map_err(CreateImageLayersError::Other)?;

        let mut guard = self.layers.write(LayerManagerLockHolder::Compaction).await;

        // FIXME: we could add the images to be uploaded *before* returning from here, but right
        // now they are being scheduled outside of write lock; current way is inconsistent with
        // compaction lock order.
        guard
            .open_mut()?
            .track_new_image_layers(&image_layers, &self.metrics);
        drop_layer_manager_wlock(guard);
        let duration = timer.stop_and_record();

        // Creating image layers may have caused some previously visible layers to be covered
        if !image_layers.is_empty() {
            self.update_layer_visibility().await?;
        }

        let total_layer_size = image_layers
            .iter()
            .map(|l| l.metadata().file_size)
            .sum::<u64>();

        if !image_layers.is_empty() {
            info!(
                "created {} image layers ({} bytes) in {}s, processed {} out of {} partitions",
                image_layers.len(),
                total_layer_size,
                duration.as_secs_f64(),
                partition_processed,
                total_partitions
            );
        }

        Ok((
            image_layers,
            if all_generated {
                LastImageLayerCreationStatus::Complete
            } else {
                LastImageLayerCreationStatus::Incomplete {
                    last_key: if let Some(last_partition_processed) = last_partition_processed {
                        last_partition_processed.end().unwrap_or(Key::MIN)
                    } else {
                        // This branch should be unreachable, but in case it happens, we can just return the start key.
                        Key::MIN
                    },
                }
            },
        ))
    }

    /// Wait until the background initial logical size calculation is complete, or
    /// this Timeline is shut down.  Calling this function will cause the initial
    /// logical size calculation to skip waiting for the background jobs barrier.
    pub(crate) async fn await_initial_logical_size(self: Arc<Self>) {
        if !self.shard_identity.is_shard_zero() {
            // We don't populate logical size on shard >0: skip waiting for it.
            return;
        }

        if self.remote_client.is_deleting() {
            // The timeline was created in a deletion-resume state, we don't expect logical size to be populated
            return;
        }

        if self.current_logical_size.current_size().is_exact() {
            // root timelines are initialized with exact count, but never start the background
            // calculation
            return;
        }

        if self.cancel.is_cancelled() {
            // We already requested stopping the tenant, so we cannot wait for the logical size
            // calculation to complete given the task might have been already cancelled.
            return;
        }

        if let Some(await_bg_cancel) = self
            .current_logical_size
            .cancel_wait_for_background_loop_concurrency_limit_semaphore
            .get()
        {
            await_bg_cancel.cancel();
        } else {
            // We should not wait if we were not able to explicitly instruct
            // the logical size cancellation to skip the concurrency limit semaphore.
            // TODO: this is an unexpected case.  We should restructure so that it
            // can't happen.
            tracing::warn!(
                "await_initial_logical_size: can't get semaphore cancel token, skipping"
            );
            debug_assert!(false);
        }

        tokio::select!(
            _ = self.current_logical_size.initialized.acquire() => {},
            _ = self.cancel.cancelled() => {}
        )
    }

    /// Detach this timeline from its ancestor by copying all of ancestors layers as this
    /// Timelines layers up to the ancestor_lsn.
    ///
    /// Requires a timeline that:
    /// - has an ancestor to detach from
    /// - the ancestor does not have an ancestor -- follows from the original RFC limitations, not
    ///   a technical requirement
    ///
    /// After the operation has been started, it cannot be canceled. Upon restart it needs to be
    /// polled again until completion.
    ///
    /// During the operation all timelines sharing the data with this timeline will be reparented
    /// from our ancestor to be branches of this timeline.
    pub(crate) async fn prepare_to_detach_from_ancestor(
        self: &Arc<Timeline>,
        tenant: &crate::tenant::TenantShard,
        options: detach_ancestor::Options,
        behavior: DetachBehavior,
        ctx: &RequestContext,
    ) -> Result<detach_ancestor::Progress, detach_ancestor::Error> {
        detach_ancestor::prepare(self, tenant, behavior, options, ctx).await
    }

    /// Second step of detach from ancestor; detaches the `self` from it's current ancestor and
    /// reparents any reparentable children of previous ancestor.
    ///
    /// This method is to be called while holding the TenantManager's tenant slot, so during this
    /// method we cannot be deleted nor can any timeline be deleted. After this method returns
    /// successfully, tenant must be reloaded.
    ///
    /// Final step will be to [`Self::complete_detaching_timeline_ancestor`] after optionally
    /// resetting the tenant.
    pub(crate) async fn detach_from_ancestor_and_reparent(
        self: &Arc<Timeline>,
        tenant: &crate::tenant::TenantShard,
        prepared: detach_ancestor::PreparedTimelineDetach,
        ancestor_timeline_id: TimelineId,
        ancestor_lsn: Lsn,
        behavior: DetachBehavior,
        ctx: &RequestContext,
    ) -> Result<detach_ancestor::DetachingAndReparenting, detach_ancestor::Error> {
        detach_ancestor::detach_and_reparent(
            self,
            tenant,
            prepared,
            ancestor_timeline_id,
            ancestor_lsn,
            behavior,
            ctx,
        )
        .await
    }

    /// Final step which unblocks the GC.
    ///
    /// The tenant must've been reset if ancestry was modified previously (in tenant manager).
    pub(crate) async fn complete_detaching_timeline_ancestor(
        self: &Arc<Timeline>,
        tenant: &crate::tenant::TenantShard,
        attempt: detach_ancestor::Attempt,
        ctx: &RequestContext,
    ) -> Result<(), detach_ancestor::Error> {
        detach_ancestor::complete(self, tenant, attempt, ctx).await
    }
}

impl Drop for Timeline {
    fn drop(&mut self) {
        if let Some(ancestor) = &self.ancestor_timeline {
            // This lock should never be poisoned, but in case it is we do a .map() instead of
            // an unwrap(), to avoid panicking in a destructor and thereby aborting the process.
            if let Ok(mut gc_info) = ancestor.gc_info.write() {
                if !gc_info.remove_child_not_offloaded(self.timeline_id) {
                    tracing::error!(tenant_id = %self.tenant_shard_id.tenant_id, shard_id = %self.tenant_shard_id.shard_slug(), timeline_id = %self.timeline_id,
                        "Couldn't remove retain_lsn entry from timeline's parent on drop: already removed");
                }
            }
        }
        info!(
            "Timeline {} for tenant {} is being dropped",
            self.timeline_id, self.tenant_shard_id.tenant_id
        );
    }
}

/// Top-level failure to compact.
#[derive(Debug, thiserror::Error)]
pub(crate) enum CompactionError {
    #[error("The timeline or pageserver is shutting down")]
    ShuttingDown,
    /// Compaction cannot be done right now; page reconstruction and so on.
    #[error("Failed to collect keyspace: {0}")]
    CollectKeySpaceError(#[from] CollectKeySpaceError),
    #[error(transparent)]
    Other(anyhow::Error),
}

impl CompactionError {
    /// Errors that can be ignored, i.e., cancel and shutdown.
    pub fn is_cancel(&self) -> bool {
        matches!(
            self,
            Self::ShuttingDown
                | Self::CollectKeySpaceError(CollectKeySpaceError::Cancelled)
                | Self::CollectKeySpaceError(CollectKeySpaceError::PageRead(
                    PageReconstructError::Cancelled
                ))
        )
    }

    /// Critical errors that indicate data corruption.
    pub fn is_critical(&self) -> bool {
        matches!(
            self,
            Self::CollectKeySpaceError(
                CollectKeySpaceError::Decode(_)
                    | CollectKeySpaceError::PageRead(
                        PageReconstructError::MissingKey(_) | PageReconstructError::WalRedo(_),
                    )
            )
        )
    }
}

impl From<super::upload_queue::NotInitialized> for CompactionError {
    fn from(value: super::upload_queue::NotInitialized) -> Self {
        match value {
            super::upload_queue::NotInitialized::Uninitialized => {
                CompactionError::Other(anyhow::anyhow!(value))
            }
            super::upload_queue::NotInitialized::ShuttingDown
            | super::upload_queue::NotInitialized::Stopped => CompactionError::ShuttingDown,
        }
    }
}

impl From<super::storage_layer::layer::DownloadError> for CompactionError {
    fn from(e: super::storage_layer::layer::DownloadError) -> Self {
        match e {
            super::storage_layer::layer::DownloadError::TimelineShutdown
            | super::storage_layer::layer::DownloadError::DownloadCancelled => {
                CompactionError::ShuttingDown
            }
            super::storage_layer::layer::DownloadError::ContextAndConfigReallyDeniesDownloads
            | super::storage_layer::layer::DownloadError::DownloadRequired
            | super::storage_layer::layer::DownloadError::NotFile(_)
            | super::storage_layer::layer::DownloadError::DownloadFailed
            | super::storage_layer::layer::DownloadError::PreStatFailed(_) => {
                CompactionError::Other(anyhow::anyhow!(e))
            }
            #[cfg(test)]
            super::storage_layer::layer::DownloadError::Failpoint(_) => {
                CompactionError::Other(anyhow::anyhow!(e))
            }
        }
    }
}

impl From<layer_manager::Shutdown> for CompactionError {
    fn from(_: layer_manager::Shutdown) -> Self {
        CompactionError::ShuttingDown
    }
}

impl From<super::storage_layer::errors::PutError> for CompactionError {
    fn from(e: super::storage_layer::errors::PutError) -> Self {
        if e.is_cancel() {
            CompactionError::ShuttingDown
        } else {
            CompactionError::Other(e.into_anyhow())
        }
    }
}

#[serde_as]
#[derive(serde::Serialize)]
struct RecordedDuration(#[serde_as(as = "serde_with::DurationMicroSeconds")] Duration);

#[derive(Default)]
enum DurationRecorder {
    #[default]
    NotStarted,
    Recorded(RecordedDuration, tokio::time::Instant),
}

impl DurationRecorder {
    fn till_now(&self) -> DurationRecorder {
        match self {
            DurationRecorder::NotStarted => {
                panic!("must only call on recorded measurements")
            }
            DurationRecorder::Recorded(_, ended) => {
                let now = tokio::time::Instant::now();
                DurationRecorder::Recorded(RecordedDuration(now - *ended), now)
            }
        }
    }
    fn into_recorded(self) -> Option<RecordedDuration> {
        match self {
            DurationRecorder::NotStarted => None,
            DurationRecorder::Recorded(recorded, _) => Some(recorded),
        }
    }
}

/// Descriptor for a delta layer used in testing infra. The start/end key/lsn range of the
/// delta layer might be different from the min/max key/lsn in the delta layer. Therefore,
/// the layer descriptor requires the user to provide the ranges, which should cover all
/// keys specified in the `data` field.
#[cfg(test)]
#[derive(Clone)]
pub struct DeltaLayerTestDesc {
    pub lsn_range: Range<Lsn>,
    pub key_range: Range<Key>,
    pub data: Vec<(Key, Lsn, Value)>,
}

#[cfg(test)]
#[derive(Clone)]
pub struct InMemoryLayerTestDesc {
    pub lsn_range: Range<Lsn>,
    pub data: Vec<(Key, Lsn, Value)>,
    pub is_open: bool,
}

#[cfg(test)]
impl DeltaLayerTestDesc {
    pub fn new(lsn_range: Range<Lsn>, key_range: Range<Key>, data: Vec<(Key, Lsn, Value)>) -> Self {
        Self {
            lsn_range,
            key_range,
            data,
        }
    }

    pub fn new_with_inferred_key_range(
        lsn_range: Range<Lsn>,
        data: Vec<(Key, Lsn, Value)>,
    ) -> Self {
        let key_min = data.iter().map(|(key, _, _)| key).min().unwrap();
        let key_max = data.iter().map(|(key, _, _)| key).max().unwrap();
        Self {
            key_range: (*key_min)..(key_max.next()),
            lsn_range,
            data,
        }
    }

    pub(crate) fn layer_name(&self) -> LayerName {
        LayerName::Delta(super::storage_layer::DeltaLayerName {
            key_range: self.key_range.clone(),
            lsn_range: self.lsn_range.clone(),
        })
    }
}

impl Timeline {
    async fn finish_compact_batch(
        self: &Arc<Self>,
        new_deltas: &[ResidentLayer],
        new_images: &[ResidentLayer],
        layers_to_remove: &[Layer],
    ) -> Result<(), CompactionError> {
        let mut guard = tokio::select! {
            guard = self.layers.write(LayerManagerLockHolder::Compaction) => guard,
            _ = self.cancel.cancelled() => {
                return Err(CompactionError::ShuttingDown);
            }
        };

        let mut duplicated_layers = HashSet::new();

        let mut insert_layers = Vec::with_capacity(new_deltas.len());

        for l in new_deltas {
            if guard.contains(l.as_ref()) {
                // expected in tests
                tracing::error!(layer=%l, "duplicated L1 layer");

                // good ways to cause a duplicate: we repeatedly error after taking the writelock
                // `guard`  on self.layers. as of writing this, there are no error returns except
                // for compact_level0_phase1 creating an L0, which does not happen in practice
                // because we have not implemented L0 => L0 compaction.
                duplicated_layers.insert(l.layer_desc().key());
            } else if LayerMap::is_l0(&l.layer_desc().key_range, l.layer_desc().is_delta) {
                return Err(CompactionError::Other(anyhow::anyhow!(
                    "compaction generates a L0 layer file as output, which will cause infinite compaction."
                )));
            } else {
                insert_layers.push(l.clone());
            }
        }

        // only remove those inputs which were not outputs
        let remove_layers: Vec<Layer> = layers_to_remove
            .iter()
            .filter(|l| !duplicated_layers.contains(&l.layer_desc().key()))
            .cloned()
            .collect();

        if !new_images.is_empty() {
            guard
                .open_mut()?
                .track_new_image_layers(new_images, &self.metrics);
        }

        guard
            .open_mut()?
            .finish_compact_l0(&remove_layers, &insert_layers, &self.metrics);

        self.remote_client
            .schedule_compaction_update(&remove_layers, new_deltas)?;

        drop_layer_manager_wlock(guard);

        Ok(())
    }

    async fn rewrite_layers(
        self: &Arc<Self>,
        mut replace_layers: Vec<(Layer, ResidentLayer)>,
        mut drop_layers: Vec<Layer>,
    ) -> Result<(), CompactionError> {
        let mut guard = self.layers.write(LayerManagerLockHolder::Compaction).await;

        // Trim our lists in case our caller (compaction) raced with someone else (GC) removing layers: we want
        // to avoid double-removing, and avoid rewriting something that was removed.
        replace_layers.retain(|(l, _)| guard.contains(l));
        drop_layers.retain(|l| guard.contains(l));

        guard
            .open_mut()?
            .rewrite_layers(&replace_layers, &drop_layers, &self.metrics);

        let upload_layers: Vec<_> = replace_layers.into_iter().map(|r| r.1).collect();

        self.remote_client
            .schedule_compaction_update(&drop_layers, &upload_layers)?;

        Ok(())
    }

    /// Schedules the uploads of the given image layers
    fn upload_new_image_layers(
        self: &Arc<Self>,
        new_images: impl IntoIterator<Item = ResidentLayer>,
    ) -> Result<(), super::upload_queue::NotInitialized> {
        for layer in new_images {
            self.remote_client.schedule_layer_file_upload(layer)?;
        }
        // should any new image layer been created, not uploading index_part will
        // result in a mismatch between remote_physical_size and layermap calculated
        // size, which will fail some tests, but should not be an issue otherwise.
        self.remote_client
            .schedule_index_upload_for_file_changes()?;
        Ok(())
    }

    async fn find_gc_time_cutoff(
        &self,
        now: SystemTime,
        pitr: Duration,
        cancel: &CancellationToken,
        ctx: &RequestContext,
    ) -> Result<Option<Lsn>, PageReconstructError> {
        debug_assert_current_span_has_tenant_and_timeline_id();
        if self.shard_identity.is_shard_zero() {
            // Shard Zero has SLRU data and can calculate the PITR time -> LSN mapping itself
            let time_range = if pitr == Duration::ZERO {
                humantime::parse_duration(DEFAULT_PITR_INTERVAL).expect("constant is invalid")
            } else {
                pitr
            };

            // If PITR is so large or `now` is so small that this underflows, we will retain no history (highly unexpected case)
            let time_cutoff = now.checked_sub(time_range).unwrap_or(now);
            let timestamp = to_pg_timestamp(time_cutoff);

            let time_cutoff = match self.find_lsn_for_timestamp(timestamp, cancel, ctx).await? {
                LsnForTimestamp::Present(lsn) => Some(lsn),
                LsnForTimestamp::Future(lsn) => {
                    // The timestamp is in the future. That sounds impossible,
                    // but what it really means is that there hasn't been
                    // any commits since the cutoff timestamp.
                    //
                    // In this case we should use the LSN of the most recent commit,
                    // which is implicitly the last LSN in the log.
                    debug!("future({})", lsn);
                    Some(self.get_last_record_lsn())
                }
                LsnForTimestamp::Past(lsn) => {
                    debug!("past({})", lsn);
                    None
                }
                LsnForTimestamp::NoData(lsn) => {
                    debug!("nodata({})", lsn);
                    None
                }
            };
            Ok(time_cutoff)
        } else {
            // Shards other than shard zero cannot do timestamp->lsn lookups, and must instead learn their GC cutoff
            // from shard zero's index.  The index doesn't explicitly tell us the time cutoff, but we may assume that
            // the point up to which shard zero's last_gc_cutoff has advanced will either be the time cutoff, or a
            // space cutoff that we would also have respected ourselves.
            match self
                .remote_client
                .download_foreign_index(ShardNumber(0), cancel)
                .await
            {
                Ok((index_part, index_generation, _index_mtime)) => {
                    tracing::info!(
                        "GC loaded shard zero metadata (gen {index_generation:?}): latest_gc_cutoff_lsn: {}",
                        index_part.metadata.latest_gc_cutoff_lsn()
                    );
                    Ok(Some(index_part.metadata.latest_gc_cutoff_lsn()))
                }
                Err(DownloadError::NotFound) => {
                    // This is unexpected, because during timeline creations shard zero persists to remote
                    // storage before other shards are called, and during timeline deletion non-zeroth shards are
                    // deleted before the zeroth one.  However, it should be harmless: if we somehow end up in this
                    // state, then shard zero should _eventually_ write an index when it GCs.
                    tracing::warn!("GC couldn't find shard zero's index for timeline");
                    Ok(None)
                }
                Err(e) => {
                    // TODO: this function should return a different error type than page reconstruct error
                    Err(PageReconstructError::Other(anyhow::anyhow!(e)))
                }
            }

            // TODO: after reading shard zero's GC cutoff, we should validate its generation with the storage
            // controller.  Otherwise, it is possible that we see the GC cutoff go backwards while shard zero
            // is going through a migration if we read the old location's index and it has GC'd ahead of the
            // new location.  This is legal in principle, but problematic in practice because it might result
            // in a timeline creation succeeding on shard zero ('s new location) but then failing on other shards
            // because they have GC'd past the branch point.
        }
    }

    /// Find the Lsns above which layer files need to be retained on
    /// garbage collection.
    ///
    /// We calculate two cutoffs, one based on time and one based on WAL size.  `pitr`
    /// controls the time cutoff (or ZERO to disable time-based retention), and `space_cutoff` controls
    /// the space-based retention.
    ///
    /// This function doesn't simply to calculate time & space based retention: it treats time-based
    /// retention as authoritative if enabled, and falls back to space-based retention if calculating
    /// the LSN for a time point isn't possible.  Therefore the GcCutoffs::horizon in the response might
    /// be different to the `space_cutoff` input.  Callers should treat the min() of the two cutoffs
    /// in the response as the GC cutoff point for the timeline.
    #[instrument(skip_all, fields(timeline_id=%self.timeline_id))]
    pub(super) async fn find_gc_cutoffs(
        &self,
        now: SystemTime,
        space_cutoff: Lsn,
        pitr: Duration,
        cancel: &CancellationToken,
        ctx: &RequestContext,
    ) -> Result<GcCutoffs, PageReconstructError> {
        let _timer = self
            .metrics
            .find_gc_cutoffs_histo
            .start_timer()
            .record_on_drop();

        pausable_failpoint!("Timeline::find_gc_cutoffs-pausable");

        if cfg!(test) && pitr == Duration::ZERO {
            // Unit tests which specify zero PITR interval expect to avoid doing any I/O for timestamp lookup
            return Ok(GcCutoffs {
                time: Some(self.get_last_record_lsn()),
                space: space_cutoff,
            });
        }

        // Calculate a time-based limit on how much to retain:
        // - if PITR interval is set, then this is our cutoff.
        // - if PITR interval is not set, then we do a lookup
        //   based on DEFAULT_PITR_INTERVAL, so that size-based retention does not result in keeping history around permanently on idle databases.
        let time_cutoff = self.find_gc_time_cutoff(now, pitr, cancel, ctx).await?;

        Ok(match (pitr, time_cutoff) {
            (Duration::ZERO, Some(time_cutoff)) => {
                // PITR is not set. Retain the size-based limit, or the default time retention,
                // whichever requires less data.
                GcCutoffs {
                    time: Some(self.get_last_record_lsn()),
                    space: std::cmp::max(time_cutoff, space_cutoff),
                }
            }
            (Duration::ZERO, None) => {
                // PITR is not set, and time lookup failed
                GcCutoffs {
                    time: Some(self.get_last_record_lsn()),
                    space: space_cutoff,
                }
            }
            (_, None) => {
                // PITR interval is set & we didn't look up a timestamp successfully.  Conservatively assume PITR
                // cannot advance beyond what was already GC'd, and respect space-based retention
                GcCutoffs {
                    time: Some(*self.get_applied_gc_cutoff_lsn()),
                    space: space_cutoff,
                }
            }
            (_, Some(time_cutoff)) => {
                // PITR interval is set and we looked up timestamp successfully.  Ignore
                // size based retention and make time cutoff authoritative
                GcCutoffs {
                    time: Some(time_cutoff),
                    space: time_cutoff,
                }
            }
        })
    }

    /// Garbage collect layer files on a timeline that are no longer needed.
    ///
    /// Currently, we don't make any attempt at removing unneeded page versions
    /// within a layer file. We can only remove the whole file if it's fully
    /// obsolete.
    pub(super) async fn gc(&self) -> Result<GcResult, GcError> {
        // this is most likely the background tasks, but it might be the spawned task from
        // immediate_gc
        let _g = tokio::select! {
            guard = self.gc_lock.lock() => guard,
            _ = self.cancel.cancelled() => return Ok(GcResult::default()),
        };
        let timer = self.metrics.garbage_collect_histo.start_timer();

        fail_point!("before-timeline-gc");

        // Is the timeline being deleted?
        if self.is_stopping() {
            return Err(GcError::TimelineCancelled);
        }

        let (space_cutoff, time_cutoff, retain_lsns, max_lsn_with_valid_lease) = {
            let gc_info = self.gc_info.read().unwrap();

            let space_cutoff = min(gc_info.cutoffs.space, self.get_disk_consistent_lsn());
            let time_cutoff = gc_info.cutoffs.time;
            let retain_lsns = gc_info
                .retain_lsns
                .iter()
                .map(|(lsn, _child_id, _is_offloaded)| *lsn)
                .collect();

            // Gets the maximum LSN that holds the valid lease.
            //
            // Caveat: `refresh_gc_info` is in charged of updating the lease map.
            // Here, we do not check for stale leases again.
            let max_lsn_with_valid_lease = gc_info.leases.last_key_value().map(|(lsn, _)| *lsn);

            (
                space_cutoff,
                time_cutoff,
                retain_lsns,
                max_lsn_with_valid_lease,
            )
        };

        let mut new_gc_cutoff = space_cutoff.min(time_cutoff.unwrap_or_default());
        let standby_horizon = self.standby_horizon.load();
        // Hold GC for the standby, but as a safety guard do it only within some
        // reasonable lag.
        if standby_horizon != Lsn::INVALID {
            if let Some(standby_lag) = new_gc_cutoff.checked_sub(standby_horizon) {
                const MAX_ALLOWED_STANDBY_LAG: u64 = 10u64 << 30; // 10 GB
                if standby_lag.0 < MAX_ALLOWED_STANDBY_LAG {
                    new_gc_cutoff = Lsn::min(standby_horizon, new_gc_cutoff);
                    trace!("holding off GC for standby apply LSN {}", standby_horizon);
                } else {
                    warn!(
                        "standby is lagging for more than {}MB, not holding gc for it",
                        MAX_ALLOWED_STANDBY_LAG / 1024 / 1024
                    )
                }
            }
        }

        // Reset standby horizon to ignore it if it is not updated till next GC.
        // It is an easy way to unset it when standby disappears without adding
        // more conf options.
        self.standby_horizon.store(Lsn::INVALID);
        self.metrics
            .standby_horizon_gauge
            .set(Lsn::INVALID.0 as i64);

        let res = self
            .gc_timeline(
                space_cutoff,
                time_cutoff,
                retain_lsns,
                max_lsn_with_valid_lease,
                new_gc_cutoff,
            )
            .instrument(
                info_span!("gc_timeline", timeline_id = %self.timeline_id, cutoff = %new_gc_cutoff),
            )
            .await?;

        // only record successes
        timer.stop_and_record();

        Ok(res)
    }

    async fn gc_timeline(
        &self,
        space_cutoff: Lsn,
        time_cutoff: Option<Lsn>, // None if uninitialized
        retain_lsns: Vec<Lsn>,
        max_lsn_with_valid_lease: Option<Lsn>,
        new_gc_cutoff: Lsn,
    ) -> Result<GcResult, GcError> {
        // FIXME: if there is an ongoing detach_from_ancestor, we should just skip gc

        let now = SystemTime::now();
        let mut result: GcResult = GcResult::default();

        // Nothing to GC. Return early.
        let latest_gc_cutoff = *self.get_applied_gc_cutoff_lsn();
        if latest_gc_cutoff >= new_gc_cutoff {
            info!(
                "Nothing to GC: new_gc_cutoff_lsn {new_gc_cutoff}, latest_gc_cutoff_lsn {latest_gc_cutoff}",
            );
            return Ok(result);
        }

        let Some(time_cutoff) = time_cutoff else {
            // The GC cutoff should have been computed by now, but let's be defensive.
            info!("Nothing to GC: time_cutoff not yet computed");
            return Ok(result);
        };

        // We need to ensure that no one tries to read page versions or create
        // branches at a point before latest_gc_cutoff_lsn. See branch_timeline()
        // for details. This will block until the old value is no longer in use.
        //
        // The GC cutoff should only ever move forwards.
        let waitlist = {
            let write_guard = self.applied_gc_cutoff_lsn.lock_for_write();
            if *write_guard > new_gc_cutoff {
                return Err(GcError::BadLsn {
                    why: format!(
                        "Cannot move GC cutoff LSN backwards (was {}, new {})",
                        *write_guard, new_gc_cutoff
                    ),
                });
            }

            write_guard.store_and_unlock(new_gc_cutoff)
        };
        waitlist.wait().await;

        info!("GC starting");

        debug!("retain_lsns: {:?}", retain_lsns);

        let max_retain_lsn = retain_lsns.iter().max();

        // Scan all layers in the timeline (remote or on-disk).
        //
        // Garbage collect the layer if all conditions are satisfied:
        // 1. it is older than cutoff LSN;
        // 2. it is older than PITR interval;
        // 3. it doesn't need to be retained for 'retain_lsns';
        // 4. it does not need to be kept for LSNs holding valid leases.
        // 5. newer on-disk image layers cover the layer's whole key range
        let layers_to_remove = {
            let mut layers_to_remove = Vec::new();

            let guard = self
                .layers
                .read(LayerManagerLockHolder::GarbageCollection)
                .await;
            let layers = guard.layer_map()?;
            'outer: for l in layers.iter_historic_layers() {
                result.layers_total += 1;

                // 1. Is it newer than GC horizon cutoff point?
                if l.get_lsn_range().end > space_cutoff {
                    debug!(
                        "keeping {} because it's newer than space_cutoff {}",
                        l.layer_name(),
                        space_cutoff,
                    );
                    result.layers_needed_by_cutoff += 1;
                    continue 'outer;
                }

                // 2. It is newer than PiTR cutoff point?
                if l.get_lsn_range().end > time_cutoff {
                    debug!(
                        "keeping {} because it's newer than time_cutoff {}",
                        l.layer_name(),
                        time_cutoff,
                    );
                    result.layers_needed_by_pitr += 1;
                    continue 'outer;
                }

                // 3. Is it needed by a child branch?
                // NOTE With that we would keep data that
                // might be referenced by child branches forever.
                // We can track this in child timeline GC and delete parent layers when
                // they are no longer needed. This might be complicated with long inheritance chains.
                if let Some(retain_lsn) = max_retain_lsn {
                    // start_lsn is inclusive
                    if &l.get_lsn_range().start <= retain_lsn {
                        debug!(
                            "keeping {} because it's still might be referenced by child branch forked at {} is_dropped: xx is_incremental: {}",
                            l.layer_name(),
                            retain_lsn,
                            l.is_incremental(),
                        );
                        result.layers_needed_by_branches += 1;
                        continue 'outer;
                    }
                }

                // 4. Is there a valid lease that requires us to keep this layer?
                if let Some(lsn) = &max_lsn_with_valid_lease {
                    // keep if layer start <= any of the lease
                    if &l.get_lsn_range().start <= lsn {
                        debug!(
                            "keeping {} because there is a valid lease preventing GC at {}",
                            l.layer_name(),
                            lsn,
                        );
                        result.layers_needed_by_leases += 1;
                        continue 'outer;
                    }
                }

                // 5. Is there a later on-disk layer for this relation?
                //
                // The end-LSN is exclusive, while disk_consistent_lsn is
                // inclusive. For example, if disk_consistent_lsn is 100, it is
                // OK for a delta layer to have end LSN 101, but if the end LSN
                // is 102, then it might not have been fully flushed to disk
                // before crash.
                //
                // For example, imagine that the following layers exist:
                //
                // 1000      - image (A)
                // 1000-2000 - delta (B)
                // 2000      - image (C)
                // 2000-3000 - delta (D)
                // 3000      - image (E)
                //
                // If GC horizon is at 2500, we can remove layers A and B, but
                // we cannot remove C, even though it's older than 2500, because
                // the delta layer 2000-3000 depends on it.
                if !layers
                    .image_layer_exists(&l.get_key_range(), &(l.get_lsn_range().end..new_gc_cutoff))
                {
                    debug!("keeping {} because it is the latest layer", l.layer_name());
                    result.layers_not_updated += 1;
                    continue 'outer;
                }

                // We didn't find any reason to keep this file, so remove it.
                info!(
                    "garbage collecting {} is_dropped: xx is_incremental: {}",
                    l.layer_name(),
                    l.is_incremental(),
                );
                layers_to_remove.push(l);
            }

            layers_to_remove
        };

        if !layers_to_remove.is_empty() {
            // Persist the new GC cutoff value before we actually remove anything.
            // This unconditionally schedules also an index_part.json update, even though, we will
            // be doing one a bit later with the unlinked gc'd layers.
            let disk_consistent_lsn = self.disk_consistent_lsn.load();
            self.schedule_uploads(disk_consistent_lsn, None)
                .map_err(|e| {
                    if self.cancel.is_cancelled() {
                        GcError::TimelineCancelled
                    } else {
                        GcError::Remote(e)
                    }
                })?;

            let mut guard = self
                .layers
                .write(LayerManagerLockHolder::GarbageCollection)
                .await;

            let gc_layers = layers_to_remove
                .iter()
                .flat_map(|desc| guard.try_get_from_key(&desc.key()).cloned())
                .collect::<Vec<Layer>>();

            result.layers_removed = gc_layers.len() as u64;

            self.remote_client.schedule_gc_update(&gc_layers)?;
            guard.open_mut()?.finish_gc_timeline(&gc_layers);

            #[cfg(feature = "testing")]
            {
                result.doomed_layers = gc_layers;
            }
        }

        info!(
            "GC completed removing {} layers, cutoff {}",
            result.layers_removed, new_gc_cutoff
        );

        result.elapsed = now.elapsed().unwrap_or(Duration::ZERO);
        Ok(result)
    }

    /// Reconstruct a value, using the given base image and WAL records in 'data'.
    pub(crate) async fn reconstruct_value(
        &self,
        key: Key,
        request_lsn: Lsn,
        mut data: ValueReconstructState,
        redo_attempt_type: RedoAttemptType,
    ) -> Result<Bytes, PageReconstructError> {
        // Perform WAL redo if needed
        data.records.reverse();

        let fire_critical_error = match redo_attempt_type {
            RedoAttemptType::ReadPage => true,
            RedoAttemptType::LegacyCompaction => true,
            RedoAttemptType::GcCompaction => false,
        };

        // If we have a page image, and no WAL, we're all set
        if data.records.is_empty() {
            if let Some((img_lsn, img)) = &data.img {
                trace!(
                    "found page image for key {} at {}, no WAL redo required, req LSN {}",
                    key, img_lsn, request_lsn,
                );
                Ok(img.clone())
            } else {
                Err(PageReconstructError::from(anyhow!(
                    "base image for {key} at {request_lsn} not found"
                )))
            }
        } else {
            // We need to do WAL redo.
            //
            // If we don't have a base image, then the oldest WAL record better initialize
            // the page
            if data.img.is_none() && !data.records.first().unwrap().1.will_init() {
                Err(PageReconstructError::from(anyhow!(
                    "Base image for {} at {} not found, but got {} WAL records",
                    key,
                    request_lsn,
                    data.records.len()
                )))
            } else {
                if data.img.is_some() {
                    trace!(
                        "found {} WAL records and a base image for {} at {}, performing WAL redo",
                        data.records.len(),
                        key,
                        request_lsn
                    );
                } else {
                    trace!(
                        "found {} WAL records that will init the page for {} at {}, performing WAL redo",
                        data.records.len(),
                        key,
                        request_lsn
                    );
                };
                let res = self
                    .walredo_mgr
                    .as_ref()
                    .context("timeline has no walredo manager")
                    .map_err(PageReconstructError::WalRedo)?
                    .request_redo(
                        key,
                        request_lsn,
                        data.img,
                        data.records,
                        self.pg_version,
                        redo_attempt_type,
                    )
                    .await;
                let img = match res {
                    Ok(img) => img,
                    Err(walredo::Error::Cancelled) => return Err(PageReconstructError::Cancelled),
                    Err(walredo::Error::Other(err)) => {
                        if fire_critical_error {
                            critical_timeline!(
                                self.tenant_shard_id,
                                self.timeline_id,
                                "walredo failure during page reconstruction: {err:?}"
                            );
                        }
                        return Err(PageReconstructError::WalRedo(
                            err.context("reconstruct a page image"),
                        ));
                    }
                };
                Ok(img)
            }
        }
    }

    pub(crate) async fn spawn_download_all_remote_layers(
        self: Arc<Self>,
        request: DownloadRemoteLayersTaskSpawnRequest,
        ctx: &RequestContext,
    ) -> Result<DownloadRemoteLayersTaskInfo, DownloadRemoteLayersTaskInfo> {
        use pageserver_api::models::DownloadRemoteLayersTaskState;

        // this is not really needed anymore; it has tests which really check the return value from
        // http api. it would be better not to maintain this anymore.

        let mut status_guard = self.download_all_remote_layers_task_info.write().unwrap();
        if let Some(st) = &*status_guard {
            match &st.state {
                DownloadRemoteLayersTaskState::Running => {
                    return Err(st.clone());
                }
                DownloadRemoteLayersTaskState::ShutDown
                | DownloadRemoteLayersTaskState::Completed => {
                    *status_guard = None;
                }
            }
        }

        let self_clone = Arc::clone(&self);
        let task_ctx = ctx.detached_child(
            TaskKind::DownloadAllRemoteLayers,
            DownloadBehavior::Download,
        );
        let task_id = task_mgr::spawn(
            task_mgr::BACKGROUND_RUNTIME.handle(),
            task_mgr::TaskKind::DownloadAllRemoteLayers,
            self.tenant_shard_id,
            Some(self.timeline_id),
            "download all remote layers task",
            async move {
                self_clone.download_all_remote_layers(request, &task_ctx).await;
                let mut status_guard = self_clone.download_all_remote_layers_task_info.write().unwrap();
                 match &mut *status_guard {
                    None => {
                        warn!("tasks status is supposed to be Some(), since we are running");
                    }
                    Some(st) => {
                        let exp_task_id = format!("{}", task_mgr::current_task_id().unwrap());
                        if st.task_id != exp_task_id {
                            warn!("task id changed while we were still running, expecting {} but have {}", exp_task_id, st.task_id);
                        } else {
                            st.state = DownloadRemoteLayersTaskState::Completed;
                        }
                    }
                };
                Ok(())
            }
            .instrument(info_span!(parent: None, "download_all_remote_layers", tenant_id = %self.tenant_shard_id.tenant_id, shard_id = %self.tenant_shard_id.shard_slug(), timeline_id = %self.timeline_id))
        );

        let initial_info = DownloadRemoteLayersTaskInfo {
            task_id: format!("{task_id}"),
            state: DownloadRemoteLayersTaskState::Running,
            total_layer_count: 0,
            successful_download_count: 0,
            failed_download_count: 0,
        };
        *status_guard = Some(initial_info.clone());

        Ok(initial_info)
    }

    async fn download_all_remote_layers(
        self: &Arc<Self>,
        request: DownloadRemoteLayersTaskSpawnRequest,
        ctx: &RequestContext,
    ) {
        use pageserver_api::models::DownloadRemoteLayersTaskState;

        let remaining = {
            let guard = self
                .layers
                .read(LayerManagerLockHolder::GetLayerMapInfo)
                .await;
            let Ok(lm) = guard.layer_map() else {
                // technically here we could look into iterating accessible layers, but downloading
                // all layers of a shutdown timeline makes no sense regardless.
                tracing::info!("attempted to download all layers of shutdown timeline");
                return;
            };
            lm.iter_historic_layers()
                .map(|desc| guard.get_from_desc(&desc))
                .collect::<Vec<_>>()
        };
        let total_layer_count = remaining.len();

        macro_rules! lock_status {
            ($st:ident) => {
                let mut st = self.download_all_remote_layers_task_info.write().unwrap();
                let st = st
                    .as_mut()
                    .expect("this function is only called after the task has been spawned");
                assert_eq!(
                    st.task_id,
                    format!(
                        "{}",
                        task_mgr::current_task_id().expect("we run inside a task_mgr task")
                    )
                );
                let $st = st;
            };
        }

        {
            lock_status!(st);
            st.total_layer_count = total_layer_count as u64;
        }

        let mut remaining = remaining.into_iter();
        let mut have_remaining = true;
        let mut js = tokio::task::JoinSet::new();

        let cancel = task_mgr::shutdown_token();

        let limit = request.max_concurrent_downloads;

        loop {
            while js.len() < limit.get() && have_remaining && !cancel.is_cancelled() {
                let Some(next) = remaining.next() else {
                    have_remaining = false;
                    break;
                };

                let span = tracing::info_span!("download", layer = %next);

                let ctx = ctx.attached_child();
                js.spawn(
                    async move {
                        let res = next.download(&ctx).await;
                        (next, res)
                    }
                    .instrument(span),
                );
            }

            while let Some(res) = js.join_next().await {
                match res {
                    Ok((_, Ok(_))) => {
                        lock_status!(st);
                        st.successful_download_count += 1;
                    }
                    Ok((layer, Err(e))) => {
                        tracing::error!(%layer, "download failed: {e:#}");
                        lock_status!(st);
                        st.failed_download_count += 1;
                    }
                    Err(je) if je.is_cancelled() => unreachable!("not used here"),
                    Err(je) if je.is_panic() => {
                        lock_status!(st);
                        st.failed_download_count += 1;
                    }
                    Err(je) => tracing::warn!("unknown joinerror: {je:?}"),
                }
            }

            if js.is_empty() && (!have_remaining || cancel.is_cancelled()) {
                break;
            }
        }

        {
            lock_status!(st);
            st.state = DownloadRemoteLayersTaskState::Completed;
        }
    }

    pub(crate) fn get_download_all_remote_layers_task_info(
        &self,
    ) -> Option<DownloadRemoteLayersTaskInfo> {
        self.download_all_remote_layers_task_info
            .read()
            .unwrap()
            .clone()
    }
}

impl Timeline {
    /// Returns non-remote layers for eviction.
    pub(crate) async fn get_local_layers_for_disk_usage_eviction(&self) -> DiskUsageEvictionInfo {
        let guard = self.layers.read(LayerManagerLockHolder::Eviction).await;
        let mut max_layer_size: Option<u64> = None;

        let resident_layers = guard
            .likely_resident_layers()
            .map(|layer| {
                let file_size = layer.layer_desc().file_size;
                max_layer_size = max_layer_size.map_or(Some(file_size), |m| Some(m.max(file_size)));

                let last_activity_ts = layer.latest_activity();

                EvictionCandidate {
                    layer: layer.to_owned().into(),
                    last_activity_ts,
                    relative_last_activity: finite_f32::FiniteF32::ZERO,
                    visibility: layer.visibility(),
                }
            })
            .collect();

        DiskUsageEvictionInfo {
            max_layer_size,
            resident_layers,
        }
    }

    pub(crate) fn get_shard_index(&self) -> ShardIndex {
        ShardIndex {
            shard_number: self.tenant_shard_id.shard_number,
            shard_count: self.tenant_shard_id.shard_count,
        }
    }

    /// Persistently blocks gc for `Manual` reason.
    ///
    /// Returns true if no such block existed before, false otherwise.
    pub(crate) async fn block_gc(&self, tenant: &super::TenantShard) -> anyhow::Result<bool> {
        use crate::tenant::remote_timeline_client::index::GcBlockingReason;
        assert_eq!(self.tenant_shard_id, tenant.tenant_shard_id);
        tenant.gc_block.insert(self, GcBlockingReason::Manual).await
    }

    /// Persistently unblocks gc for `Manual` reason.
    pub(crate) async fn unblock_gc(&self, tenant: &super::TenantShard) -> anyhow::Result<()> {
        use crate::tenant::remote_timeline_client::index::GcBlockingReason;
        assert_eq!(self.tenant_shard_id, tenant.tenant_shard_id);
        tenant.gc_block.remove(self, GcBlockingReason::Manual).await
    }

    #[cfg(test)]
    pub(super) fn force_advance_lsn(self: &Arc<Timeline>, new_lsn: Lsn) {
        self.last_record_lsn.advance(new_lsn);
    }

    #[cfg(test)]
    pub(super) fn force_set_disk_consistent_lsn(&self, new_value: Lsn) {
        self.disk_consistent_lsn.store(new_value);
    }

    /// Force create an image layer and place it into the layer map.
    ///
    /// DO NOT use this function directly. Use [`TenantShard::branch_timeline_test_with_layers`]
    /// or [`TenantShard::create_test_timeline_with_layers`] to ensure all these layers are
    /// placed into the layer map in one run AND be validated.
    #[cfg(test)]
    pub(super) async fn force_create_image_layer(
        self: &Arc<Timeline>,
        lsn: Lsn,
        mut images: Vec<(Key, Bytes)>,
        check_start_lsn: Option<Lsn>,
        ctx: &RequestContext,
    ) -> anyhow::Result<()> {
        let last_record_lsn = self.get_last_record_lsn();
        assert!(
            lsn <= last_record_lsn,
            "advance last record lsn before inserting a layer, lsn={lsn}, last_record_lsn={last_record_lsn}"
        );
        if let Some(check_start_lsn) = check_start_lsn {
            assert!(lsn >= check_start_lsn);
        }
        images.sort_unstable_by(|(ka, _), (kb, _)| ka.cmp(kb));
        let min_key = *images.first().map(|(k, _)| k).unwrap();
        let end_key = images.last().map(|(k, _)| k).unwrap().next();
        let mut image_layer_writer = ImageLayerWriter::new(
            self.conf,
            self.timeline_id,
            self.tenant_shard_id,
            &(min_key..end_key),
            lsn,
            &self.gate,
            self.cancel.clone(),
            ctx,
        )
        .await?;
        for (key, img) in images {
            image_layer_writer.put_image(key, img, ctx).await?;
        }
        let (desc, path) = image_layer_writer.finish(ctx).await?;
        let image_layer = Layer::finish_creating(self.conf, self, desc, &path)?;
        info!("force created image layer {}", image_layer.local_path());
        {
            let mut guard = self.layers.write(LayerManagerLockHolder::Testing).await;
            guard
                .open_mut()
                .unwrap()
                .force_insert_layer(image_layer.clone());
        }

        // Update remote_timeline_client state to reflect existence of this layer
        self.remote_client
            .schedule_layer_file_upload(image_layer)
            .unwrap();

        Ok(())
    }

    /// Force create a delta layer and place it into the layer map.
    ///
    /// DO NOT use this function directly. Use [`TenantShard::branch_timeline_test_with_layers`]
    /// or [`TenantShard::create_test_timeline_with_layers`] to ensure all these layers are
    /// placed into the layer map in one run AND be validated.
    #[cfg(test)]
    pub(super) async fn force_create_delta_layer(
        self: &Arc<Timeline>,
        mut deltas: DeltaLayerTestDesc,
        check_start_lsn: Option<Lsn>,
        ctx: &RequestContext,
    ) -> anyhow::Result<()> {
        let last_record_lsn = self.get_last_record_lsn();
        deltas
            .data
            .sort_unstable_by(|(ka, la, _), (kb, lb, _)| (ka, la).cmp(&(kb, lb)));
        assert!(deltas.data.first().unwrap().0 >= deltas.key_range.start);
        assert!(deltas.data.last().unwrap().0 < deltas.key_range.end);
        for (_, lsn, _) in &deltas.data {
            assert!(deltas.lsn_range.start <= *lsn && *lsn < deltas.lsn_range.end);
        }
        assert!(
            deltas.lsn_range.end <= last_record_lsn,
            "advance last record lsn before inserting a layer, end_lsn={}, last_record_lsn={}",
            deltas.lsn_range.end,
            last_record_lsn
        );
        if let Some(check_start_lsn) = check_start_lsn {
            assert!(deltas.lsn_range.start >= check_start_lsn);
        }
        let mut delta_layer_writer = DeltaLayerWriter::new(
            self.conf,
            self.timeline_id,
            self.tenant_shard_id,
            deltas.key_range.start,
            deltas.lsn_range,
            &self.gate,
            self.cancel.clone(),
            ctx,
        )
        .await?;
        for (key, lsn, val) in deltas.data {
            delta_layer_writer.put_value(key, lsn, val, ctx).await?;
        }
        let (desc, path) = delta_layer_writer.finish(deltas.key_range.end, ctx).await?;
        let delta_layer = Layer::finish_creating(self.conf, self, desc, &path)?;
        info!("force created delta layer {}", delta_layer.local_path());
        {
            let mut guard = self.layers.write(LayerManagerLockHolder::Testing).await;
            guard
                .open_mut()
                .unwrap()
                .force_insert_layer(delta_layer.clone());
        }

        // Update remote_timeline_client state to reflect existence of this layer
        self.remote_client
            .schedule_layer_file_upload(delta_layer)
            .unwrap();

        Ok(())
    }

    /// Force create an in-memory layer and place them into the layer map.
    #[cfg(test)]
    pub(super) async fn force_create_in_memory_layer(
        self: &Arc<Timeline>,
        mut in_memory: InMemoryLayerTestDesc,
        check_start_lsn: Option<Lsn>,
        ctx: &RequestContext,
    ) -> anyhow::Result<()> {
        use utils::bin_ser::BeSer;

        // Validate LSNs
        if let Some(check_start_lsn) = check_start_lsn {
            assert!(in_memory.lsn_range.start >= check_start_lsn);
        }

        let last_record_lsn = self.get_last_record_lsn();
        let layer_end_lsn = if in_memory.is_open {
            in_memory
                .data
                .iter()
                .map(|(_key, lsn, _value)| lsn)
                .max()
                .cloned()
        } else {
            Some(in_memory.lsn_range.end)
        };

        if let Some(end) = layer_end_lsn {
            assert!(
                end <= last_record_lsn,
                "advance last record lsn before inserting a layer, end_lsn={end}, last_record_lsn={last_record_lsn}",
            );
        }

        in_memory.data.iter().for_each(|(_key, lsn, _value)| {
            assert!(*lsn >= in_memory.lsn_range.start);
            assert!(*lsn < in_memory.lsn_range.end);
        });

        // Build the batch
        in_memory
            .data
            .sort_unstable_by(|(ka, la, _), (kb, lb, _)| (ka, la).cmp(&(kb, lb)));

        let data = in_memory
            .data
            .into_iter()
            .map(|(key, lsn, value)| {
                let value_size = value.serialized_size().unwrap() as usize;
                (key.to_compact(), lsn, value_size, value)
            })
            .collect::<Vec<_>>();

        let batch = SerializedValueBatch::from_values(data);

        // Create the in-memory layer and write the batch into it
        let layer = InMemoryLayer::create(
            self.conf,
            self.timeline_id,
            self.tenant_shard_id,
            in_memory.lsn_range.start,
            &self.gate,
            // TODO: if we ever use this function in production code, we need to pass the real cancellation token
            &CancellationToken::new(),
            ctx,
        )
        .await
        .unwrap();

        layer.put_batch(batch, ctx).await.unwrap();
        if !in_memory.is_open {
            layer.freeze(in_memory.lsn_range.end).await;
        }

        info!("force created in-memory layer {:?}", in_memory.lsn_range);

        // Link the layer to the layer map
        {
            let mut guard = self.layers.write(LayerManagerLockHolder::Testing).await;
            let layer_map = guard.open_mut().unwrap();
            layer_map.force_insert_in_memory_layer(Arc::new(layer));
        }

        Ok(())
    }

    /// Return all keys at the LSN in the image layers
    #[cfg(test)]
    pub(crate) async fn inspect_image_layers(
        self: &Arc<Timeline>,
        lsn: Lsn,
        ctx: &RequestContext,
        io_concurrency: IoConcurrency,
    ) -> anyhow::Result<Vec<(Key, Bytes)>> {
        let mut all_data = Vec::new();
        let guard = self.layers.read(LayerManagerLockHolder::Testing).await;
        for layer in guard.layer_map()?.iter_historic_layers() {
            if !layer.is_delta() && layer.image_layer_lsn() == lsn {
                let layer = guard.get_from_desc(&layer);
                let mut reconstruct_data = ValuesReconstructState::new(io_concurrency.clone());
                layer
                    .get_values_reconstruct_data(
                        KeySpace::single(Key::MIN..Key::MAX),
                        lsn..Lsn(lsn.0 + 1),
                        &mut reconstruct_data,
                        ctx,
                    )
                    .await?;
                for (k, v) in std::mem::take(&mut reconstruct_data.keys) {
                    let v = v.collect_pending_ios().await?;
                    all_data.push((k, v.img.unwrap().1));
                }
            }
        }
        all_data.sort();
        Ok(all_data)
    }

    /// Get all historic layer descriptors in the layer map
    #[cfg(test)]
    pub(crate) async fn inspect_historic_layers(
        self: &Arc<Timeline>,
    ) -> anyhow::Result<Vec<super::storage_layer::PersistentLayerKey>> {
        let mut layers = Vec::new();
        let guard = self.layers.read(LayerManagerLockHolder::Testing).await;
        for layer in guard.layer_map()?.iter_historic_layers() {
            layers.push(layer.key());
        }
        Ok(layers)
    }

    #[cfg(test)]
    pub(crate) fn add_extra_test_dense_keyspace(&self, ks: KeySpace) {
        let mut keyspace = self.extra_test_dense_keyspace.load().as_ref().clone();
        keyspace.merge(&ks);
        self.extra_test_dense_keyspace.store(Arc::new(keyspace));
    }
}

/// Tracking writes ingestion does to a particular in-memory layer.
///
/// Cleared upon freezing a layer.
pub(crate) struct TimelineWriterState {
    open_layer: Arc<InMemoryLayer>,
    current_size: u64,
    // Previous Lsn which passed through
    prev_lsn: Option<Lsn>,
    // Largest Lsn which passed through the current writer
    max_lsn: Option<Lsn>,
    // Cached details of the last freeze. Avoids going trough the atomic/lock on every put.
    cached_last_freeze_at: Lsn,
}

impl TimelineWriterState {
    fn new(open_layer: Arc<InMemoryLayer>, current_size: u64, last_freeze_at: Lsn) -> Self {
        Self {
            open_layer,
            current_size,
            prev_lsn: None,
            max_lsn: None,
            cached_last_freeze_at: last_freeze_at,
        }
    }
}

/// Various functions to mutate the timeline.
// TODO Currently, Deref is used to allow easy access to read methods from this trait.
// This is probably considered a bad practice in Rust and should be fixed eventually,
// but will cause large code changes.
pub(crate) struct TimelineWriter<'a> {
    tl: &'a Timeline,
    write_guard: tokio::sync::MutexGuard<'a, Option<TimelineWriterState>>,
}

impl Deref for TimelineWriter<'_> {
    type Target = Timeline;

    fn deref(&self) -> &Self::Target {
        self.tl
    }
}

#[derive(PartialEq)]
enum OpenLayerAction {
    Roll,
    Open,
    None,
}

impl TimelineWriter<'_> {
    async fn handle_open_layer_action(
        &mut self,
        at: Lsn,
        action: OpenLayerAction,
        ctx: &RequestContext,
    ) -> anyhow::Result<&Arc<InMemoryLayer>> {
        match action {
            OpenLayerAction::Roll => {
                let freeze_at = self.write_guard.as_ref().unwrap().max_lsn.unwrap();
                self.roll_layer(freeze_at).await?;
                self.open_layer(at, ctx).await?;
            }
            OpenLayerAction::Open => self.open_layer(at, ctx).await?,
            OpenLayerAction::None => {
                assert!(self.write_guard.is_some());
            }
        }

        Ok(&self.write_guard.as_ref().unwrap().open_layer)
    }

    async fn open_layer(&mut self, at: Lsn, ctx: &RequestContext) -> anyhow::Result<()> {
        let layer = self
            .tl
            .get_layer_for_write(at, &self.write_guard, ctx)
            .await?;
        let initial_size = layer.len();

        let last_freeze_at = self.last_freeze_at.load();
        self.write_guard.replace(TimelineWriterState::new(
            layer,
            initial_size,
            last_freeze_at,
        ));

        Ok(())
    }

    async fn roll_layer(&mut self, freeze_at: Lsn) -> Result<(), FlushLayerError> {
        let current_size = self.write_guard.as_ref().unwrap().current_size;

        // If layer flushes are backpressured due to compaction not keeping up, wait for the flush
        // to propagate the backpressure up into WAL ingestion.
        let l0_count = self
            .tl
            .layers
            .read(LayerManagerLockHolder::GetLayerMapInfo)
            .await
            .layer_map()?
            .level0_deltas()
            .len();
        let wait_thresholds = [
            self.get_l0_flush_delay_threshold(),
            self.get_l0_flush_stall_threshold(),
        ];
        let wait_threshold = wait_thresholds.into_iter().flatten().min();

        // self.write_guard will be taken by the freezing
        let flush_id = self
            .tl
            .freeze_inmem_layer_at(freeze_at, &mut self.write_guard)
            .await?;

        assert!(self.write_guard.is_none());

        if let Some(wait_threshold) = wait_threshold {
            if l0_count >= wait_threshold {
                debug!(
                    "layer roll waiting for flush due to compaction backpressure at {l0_count} L0 layers"
                );
                self.tl.wait_flush_completion(flush_id).await?;
            }
        }

        if current_size >= self.get_checkpoint_distance() * 2 {
            warn!("Flushed oversized open layer with size {}", current_size)
        }

        Ok(())
    }

    fn get_open_layer_action(&self, lsn: Lsn, new_value_size: u64) -> OpenLayerAction {
        let state = &*self.write_guard;
        let Some(state) = &state else {
            return OpenLayerAction::Open;
        };

        #[cfg(feature = "testing")]
        if state.cached_last_freeze_at < self.tl.last_freeze_at.load() {
            // this check and assertion are not really needed because
            // LayerManager::try_freeze_in_memory_layer will always clear out the
            // TimelineWriterState if something is frozen. however, we can advance last_freeze_at when there
            // is no TimelineWriterState.
            assert!(
                state.open_layer.end_lsn.get().is_some(),
                "our open_layer must be outdated"
            );

            // this would be a memory leak waiting to happen because the in-memory layer always has
            // an index
            panic!("BUG: TimelineWriterState held on to frozen in-memory layer.");
        }

        if state.prev_lsn == Some(lsn) {
            // Rolling mid LSN is not supported by [downstream code].
            // Hence, only roll at LSN boundaries.
            //
            // [downstream code]: https://github.com/neondatabase/neon/pull/7993#discussion_r1633345422
            return OpenLayerAction::None;
        }

        if state.current_size == 0 {
            // Don't roll empty layers
            return OpenLayerAction::None;
        }

        if self.tl.should_roll(
            state.current_size,
            state.current_size + new_value_size,
            self.get_checkpoint_distance(),
            lsn,
            state.cached_last_freeze_at,
            state.open_layer.get_opened_at(),
        ) {
            OpenLayerAction::Roll
        } else {
            OpenLayerAction::None
        }
    }

    /// Put a batch of keys at the specified Lsns.
    pub(crate) async fn put_batch(
        &mut self,
        batch: SerializedValueBatch,
        ctx: &RequestContext,
    ) -> anyhow::Result<()> {
        if !batch.has_data() {
            return Ok(());
        }

        // In debug builds, assert that we don't write any keys that don't belong to this shard.
        // We don't assert this in release builds, since key ownership policies may change over
        // time. Stray keys will be removed during compaction.
        if cfg!(debug_assertions) {
            for metadata in &batch.metadata {
                if let ValueMeta::Serialized(metadata) = metadata {
                    let key = Key::from_compact(metadata.key);
                    assert!(
                        self.shard_identity.is_key_local(&key)
                            || self.shard_identity.is_key_global(&key),
                        "key {key} does not belong on shard {}",
                        self.shard_identity.shard_index()
                    );
                }
            }
        }

        let batch_max_lsn = batch.max_lsn;
        let buf_size: u64 = batch.buffer_size() as u64;

        let action = self.get_open_layer_action(batch_max_lsn, buf_size);
        let layer = self
            .handle_open_layer_action(batch_max_lsn, action, ctx)
            .await?;

        let res = layer.put_batch(batch, ctx).await;

        if res.is_ok() {
            // Update the current size only when the entire write was ok.
            // In case of failures, we may have had partial writes which
            // render the size tracking out of sync. That's ok because
            // the checkpoint distance should be significantly smaller
            // than the S3 single shot upload limit of 5GiB.
            let state = self.write_guard.as_mut().unwrap();

            state.current_size += buf_size;
            state.prev_lsn = Some(batch_max_lsn);
            state.max_lsn = std::cmp::max(state.max_lsn, Some(batch_max_lsn));
        }

        res
    }

    #[cfg(test)]
    /// Test helper, for tests that would like to poke individual values without composing a batch
    pub(crate) async fn put(
        &mut self,
        key: Key,
        lsn: Lsn,
        value: &Value,
        ctx: &RequestContext,
    ) -> anyhow::Result<()> {
        use utils::bin_ser::BeSer;
        if !key.is_valid_key_on_write_path() {
            bail!(
                "the request contains data not supported by pageserver at TimelineWriter::put: {}",
                key
            );
        }
        let val_ser_size = value.serialized_size().unwrap() as usize;
        let batch = SerializedValueBatch::from_values(vec![(
            key.to_compact(),
            lsn,
            val_ser_size,
            value.clone(),
        )]);

        self.put_batch(batch, ctx).await
    }

    pub(crate) async fn delete_batch(
        &mut self,
        batch: &[(Range<Key>, Lsn)],
        ctx: &RequestContext,
    ) -> anyhow::Result<()> {
        if let Some((_, lsn)) = batch.first() {
            let action = self.get_open_layer_action(*lsn, 0);
            let layer = self.handle_open_layer_action(*lsn, action, ctx).await?;
            layer.put_tombstones(batch).await?;
        }

        Ok(())
    }

    /// Track the end of the latest digested WAL record.
    /// Remember the (end of) last valid WAL record remembered in the timeline.
    ///
    /// Call this after you have finished writing all the WAL up to 'lsn'.
    ///
    /// 'lsn' must be aligned. This wakes up any wait_lsn() callers waiting for
    /// the 'lsn' or anything older. The previous last record LSN is stored alongside
    /// the latest and can be read.
    pub(crate) fn finish_write(&self, new_lsn: Lsn) {
        self.tl.finish_write(new_lsn);
    }

    pub(crate) fn update_current_logical_size(&self, delta: i64) {
        self.tl.update_current_logical_size(delta)
    }
}

// We need TimelineWriter to be send in upcoming conversion of
// Timeline::layers to tokio::sync::RwLock.
#[test]
fn is_send() {
    fn _assert_send<T: Send>() {}
    _assert_send::<TimelineWriter<'_>>();
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use pageserver_api::key::Key;
    use postgres_ffi::PgMajorVersion;
    use std::iter::Iterator;
    use tracing::Instrument;
    use utils::id::TimelineId;
    use utils::lsn::Lsn;
    use wal_decoder::models::value::Value;

    use super::HeatMapTimeline;
    use crate::context::RequestContextBuilder;
    use crate::tenant::harness::{TenantHarness, test_img};
    use crate::tenant::layer_map::LayerMap;
    use crate::tenant::storage_layer::{Layer, LayerName, LayerVisibilityHint};
    use crate::tenant::timeline::layer_manager::LayerManagerLockHolder;
    use crate::tenant::timeline::{DeltaLayerTestDesc, EvictionError};
    use crate::tenant::{PreviousHeatmap, Timeline};

    fn assert_heatmaps_have_same_layers(lhs: &HeatMapTimeline, rhs: &HeatMapTimeline) {
        assert_eq!(lhs.all_layers().count(), rhs.all_layers().count());
        let lhs_rhs = lhs.all_layers().zip(rhs.all_layers());
        for (l, r) in lhs_rhs {
            assert_eq!(l.name, r.name);
            assert_eq!(l.metadata, r.metadata);
        }
    }

    #[tokio::test]
    async fn test_heatmap_generation() {
        let harness = TenantHarness::create("heatmap_generation").await.unwrap();

        let covered_delta = DeltaLayerTestDesc::new_with_inferred_key_range(
            Lsn(0x10)..Lsn(0x20),
            vec![(
                Key::from_hex("620000000033333333444444445500000000").unwrap(),
                Lsn(0x11),
                Value::Image(test_img("foo")),
            )],
        );
        let visible_delta = DeltaLayerTestDesc::new_with_inferred_key_range(
            Lsn(0x10)..Lsn(0x20),
            vec![(
                Key::from_hex("720000000033333333444444445500000000").unwrap(),
                Lsn(0x11),
                Value::Image(test_img("foo")),
            )],
        );
        let l0_delta = DeltaLayerTestDesc::new(
            Lsn(0x20)..Lsn(0x30),
            Key::from_hex("000000000000000000000000000000000000").unwrap()
                ..Key::from_hex("FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF").unwrap(),
            vec![(
                Key::from_hex("720000000033333333444444445500000000").unwrap(),
                Lsn(0x25),
                Value::Image(test_img("foo")),
            )],
        );
        let delta_layers = vec![
            covered_delta.clone(),
            visible_delta.clone(),
            l0_delta.clone(),
        ];

        let image_layer = (
            Lsn(0x40),
            vec![(
                Key::from_hex("620000000033333333444444445500000000").unwrap(),
                test_img("bar"),
            )],
        );
        let image_layers = vec![image_layer];

        let (tenant, ctx) = harness.load().await;
        let timeline = tenant
            .create_test_timeline_with_layers(
                TimelineId::generate(),
                Lsn(0x10),
                PgMajorVersion::PG14,
                &ctx,
                Vec::new(), // in-memory layers
                delta_layers,
                image_layers,
                Lsn(0x100),
            )
            .await
            .unwrap();
        let ctx = &ctx.with_scope_timeline(&timeline);

        // Layer visibility is an input to heatmap generation, so refresh it first
        timeline.update_layer_visibility().await.unwrap();

        let heatmap = timeline
            .generate_heatmap()
            .await
            .expect("Infallible while timeline is not shut down");

        assert_eq!(heatmap.timeline_id, timeline.timeline_id);

        // L0 should come last
        let heatmap_layers = heatmap.all_layers().collect::<Vec<_>>();
        assert_eq!(heatmap_layers.last().unwrap().name, l0_delta.layer_name());

        let mut last_lsn = Lsn::MAX;
        for layer in heatmap_layers {
            // Covered layer should be omitted
            assert!(layer.name != covered_delta.layer_name());

            let layer_lsn = match &layer.name {
                LayerName::Delta(d) => d.lsn_range.end,
                LayerName::Image(i) => i.lsn,
            };

            // Apart from L0s, newest Layers should come first
            if !LayerMap::is_l0(layer.name.key_range(), layer.name.is_delta()) {
                assert!(layer_lsn <= last_lsn);
                last_lsn = layer_lsn;
            }
        }

        // Evict all the layers and stash the old heatmap in the timeline.
        // This simulates a migration to a cold secondary location.

        let guard = timeline.layers.read(LayerManagerLockHolder::Testing).await;
        let mut all_layers = Vec::new();
        let forever = std::time::Duration::from_secs(120);
        for layer in guard.likely_resident_layers() {
            all_layers.push(layer.clone());
            layer.evict_and_wait(forever).await.unwrap();
        }
        drop(guard);

        timeline
            .previous_heatmap
            .store(Some(Arc::new(PreviousHeatmap::Active {
                heatmap: heatmap.clone(),
                read_at: std::time::Instant::now(),
                end_lsn: None,
            })));

        // Generate a new heatmap and assert that it contains the same layers as the old one.
        let post_migration_heatmap = timeline.generate_heatmap().await.unwrap();
        assert_heatmaps_have_same_layers(&heatmap, &post_migration_heatmap);

        // Download each layer one by one. Generate the heatmap at each step and check
        // that it's stable.
        for layer in all_layers {
            if layer.visibility() == LayerVisibilityHint::Covered {
                continue;
            }

            eprintln!("Downloading {layer} and re-generating heatmap");

            let ctx = &RequestContextBuilder::from(ctx)
                .download_behavior(crate::context::DownloadBehavior::Download)
                .attached_child();

            let _resident = layer
                .download_and_keep_resident(ctx)
                .instrument(tracing::info_span!(
                    parent: None,
                    "download_layer",
                    tenant_id = %timeline.tenant_shard_id.tenant_id,
                    shard_id = %timeline.tenant_shard_id.shard_slug(),
                    timeline_id = %timeline.timeline_id
                ))
                .await
                .unwrap();

            let post_download_heatmap = timeline.generate_heatmap().await.unwrap();
            assert_heatmaps_have_same_layers(&heatmap, &post_download_heatmap);
        }

        // Everything from the post-migration heatmap is now resident.
        // Check that we drop it from memory.
        assert!(matches!(
            timeline.previous_heatmap.load().as_deref(),
            Some(PreviousHeatmap::Obsolete)
        ));
    }

    #[tokio::test]
    async fn test_previous_heatmap_obsoletion() {
        let harness = TenantHarness::create("heatmap_previous_heatmap_obsoletion")
            .await
            .unwrap();

        let l0_delta = DeltaLayerTestDesc::new(
            Lsn(0x20)..Lsn(0x30),
            Key::from_hex("000000000000000000000000000000000000").unwrap()
                ..Key::from_hex("FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF").unwrap(),
            vec![(
                Key::from_hex("720000000033333333444444445500000000").unwrap(),
                Lsn(0x25),
                Value::Image(test_img("foo")),
            )],
        );

        let image_layer = (
            Lsn(0x40),
            vec![(
                Key::from_hex("620000000033333333444444445500000000").unwrap(),
                test_img("bar"),
            )],
        );

        let delta_layers = vec![l0_delta];
        let image_layers = vec![image_layer];

        let (tenant, ctx) = harness.load().await;
        let timeline = tenant
            .create_test_timeline_with_layers(
                TimelineId::generate(),
                Lsn(0x10),
                PgMajorVersion::PG14,
                &ctx,
                Vec::new(), // in-memory layers
                delta_layers,
                image_layers,
                Lsn(0x100),
            )
            .await
            .unwrap();

        // Layer visibility is an input to heatmap generation, so refresh it first
        timeline.update_layer_visibility().await.unwrap();

        let heatmap = timeline
            .generate_heatmap()
            .await
            .expect("Infallible while timeline is not shut down");

        // Both layers should be in the heatmap
        assert!(heatmap.all_layers().count() > 0);

        // Now simulate a migration.
        timeline
            .previous_heatmap
            .store(Some(Arc::new(PreviousHeatmap::Active {
                heatmap: heatmap.clone(),
                read_at: std::time::Instant::now(),
                end_lsn: None,
            })));

        // Evict all the layers in the previous heatmap
        let guard = timeline.layers.read(LayerManagerLockHolder::Testing).await;
        let forever = std::time::Duration::from_secs(120);
        for layer in guard.likely_resident_layers() {
            layer.evict_and_wait(forever).await.unwrap();
        }
        drop(guard);

        // Generate a new heatmap and check that the previous heatmap
        // has been marked obsolete.
        let post_eviction_heatmap = timeline
            .generate_heatmap()
            .await
            .expect("Infallible while timeline is not shut down");

        assert_eq!(post_eviction_heatmap.all_layers().count(), 0);
        assert!(matches!(
            timeline.previous_heatmap.load().as_deref(),
            Some(PreviousHeatmap::Obsolete)
        ));
    }

    #[tokio::test]
    async fn two_layer_eviction_attempts_at_the_same_time() {
        let harness = TenantHarness::create("two_layer_eviction_attempts_at_the_same_time")
            .await
            .unwrap();

        let (tenant, ctx) = harness.load().await;
        let timeline = tenant
            .create_test_timeline(
                TimelineId::generate(),
                Lsn(0x10),
                PgMajorVersion::PG14,
                &ctx,
            )
            .await
            .unwrap();

        let layer = find_some_layer(&timeline).await;
        let layer = layer
            .keep_resident()
            .await
            .expect("no download => no downloading errors")
            .drop_eviction_guard();

        let forever = std::time::Duration::from_secs(120);

        let first = layer.evict_and_wait(forever);
        let second = layer.evict_and_wait(forever);

        let (first, second) = tokio::join!(first, second);

        let res = layer.keep_resident().await;
        assert!(res.is_none(), "{res:?}");

        match (first, second) {
            (Ok(()), Ok(())) => {
                // because there are no more timeline locks being taken on eviction path, we can
                // witness all three outcomes here.
            }
            (Ok(()), Err(EvictionError::NotFound)) | (Err(EvictionError::NotFound), Ok(())) => {
                // if one completes before the other, this is fine just as well.
            }
            other => unreachable!("unexpected {:?}", other),
        }
    }

    async fn find_some_layer(timeline: &Timeline) -> Layer {
        let layers = timeline
            .layers
            .read(LayerManagerLockHolder::GetLayerMapInfo)
            .await;
        let desc = layers
            .layer_map()
            .unwrap()
            .iter_historic_layers()
            .next()
            .expect("must find one layer to evict");

        layers.get_from_desc(&desc)
    }
}

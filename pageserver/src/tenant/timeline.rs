mod compaction;
pub mod delete;
mod eviction_task;
mod init;
pub mod layer_manager;
pub(crate) mod logical_size;
pub mod span;
pub mod uninit;
mod walreceiver;

use anyhow::{anyhow, bail, ensure, Context, Result};
use arc_swap::ArcSwap;
use bytes::Bytes;
use camino::Utf8Path;
use enumset::EnumSet;
use fail::fail_point;
use once_cell::sync::Lazy;
use pageserver_api::{
    key::AUX_FILES_KEY,
    keyspace::KeySpaceAccum,
    models::{
        CompactionAlgorithm, DownloadRemoteLayersTaskInfo, DownloadRemoteLayersTaskSpawnRequest,
        EvictionPolicy, InMemoryLayerInfo, LayerMapInfo, TimelineState,
    },
    reltag::BlockNumber,
    shard::{ShardIdentity, TenantShardId},
};
use rand::Rng;
use serde_with::serde_as;
use storage_broker::BrokerClientChannel;
use tokio::{
    runtime::Handle,
    sync::{oneshot, watch},
};
use tokio_util::sync::CancellationToken;
use tracing::*;
use utils::{
    bin_ser::BeSer,
    sync::gate::{Gate, GateGuard},
    vec_map::VecMap,
};

use std::ops::{Deref, Range};
use std::pin::pin;
use std::sync::atomic::Ordering as AtomicOrdering;
use std::sync::{Arc, Mutex, RwLock, Weak};
use std::time::{Duration, Instant, SystemTime};
use std::{
    array,
    collections::{BTreeMap, HashMap, HashSet},
    sync::atomic::AtomicU64,
};
use std::{
    cmp::{max, min, Ordering},
    ops::ControlFlow,
};

use crate::deletion_queue::DeletionQueueClient;
use crate::tenant::timeline::logical_size::CurrentLogicalSize;
use crate::tenant::{
    layer_map::{LayerMap, SearchResult},
    metadata::TimelineMetadata,
};
use crate::{
    context::{DownloadBehavior, RequestContext},
    disk_usage_eviction_task::DiskUsageEvictionInfo,
    pgdatadir_mapping::CollectKeySpaceError,
};
use crate::{
    disk_usage_eviction_task::finite_f32,
    tenant::storage_layer::{
        AsLayerDesc, DeltaLayerWriter, EvictionError, ImageLayerWriter, InMemoryLayer, Layer,
        LayerAccessStatsReset, LayerFileName, ResidentLayer, ValueReconstructResult,
        ValueReconstructState, ValuesReconstructState,
    },
};
use crate::{
    disk_usage_eviction_task::EvictionCandidate, tenant::storage_layer::delta_layer::DeltaEntry,
};
use crate::{pgdatadir_mapping::LsnForTimestamp, tenant::tasks::BackgroundLoopKind};
use crate::{
    pgdatadir_mapping::{AuxFilesDirectory, DirectoryKind},
    virtual_file::{MaybeFatalIo, VirtualFile},
};

use crate::config::PageServerConf;
use crate::keyspace::{KeyPartitioning, KeySpace};
use crate::metrics::{
    TimelineMetrics, MATERIALIZED_PAGE_CACHE_HIT, MATERIALIZED_PAGE_CACHE_HIT_DIRECT,
};
use crate::pgdatadir_mapping::CalculateLogicalSizeError;
use crate::tenant::config::TenantConfOpt;
use pageserver_api::key::{is_inherited_key, is_rel_fsm_block_key, is_rel_vm_block_key};
use pageserver_api::reltag::RelTag;
use pageserver_api::shard::ShardIndex;

use postgres_connection::PgConnectionConfig;
use postgres_ffi::to_pg_timestamp;
use utils::{
    completion,
    generation::Generation,
    id::TimelineId,
    lsn::{AtomicLsn, Lsn, RecordLsn},
    seqwait::SeqWait,
    simple_rcu::{Rcu, RcuReadGuard},
};

use crate::page_cache;
use crate::repository::GcResult;
use crate::repository::{Key, Value};
use crate::task_mgr;
use crate::task_mgr::TaskKind;
use crate::ZERO_PAGE;

use self::delete::DeleteTimelineFlow;
pub(super) use self::eviction_task::EvictionTaskTenantState;
use self::eviction_task::EvictionTaskTimelineState;
use self::layer_manager::LayerManager;
use self::logical_size::LogicalSize;
use self::walreceiver::{WalReceiver, WalReceiverConf};

use super::config::TenantConf;
use super::secondary::heatmap::{HeatMapLayer, HeatMapTimeline};
use super::{debug_assert_current_span_has_tenant_and_timeline_id, AttachedTenantConf};
use super::{remote_timeline_client::index::IndexPart, storage_layer::LayerFringe};
use super::{remote_timeline_client::RemoteTimelineClient, storage_layer::ReadableLayer};

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub(super) enum FlushLoopState {
    NotStarted,
    Running {
        #[cfg(test)]
        expect_initdb_optimization: bool,
        #[cfg(test)]
        initdb_optimization_count: usize,
    },
    Exited,
}

/// Wrapper for key range to provide reverse ordering by range length for BinaryHeap
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Hole {
    key_range: Range<Key>,
    coverage_size: usize,
}

impl Ord for Hole {
    fn cmp(&self, other: &Self) -> Ordering {
        other.coverage_size.cmp(&self.coverage_size) // inverse order
    }
}

impl PartialOrd for Hole {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// Temporary function for immutable storage state refactor, ensures we are dropping mutex guard instead of other things.
/// Can be removed after all refactors are done.
fn drop_rlock<T>(rlock: tokio::sync::OwnedRwLockReadGuard<T>) {
    drop(rlock)
}

/// Temporary function for immutable storage state refactor, ensures we are dropping mutex guard instead of other things.
/// Can be removed after all refactors are done.
fn drop_wlock<T>(rlock: tokio::sync::RwLockWriteGuard<'_, T>) {
    drop(rlock)
}

/// The outward-facing resources required to build a Timeline
pub struct TimelineResources {
    pub remote_client: Option<RemoteTimelineClient>,
    pub deletion_queue_client: DeletionQueueClient,
    pub timeline_get_throttle: Arc<
        crate::tenant::throttle::Throttle<&'static crate::metrics::tenant_throttling::TimelineGet>,
    >,
}

pub(crate) struct AuxFilesState {
    pub(crate) dir: Option<AuxFilesDirectory>,
    pub(crate) n_deltas: usize,
}

/// The relation size cache caches relation sizes at the end of the timeline. It speeds up WAL
/// ingestion considerably, because WAL ingestion needs to check on most records if the record
/// implicitly extends the relation.  At startup, `complete_as_of` is initialized to the current end
/// of the timeline (disk_consistent_lsn).  It's used on reads of relation sizes to check if the
/// value can be used to also update the cache, see [`Timeline::update_cached_rel_size`].
pub(crate) struct RelSizeCache {
    pub(crate) complete_as_of: Lsn,
    pub(crate) map: HashMap<RelTag, (Lsn, BlockNumber)>,
}

pub struct Timeline {
    conf: &'static PageServerConf,
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

    pub pg_version: u32,

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
    pub(crate) layers: Arc<tokio::sync::RwLock<LayerManager>>,

    last_freeze_at: AtomicLsn,
    // Atomic would be more appropriate here.
    last_freeze_ts: RwLock<Instant>,

    // WAL redo manager. `None` only for broken tenants.
    walredo_mgr: Option<Arc<super::WalRedoManager>>,

    /// Remote storage client.
    /// See [`remote_timeline_client`](super::remote_timeline_client) module comment for details.
    pub remote_client: Option<Arc<RemoteTimelineClient>>,

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

    pub(super) metrics: TimelineMetrics,

    // `Timeline` doesn't write these metrics itself, but it manages the lifetime.  Code
    // in `crate::page_service` writes these metrics.
    pub(crate) query_metrics: crate::metrics::SmgrQueryTimePerTimeline,

    directory_metrics: [AtomicU64; DirectoryKind::KINDS_NUM],

    /// Ensures layers aren't frozen by checkpointer between
    /// [`Timeline::get_layer_for_write`] and layer reads.
    /// Locked automatically by [`TimelineWriter`] and checkpointer.
    /// Must always be acquired before the layer map/individual layer lock
    /// to avoid deadlock.
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

    // Needed to ensure that we can't create a branch at a point that was already garbage collected
    pub latest_gc_cutoff_lsn: Rcu<Lsn>,

    // List of child timelines and their branch points. This is needed to avoid
    // garbage collecting data that is still needed by the child timelines.
    pub gc_info: std::sync::RwLock<GcInfo>,

    // It may change across major versions so for simplicity
    // keep it after running initdb for a timeline.
    // It is needed in checks when we want to error on some operations
    // when they are requested for pre-initdb lsn.
    // It can be unified with latest_gc_cutoff_lsn under some "first_valid_lsn",
    // though let's keep them both for better error visibility.
    pub initdb_lsn: Lsn,

    /// When did we last calculate the partitioning?
    partitioning: tokio::sync::Mutex<(KeyPartitioning, Lsn)>,

    /// Configuration: how often should the partitioning be recalculated.
    repartition_threshold: u64,

    last_image_layer_creation_check_at: AtomicLsn,

    /// Current logical size of the "datadir", at the last LSN.
    current_logical_size: LogicalSize,

    /// Information about the last processed message by the WAL receiver,
    /// or None if WAL receiver has not received anything for this timeline
    /// yet.
    pub last_received_wal: Mutex<Option<WalReceiverInfo>>,
    pub walreceiver: Mutex<Option<WalReceiver>>,

    /// Relation size cache
    pub(crate) rel_size_cache: RwLock<RelSizeCache>,

    download_all_remote_layers_task_info: RwLock<Option<DownloadRemoteLayersTaskInfo>>,

    state: watch::Sender<TimelineState>,

    /// Prevent two tasks from deleting the timeline at the same time. If held, the
    /// timeline is being deleted. If 'true', the timeline has already been deleted.
    pub delete_progress: Arc<tokio::sync::Mutex<DeleteTimelineFlow>>,

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

    /// Make sure we only have one running gc at a time.
    ///
    /// Must only be taken in two places:
    /// - [`Timeline::gc`] (this file)
    /// - [`delete::delete_local_timeline_directory`]
    ///
    /// Timeline deletion will acquire both compaction and gc locks in whatever order.
    gc_lock: tokio::sync::Mutex<()>,

    /// Cloned from [`super::Tenant::timeline_get_throttle`] on construction.
    timeline_get_throttle: Arc<
        crate::tenant::throttle::Throttle<&'static crate::metrics::tenant_throttling::TimelineGet>,
    >,

    /// Keep aux directory cache to avoid it's reconstruction on each update
    pub(crate) aux_files: tokio::sync::Mutex<AuxFilesState>,
}

pub struct WalReceiverInfo {
    pub wal_source_connconf: PgConnectionConfig,
    pub last_received_msg_lsn: Lsn,
    pub last_received_msg_ts: u128,
}

///
/// Information about how much history needs to be retained, needed by
/// Garbage Collection.
///
pub struct GcInfo {
    /// Specific LSNs that are needed.
    ///
    /// Currently, this includes all points where child branches have
    /// been forked off from. In the future, could also include
    /// explicit user-defined snapshot points.
    pub retain_lsns: Vec<Lsn>,

    /// In addition to 'retain_lsns', keep everything newer than this
    /// point.
    ///
    /// This is calculated by subtracting 'gc_horizon' setting from
    /// last-record LSN
    ///
    /// FIXME: is this inclusive or exclusive?
    pub horizon_cutoff: Lsn,

    /// In addition to 'retain_lsns' and 'horizon_cutoff', keep everything newer than this
    /// point.
    ///
    /// This is calculated by finding a number such that a record is needed for PITR
    /// if only if its LSN is larger than 'pitr_cutoff'.
    pub pitr_cutoff: Lsn,
}

/// An error happened in a get() operation.
#[derive(thiserror::Error, Debug)]
pub(crate) enum PageReconstructError {
    #[error(transparent)]
    Other(#[from] anyhow::Error),

    #[error("Ancestor LSN wait error: {0}")]
    AncestorLsnTimeout(#[from] WaitLsnError),

    #[error("timeline shutting down")]
    Cancelled,

    /// The ancestor of this is being stopped
    #[error("ancestor timeline {0} is being stopped")]
    AncestorStopping(TimelineId),

    /// An error happened replaying WAL records
    #[error(transparent)]
    WalRedo(anyhow::Error),
}

impl PageReconstructError {
    /// Returns true if this error indicates a tenant/timeline shutdown alike situation
    pub(crate) fn is_stopping(&self) -> bool {
        use PageReconstructError::*;
        match self {
            Other(_) => false,
            AncestorLsnTimeout(_) => false,
            Cancelled | AncestorStopping(_) => true,
            WalRedo(_) => false,
        }
    }
}

#[derive(thiserror::Error, Debug)]
enum CreateImageLayersError {
    #[error("timeline shutting down")]
    Cancelled,

    #[error(transparent)]
    GetVectoredError(GetVectoredError),

    #[error(transparent)]
    PageReconstructError(PageReconstructError),

    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

#[derive(thiserror::Error, Debug)]
enum FlushLayerError {
    /// Timeline cancellation token was cancelled
    #[error("timeline shutting down")]
    Cancelled,

    #[error(transparent)]
    CreateImageLayersError(CreateImageLayersError),

    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

#[derive(thiserror::Error, Debug)]
pub(crate) enum GetVectoredError {
    #[error("timeline shutting down")]
    Cancelled,

    #[error("Requested too many keys: {0} > {}", Timeline::MAX_GET_VECTORED_KEYS)]
    Oversized(u64),

    #[error("Requested at invalid LSN: {0}")]
    InvalidLsn(Lsn),

    #[error("Requested key {0} not found")]
    MissingKey(Key),

    #[error(transparent)]
    GetReadyAncestorError(GetReadyAncestorError),

    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

#[derive(thiserror::Error, Debug)]
pub(crate) enum GetReadyAncestorError {
    #[error("ancestor timeline {0} is being stopped")]
    AncestorStopping(TimelineId),

    #[error("Ancestor LSN wait error: {0}")]
    AncestorLsnTimeout(#[from] WaitLsnError),

    #[error("Cancelled")]
    Cancelled,

    #[error(transparent)]
    Other(#[from] anyhow::Error),
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

#[derive(enumset::EnumSetType)]
pub(crate) enum CompactFlags {
    ForceRepartition,
    ForceImageLayerCreation,
}

impl std::fmt::Debug for Timeline {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "Timeline<{}>", self.timeline_id)
    }
}

#[derive(thiserror::Error, Debug)]
pub(crate) enum WaitLsnError {
    // Called on a timeline which is shutting down
    #[error("Shutdown")]
    Shutdown,

    // Called on an timeline not in active state or shutting down
    #[error("Bad state (not active)")]
    BadState,

    // Timeout expired while waiting for LSN to catch up with goal.
    #[error("{0}")]
    Timeout(String),
}

// The impls below achieve cancellation mapping for errors.
// Perhaps there's a way of achieving this with less cruft.

impl From<CreateImageLayersError> for CompactionError {
    fn from(e: CreateImageLayersError) -> Self {
        match e {
            CreateImageLayersError::Cancelled => CompactionError::ShuttingDown,
            _ => CompactionError::Other(e.into()),
        }
    }
}

impl From<CreateImageLayersError> for FlushLayerError {
    fn from(e: CreateImageLayersError) -> Self {
        match e {
            CreateImageLayersError::Cancelled => FlushLayerError::Cancelled,
            any => FlushLayerError::CreateImageLayersError(any),
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

impl From<GetVectoredError> for CreateImageLayersError {
    fn from(e: GetVectoredError) -> Self {
        match e {
            GetVectoredError::Cancelled => CreateImageLayersError::Cancelled,
            _ => CreateImageLayersError::GetVectoredError(e),
        }
    }
}

impl From<GetReadyAncestorError> for PageReconstructError {
    fn from(e: GetReadyAncestorError) -> Self {
        use GetReadyAncestorError::*;
        match e {
            AncestorStopping(tid) => PageReconstructError::AncestorStopping(tid),
            AncestorLsnTimeout(wait_err) => PageReconstructError::AncestorLsnTimeout(wait_err),
            Cancelled => PageReconstructError::Cancelled,
            Other(other) => PageReconstructError::Other(other),
        }
    }
}

#[derive(
    Eq,
    PartialEq,
    Debug,
    Copy,
    Clone,
    strum_macros::EnumString,
    strum_macros::Display,
    serde_with::DeserializeFromStr,
    serde_with::SerializeDisplay,
)]
#[strum(serialize_all = "kebab-case")]
pub enum GetVectoredImpl {
    Sequential,
    Vectored,
}

pub(crate) enum WaitLsnWaiter<'a> {
    Timeline(&'a Timeline),
    Tenant,
    PageService,
}

/// Argument to [`Timeline::shutdown`].
#[derive(Debug, Clone, Copy)]
pub(crate) enum ShutdownMode {
    /// Graceful shutdown, may do a lot of I/O as we flush any open layers to disk and then
    /// also to remote storage.  This method can easily take multiple seconds for a busy timeline.
    ///
    /// While we are flushing, we continue to accept read I/O for LSNs ingested before
    /// the call to [`Timeline::shutdown`].
    FreezeAndFlush,
    /// Shut down immediately, without waiting for any open layers to flush.
    Hard,
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

    /// Lock and get timeline's GC cutoff
    pub(crate) fn get_latest_gc_cutoff_lsn(&self) -> RcuReadGuard<Lsn> {
        self.latest_gc_cutoff_lsn.read()
    }

    /// Look up given page version.
    ///
    /// If a remote layer file is needed, it is downloaded as part of this
    /// call.
    ///
    /// This method enforces [`Self::timeline_get_throttle`] internally.
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
        self.timeline_get_throttle.throttle(ctx, 1).await;
        self.get_impl(key, lsn, ctx).await
    }
    /// Not subject to [`Self::timeline_get_throttle`].
    async fn get_impl(
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

        // XXX: structured stats collection for layer eviction here.
        trace!(
            "get page request for {}@{} from task kind {:?}",
            key,
            lsn,
            ctx.task_kind()
        );

        // Check the page cache. We will get back the most recent page with lsn <= `lsn`.
        // The cached image can be returned directly if there is no WAL between the cached image
        // and requested LSN. The cached image can also be used to reduce the amount of WAL needed
        // for redo.
        let cached_page_img = match self.lookup_cached_page(&key, lsn, ctx).await {
            Some((cached_lsn, cached_img)) => {
                match cached_lsn.cmp(&lsn) {
                    Ordering::Less => {} // there might be WAL between cached_lsn and lsn, we need to check
                    Ordering::Equal => {
                        MATERIALIZED_PAGE_CACHE_HIT_DIRECT.inc();
                        return Ok(cached_img); // exact LSN match, return the image
                    }
                    Ordering::Greater => {
                        unreachable!("the returned lsn should never be after the requested lsn")
                    }
                }
                Some((cached_lsn, cached_img))
            }
            None => None,
        };

        let mut reconstruct_state = ValueReconstructState {
            records: Vec::new(),
            img: cached_page_img,
        };

        let timer = crate::metrics::GET_RECONSTRUCT_DATA_TIME.start_timer();
        let path = self
            .get_reconstruct_data(key, lsn, &mut reconstruct_state, ctx)
            .await?;
        timer.stop_and_record();

        let start = Instant::now();
        let res = self.reconstruct_value(key, lsn, reconstruct_state).await;
        let elapsed = start.elapsed();
        crate::metrics::RECONSTRUCT_TIME
            .for_result(&res)
            .observe(elapsed.as_secs_f64());

        if cfg!(feature = "testing") && res.is_err() {
            // it can only be walredo issue
            use std::fmt::Write;

            let mut msg = String::new();

            path.into_iter().for_each(|(res, cont_lsn, layer)| {
                writeln!(
                    msg,
                    "- layer traversal: result {res:?}, cont_lsn {cont_lsn}, layer: {}",
                    layer(),
                )
                .expect("string grows")
            });

            // this is to rule out or provide evidence that we could in some cases read a duplicate
            // walrecord
            tracing::info!("walredo failed, path:\n{msg}");
        }

        res
    }

    pub(crate) const MAX_GET_VECTORED_KEYS: u64 = 32;

    /// Look up multiple page versions at a given LSN
    ///
    /// This naive implementation will be replaced with a more efficient one
    /// which actually vectorizes the read path.
    pub(crate) async fn get_vectored(
        &self,
        keyspace: KeySpace,
        lsn: Lsn,
        ctx: &RequestContext,
    ) -> Result<BTreeMap<Key, Result<Bytes, PageReconstructError>>, GetVectoredError> {
        if !lsn.is_valid() {
            return Err(GetVectoredError::InvalidLsn(lsn));
        }

        let key_count = keyspace.total_size().try_into().unwrap();
        if key_count > Timeline::MAX_GET_VECTORED_KEYS {
            return Err(GetVectoredError::Oversized(key_count));
        }

        for range in &keyspace.ranges {
            let mut key = range.start;
            while key != range.end {
                assert!(!self.shard_identity.is_key_disposable(&key));
                key = key.next();
            }
        }

        trace!(
            "get vectored request for {:?}@{} from task kind {:?} will use {} implementation",
            keyspace,
            lsn,
            ctx.task_kind(),
            self.conf.get_vectored_impl
        );

        let start = crate::metrics::GET_VECTORED_LATENCY
            .for_task_kind(ctx.task_kind())
            .map(|metric| (metric, Instant::now()));

        // start counting after throttle so that throttle time
        // is always less than observation time
        let throttled = self
            .timeline_get_throttle
            .throttle(ctx, key_count as usize)
            .await;

        let res = match self.conf.get_vectored_impl {
            GetVectoredImpl::Sequential => {
                self.get_vectored_sequential_impl(keyspace, lsn, ctx).await
            }
            GetVectoredImpl::Vectored => {
                let vectored_res = self.get_vectored_impl(keyspace.clone(), lsn, ctx).await;

                if self.conf.validate_vectored_get {
                    self.validate_get_vectored_impl(&vectored_res, keyspace, lsn, ctx)
                        .await;
                }

                vectored_res
            }
        };

        if let Some((metric, start)) = start {
            let elapsed = start.elapsed();
            let ex_throttled = if let Some(throttled) = throttled {
                elapsed.checked_sub(throttled)
            } else {
                Some(elapsed)
            };

            if let Some(ex_throttled) = ex_throttled {
                metric.observe(ex_throttled.as_secs_f64());
            } else {
                use utils::rate_limit::RateLimit;
                static LOGGED: Lazy<Mutex<RateLimit>> =
                    Lazy::new(|| Mutex::new(RateLimit::new(Duration::from_secs(10))));
                let mut rate_limit = LOGGED.lock().unwrap();
                rate_limit.call(|| {
                    warn!("error deducting time spent throttled; this message is logged at a global rate limit");
                });
            }
        }

        res
    }

    /// Not subject to [`Self::timeline_get_throttle`].
    pub(super) async fn get_vectored_sequential_impl(
        &self,
        keyspace: KeySpace,
        lsn: Lsn,
        ctx: &RequestContext,
    ) -> Result<BTreeMap<Key, Result<Bytes, PageReconstructError>>, GetVectoredError> {
        let mut values = BTreeMap::new();
        for range in keyspace.ranges {
            let mut key = range.start;
            while key != range.end {
                let block = self.get_impl(key, lsn, ctx).await;

                use PageReconstructError::*;
                match block {
                    Err(Cancelled | AncestorStopping(_)) => {
                        return Err(GetVectoredError::Cancelled)
                    }
                    Err(Other(err)) if err.to_string().contains("could not find data for key") => {
                        return Err(GetVectoredError::MissingKey(key))
                    }
                    _ => {
                        values.insert(key, block);
                        key = key.next();
                    }
                }
            }
        }

        Ok(values)
    }

    pub(super) async fn get_vectored_impl(
        &self,
        keyspace: KeySpace,
        lsn: Lsn,
        ctx: &RequestContext,
    ) -> Result<BTreeMap<Key, Result<Bytes, PageReconstructError>>, GetVectoredError> {
        let mut reconstruct_state = ValuesReconstructState::new();

        self.get_vectored_reconstruct_data(keyspace, lsn, &mut reconstruct_state, ctx)
            .await?;

        let mut results: BTreeMap<Key, Result<Bytes, PageReconstructError>> = BTreeMap::new();
        for (key, res) in reconstruct_state.keys {
            match res {
                Err(err) => {
                    results.insert(key, Err(err));
                }
                Ok(state) => {
                    let state = ValueReconstructState::from(state);

                    let reconstruct_res = self.reconstruct_value(key, lsn, state).await;
                    results.insert(key, reconstruct_res);
                }
            }
        }

        Ok(results)
    }

    /// Not subject to [`Self::timeline_get_throttle`].
    pub(super) async fn validate_get_vectored_impl(
        &self,
        vectored_res: &Result<BTreeMap<Key, Result<Bytes, PageReconstructError>>, GetVectoredError>,
        keyspace: KeySpace,
        lsn: Lsn,
        ctx: &RequestContext,
    ) {
        let sequential_res = self
            .get_vectored_sequential_impl(keyspace.clone(), lsn, ctx)
            .await;

        fn errors_match(lhs: &GetVectoredError, rhs: &GetVectoredError) -> bool {
            use GetVectoredError::*;
            match (lhs, rhs) {
                (Oversized(l), Oversized(r)) => l == r,
                (InvalidLsn(l), InvalidLsn(r)) => l == r,
                (MissingKey(l), MissingKey(r)) => l == r,
                (GetReadyAncestorError(_), GetReadyAncestorError(_)) => true,
                (Other(_), Other(_)) => true,
                _ => false,
            }
        }

        match (&sequential_res, vectored_res) {
            (Err(GetVectoredError::Cancelled), _) => {},
            (_, Err(GetVectoredError::Cancelled)) => {},
            (Err(seq_err), Ok(_)) => {
                panic!(concat!("Sequential get failed with {}, but vectored get did not",
                               " - keyspace={:?} lsn={}"),
                       seq_err, keyspace, lsn) },
            (Ok(_), Err(vec_err)) => {
                panic!(concat!("Vectored get failed with {}, but sequential get did not",
                               " - keyspace={:?} lsn={}"),
                       vec_err, keyspace, lsn) },
            (Err(seq_err), Err(vec_err)) => {
                assert!(errors_match(seq_err, vec_err),
                        "Mismatched errors: {seq_err} != {vec_err} - keyspace={keyspace:?} lsn={lsn}")},
            (Ok(seq_values), Ok(vec_values)) => {
                seq_values.iter().zip(vec_values.iter()).for_each(|((seq_key, seq_res), (vec_key, vec_res))| {
                    assert_eq!(seq_key, vec_key);
                    match (seq_res, vec_res) {
                        (Ok(seq_blob), Ok(vec_blob)) => {
                            Self::validate_key_equivalence(seq_key, &keyspace, lsn, seq_blob, vec_blob);
                        },
                        (Err(err), Ok(_)) => {
                            panic!(
                                concat!("Sequential get failed with {} for key {}, but vectored get did not",
                                        " - keyspace={:?} lsn={}"),
                                err, seq_key, keyspace, lsn) },
                        (Ok(_), Err(err)) => {
                            panic!(
                                concat!("Vectored get failed with {} for key {}, but sequential get did not",
                                        " - keyspace={:?} lsn={}"),
                                err, seq_key, keyspace, lsn) },
                        (Err(_), Err(_)) => {}
                    }
                })
            }
        }
    }

    fn validate_key_equivalence(
        key: &Key,
        keyspace: &KeySpace,
        lsn: Lsn,
        seq: &Bytes,
        vec: &Bytes,
    ) {
        if *key == AUX_FILES_KEY {
            // The value reconstruct of AUX_FILES_KEY from records is not deterministic
            // since it uses a hash map under the hood. Hence, deserialise both results
            // before comparing.
            let seq_aux_dir_res = AuxFilesDirectory::des(seq);
            let vec_aux_dir_res = AuxFilesDirectory::des(vec);
            match (&seq_aux_dir_res, &vec_aux_dir_res) {
                (Ok(seq_aux_dir), Ok(vec_aux_dir)) => {
                    assert_eq!(
                        seq_aux_dir, vec_aux_dir,
                        "Mismatch for key {} - keyspace={:?} lsn={}",
                        key, keyspace, lsn
                    );
                }
                (Err(_), Err(_)) => {}
                _ => {
                    panic!("Mismatch for {key}: {seq_aux_dir_res:?} != {vec_aux_dir_res:?}");
                }
            }
        } else {
            // All other keys should reconstruct deterministically, so we simply compare the blobs.
            assert_eq!(
                seq, vec,
                "Image mismatch for key {key} - keyspace={keyspace:?} lsn={lsn}"
            );
        }
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

    pub(crate) fn get_disk_consistent_lsn(&self) -> Lsn {
        self.disk_consistent_lsn.load()
    }

    /// remote_consistent_lsn from the perspective of the tenant's current generation,
    /// not validated with control plane yet.
    /// See [`Self::get_remote_consistent_lsn_visible`].
    pub(crate) fn get_remote_consistent_lsn_projected(&self) -> Option<Lsn> {
        if let Some(remote_client) = &self.remote_client {
            remote_client.remote_consistent_lsn_projected()
        } else {
            None
        }
    }

    /// remote_consistent_lsn which the tenant is guaranteed not to go backward from,
    /// i.e. a value of remote_consistent_lsn_projected which has undergone
    /// generation validation in the deletion queue.
    pub(crate) fn get_remote_consistent_lsn_visible(&self) -> Option<Lsn> {
        if let Some(remote_client) = &self.remote_client {
            remote_client.remote_consistent_lsn_visible()
        } else {
            None
        }
    }

    /// The sum of the file size of all historic layers in the layer map.
    /// This method makes no distinction between local and remote layers.
    /// Hence, the result **does not represent local filesystem usage**.
    pub(crate) async fn layer_size_sum(&self) -> u64 {
        let guard = self.layers.read().await;
        let layer_map = guard.layer_map();
        let mut size = 0;
        for l in layer_map.iter_historic_layers() {
            size += l.file_size();
        }
        size
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
        ctx: &RequestContext, /* Prepare for use by cancellation */
    ) -> Result<(), WaitLsnError> {
        if self.cancel.is_cancelled() {
            return Err(WaitLsnError::Shutdown);
        } else if !self.is_active() {
            return Err(WaitLsnError::BadState);
        }

        if cfg!(debug_assertions) {
            match ctx.task_kind() {
                TaskKind::WalReceiverManager
                | TaskKind::WalReceiverConnectionHandler
                | TaskKind::WalReceiverConnectionPoller => {
                    let is_myself = match who_is_waiting {
                        WaitLsnWaiter::Timeline(waiter) => Weak::ptr_eq(&waiter.myself, &self.myself),
                        WaitLsnWaiter::Tenant | WaitLsnWaiter::PageService => unreachable!("tenant or page_service context are not expected to have task kind {:?}", ctx.task_kind()),
                    };
                    if is_myself {
                        if let Err(current) = self.last_record_lsn.would_wait_for(lsn) {
                            // walingest is the only one that can advance last_record_lsn; it should make sure to never reach here
                            panic!("this timeline's walingest task is calling wait_lsn({lsn}) but we only have last_record_lsn={current}; would deadlock");
                        }
                    } else {
                        // if another  timeline's  is waiting for us, there's no deadlock risk because
                        // our walreceiver task can make progress independent of theirs
                    }
                }
                _ => {}
            }
        }

        let _timer = crate::metrics::WAIT_LSN_TIME.start_timer();

        match self
            .last_record_lsn
            .wait_for_timeout(lsn, self.conf.wait_lsn_timeout)
            .await
        {
            Ok(()) => Ok(()),
            Err(e) => {
                use utils::seqwait::SeqWaitError::*;
                match e {
                    Shutdown => Err(WaitLsnError::Shutdown),
                    Timeout => {
                        // don't count the time spent waiting for lock below, and also in walreceiver.status(), towards the wait_lsn_time_histo
                        drop(_timer);
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
            "LSN {} is earlier than latest GC horizon {} (we might've already garbage collected needed data)",
            lsn,
            **latest_gc_cutoff_lsn,
        );
        Ok(())
    }

    /// Flush to disk all data that was written with the put_* functions
    #[instrument(skip(self), fields(tenant_id=%self.tenant_shard_id.tenant_id, shard_id=%self.tenant_shard_id.shard_slug(), timeline_id=%self.timeline_id))]
    pub(crate) async fn freeze_and_flush(&self) -> anyhow::Result<()> {
        let to_lsn = self.freeze_inmem_layer(false).await;
        self.flush_frozen_layers_and_wait(to_lsn).await
    }

    /// If there is no writer, and conditions for rolling the latest layer are met, then freeze it.
    ///
    /// This is for use in background housekeeping, to provide guarantees of layers closing eventually
    /// even if there are no ongoing writes to drive that.
    async fn maybe_freeze_ephemeral_layer(&self) {
        let Ok(_write_guard) = self.write_lock.try_lock() else {
            // If the write lock is held, there is an active wal receiver: rolling open layers
            // is their responsibility while they hold this lock.
            return;
        };

        let Ok(layers_guard) = self.layers.try_read() else {
            // Don't block if the layer lock is busy
            return;
        };

        let Some(open_layer) = &layers_guard.layer_map().open_layer else {
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
                    // This should be somewhat rare, so we log it at INFO level.
                    //
                    // We checked for checkpoint timeout so that a shard without any
                    // data ingested (yet) doesn't write a remote index as soon as it
                    // sees its LSN advance: we only do this if we've been layer-less
                    // for some time.
                    tracing::info!(
                        "Advancing disk_consistent_lsn past WAL ingest gap {} -> {}",
                        disk_consistent_lsn,
                        last_record_lsn
                    );

                    // The flush loop will update remote consistent LSN as well as disk consistent LSN.
                    self.flush_frozen_layers_and_wait(last_record_lsn)
                        .await
                        .ok();
                }
            }

            return;
        };

        let Some(current_size) = open_layer.try_len() else {
            // Unexpected: since we hold the write guard, nobody else should be writing to this layer, so
            // read lock to get size should always succeed.
            tracing::warn!("Lock conflict while reading size of open layer");
            return;
        };

        let current_lsn = self.get_last_record_lsn();

        let checkpoint_distance_override = open_layer.tick().await;

        if let Some(size_override) = checkpoint_distance_override {
            if current_size > size_override {
                // This is not harmful, but it only happens in relatively rare cases where
                // time-based checkpoints are not happening fast enough to keep the amount of
                // ephemeral data within configured limits.  It's a sign of stress on the system.
                tracing::info!("Early-rolling open layer at size {current_size} (limit {size_override}) due to dirty data pressure");
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
                    let mut layers_guard = self.layers.write().await;
                    layers_guard
                        .try_freeze_in_memory_layer(current_lsn, &self.last_freeze_at)
                        .await;
                }
            }
            self.flush_frozen_layers();
        }
    }

    /// Outermost timeline compaction operation; downloads needed layers.
    pub(crate) async fn compact(
        self: &Arc<Self>,
        cancel: &CancellationToken,
        flags: EnumSet<CompactFlags>,
        ctx: &RequestContext,
    ) -> Result<(), CompactionError> {
        // most likely the cancellation token is from background task, but in tests it could be the
        // request task as well.

        let prepare = async move {
            let guard = self.compaction_lock.lock().await;

            let permit = super::tasks::concurrent_background_tasks_rate_limit_permit(
                BackgroundLoopKind::Compaction,
                ctx,
            )
            .await;

            (guard, permit)
        };

        // Prior to compaction, check if an open ephemeral layer should be closed: this provides
        // background enforcement of checkpoint interval if there is no active WAL receiver, to avoid keeping
        // an ephemeral layer open forever when idle.
        self.maybe_freeze_ephemeral_layer().await;

        // this wait probably never needs any "long time spent" logging, because we already nag if
        // compaction task goes over it's period (20s) which is quite often in production.
        let (_guard, _permit) = tokio::select! {
            tuple = prepare => { tuple },
            _ = self.cancel.cancelled() => return Ok(()),
            _ = cancel.cancelled() => return Ok(()),
        };

        let last_record_lsn = self.get_last_record_lsn();

        // Last record Lsn could be zero in case the timeline was just created
        if !last_record_lsn.is_valid() {
            warn!("Skipping compaction for potentially just initialized timeline, it has invalid last record lsn: {last_record_lsn}");
            return Ok(());
        }

        match self.get_compaction_algorithm() {
            CompactionAlgorithm::Tiered => self.compact_tiered(cancel, ctx).await,
            CompactionAlgorithm::Legacy => self.compact_legacy(cancel, flags, ctx).await,
        }
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
        parent: Arc<crate::tenant::Tenant>,
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

        let try_freeze_and_flush = match mode {
            ShutdownMode::FreezeAndFlush => true,
            ShutdownMode::Hard => false,
        };

        // Regardless of whether we're going to try_freeze_and_flush
        // or not, stop ingesting any more data. Walreceiver only provides
        // cancellation but no "wait until gone", because it uses the Timeline::gate.
        // So, only after the self.gate.close() below will we know for sure that
        // no walreceiver tasks are left.
        // For `try_freeze_and_flush=true`, this means that we might still be ingesting
        // data during the call to `self.freeze_and_flush()` below.
        // That's not ideal, but, we don't have the concept of a ChildGuard,
        // which is what we'd need to properly model early shutdown of the walreceiver
        // task sub-tree before the other Timeline task sub-trees.
        let walreceiver = self.walreceiver.lock().unwrap().take();
        tracing::debug!(
            is_some = walreceiver.is_some(),
            "Waiting for WalReceiverManager..."
        );
        if let Some(walreceiver) = walreceiver {
            walreceiver.cancel();
        }
        // ... and inform any waiters for newer LSNs that there won't be any.
        self.last_record_lsn.shutdown();

        if try_freeze_and_flush {
            // we shut down walreceiver above, so, we won't add anything more
            // to the InMemoryLayer; freeze it and wait for all frozen layers
            // to reach the disk & upload queue, then shut the upload queue and
            // wait for it to drain.
            match self.freeze_and_flush().await {
                Ok(_) => {
                    // drain the upload queue
                    if let Some(client) = self.remote_client.as_ref() {
                        // if we did not wait for completion here, it might be our shutdown process
                        // didn't wait for remote uploads to complete at all, as new tasks can forever
                        // be spawned.
                        //
                        // what is problematic is the shutting down of RemoteTimelineClient, because
                        // obviously it does not make sense to stop while we wait for it, but what
                        // about corner cases like s3 suddenly hanging up?
                        client.shutdown().await;
                    }
                }
                Err(e) => {
                    // Non-fatal.  Shutdown is infallible.  Failures to flush just mean that
                    // we have some extra WAL replay to do next time the timeline starts.
                    warn!("failed to freeze and flush: {e:#}");
                }
            }
        }

        // Signal any subscribers to our cancellation token to drop out
        tracing::debug!("Cancelling CancellationToken");
        self.cancel.cancel();

        // Transition the remote_client into a state where it's only useful for timeline deletion.
        // (The deletion use case is why we can't just hook up remote_client to Self::cancel).)
        if let Some(remote_client) = self.remote_client.as_ref() {
            remote_client.stop();
            // As documented in remote_client.stop()'s doc comment, it's our responsibility
            // to shut down the upload queue tasks.
            // TODO: fix that, task management should be encapsulated inside remote_client.
            task_mgr::shutdown_tasks(
                Some(TaskKind::RemoteUploadTask),
                Some(self.tenant_shard_id),
                Some(self.timeline_id),
            )
            .await;
        }

        // TODO: work toward making this a no-op. See this funciton's doc comment for more context.
        tracing::debug!("Waiting for tasks...");
        task_mgr::shutdown_tasks(None, Some(self.tenant_shard_id), Some(self.timeline_id)).await;

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
                TimelineState::Active { .. } => {
                    return Ok(());
                }
                TimelineState::Broken { .. } | TimelineState::Stopping => {
                    // There's no chance the timeline can transition back into ::Active
                    return Err(current_state);
                }
            }
        }
    }

    pub(crate) async fn layer_map_info(&self, reset: LayerAccessStatsReset) -> LayerMapInfo {
        let guard = self.layers.read().await;
        let layer_map = guard.layer_map();
        let mut in_memory_layers = Vec::with_capacity(layer_map.frozen_layers.len() + 1);
        if let Some(open_layer) = &layer_map.open_layer {
            in_memory_layers.push(open_layer.info());
        }
        for frozen_layer in &layer_map.frozen_layers {
            in_memory_layers.push(frozen_layer.info());
        }

        let mut historic_layers = Vec::new();
        for historic_layer in layer_map.iter_historic_layers() {
            let historic_layer = guard.get_from_desc(&historic_layer);
            historic_layers.push(historic_layer.info(reset));
        }

        LayerMapInfo {
            in_memory_layers,
            historic_layers,
        }
    }

    #[instrument(skip_all, fields(tenant_id = %self.tenant_shard_id.tenant_id, shard_id = %self.tenant_shard_id.shard_slug(), timeline_id = %self.timeline_id))]
    pub(crate) async fn download_layer(
        &self,
        layer_file_name: &str,
    ) -> anyhow::Result<Option<bool>> {
        let Some(layer) = self.find_layer(layer_file_name).await else {
            return Ok(None);
        };

        if self.remote_client.is_none() {
            return Ok(Some(false));
        }

        layer.download().await?;

        Ok(Some(true))
    }

    /// Evict just one layer.
    ///
    /// Returns `Ok(None)` in the case where the layer could not be found by its `layer_file_name`.
    pub(crate) async fn evict_layer(&self, layer_file_name: &str) -> anyhow::Result<Option<bool>> {
        let _gate = self
            .gate
            .enter()
            .map_err(|_| anyhow::anyhow!("Shutting down"))?;

        let Some(local_layer) = self.find_layer(layer_file_name).await else {
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
}

/// Number of times we will compute partition within a checkpoint distance.
const REPARTITION_FREQ_IN_CHECKPOINT_DISTANCE: u64 = 10;

// Private functions
impl Timeline {
    pub(crate) fn get_lazy_slru_download(&self) -> bool {
        let tenant_conf = self.tenant_conf.load();
        tenant_conf
            .tenant_conf
            .lazy_slru_download
            .unwrap_or(self.conf.default_tenant_conf.lazy_slru_download)
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

    fn get_image_creation_threshold(&self) -> usize {
        let tenant_conf = self.tenant_conf.load();
        tenant_conf
            .tenant_conf
            .image_creation_threshold
            .unwrap_or(self.conf.default_tenant_conf.image_creation_threshold)
    }

    fn get_compaction_algorithm(&self) -> CompactionAlgorithm {
        let tenant_conf = &self.tenant_conf.load();
        tenant_conf
            .tenant_conf
            .compaction_algorithm
            .unwrap_or(self.conf.default_tenant_conf.compaction_algorithm)
    }

    fn get_eviction_policy(&self) -> EvictionPolicy {
        let tenant_conf = self.tenant_conf.load();
        tenant_conf
            .tenant_conf
            .eviction_policy
            .unwrap_or(self.conf.default_tenant_conf.eviction_policy)
    }

    fn get_evictions_low_residence_duration_metric_threshold(
        tenant_conf: &TenantConfOpt,
        default_tenant_conf: &TenantConf,
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

    pub(super) fn tenant_conf_updated(&self, new_conf: &TenantConfOpt) {
        // NB: Most tenant conf options are read by background loops, so,
        // changes will automatically be picked up.

        // The threshold is embedded in the metric. So, we need to update it.
        {
            let new_threshold = Self::get_evictions_low_residence_duration_metric_threshold(
                new_conf,
                &self.conf.default_tenant_conf,
            );

            let tenant_id_str = self.tenant_shard_id.tenant_id.to_string();
            let shard_id_str = format!("{}", self.tenant_shard_id.shard_slug());

            let timeline_id_str = self.timeline_id.to_string();
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
        ancestor: Option<Arc<Timeline>>,
        timeline_id: TimelineId,
        tenant_shard_id: TenantShardId,
        generation: Generation,
        shard_identity: ShardIdentity,
        walredo_mgr: Option<Arc<super::WalRedoManager>>,
        resources: TimelineResources,
        pg_version: u32,
        state: TimelineState,
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

        Arc::new_cyclic(|myself| {
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

                walredo_mgr,
                walreceiver: Mutex::new(None),

                remote_client: resources.remote_client.map(Arc::new),

                // initialize in-memory 'last_record_lsn' from 'disk_consistent_lsn'.
                last_record_lsn: SeqWait::new(RecordLsn {
                    last: disk_consistent_lsn,
                    prev: metadata.prev_record_lsn().unwrap_or(Lsn(0)),
                }),
                disk_consistent_lsn: AtomicLsn::new(disk_consistent_lsn.0),

                last_freeze_at: AtomicLsn::new(disk_consistent_lsn.0),
                last_freeze_ts: RwLock::new(Instant::now()),

                loaded_at: (disk_consistent_lsn, SystemTime::now()),

                ancestor_timeline: ancestor,
                ancestor_lsn: metadata.ancestor_lsn(),

                metrics: TimelineMetrics::new(
                    &tenant_shard_id,
                    &timeline_id,
                    crate::metrics::EvictionsWithLowResidenceDurationBuilder::new(
                        "mtime",
                        evictions_low_residence_duration_metric_threshold,
                    ),
                ),

                query_metrics: crate::metrics::SmgrQueryTimePerTimeline::new(
                    &tenant_shard_id,
                    &timeline_id,
                ),

                directory_metrics: array::from_fn(|_| AtomicU64::new(0)),

                flush_loop_state: Mutex::new(FlushLoopState::NotStarted),

                layer_flush_start_tx,
                layer_flush_done_tx,

                write_lock: tokio::sync::Mutex::new(None),

                gc_info: std::sync::RwLock::new(GcInfo {
                    retain_lsns: Vec::new(),
                    horizon_cutoff: Lsn(0),
                    pitr_cutoff: Lsn(0),
                }),

                latest_gc_cutoff_lsn: Rcu::new(metadata.latest_gc_cutoff_lsn()),
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
                partitioning: tokio::sync::Mutex::new((KeyPartitioning::new(), Lsn(0))),
                repartition_threshold: 0,
                last_image_layer_creation_check_at: AtomicLsn::new(0),

                last_received_wal: Mutex::new(None),
                rel_size_cache: RwLock::new(RelSizeCache {
                    complete_as_of: disk_consistent_lsn,
                    map: HashMap::new(),
                }),

                download_all_remote_layers_task_info: RwLock::new(None),

                state,

                eviction_task_timeline_state: tokio::sync::Mutex::new(
                    EvictionTaskTimelineState::default(),
                ),
                delete_progress: Arc::new(tokio::sync::Mutex::new(DeleteTimelineFlow::default())),

                cancel,
                gate: Gate::default(),

                compaction_lock: tokio::sync::Mutex::default(),
                gc_lock: tokio::sync::Mutex::default(),

                timeline_get_throttle: resources.timeline_get_throttle,

                aux_files: tokio::sync::Mutex::new(AuxFilesState {
                    dir: None,
                    n_deltas: 0,
                }),
            };
            result.repartition_threshold =
                result.get_checkpoint_distance() / REPARTITION_FREQ_IN_CHECKPOINT_DISTANCE;

            result
                .metrics
                .last_record_gauge
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
                warn!(
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
            Some(self.tenant_shard_id),
            Some(self.timeline_id),
            "layer flush task",
            false,
            async move {
                let _guard = guard;
                let background_ctx = RequestContext::todo_child(TaskKind::LayerFlushTask, DownloadBehavior::Error);
                self_clone.flush_loop(layer_flush_start_rx, &background_ctx).await;
                let mut flush_loop_state = self_clone.flush_loop_state.lock().unwrap();
                assert!(matches!(*flush_loop_state, FlushLoopState::Running{ ..}));
                *flush_loop_state  = FlushLoopState::Exited;
                Ok(())
            }
            .instrument(info_span!(parent: None, "layer flush task", tenant_id = %self.tenant_shard_id.tenant_id, shard_id = %self.tenant_shard_id.shard_slug(), timeline_id = %self.timeline_id))
        );
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
        *guard = Some(WalReceiver::start(
            Arc::clone(self),
            WalReceiverConf {
                wal_connect_timeout,
                lagging_wal_timeout,
                max_lsn_wal_lag,
                auth_token: crate::config::SAFEKEEPER_AUTH_TOKEN.get().cloned(),
                availability_zone: self.conf.availability_zone.clone(),
                ingest_batch_size: self.conf.ingest_batch_size,
            },
            broker_client,
            ctx,
        ));
    }

    /// Initialize with an empty layer map. Used when creating a new timeline.
    pub(super) fn init_empty_layer_map(&self, start_lsn: Lsn) {
        let mut layers = self.layers.try_write().expect(
            "in the context where we call this function, no other task has access to the object",
        );
        layers.initialize_empty(Lsn(start_lsn.0));
    }

    /// Scan the timeline directory, cleanup, populate the layer map, and schedule uploads for local-only
    /// files.
    pub(super) async fn load_layer_map(
        &self,
        disk_consistent_lsn: Lsn,
        index_part: Option<IndexPart>,
    ) -> anyhow::Result<()> {
        use init::{Decision::*, Discovered, DismissedLayer};
        use LayerFileName::*;

        let mut guard = self.layers.write().await;

        let timer = self.metrics.load_layer_map_histo.start_timer();

        // Scan timeline directory and create ImageFileName and DeltaFilename
        // structs representing all files on disk
        let timeline_path = self
            .conf
            .timeline_path(&self.tenant_shard_id, &self.timeline_id);
        let conf = self.conf;
        let span = tracing::Span::current();

        // Copy to move into the task we're about to spawn
        let generation = self.generation;
        let shard = self.get_shard_index();
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
                        Discovered::Layer(file_name, file_size) => {
                            discovered_layers.push((file_name, file_size));
                            continue;
                        }
                        Discovered::Metadata => {
                            warn!("found legacy metadata file, these should have been removed in load_tenant_config");
                            continue;
                        }
                        Discovered::IgnoredBackup => {
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

                let decided = init::reconcile(
                    discovered_layers,
                    index_part.as_ref(),
                    disk_consistent_lsn,
                    generation,
                    shard,
                );

                let mut loaded_layers = Vec::new();
                let mut needs_cleanup = Vec::new();
                let mut total_physical_size = 0;

                for (name, decision) in decided {
                    let decision = match decision {
                        Ok(UseRemote { local, remote }) => {
                            // Remote is authoritative, but we may still choose to retain
                            // the local file if the contents appear to match
                            if local.file_size() == remote.file_size() {
                                // Use the local file, but take the remote metadata so that we pick up
                                // the correct generation.
                                UseLocal(remote)
                            } else {
                                path.push(name.file_name());
                                init::cleanup_local_file_for_remote(&path, &local, &remote)?;
                                path.pop();
                                UseRemote { local, remote }
                            }
                        }
                        Ok(decision) => decision,
                        Err(DismissedLayer::Future { local }) => {
                            if local.is_some() {
                                path.push(name.file_name());
                                init::cleanup_future_layer(&path, &name, disk_consistent_lsn)?;
                                path.pop();
                            }
                            needs_cleanup.push(name);
                            continue;
                        }
                        Err(DismissedLayer::LocalOnly(local)) => {
                            path.push(name.file_name());
                            init::cleanup_local_only_file(&path, &name, &local)?;
                            path.pop();
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
                        UseLocal(m) => {
                            total_physical_size += m.file_size();
                            Layer::for_resident(conf, &this, name, m).drop_eviction_guard()
                        }
                        Evicted(remote) | UseRemote { remote, .. } => {
                            Layer::for_evicted(conf, &this, name, remote)
                        }
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

        guard.initialize_local_layers(loaded_layers, disk_consistent_lsn + 1);

        if let Some(rtc) = self.remote_client.as_ref() {
            rtc.schedule_layer_file_deletion(&needs_cleanup)?;
            rtc.schedule_index_upload_for_file_changes()?;
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
            rtc.schedule_barrier()?;
            // Tenant::create_timeline will wait for these uploads to happen before returning, or
            // on retry.
        }

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
                        let state = self.current_state();
                        if matches!(
                            state,
                            TimelineState::Broken { .. } | TimelineState::Stopping
                        ) {

                            // Can happen when timeline detail endpoint is used when deletion is ongoing (or its broken).
                            // Don't make noise.
                        } else {
                            warn!("unexpected: cancel_wait_for_background_loop_concurrency_limit_semaphore not set, priority-boosting of logical size calculation will not work");
                        }
                    }
                };
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
            Some(self.tenant_shard_id),
            Some(self.timeline_id),
            "initial size calculation",
            false,
            // NB: don't log errors here, task_mgr will do that.
            async move {
                let cancel = task_mgr::shutdown_token();
                self_clone
                    .initial_logical_size_calculation_task(
                        initial_part_end,
                        cancel_wait_for_background_loop_concurrency_limit_semaphore,
                        cancel,
                        background_ctx,
                    )
                    .await;
                Ok(())
            }
            .instrument(info_span!(parent: None, "initial_size_calculation", tenant_id=%self.tenant_shard_id.tenant_id, shard_id=%self.tenant_shard_id.shard_slug(), timeline_id=%self.timeline_id)),
        );
    }

    async fn initial_logical_size_calculation_task(
        self: Arc<Self>,
        initial_part_end: Lsn,
        skip_concurrency_limiter: CancellationToken,
        cancel: CancellationToken,
        background_ctx: RequestContext,
    ) {
        scopeguard::defer! {
            // Irrespective of the outcome of this operation, we should unblock anyone waiting for it.
            self.current_logical_size.initialized.add_permits(1);
        }

        enum BackgroundCalculationError {
            Cancelled,
            Other(anyhow::Error),
        }

        let try_once = |attempt: usize| {
            let background_ctx = &background_ctx;
            let self_ref = &self;
            let skip_concurrency_limiter = &skip_concurrency_limiter;
            async move {
                let cancel = task_mgr::shutdown_token();
                let wait_for_permit = super::tasks::concurrent_background_tasks_rate_limit_permit(
                    BackgroundLoopKind::InitialLogicalSizeCalculation,
                    background_ctx,
                );

                use crate::metrics::initial_logical_size::StartCircumstances;
                let (_maybe_permit, circumstances) = tokio::select! {
                    permit = wait_for_permit => {
                        (Some(permit), StartCircumstances::AfterBackgroundTasksRateLimit)
                    }
                    _ = self_ref.cancel.cancelled() => {
                        return Err(BackgroundCalculationError::Cancelled);
                    }
                    _ = cancel.cancelled() => {
                        return Err(BackgroundCalculationError::Cancelled);
                    },
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

                match self_ref
                    .logical_size_calculation_task(
                        initial_part_end,
                        LogicalSizeCalculationCause::Initial,
                        background_ctx,
                    )
                    .await
                {
                    Ok(calculated_size) => Ok((calculated_size, metrics_guard)),
                    Err(CalculateLogicalSizeError::Cancelled) => {
                        Err(BackgroundCalculationError::Cancelled)
                    }
                    Err(CalculateLogicalSizeError::Other(err)) => {
                        if let Some(PageReconstructError::AncestorStopping(_)) =
                            err.root_cause().downcast_ref()
                        {
                            Err(BackgroundCalculationError::Cancelled)
                        } else {
                            Err(BackgroundCalculationError::Other(err))
                        }
                    }
                }
            }
        };

        let retrying = async {
            let mut attempt = 0;
            loop {
                attempt += 1;

                match try_once(attempt).await {
                    Ok(res) => return ControlFlow::Continue(res),
                    Err(BackgroundCalculationError::Cancelled) => return ControlFlow::Break(()),
                    Err(BackgroundCalculationError::Other(e)) => {
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
                        tokio::time::sleep(sleep_duration).await;
                    }
                }
            }
        };

        let (calculated_size, metrics_guard) = tokio::select! {
            res = retrying  => {
                match res {
                    ControlFlow::Continue(calculated_size) => calculated_size,
                    ControlFlow::Break(()) => return,
                }
            }
            _ = cancel.cancelled() => {
                return;
            }
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
            Some(self.tenant_shard_id),
            Some(self.timeline_id),
            "ondemand logical size calculation",
            false,
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

    /// # Cancel-Safety
    ///
    /// This method is cancellation-safe.
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

        let self_calculation = Arc::clone(self);

        let mut calculation = pin!(async {
            let ctx = ctx.attached_child();
            self_calculation
                .calculate_logical_size(lsn, cause, &guard, &ctx)
                .await
        });

        tokio::select! {
            res = &mut calculation => { res }
            _ = self.cancel.cancelled() => {
                debug!("cancelling logical size calculation for timeline shutdown");
                calculation.await
            }
        }
    }

    /// Calculate the logical size of the database at the latest LSN.
    ///
    /// NOTE: counted incrementally, includes ancestors. This can be a slow operation,
    /// especially if we need to download remote layers.
    ///
    /// # Cancel-Safety
    ///
    /// This method is cancellation-safe.
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

        pausable_failpoint!("timeline-calculate-logical-size-pause");

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

    pub(crate) fn update_directory_entries_count(&self, kind: DirectoryKind, count: u64) {
        self.directory_metrics[kind.offset()].store(count, AtomicOrdering::Relaxed);
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

    async fn find_layer(&self, layer_file_name: &str) -> Option<Layer> {
        let guard = self.layers.read().await;
        for historic_layer in guard.layer_map().iter_historic_layers() {
            let historic_layer_name = historic_layer.filename().file_name();
            if layer_file_name == historic_layer_name {
                return Some(guard.get_from_desc(&historic_layer));
            }
        }

        None
    }

    /// The timeline heatmap is a hint to secondary locations from the primary location,
    /// indicating which layers are currently on-disk on the primary.
    ///
    /// None is returned if the Timeline is in a state where uploading a heatmap
    /// doesn't make sense, such as shutting down or initializing.  The caller
    /// should treat this as a cue to simply skip doing any heatmap uploading
    /// for this timeline.
    pub(crate) async fn generate_heatmap(&self) -> Option<HeatMapTimeline> {
        // no point in heatmaps without remote client
        let _remote_client = self.remote_client.as_ref()?;

        if !self.is_active() {
            return None;
        }

        let guard = self.layers.read().await;

        let resident = guard.likely_resident_layers().map(|layer| {
            let last_activity_ts = layer.access_stats().latest_activity_or_now();

            HeatMapLayer::new(
                layer.layer_desc().filename(),
                layer.metadata().into(),
                last_activity_ts,
            )
        });

        let layers = resident.collect();

        Some(HeatMapTimeline::new(self.timeline_id, layers))
    }
}

type TraversalId = String;

trait TraversalLayerExt {
    fn traversal_id(&self) -> TraversalId;
}

impl TraversalLayerExt for Layer {
    fn traversal_id(&self) -> TraversalId {
        self.local_path().to_string()
    }
}

impl TraversalLayerExt for Arc<InMemoryLayer> {
    fn traversal_id(&self) -> TraversalId {
        format!("timeline {} in-memory {self}", self.get_timeline_id())
    }
}

impl Timeline {
    ///
    /// Get a handle to a Layer for reading.
    ///
    /// The returned Layer might be from an ancestor timeline, if the
    /// segment hasn't been updated on this timeline yet.
    ///
    /// This function takes the current timeline's locked LayerMap as an argument,
    /// so callers can avoid potential race conditions.
    ///
    /// # Cancel-Safety
    ///
    /// This method is cancellation-safe.
    async fn get_reconstruct_data(
        &self,
        key: Key,
        request_lsn: Lsn,
        reconstruct_state: &mut ValueReconstructState,
        ctx: &RequestContext,
    ) -> Result<Vec<TraversalPathItem>, PageReconstructError> {
        // Start from the current timeline.
        let mut timeline_owned;
        let mut timeline = self;

        let mut read_count = scopeguard::guard(0, |cnt| {
            crate::metrics::READ_NUM_FS_LAYERS.observe(cnt as f64)
        });

        // For debugging purposes, collect the path of layers that we traversed
        // through. It's included in the error message if we fail to find the key.
        let mut traversal_path = Vec::<TraversalPathItem>::new();

        let cached_lsn = if let Some((cached_lsn, _)) = &reconstruct_state.img {
            *cached_lsn
        } else {
            Lsn(0)
        };

        // 'prev_lsn' tracks the last LSN that we were at in our search. It's used
        // to check that each iteration make some progress, to break infinite
        // looping if something goes wrong.
        let mut prev_lsn = None;

        let mut result = ValueReconstructResult::Continue;
        let mut cont_lsn = Lsn(request_lsn.0 + 1);

        'outer: loop {
            if self.cancel.is_cancelled() {
                return Err(PageReconstructError::Cancelled);
            }

            // The function should have updated 'state'
            //info!("CALLED for {} at {}: {:?} with {} records, cached {}", key, cont_lsn, result, reconstruct_state.records.len(), cached_lsn);
            match result {
                ValueReconstructResult::Complete => return Ok(traversal_path),
                ValueReconstructResult::Continue => {
                    // If we reached an earlier cached page image, we're done.
                    if cont_lsn == cached_lsn + 1 {
                        MATERIALIZED_PAGE_CACHE_HIT.inc_by(1);
                        return Ok(traversal_path);
                    }
                    if let Some(prev) = prev_lsn {
                        if prev <= cont_lsn {
                            // Didn't make any progress in last iteration. Error out to avoid
                            // getting stuck in the loop.
                            return Err(layer_traversal_error(format!(
                                "could not find layer with more data for key {} at LSN {}, request LSN {}, ancestor {}",
                                key,
                                Lsn(cont_lsn.0 - 1),
                                request_lsn,
                                timeline.ancestor_lsn
                            ), traversal_path));
                        }
                    }
                    prev_lsn = Some(cont_lsn);
                }
                ValueReconstructResult::Missing => {
                    return Err(layer_traversal_error(
                        if cfg!(test) {
                            format!(
                                "could not find data for key {} (shard {:?}) at LSN {}, for request at LSN {}\n{}",
                                key, self.shard_identity.get_shard_number(&key), cont_lsn, request_lsn, std::backtrace::Backtrace::force_capture(),
                            )
                        } else {
                            format!(
                                "could not find data for key {} (shard {:?}) at LSN {}, for request at LSN {}",
                                key, self.shard_identity.get_shard_number(&key), cont_lsn, request_lsn
                            )
                        },
                        traversal_path,
                    ));
                }
            }

            // Recurse into ancestor if needed
            if is_inherited_key(key) && Lsn(cont_lsn.0 - 1) <= timeline.ancestor_lsn {
                trace!(
                    "going into ancestor {}, cont_lsn is {}",
                    timeline.ancestor_lsn,
                    cont_lsn
                );

                timeline_owned = timeline.get_ready_ancestor_timeline(ctx).await?;
                timeline = &*timeline_owned;
                prev_lsn = None;
                continue 'outer;
            }

            let guard = timeline.layers.read().await;
            let layers = guard.layer_map();

            // Check the open and frozen in-memory layers first, in order from newest
            // to oldest.
            if let Some(open_layer) = &layers.open_layer {
                let start_lsn = open_layer.get_lsn_range().start;
                if cont_lsn > start_lsn {
                    //info!("CHECKING for {} at {} on open layer {}", key, cont_lsn, open_layer.filename().display());
                    // Get all the data needed to reconstruct the page version from this layer.
                    // But if we have an older cached page image, no need to go past that.
                    let lsn_floor = max(cached_lsn + 1, start_lsn);

                    let open_layer = open_layer.clone();
                    drop(guard);

                    result = match open_layer
                        .get_value_reconstruct_data(
                            key,
                            lsn_floor..cont_lsn,
                            reconstruct_state,
                            ctx,
                        )
                        .await
                    {
                        Ok(result) => result,
                        Err(e) => return Err(PageReconstructError::from(e)),
                    };
                    cont_lsn = lsn_floor;
                    // metrics: open_layer does not count as fs access, so we are not updating `read_count`
                    traversal_path.push((
                        result,
                        cont_lsn,
                        Box::new(move || open_layer.traversal_id()),
                    ));
                    continue 'outer;
                }
            }
            for frozen_layer in layers.frozen_layers.iter().rev() {
                let start_lsn = frozen_layer.get_lsn_range().start;
                if cont_lsn > start_lsn {
                    //info!("CHECKING for {} at {} on frozen layer {}", key, cont_lsn, frozen_layer.filename().display());
                    let lsn_floor = max(cached_lsn + 1, start_lsn);

                    let frozen_layer = frozen_layer.clone();
                    drop(guard);

                    result = match frozen_layer
                        .get_value_reconstruct_data(
                            key,
                            lsn_floor..cont_lsn,
                            reconstruct_state,
                            ctx,
                        )
                        .await
                    {
                        Ok(result) => result,
                        Err(e) => return Err(PageReconstructError::from(e)),
                    };
                    cont_lsn = lsn_floor;
                    // metrics: open_layer does not count as fs access, so we are not updating `read_count`
                    traversal_path.push((
                        result,
                        cont_lsn,
                        Box::new(move || frozen_layer.traversal_id()),
                    ));
                    continue 'outer;
                }
            }

            if let Some(SearchResult { lsn_floor, layer }) = layers.search(key, cont_lsn) {
                let layer = guard.get_from_desc(&layer);
                drop(guard);

                // Get all the data needed to reconstruct the page version from this layer.
                // But if we have an older cached page image, no need to go past that.
                let lsn_floor = max(cached_lsn + 1, lsn_floor);
                result = match layer
                    .get_value_reconstruct_data(key, lsn_floor..cont_lsn, reconstruct_state, ctx)
                    .await
                {
                    Ok(result) => result,
                    Err(e) => return Err(PageReconstructError::from(e)),
                };
                cont_lsn = lsn_floor;
                *read_count += 1;
                traversal_path.push((
                    result,
                    cont_lsn,
                    Box::new({
                        let layer = layer.to_owned();
                        move || layer.traversal_id()
                    }),
                ));
                continue 'outer;
            } else if timeline.ancestor_timeline.is_some() {
                // Nothing on this timeline. Traverse to parent
                result = ValueReconstructResult::Continue;
                cont_lsn = Lsn(timeline.ancestor_lsn.0 + 1);
                continue 'outer;
            } else {
                // Nothing found
                result = ValueReconstructResult::Missing;
                continue 'outer;
            }
        }
    }

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
        mut keyspace: KeySpace,
        request_lsn: Lsn,
        reconstruct_state: &mut ValuesReconstructState,
        ctx: &RequestContext,
    ) -> Result<(), GetVectoredError> {
        let mut timeline_owned: Arc<Timeline>;
        let mut timeline = self;

        let mut cont_lsn = Lsn(request_lsn.0 + 1);

        loop {
            if self.cancel.is_cancelled() {
                return Err(GetVectoredError::Cancelled);
            }

            let completed = Self::get_vectored_reconstruct_data_timeline(
                timeline,
                keyspace.clone(),
                cont_lsn,
                reconstruct_state,
                &self.cancel,
                ctx,
            )
            .await?;

            keyspace.remove_overlapping_with(&completed);
            if keyspace.total_size() == 0 || timeline.ancestor_timeline.is_none() {
                break;
            }

            // Take the min to avoid reconstructing a page with data newer than request Lsn.
            cont_lsn = std::cmp::min(Lsn(request_lsn.0 + 1), Lsn(timeline.ancestor_lsn.0 + 1));
            timeline_owned = timeline
                .get_ready_ancestor_timeline(ctx)
                .await
                .map_err(GetVectoredError::GetReadyAncestorError)?;
            timeline = &*timeline_owned;
        }

        if keyspace.total_size() != 0 {
            return Err(GetVectoredError::MissingKey(keyspace.start().unwrap()));
        }

        Ok(())
    }

    /// Collect the reconstruct data for a ketspace from the specified timeline.
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
    async fn get_vectored_reconstruct_data_timeline(
        timeline: &Timeline,
        keyspace: KeySpace,
        mut cont_lsn: Lsn,
        reconstruct_state: &mut ValuesReconstructState,
        cancel: &CancellationToken,
        ctx: &RequestContext,
    ) -> Result<KeySpace, GetVectoredError> {
        let mut unmapped_keyspace = keyspace.clone();
        let mut fringe = LayerFringe::new();

        let mut completed_keyspace = KeySpace::default();

        loop {
            if cancel.is_cancelled() {
                return Err(GetVectoredError::Cancelled);
            }

            let keys_done_last_step = reconstruct_state.consume_done_keys();
            unmapped_keyspace.remove_overlapping_with(&keys_done_last_step);
            completed_keyspace.merge(&keys_done_last_step);

            let guard = timeline.layers.read().await;
            let layers = guard.layer_map();

            let in_memory_layer = layers.find_in_memory_layer(|l| {
                let start_lsn = l.get_lsn_range().start;
                cont_lsn > start_lsn
            });

            match in_memory_layer {
                Some(l) => {
                    let lsn_range = l.get_lsn_range().start..cont_lsn;
                    fringe.update(
                        ReadableLayer::InMemoryLayer(l),
                        unmapped_keyspace.clone(),
                        lsn_range,
                    );
                }
                None => {
                    for range in unmapped_keyspace.ranges.iter() {
                        let results = layers.range_search(range.clone(), cont_lsn);

                        results
                            .found
                            .into_iter()
                            .map(|(SearchResult { layer, lsn_floor }, keyspace_accum)| {
                                (
                                    ReadableLayer::PersistentLayer(guard.get_from_desc(&layer)),
                                    keyspace_accum.to_keyspace(),
                                    lsn_floor..cont_lsn,
                                )
                            })
                            .for_each(|(layer, keyspace, lsn_range)| {
                                fringe.update(layer, keyspace, lsn_range)
                            });
                    }
                }
            }

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

            if let Some((layer_to_read, keyspace_to_read, lsn_range)) = fringe.next_layer() {
                let next_cont_lsn = lsn_range.start;
                layer_to_read
                    .get_values_reconstruct_data(
                        keyspace_to_read.clone(),
                        lsn_range,
                        reconstruct_state,
                        ctx,
                    )
                    .await?;

                unmapped_keyspace = keyspace_to_read;
                cont_lsn = next_cont_lsn;
            } else {
                break;
            }
        }

        Ok(completed_keyspace)
    }

    /// # Cancel-safety
    ///
    /// This method is cancellation-safe.
    async fn lookup_cached_page(
        &self,
        key: &Key,
        lsn: Lsn,
        ctx: &RequestContext,
    ) -> Option<(Lsn, Bytes)> {
        let cache = page_cache::get();

        // FIXME: It's pointless to check the cache for things that are not 8kB pages.
        // We should look at the key to determine if it's a cacheable object
        let (lsn, read_guard) = cache
            .lookup_materialized_page(self.tenant_shard_id, self.timeline_id, key, lsn, ctx)
            .await?;
        let img = Bytes::from(read_guard.to_vec());
        Some((lsn, img))
    }

    async fn get_ready_ancestor_timeline(
        &self,
        ctx: &RequestContext,
    ) -> Result<Arc<Timeline>, GetReadyAncestorError> {
        let ancestor = match self.get_ancestor_timeline() {
            Ok(timeline) => timeline,
            Err(e) => return Err(GetReadyAncestorError::from(e)),
        };

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
                return Err(GetReadyAncestorError::AncestorStopping(
                    ancestor.timeline_id,
                ));
            }
            Err(state) => {
                return Err(GetReadyAncestorError::Other(anyhow::anyhow!(
                    "Timeline {} will not become active. Current state: {:?}",
                    ancestor.timeline_id,
                    &state,
                )));
            }
        }
        ancestor
            .wait_lsn(self.ancestor_lsn, WaitLsnWaiter::Timeline(self), ctx)
            .await
            .map_err(|e| match e {
                e @ WaitLsnError::Timeout(_) => GetReadyAncestorError::AncestorLsnTimeout(e),
                WaitLsnError::Shutdown => GetReadyAncestorError::Cancelled,
                e @ WaitLsnError::BadState => GetReadyAncestorError::Other(anyhow::anyhow!(e)),
            })?;

        Ok(ancestor)
    }

    fn get_ancestor_timeline(&self) -> anyhow::Result<Arc<Timeline>> {
        let ancestor = self.ancestor_timeline.as_ref().with_context(|| {
            format!(
                "Ancestor is missing. Timeline id: {} Ancestor id {:?}",
                self.timeline_id,
                self.get_ancestor_timeline_id(),
            )
        })?;
        Ok(Arc::clone(ancestor))
    }

    pub(crate) fn get_shard_identity(&self) -> &ShardIdentity {
        &self.shard_identity
    }

    ///
    /// Get a handle to the latest layer for appending.
    ///
    async fn get_layer_for_write(&self, lsn: Lsn) -> anyhow::Result<Arc<InMemoryLayer>> {
        let mut guard = self.layers.write().await;
        let layer = guard
            .get_layer_for_write(
                lsn,
                self.get_last_record_lsn(),
                self.conf,
                self.timeline_id,
                self.tenant_shard_id,
            )
            .await?;
        Ok(layer)
    }

    pub(crate) fn finish_write(&self, new_lsn: Lsn) {
        assert!(new_lsn.is_aligned());

        self.metrics.last_record_gauge.set(new_lsn.0 as i64);
        self.last_record_lsn.advance(new_lsn);
    }

    /// Whether there was a layer to freeze or not, return the value of get_last_record_lsn
    /// before we attempted the freeze: this guarantees that ingested data is frozen up to this lsn (inclusive).
    async fn freeze_inmem_layer(&self, write_lock_held: bool) -> Lsn {
        // Freeze the current open in-memory layer. It will be written to disk on next
        // iteration.

        let _write_guard = if write_lock_held {
            None
        } else {
            Some(self.write_lock.lock().await)
        };

        let to_lsn = self.get_last_record_lsn();
        self.freeze_inmem_layer_at(to_lsn).await;
        to_lsn
    }

    async fn freeze_inmem_layer_at(&self, at: Lsn) {
        let mut guard = self.layers.write().await;
        guard
            .try_freeze_in_memory_layer(at, &self.last_freeze_at)
            .await;
    }

    /// Layer flusher task's main loop.
    async fn flush_loop(
        self: &Arc<Self>,
        mut layer_flush_start_rx: tokio::sync::watch::Receiver<(u64, Lsn)>,
        ctx: &RequestContext,
    ) {
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
                    // Note: we do not bother transmitting into [`layer_flush_done_tx`], because
                    // anyone waiting on that will respect self.cancel as well: they will stop
                    // waiting at the same time we as drop out of this loop.
                    return;
                }

                let timer = self.metrics.flush_time_histo.start_timer();

                let layer_to_flush = {
                    let guard = self.layers.read().await;
                    guard.layer_map().frozen_layers.front().cloned()
                    // drop 'layers' lock to allow concurrent reads and writes
                };
                let Some(layer_to_flush) = layer_to_flush else {
                    break Ok(());
                };
                match self.flush_frozen_layer(layer_to_flush, ctx).await {
                    Ok(this_layer_to_lsn) => {
                        flushed_to_lsn = std::cmp::max(flushed_to_lsn, this_layer_to_lsn);
                    }
                    Err(FlushLayerError::Cancelled) => {
                        info!("dropping out of flush loop for timeline shutdown");
                        return;
                    }
                    err @ Err(
                        FlushLayerError::Other(_) | FlushLayerError::CreateImageLayersError(_),
                    ) => {
                        error!("could not flush frozen layer: {err:?}");
                        break err.map(|_| ());
                    }
                }
                timer.stop_and_record();
            };

            // Unsharded tenants should never advance their LSN beyond the end of the
            // highest layer they write: such gaps between layer data and the frozen LSN
            // are only legal on sharded tenants.
            debug_assert!(
                self.shard_identity.count.count() > 1
                    || flushed_to_lsn >= frozen_to_lsn
                    || !flushed_to_lsn.is_valid()
            );

            if flushed_to_lsn < frozen_to_lsn && self.shard_identity.count.count() > 1 {
                // If our layer flushes didn't carry disk_consistent_lsn up to the `to_lsn` advertised
                // to us via layer_flush_start_rx, then advance it here.
                //
                // This path is only taken for tenants with multiple shards: single sharded tenants should
                // never encounter a gap in the wal.
                let old_disk_consistent_lsn = self.disk_consistent_lsn.load();
                tracing::debug!("Advancing disk_consistent_lsn across layer gap {old_disk_consistent_lsn}->{frozen_to_lsn}");
                if self.set_disk_consistent_lsn(frozen_to_lsn) {
                    if let Err(e) = self.schedule_uploads(frozen_to_lsn, vec![]) {
                        tracing::warn!("Failed to schedule metadata upload after updating disk_consistent_lsn: {e}");
                    }
                }
            }

            // Notify any listeners that we're done
            let _ = self
                .layer_flush_done_tx
                .send_replace((flush_counter, result));
        }
    }

    /// Request the flush loop to write out all frozen layers up to `to_lsn` as Delta L0 files to disk.
    /// The caller is responsible for the freezing, e.g., [`Self::freeze_inmem_layer`].
    ///
    /// `last_record_lsn` may be higher than the highest LSN of a frozen layer: if this is the case,
    /// it means no data will be written between the top of the highest frozen layer and to_lsn,
    /// e.g. because this tenant shard has ingested up to to_lsn and not written any data locally for that part of the WAL.
    async fn flush_frozen_layers_and_wait(&self, last_record_lsn: Lsn) -> anyhow::Result<()> {
        let mut rx = self.layer_flush_done_tx.subscribe();

        // Increment the flush cycle counter and wake up the flush task.
        // Remember the new value, so that when we listen for the flush
        // to finish, we know when the flush that we initiated has
        // finished, instead of some other flush that was started earlier.
        let mut my_flush_request = 0;

        let flush_loop_state = { *self.flush_loop_state.lock().unwrap() };
        if !matches!(flush_loop_state, FlushLoopState::Running { .. }) {
            anyhow::bail!("cannot flush frozen layers when flush_loop is not running, state is {flush_loop_state:?}")
        }

        self.layer_flush_start_tx.send_modify(|(counter, lsn)| {
            my_flush_request = *counter + 1;
            *counter = my_flush_request;
            *lsn = std::cmp::max(last_record_lsn, *lsn);
        });

        loop {
            {
                let (last_result_counter, last_result) = &*rx.borrow();
                if *last_result_counter >= my_flush_request {
                    if let Err(_err) = last_result {
                        // We already logged the original error in
                        // flush_loop. We cannot propagate it to the caller
                        // here, because it might not be Cloneable
                        anyhow::bail!(
                            "Could not flush frozen layer. Request id: {}",
                            my_flush_request
                        );
                    } else {
                        return Ok(());
                    }
                }
            }
            trace!("waiting for flush to complete");
            tokio::select! {
                rx_e = rx.changed() => {
                    rx_e?;
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

    fn flush_frozen_layers(&self) {
        self.layer_flush_start_tx.send_modify(|(counter, lsn)| {
            *counter += 1;

            *lsn = std::cmp::max(*lsn, Lsn(self.last_freeze_at.load().0 - 1));
        });
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
        let (layers_to_upload, delta_layer_to_add) =
            if lsn_range.start == self.initdb_lsn && lsn_range.end == Lsn(self.initdb_lsn.0 + 1) {
                #[cfg(test)]
                match &mut *self.flush_loop_state.lock().unwrap() {
                    FlushLoopState::NotStarted | FlushLoopState::Exited => {
                        panic!("flush loop not running")
                    }
                    FlushLoopState::Running {
                        initdb_optimization_count,
                        ..
                    } => {
                        *initdb_optimization_count += 1;
                    }
                }
                // Note: The 'ctx' in use here has DownloadBehavior::Error. We should not
                // require downloading anything during initial import.
                let (partitioning, _lsn) = self
                    .repartition(
                        self.initdb_lsn,
                        self.get_compaction_target_size(),
                        EnumSet::empty(),
                        ctx,
                    )
                    .await?;

                if self.cancel.is_cancelled() {
                    return Err(FlushLayerError::Cancelled);
                }

                // For image layers, we add them immediately into the layer map.
                (
                    self.create_image_layers(&partitioning, self.initdb_lsn, true, ctx)
                        .await?,
                    None,
                )
            } else {
                #[cfg(test)]
                match &mut *self.flush_loop_state.lock().unwrap() {
                    FlushLoopState::NotStarted | FlushLoopState::Exited => {
                        panic!("flush loop not running")
                    }
                    FlushLoopState::Running {
                        expect_initdb_optimization,
                        ..
                    } => {
                        assert!(!*expect_initdb_optimization, "expected initdb optimization");
                    }
                }
                // Normal case, write out a L0 delta layer file.
                // `create_delta_layer` will not modify the layer map.
                // We will remove frozen layer and add delta layer in one atomic operation later.
                let layer = self.create_delta_layer(&frozen_layer, ctx).await?;
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

        let disk_consistent_lsn = Lsn(lsn_range.end.0 - 1);

        // The new on-disk layers are now in the layer map. We can remove the
        // in-memory layer from the map now. The flushed layer is stored in
        // the mapping in `create_delta_layer`.
        {
            let mut guard = self.layers.write().await;

            if self.cancel.is_cancelled() {
                return Err(FlushLayerError::Cancelled);
            }

            guard.finish_flush_l0_layer(delta_layer_to_add.as_ref(), &frozen_layer, &self.metrics);

            if self.set_disk_consistent_lsn(disk_consistent_lsn) {
                // Schedule remote uploads that will reflect our new disk_consistent_lsn
                self.schedule_uploads(disk_consistent_lsn, layers_to_upload)?;
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
    /// This function must only be used from the layer flush task, and may not be called concurrently.
    fn set_disk_consistent_lsn(&self, new_value: Lsn) -> bool {
        // We do a simple load/store cycle: that's why this function isn't safe for concurrent use.
        let old_value = self.disk_consistent_lsn.load();
        if new_value != old_value {
            assert!(new_value >= old_value);
            self.disk_consistent_lsn.store(new_value);
            true
        } else {
            false
        }
    }

    /// Update metadata file
    fn schedule_uploads(
        &self,
        disk_consistent_lsn: Lsn,
        layers_to_upload: impl IntoIterator<Item = ResidentLayer>,
    ) -> anyhow::Result<TimelineMetadata> {
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

        let ancestor_timeline_id = self
            .ancestor_timeline
            .as_ref()
            .map(|ancestor| ancestor.timeline_id);

        let metadata = TimelineMetadata::new(
            disk_consistent_lsn,
            ondisk_prev_record_lsn,
            ancestor_timeline_id,
            self.ancestor_lsn,
            *self.latest_gc_cutoff_lsn.read(),
            self.initdb_lsn,
            self.pg_version,
        );

        fail_point!("checkpoint-before-saving-metadata", |x| bail!(
            "{}",
            x.unwrap()
        ));

        if let Some(remote_client) = &self.remote_client {
            for layer in layers_to_upload {
                remote_client.schedule_layer_file_upload(layer)?;
            }
            remote_client.schedule_index_upload_for_metadata_update(&metadata)?;
        }

        Ok(metadata)
    }

    pub(crate) async fn preserve_initdb_archive(&self) -> anyhow::Result<()> {
        if let Some(remote_client) = &self.remote_client {
            remote_client
                .preserve_initdb_archive(
                    &self.tenant_shard_id.tenant_id,
                    &self.timeline_id,
                    &self.cancel,
                )
                .await?;
        } else {
            bail!("No remote storage configured, but was asked to backup the initdb archive for {} / {}", self.tenant_shard_id.tenant_id, self.timeline_id);
        }
        Ok(())
    }

    // Write out the given frozen in-memory layer as a new L0 delta file. This L0 file will not be tracked
    // in layer map immediately. The caller is responsible to put it into the layer map.
    async fn create_delta_layer(
        self: &Arc<Self>,
        frozen_layer: &Arc<InMemoryLayer>,
        ctx: &RequestContext,
    ) -> anyhow::Result<ResidentLayer> {
        let self_clone = Arc::clone(self);
        let frozen_layer = Arc::clone(frozen_layer);
        let ctx = ctx.attached_child();
        let work = async move {
            let new_delta = frozen_layer.write_to_disk(&self_clone, &ctx).await?;
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
            )
            .await
            .fatal_err("VirtualFile::open for timeline dir fsync");
            timeline_dir
                .sync_all()
                .await
                .fatal_err("VirtualFile::sync_all timeline dir");
            anyhow::Ok(new_delta)
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
    ) -> anyhow::Result<(KeyPartitioning, Lsn)> {
        let Ok(mut partitioning_guard) = self.partitioning.try_lock() else {
            // NB: there are two callers, one is the compaction task, of which there is only one per struct Tenant and hence Timeline.
            // The other is the initdb optimization in flush_frozen_layer, used by `boostrap_timeline`, which runs before `.activate()`
            // and hence before the compaction task starts.
            anyhow::bail!("repartition() called concurrently, this should not happen");
        };
        if lsn < partitioning_guard.1 {
            anyhow::bail!("repartition() called with LSN going backwards, this should not happen");
        }

        let distance = lsn.0 - partitioning_guard.1 .0;
        if partitioning_guard.1 != Lsn(0)
            && distance <= self.repartition_threshold
            && !flags.contains(CompactFlags::ForceRepartition)
        {
            debug!(
                distance,
                threshold = self.repartition_threshold,
                "no repartitioning needed"
            );
            return Ok((partitioning_guard.0.clone(), partitioning_guard.1));
        }

        let keyspace = self.collect_keyspace(lsn, ctx).await?;
        let partitioning = keyspace.partition(partition_size);

        *partitioning_guard = (partitioning, lsn);

        Ok((partitioning_guard.0.clone(), partitioning_guard.1))
    }

    // Is it time to create a new image layer for the given partition?
    async fn time_for_new_image_layer(&self, partition: &KeySpace, lsn: Lsn) -> bool {
        let last = self.last_image_layer_creation_check_at.load();
        if lsn != Lsn(0) {
            let distance = lsn
                .checked_sub(last)
                .expect("Attempt to compact with LSN going backwards");

            let min_distance = self.get_image_layer_creation_check_threshold() as u64
                * self.get_checkpoint_distance();

            // Skip the expensive delta layer counting below if we've not ingested
            // sufficient WAL since the last check.
            if distance.0 < min_distance {
                return false;
            }
        }

        self.last_image_layer_creation_check_at.store(lsn);

        let threshold = self.get_image_creation_threshold();

        let guard = self.layers.read().await;
        let layers = guard.layer_map();

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

    #[tracing::instrument(skip_all, fields(%lsn, %force))]
    async fn create_image_layers(
        self: &Arc<Timeline>,
        partitioning: &KeyPartitioning,
        lsn: Lsn,
        force: bool,
        ctx: &RequestContext,
    ) -> Result<Vec<ResidentLayer>, CreateImageLayersError> {
        let timer = self.metrics.create_images_time_histo.start_timer();
        let mut image_layers = Vec::new();

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

        for partition in partitioning.parts.iter() {
            let img_range = start..partition.ranges.last().unwrap().end;
            if !force && !self.time_for_new_image_layer(partition, lsn).await {
                start = img_range.end;
                continue;
            }

            let mut image_layer_writer = ImageLayerWriter::new(
                self.conf,
                self.timeline_id,
                self.tenant_shard_id,
                &img_range,
                lsn,
            )
            .await?;

            fail_point!("image-layer-writer-fail-before-finish", |_| {
                Err(CreateImageLayersError::Other(anyhow::anyhow!(
                    "failpoint image-layer-writer-fail-before-finish"
                )))
            });

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
                    if key_request_accum.size() >= Timeline::MAX_GET_VECTORED_KEYS
                        || last_key_in_range
                    {
                        let results = self
                            .get_vectored(key_request_accum.consume_keyspace(), lsn, ctx)
                            .await?;

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
                                    if is_rel_fsm_block_key(img_key) || is_rel_vm_block_key(img_key)
                                    {
                                        warn!("could not reconstruct FSM or VM key {img_key}, filling with zeros: {err:?}");
                                        ZERO_PAGE.clone()
                                    } else {
                                        return Err(CreateImageLayersError::PageReconstructError(
                                            err,
                                        ));
                                    }
                                }
                            };

                            // Write all the keys we just read into our new image layer.
                            image_layer_writer.put_image(img_key, img).await?;
                            wrote_keys = true;
                        }
                    }
                }
            }

            if wrote_keys {
                // Normal path: we have written some data into the new image layer for this
                // partition, so flush it to disk.
                start = img_range.end;
                let image_layer = image_layer_writer.finish(self).await?;
                image_layers.push(image_layer);
            } else {
                // Special case: the image layer may be empty if this is a sharded tenant and the
                // partition does not cover any keys owned by this shard.  In this case, to ensure
                // we don't leave gaps between image layers, leave `start` where it is, so that the next
                // layer we write will cover the key range that we just scanned.
                tracing::debug!("no data in range {}-{}", img_range.start, img_range.end);
            }
        }

        // The writer.finish() above already did the fsync of the inodes.
        // We just need to fsync the directory in which these inodes are linked,
        // which we know to be the timeline directory.
        if !image_layers.is_empty() {
            // We use fatal_err() below because the after writer.finish() returns with success,
            // the in-memory state of the filesystem already has the layer file in its final place,
            // and subsequent pageserver code could think it's durable while it really isn't.
            let timeline_dir = VirtualFile::open(
                &self
                    .conf
                    .timeline_path(&self.tenant_shard_id, &self.timeline_id),
            )
            .await
            .fatal_err("VirtualFile::open for timeline dir fsync");
            timeline_dir
                .sync_all()
                .await
                .fatal_err("VirtualFile::sync_all timeline dir");
        }

        let mut guard = self.layers.write().await;

        // FIXME: we could add the images to be uploaded *before* returning from here, but right
        // now they are being scheduled outside of write lock
        guard.track_new_image_layers(&image_layers, &self.metrics);
        drop_wlock(guard);
        timer.stop_and_record();

        Ok(image_layers)
    }

    /// Wait until the background initial logical size calculation is complete, or
    /// this Timeline is shut down.  Calling this function will cause the initial
    /// logical size calculation to skip waiting for the background jobs barrier.
    pub(crate) async fn await_initial_logical_size(self: Arc<Self>) {
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
            tracing::info!(
                "await_initial_logical_size: can't get semaphore cancel token, skipping"
            );
        }

        tokio::select!(
            _ = self.current_logical_size.initialized.acquire() => {},
            _ = self.cancel.cancelled() => {}
        )
    }
}

/// Top-level failure to compact.
#[derive(Debug, thiserror::Error)]
pub(crate) enum CompactionError {
    #[error("The timeline or pageserver is shutting down")]
    ShuttingDown,
    /// Compaction cannot be done right now; page reconstruction and so on.
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

impl From<CollectKeySpaceError> for CompactionError {
    fn from(err: CollectKeySpaceError) -> Self {
        match err {
            CollectKeySpaceError::Cancelled
            | CollectKeySpaceError::PageRead(PageReconstructError::Cancelled) => {
                CompactionError::ShuttingDown
            }
            e => CompactionError::Other(e.into()),
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

impl Timeline {
    async fn finish_compact_batch(
        self: &Arc<Self>,
        new_deltas: &[ResidentLayer],
        new_images: &[ResidentLayer],
        layers_to_remove: &[Layer],
    ) -> anyhow::Result<()> {
        let mut guard = self.layers.write().await;

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
            } else if LayerMap::is_l0(l.layer_desc()) {
                bail!("compaction generates a L0 layer file as output, which will cause infinite compaction.");
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
            guard.track_new_image_layers(new_images, &self.metrics);
        }

        // deletion will happen later, the layer file manager calls garbage_collect_on_drop
        guard.finish_compact_l0(&remove_layers, &insert_layers, &self.metrics);

        if let Some(remote_client) = self.remote_client.as_ref() {
            remote_client.schedule_compaction_update(&remove_layers, new_deltas)?;
        }

        drop_wlock(guard);

        Ok(())
    }

    /// Schedules the uploads of the given image layers
    fn upload_new_image_layers(
        self: &Arc<Self>,
        new_images: impl IntoIterator<Item = ResidentLayer>,
    ) -> anyhow::Result<()> {
        let Some(remote_client) = &self.remote_client else {
            return Ok(());
        };
        for layer in new_images {
            remote_client.schedule_layer_file_upload(layer)?;
        }
        // should any new image layer been created, not uploading index_part will
        // result in a mismatch between remote_physical_size and layermap calculated
        // size, which will fail some tests, but should not be an issue otherwise.
        remote_client.schedule_index_upload_for_file_changes()?;
        Ok(())
    }

    /// Update information about which layer files need to be retained on
    /// garbage collection. This is separate from actually performing the GC,
    /// and is updated more frequently, so that compaction can remove obsolete
    /// page versions more aggressively.
    ///
    /// TODO: that's wishful thinking, compaction doesn't actually do that
    /// currently.
    ///
    /// The caller specifies how much history is needed with the 3 arguments:
    ///
    /// retain_lsns: keep a version of each page at these LSNs
    /// cutoff_horizon: also keep everything newer than this LSN
    /// pitr: the time duration required to keep data for PITR
    ///
    /// The 'retain_lsns' list is currently used to prevent removing files that
    /// are needed by child timelines. In the future, the user might be able to
    /// name additional points in time to retain. The caller is responsible for
    /// collecting that information.
    ///
    /// The 'cutoff_horizon' point is used to retain recent versions that might still be
    /// needed by read-only nodes. (As of this writing, the caller just passes
    /// the latest LSN subtracted by a constant, and doesn't do anything smart
    /// to figure out what read-only nodes might actually need.)
    ///
    /// The 'pitr' duration is used to calculate a 'pitr_cutoff', which can be used to determine
    /// whether a record is needed for PITR.
    ///
    /// NOTE: This function holds a short-lived lock to protect the 'gc_info'
    /// field, so that the three values passed as argument are stored
    /// atomically. But the caller is responsible for ensuring that no new
    /// branches are created that would need to be included in 'retain_lsns',
    /// for example. The caller should hold `Tenant::gc_cs` lock to ensure
    /// that.
    ///
    #[instrument(skip_all, fields(timeline_id=%self.timeline_id))]
    pub(super) async fn update_gc_info(
        &self,
        retain_lsns: Vec<Lsn>,
        cutoff_horizon: Lsn,
        pitr: Duration,
        cancel: &CancellationToken,
        ctx: &RequestContext,
    ) -> anyhow::Result<()> {
        // First, calculate pitr_cutoff_timestamp and then convert it to LSN.
        //
        // Some unit tests depend on garbage-collection working even when
        // CLOG data is missing, so that find_lsn_for_timestamp() doesn't
        // work, so avoid calling it altogether if time-based retention is not
        // configured. It would be pointless anyway.
        let pitr_cutoff = if pitr != Duration::ZERO {
            let now = SystemTime::now();
            if let Some(pitr_cutoff_timestamp) = now.checked_sub(pitr) {
                let pitr_timestamp = to_pg_timestamp(pitr_cutoff_timestamp);

                match self
                    .find_lsn_for_timestamp(pitr_timestamp, cancel, ctx)
                    .await?
                {
                    LsnForTimestamp::Present(lsn) => lsn,
                    LsnForTimestamp::Future(lsn) => {
                        // The timestamp is in the future. That sounds impossible,
                        // but what it really means is that there hasn't been
                        // any commits since the cutoff timestamp.
                        //
                        // In this case we should use the LSN of the most recent commit,
                        // which is implicitly the last LSN in the log.
                        debug!("future({})", lsn);
                        self.get_last_record_lsn()
                    }
                    LsnForTimestamp::Past(lsn) => {
                        debug!("past({})", lsn);
                        // conservative, safe default is to remove nothing, when we
                        // have no commit timestamp data available
                        *self.get_latest_gc_cutoff_lsn()
                    }
                    LsnForTimestamp::NoData(lsn) => {
                        debug!("nodata({})", lsn);
                        // conservative, safe default is to remove nothing, when we
                        // have no commit timestamp data available
                        *self.get_latest_gc_cutoff_lsn()
                    }
                }
            } else {
                // If we don't have enough data to convert to LSN,
                // play safe and don't remove any layers.
                *self.get_latest_gc_cutoff_lsn()
            }
        } else {
            // No time-based retention was configured. Set time-based cutoff to
            // same as LSN based.
            cutoff_horizon
        };

        // Grab the lock and update the values
        *self.gc_info.write().unwrap() = GcInfo {
            retain_lsns,
            horizon_cutoff: cutoff_horizon,
            pitr_cutoff,
        };

        Ok(())
    }

    /// Garbage collect layer files on a timeline that are no longer needed.
    ///
    /// Currently, we don't make any attempt at removing unneeded page versions
    /// within a layer file. We can only remove the whole file if it's fully
    /// obsolete.
    pub(super) async fn gc(&self) -> anyhow::Result<GcResult> {
        // this is most likely the background tasks, but it might be the spawned task from
        // immediate_gc
        let cancel = crate::task_mgr::shutdown_token();
        let _g = tokio::select! {
            guard = self.gc_lock.lock() => guard,
            _ = self.cancel.cancelled() => return Ok(GcResult::default()),
            _ = cancel.cancelled() => return Ok(GcResult::default()),
        };
        let timer = self.metrics.garbage_collect_histo.start_timer();

        fail_point!("before-timeline-gc");

        // Is the timeline being deleted?
        if self.is_stopping() {
            anyhow::bail!("timeline is Stopping");
        }

        let (horizon_cutoff, pitr_cutoff, retain_lsns) = {
            let gc_info = self.gc_info.read().unwrap();

            let horizon_cutoff = min(gc_info.horizon_cutoff, self.get_disk_consistent_lsn());
            let pitr_cutoff = gc_info.pitr_cutoff;
            let retain_lsns = gc_info.retain_lsns.clone();
            (horizon_cutoff, pitr_cutoff, retain_lsns)
        };

        let new_gc_cutoff = Lsn::min(horizon_cutoff, pitr_cutoff);

        let res = self
            .gc_timeline(horizon_cutoff, pitr_cutoff, retain_lsns, new_gc_cutoff)
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
        horizon_cutoff: Lsn,
        pitr_cutoff: Lsn,
        retain_lsns: Vec<Lsn>,
        new_gc_cutoff: Lsn,
    ) -> anyhow::Result<GcResult> {
        let now = SystemTime::now();
        let mut result: GcResult = GcResult::default();

        // Nothing to GC. Return early.
        let latest_gc_cutoff = *self.get_latest_gc_cutoff_lsn();
        if latest_gc_cutoff >= new_gc_cutoff {
            info!(
                "Nothing to GC: new_gc_cutoff_lsn {new_gc_cutoff}, latest_gc_cutoff_lsn {latest_gc_cutoff}",
            );
            return Ok(result);
        }

        // We need to ensure that no one tries to read page versions or create
        // branches at a point before latest_gc_cutoff_lsn. See branch_timeline()
        // for details. This will block until the old value is no longer in use.
        //
        // The GC cutoff should only ever move forwards.
        let waitlist = {
            let write_guard = self.latest_gc_cutoff_lsn.lock_for_write();
            ensure!(
                *write_guard <= new_gc_cutoff,
                "Cannot move GC cutoff LSN backwards (was {}, new {})",
                *write_guard,
                new_gc_cutoff
            );
            write_guard.store_and_unlock(new_gc_cutoff)
        };
        waitlist.wait().await;

        info!("GC starting");

        debug!("retain_lsns: {:?}", retain_lsns);

        let mut layers_to_remove = Vec::new();

        // Scan all layers in the timeline (remote or on-disk).
        //
        // Garbage collect the layer if all conditions are satisfied:
        // 1. it is older than cutoff LSN;
        // 2. it is older than PITR interval;
        // 3. it doesn't need to be retained for 'retain_lsns';
        // 4. newer on-disk image layers cover the layer's whole key range
        //
        // TODO holding a write lock is too agressive and avoidable
        let mut guard = self.layers.write().await;
        let layers = guard.layer_map();
        'outer: for l in layers.iter_historic_layers() {
            result.layers_total += 1;

            // 1. Is it newer than GC horizon cutoff point?
            if l.get_lsn_range().end > horizon_cutoff {
                debug!(
                    "keeping {} because it's newer than horizon_cutoff {}",
                    l.filename(),
                    horizon_cutoff,
                );
                result.layers_needed_by_cutoff += 1;
                continue 'outer;
            }

            // 2. It is newer than PiTR cutoff point?
            if l.get_lsn_range().end > pitr_cutoff {
                debug!(
                    "keeping {} because it's newer than pitr_cutoff {}",
                    l.filename(),
                    pitr_cutoff,
                );
                result.layers_needed_by_pitr += 1;
                continue 'outer;
            }

            // 3. Is it needed by a child branch?
            // NOTE With that we would keep data that
            // might be referenced by child branches forever.
            // We can track this in child timeline GC and delete parent layers when
            // they are no longer needed. This might be complicated with long inheritance chains.
            //
            // TODO Vec is not a great choice for `retain_lsns`
            for retain_lsn in &retain_lsns {
                // start_lsn is inclusive
                if &l.get_lsn_range().start <= retain_lsn {
                    debug!(
                        "keeping {} because it's still might be referenced by child branch forked at {} is_dropped: xx is_incremental: {}",
                        l.filename(),
                        retain_lsn,
                        l.is_incremental(),
                    );
                    result.layers_needed_by_branches += 1;
                    continue 'outer;
                }
            }

            // 4. Is there a later on-disk layer for this relation?
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
                debug!("keeping {} because it is the latest layer", l.filename());
                result.layers_not_updated += 1;
                continue 'outer;
            }

            // We didn't find any reason to keep this file, so remove it.
            debug!(
                "garbage collecting {} is_dropped: xx is_incremental: {}",
                l.filename(),
                l.is_incremental(),
            );
            layers_to_remove.push(l);
        }

        if !layers_to_remove.is_empty() {
            // Persist the new GC cutoff value before we actually remove anything.
            // This unconditionally schedules also an index_part.json update, even though, we will
            // be doing one a bit later with the unlinked gc'd layers.
            let disk_consistent_lsn = self.disk_consistent_lsn.load();
            self.schedule_uploads(disk_consistent_lsn, None)?;

            let gc_layers = layers_to_remove
                .iter()
                .map(|x| guard.get_from_desc(x))
                .collect::<Vec<Layer>>();

            result.layers_removed = gc_layers.len() as u64;

            if let Some(remote_client) = self.remote_client.as_ref() {
                remote_client.schedule_gc_update(&gc_layers)?;
            }

            guard.finish_gc_timeline(&gc_layers);

            #[cfg(feature = "testing")]
            {
                result.doomed_layers = gc_layers;
            }
        }

        info!(
            "GC completed removing {} layers, cutoff {}",
            result.layers_removed, new_gc_cutoff
        );

        result.elapsed = now.elapsed()?;
        Ok(result)
    }

    /// Reconstruct a value, using the given base image and WAL records in 'data'.
    async fn reconstruct_value(
        &self,
        key: Key,
        request_lsn: Lsn,
        mut data: ValueReconstructState,
    ) -> Result<Bytes, PageReconstructError> {
        // Perform WAL redo if needed
        data.records.reverse();

        // If we have a page image, and no WAL, we're all set
        if data.records.is_empty() {
            if let Some((img_lsn, img)) = &data.img {
                trace!(
                    "found page image for key {} at {}, no WAL redo required, req LSN {}",
                    key,
                    img_lsn,
                    request_lsn,
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
                    trace!("found {} WAL records that will init the page for {} at {}, performing WAL redo", data.records.len(), key, request_lsn);
                };

                let last_rec_lsn = data.records.last().unwrap().0;

                let img = match self
                    .walredo_mgr
                    .as_ref()
                    .context("timeline has no walredo manager")
                    .map_err(PageReconstructError::WalRedo)?
                    .request_redo(key, request_lsn, data.img, data.records, self.pg_version)
                    .await
                    .context("reconstruct a page image")
                {
                    Ok(img) => img,
                    Err(e) => return Err(PageReconstructError::WalRedo(e)),
                };

                if img.len() == page_cache::PAGE_SZ {
                    let cache = page_cache::get();
                    if let Err(e) = cache
                        .memorize_materialized_page(
                            self.tenant_shard_id,
                            self.timeline_id,
                            key,
                            last_rec_lsn,
                            &img,
                        )
                        .await
                        .context("Materialized page memoization failed")
                    {
                        return Err(PageReconstructError::from(e));
                    }
                }

                Ok(img)
            }
        }
    }

    pub(crate) async fn spawn_download_all_remote_layers(
        self: Arc<Self>,
        request: DownloadRemoteLayersTaskSpawnRequest,
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
        let task_id = task_mgr::spawn(
            task_mgr::BACKGROUND_RUNTIME.handle(),
            task_mgr::TaskKind::DownloadAllRemoteLayers,
            Some(self.tenant_shard_id),
            Some(self.timeline_id),
            "download all remote layers task",
            false,
            async move {
                self_clone.download_all_remote_layers(request).await;
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
    ) {
        use pageserver_api::models::DownloadRemoteLayersTaskState;

        let remaining = {
            let guard = self.layers.read().await;
            guard
                .layer_map()
                .iter_historic_layers()
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

                js.spawn(
                    async move {
                        let res = next.download().await;
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
        let guard = self.layers.read().await;
        let mut max_layer_size: Option<u64> = None;

        let resident_layers = guard
            .likely_resident_layers()
            .map(|layer| {
                let file_size = layer.layer_desc().file_size;
                max_layer_size = max_layer_size.map_or(Some(file_size), |m| Some(m.max(file_size)));

                let last_activity_ts = layer.access_stats().latest_activity_or_now();

                EvictionCandidate {
                    layer: layer.into(),
                    last_activity_ts,
                    relative_last_activity: finite_f32::FiniteF32::ZERO,
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
}

type TraversalPathItem = (
    ValueReconstructResult,
    Lsn,
    Box<dyn Send + FnOnce() -> TraversalId>,
);

/// Helper function for get_reconstruct_data() to add the path of layers traversed
/// to an error, as anyhow context information.
fn layer_traversal_error(msg: String, path: Vec<TraversalPathItem>) -> PageReconstructError {
    // We want the original 'msg' to be the outermost context. The outermost context
    // is the most high-level information, which also gets propagated to the client.
    let mut msg_iter = path
        .into_iter()
        .map(|(r, c, l)| {
            format!(
                "layer traversal: result {:?}, cont_lsn {}, layer: {}",
                r,
                c,
                l(),
            )
        })
        .chain(std::iter::once(msg));
    // Construct initial message from the first traversed layer
    let err = anyhow!(msg_iter.next().unwrap());

    // Append all subsequent traversals, and the error message 'msg', as contexts.
    let msg = msg_iter.fold(err, |err, msg| err.context(msg));
    PageReconstructError::from(msg)
}

struct TimelineWriterState {
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

impl Drop for TimelineWriter<'_> {
    fn drop(&mut self) {
        self.write_guard.take();
    }
}

#[derive(PartialEq)]
enum OpenLayerAction {
    Roll,
    Open,
    None,
}

impl<'a> TimelineWriter<'a> {
    /// Put a new page version that can be constructed from a WAL record
    ///
    /// This will implicitly extend the relation, if the page is beyond the
    /// current end-of-file.
    pub(crate) async fn put(
        &mut self,
        key: Key,
        lsn: Lsn,
        value: &Value,
        ctx: &RequestContext,
    ) -> anyhow::Result<()> {
        // Avoid doing allocations for "small" values.
        // In the regression test suite, the limit of 256 avoided allocations in 95% of cases:
        // https://github.com/neondatabase/neon/pull/5056#discussion_r1301975061
        let mut buf = smallvec::SmallVec::<[u8; 256]>::new();
        value.ser_into(&mut buf)?;
        let buf_size: u64 = buf.len().try_into().expect("oversized value buf");

        let action = self.get_open_layer_action(lsn, buf_size);
        let layer = self.handle_open_layer_action(lsn, action).await?;
        let res = layer.put_value(key, lsn, &buf, ctx).await;

        if res.is_ok() {
            // Update the current size only when the entire write was ok.
            // In case of failures, we may have had partial writes which
            // render the size tracking out of sync. That's ok because
            // the checkpoint distance should be significantly smaller
            // than the S3 single shot upload limit of 5GiB.
            let state = self.write_guard.as_mut().unwrap();

            state.current_size += buf_size;
            state.prev_lsn = Some(lsn);
            state.max_lsn = std::cmp::max(state.max_lsn, Some(lsn));
        }

        res
    }

    async fn handle_open_layer_action(
        &mut self,
        at: Lsn,
        action: OpenLayerAction,
    ) -> anyhow::Result<&Arc<InMemoryLayer>> {
        match action {
            OpenLayerAction::Roll => {
                let freeze_at = self.write_guard.as_ref().unwrap().max_lsn.unwrap();
                self.roll_layer(freeze_at).await?;
                self.open_layer(at).await?;
            }
            OpenLayerAction::Open => self.open_layer(at).await?,
            OpenLayerAction::None => {
                assert!(self.write_guard.is_some());
            }
        }

        Ok(&self.write_guard.as_ref().unwrap().open_layer)
    }

    async fn open_layer(&mut self, at: Lsn) -> anyhow::Result<()> {
        let layer = self.tl.get_layer_for_write(at).await?;
        let initial_size = layer.size().await?;

        let last_freeze_at = self.last_freeze_at.load();
        self.write_guard.replace(TimelineWriterState::new(
            layer,
            initial_size,
            last_freeze_at,
        ));

        Ok(())
    }

    async fn roll_layer(&mut self, freeze_at: Lsn) -> anyhow::Result<()> {
        assert!(self.write_guard.is_some());

        self.tl.freeze_inmem_layer_at(freeze_at).await;

        let now = Instant::now();
        *(self.last_freeze_ts.write().unwrap()) = now;

        self.tl.flush_frozen_layers();

        let current_size = self.write_guard.as_ref().unwrap().current_size;
        if current_size > self.get_checkpoint_distance() {
            warn!("Flushed oversized open layer with size {}", current_size)
        }

        Ok(())
    }

    fn get_open_layer_action(&self, lsn: Lsn, new_value_size: u64) -> OpenLayerAction {
        let state = &*self.write_guard;
        let Some(state) = &state else {
            return OpenLayerAction::Open;
        };

        if state.prev_lsn == Some(lsn) {
            // Rolling mid LSN is not supported by downstream code.
            // Hence, only roll at LSN boundaries.
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
    ///
    /// The batch is sorted by Lsn (enforced by usage of [`utils::vec_map::VecMap`].
    pub(crate) async fn put_batch(
        &mut self,
        batch: VecMap<Lsn, (Key, Value)>,
        ctx: &RequestContext,
    ) -> anyhow::Result<()> {
        for (lsn, (key, val)) in batch {
            self.put(key, lsn, &val, ctx).await?
        }

        Ok(())
    }

    pub(crate) async fn delete_batch(&mut self, batch: &[(Range<Key>, Lsn)]) -> anyhow::Result<()> {
        if let Some((_, lsn)) = batch.first() {
            let action = self.get_open_layer_action(*lsn, 0);
            let layer = self.handle_open_layer_action(*lsn, action).await?;
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

/// Add a suffix to a layer file's name: .{num}.old
/// Uses the first available num (starts at 0)
fn rename_to_backup(path: &Utf8Path) -> anyhow::Result<()> {
    let filename = path
        .file_name()
        .ok_or_else(|| anyhow!("Path {path} don't have a file name"))?;
    let mut new_path = path.to_owned();

    for i in 0u32.. {
        new_path.set_file_name(format!("{filename}.{i}.old"));
        if !new_path.exists() {
            std::fs::rename(path, &new_path)
                .with_context(|| format!("rename {path:?} to {new_path:?}"))?;
            return Ok(());
        }
    }

    bail!("couldn't find an unused backup number for {:?}", path)
}

#[cfg(test)]
mod tests {
    use utils::{id::TimelineId, lsn::Lsn};

    use crate::tenant::{
        harness::TenantHarness, storage_layer::Layer, timeline::EvictionError, Timeline,
    };

    #[tokio::test]
    async fn two_layer_eviction_attempts_at_the_same_time() {
        let harness =
            TenantHarness::create("two_layer_eviction_attempts_at_the_same_time").unwrap();

        let (tenant, ctx) = harness.load().await;
        let timeline = tenant
            .create_test_timeline(TimelineId::generate(), Lsn(0x10), 14, &ctx)
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
        let layers = timeline.layers.read().await;
        let desc = layers
            .layer_map()
            .iter_historic_layers()
            .next()
            .expect("must find one layer to evict");

        layers.get_from_desc(&desc)
    }
}

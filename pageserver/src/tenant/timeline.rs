pub mod delete;
mod eviction_task;
mod init;
pub mod layer_manager;
pub(crate) mod logical_size;
pub mod span;
pub mod uninit;
mod walreceiver;

use anyhow::{anyhow, bail, ensure, Context, Result};
use bytes::Bytes;
use camino::{Utf8Path, Utf8PathBuf};
use enumset::EnumSet;
use fail::fail_point;
use itertools::Itertools;
use pageserver_api::{
    models::{
        DownloadRemoteLayersTaskInfo, DownloadRemoteLayersTaskSpawnRequest, LayerMapInfo,
        TimelineState,
    },
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
use utils::sync::gate::Gate;

use std::collections::{BinaryHeap, HashMap, HashSet};
use std::ops::{Deref, Range};
use std::pin::pin;
use std::sync::atomic::Ordering as AtomicOrdering;
use std::sync::{Arc, Mutex, RwLock, Weak};
use std::time::{Duration, Instant, SystemTime};
use std::{
    cmp::{max, min, Ordering},
    ops::ControlFlow,
};

use crate::context::{
    AccessStatsBehavior, DownloadBehavior, RequestContext, RequestContextBuilder,
};
use crate::tenant::storage_layer::delta_layer::DeltaEntry;
use crate::tenant::storage_layer::{
    AsLayerDesc, DeltaLayerWriter, EvictionError, ImageLayerWriter, InMemoryLayer, Layer,
    LayerAccessStatsReset, LayerFileName, ResidentLayer, ValueReconstructResult,
    ValueReconstructState,
};
use crate::tenant::tasks::{BackgroundLoopKind, RateLimitError};
use crate::tenant::timeline::logical_size::CurrentLogicalSize;
use crate::tenant::{
    layer_map::{LayerMap, SearchResult},
    metadata::{save_metadata, TimelineMetadata},
    par_fsync,
};
use crate::{deletion_queue::DeletionQueueClient, tenant::remote_timeline_client::StopError};

use crate::config::PageServerConf;
use crate::keyspace::{KeyPartitioning, KeySpace, KeySpaceRandomAccum};
use crate::metrics::{
    TimelineMetrics, MATERIALIZED_PAGE_CACHE_HIT, MATERIALIZED_PAGE_CACHE_HIT_DIRECT,
};
use crate::pgdatadir_mapping::LsnForTimestamp;
use crate::pgdatadir_mapping::{is_inherited_key, is_rel_fsm_block_key, is_rel_vm_block_key};
use crate::pgdatadir_mapping::{BlockNumber, CalculateLogicalSizeError};
use crate::tenant::config::{EvictionPolicy, TenantConfOpt};
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
use super::remote_timeline_client::index::{IndexLayerMetadata, IndexPart};
use super::remote_timeline_client::RemoteTimelineClient;
use super::secondary::heatmap::{HeatMapLayer, HeatMapTimeline};
use super::{debug_assert_current_span_has_tenant_and_timeline_id, AttachedTenantConf};

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
pub struct Hole {
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
}

pub struct Timeline {
    conf: &'static PageServerConf,
    tenant_conf: Arc<RwLock<AttachedTenantConf>>,

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

    /// Set of key ranges which should be covered by image layers to
    /// allow GC to remove old layers. This set is created by GC and its cutoff LSN is also stored.
    /// It is used by compaction task when it checks if new image layer should be created.
    /// Newly created image layer doesn't help to remove the delta layer, until the
    /// newly created image layer falls off the PITR horizon. So on next GC cycle,
    /// gc_timeline may still want the new image layer to be created. To avoid redundant
    /// image layers creation we should check if image layer exists but beyond PITR horizon.
    /// This is why we need remember GC cutoff LSN.
    ///
    wanted_image_layers: Mutex<Option<(Lsn, KeySpace)>>,

    last_freeze_at: AtomicLsn,
    // Atomic would be more appropriate here.
    last_freeze_ts: RwLock<Instant>,

    // WAL redo manager
    walredo_mgr: Arc<super::WalRedoManager>,

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

    /// Ensures layers aren't frozen by checkpointer between
    /// [`Timeline::get_layer_for_write`] and layer reads.
    /// Locked automatically by [`TimelineWriter`] and checkpointer.
    /// Must always be acquired before the layer map/individual layer lock
    /// to avoid deadlock.
    write_lock: tokio::sync::Mutex<()>,

    /// Used to avoid multiple `flush_loop` tasks running
    pub(super) flush_loop_state: Mutex<FlushLoopState>,

    /// layer_flush_start_tx can be used to wake up the layer-flushing task.
    /// The value is a counter, incremented every time a new flush cycle is requested.
    /// The flush cycle counter is sent back on the layer_flush_done channel when
    /// the flush finishes. You can use that to wait for the flush to finish.
    layer_flush_start_tx: tokio::sync::watch::Sender<u64>,
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
    partitioning: Mutex<(KeyPartitioning, Lsn)>,

    /// Configuration: how often should the partitioning be recalculated.
    repartition_threshold: u64,

    /// Current logical size of the "datadir", at the last LSN.
    current_logical_size: LogicalSize,

    /// Information about the last processed message by the WAL receiver,
    /// or None if WAL receiver has not received anything for this timeline
    /// yet.
    pub last_received_wal: Mutex<Option<WalReceiverInfo>>,
    pub walreceiver: Mutex<Option<WalReceiver>>,

    /// Relation size cache
    pub rel_size_cache: RwLock<HashMap<RelTag, (Lsn, BlockNumber)>>,

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
    /// - [`delete::delete_local_layer_files`]
    ///
    /// Timeline deletion will acquire both compaction and gc locks in whatever order.
    compaction_lock: tokio::sync::Mutex<()>,

    /// Make sure we only have one running gc at a time.
    ///
    /// Must only be taken in two places:
    /// - [`Timeline::gc`] (this file)
    /// - [`delete::delete_local_layer_files`]
    ///
    /// Timeline deletion will acquire both compaction and gc locks in whatever order.
    gc_lock: tokio::sync::Mutex<()>,
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
#[derive(thiserror::Error)]
pub enum PageReconstructError {
    #[error(transparent)]
    Other(#[from] anyhow::Error),

    /// The operation was cancelled
    Cancelled,

    /// The ancestor of this is being stopped
    AncestorStopping(TimelineId),

    /// An error happened replaying WAL records
    #[error(transparent)]
    WalRedo(anyhow::Error),
}

#[derive(thiserror::Error, Debug)]
enum FlushLayerError {
    /// Timeline cancellation token was cancelled
    #[error("timeline shutting down")]
    Cancelled,

    #[error(transparent)]
    PageReconstructError(#[from] PageReconstructError),

    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

impl std::fmt::Debug for PageReconstructError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        match self {
            Self::Other(err) => err.fmt(f),
            Self::Cancelled => write!(f, "cancelled"),
            Self::AncestorStopping(timeline_id) => {
                write!(f, "ancestor timeline {timeline_id} is being stopped")
            }
            Self::WalRedo(err) => err.fmt(f),
        }
    }
}

impl std::fmt::Display for PageReconstructError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        match self {
            Self::Other(err) => err.fmt(f),
            Self::Cancelled => write!(f, "cancelled"),
            Self::AncestorStopping(timeline_id) => {
                write!(f, "ancestor timeline {timeline_id} is being stopped")
            }
            Self::WalRedo(err) => err.fmt(f),
        }
    }
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
}

/// Public interface functions
impl Timeline {
    /// Get the LSN where this branch was created
    pub fn get_ancestor_lsn(&self) -> Lsn {
        self.ancestor_lsn
    }

    /// Get the ancestor's timeline id
    pub fn get_ancestor_timeline_id(&self) -> Option<TimelineId> {
        self.ancestor_timeline
            .as_ref()
            .map(|ancestor| ancestor.timeline_id)
    }

    /// Lock and get timeline's GC cutoff
    pub fn get_latest_gc_cutoff_lsn(&self) -> RcuReadGuard<Lsn> {
        self.latest_gc_cutoff_lsn.read()
    }

    /// Look up given page version.
    ///
    /// If a remote layer file is needed, it is downloaded as part of this
    /// call.
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
    pub async fn get(
        &self,
        key: Key,
        lsn: Lsn,
        ctx: &RequestContext,
    ) -> Result<Bytes, PageReconstructError> {
        if !lsn.is_valid() {
            return Err(PageReconstructError::Other(anyhow::anyhow!("Invalid LSN")));
        }

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

    /// Get last or prev record separately. Same as get_last_record_rlsn().last/prev.
    pub fn get_last_record_lsn(&self) -> Lsn {
        self.last_record_lsn.load().last
    }

    pub fn get_prev_record_lsn(&self) -> Lsn {
        self.last_record_lsn.load().prev
    }

    /// Atomically get both last and prev.
    pub fn get_last_record_rlsn(&self) -> RecordLsn {
        self.last_record_lsn.load()
    }

    pub fn get_disk_consistent_lsn(&self) -> Lsn {
        self.disk_consistent_lsn.load()
    }

    /// remote_consistent_lsn from the perspective of the tenant's current generation,
    /// not validated with control plane yet.
    /// See [`Self::get_remote_consistent_lsn_visible`].
    pub fn get_remote_consistent_lsn_projected(&self) -> Option<Lsn> {
        if let Some(remote_client) = &self.remote_client {
            remote_client.remote_consistent_lsn_projected()
        } else {
            None
        }
    }

    /// remote_consistent_lsn which the tenant is guaranteed not to go backward from,
    /// i.e. a value of remote_consistent_lsn_projected which has undergone
    /// generation validation in the deletion queue.
    pub fn get_remote_consistent_lsn_visible(&self) -> Option<Lsn> {
        if let Some(remote_client) = &self.remote_client {
            remote_client.remote_consistent_lsn_visible()
        } else {
            None
        }
    }

    /// The sum of the file size of all historic layers in the layer map.
    /// This method makes no distinction between local and remote layers.
    /// Hence, the result **does not represent local filesystem usage**.
    pub async fn layer_size_sum(&self) -> u64 {
        let guard = self.layers.read().await;
        let layer_map = guard.layer_map();
        let mut size = 0;
        for l in layer_map.iter_historic_layers() {
            size += l.file_size();
        }
        size
    }

    pub fn resident_physical_size(&self) -> u64 {
        self.metrics.resident_physical_size_get()
    }

    ///
    /// Wait until WAL has been received and processed up to this LSN.
    ///
    /// You should call this before any of the other get_* or list_* functions. Calling
    /// those functions with an LSN that has been processed yet is an error.
    ///
    pub async fn wait_lsn(
        &self,
        lsn: Lsn,
        _ctx: &RequestContext, /* Prepare for use by cancellation */
    ) -> anyhow::Result<()> {
        anyhow::ensure!(self.is_active(), "Cannot wait for Lsn on inactive timeline");

        // This should never be called from the WAL receiver, because that could lead
        // to a deadlock.
        anyhow::ensure!(
            task_mgr::current_task_kind() != Some(TaskKind::WalReceiverManager),
            "wait_lsn cannot be called in WAL receiver"
        );
        anyhow::ensure!(
            task_mgr::current_task_kind() != Some(TaskKind::WalReceiverConnectionHandler),
            "wait_lsn cannot be called in WAL receiver"
        );
        anyhow::ensure!(
            task_mgr::current_task_kind() != Some(TaskKind::WalReceiverConnectionPoller),
            "wait_lsn cannot be called in WAL receiver"
        );

        let _timer = crate::metrics::WAIT_LSN_TIME.start_timer();

        match self
            .last_record_lsn
            .wait_for_timeout(lsn, self.conf.wait_lsn_timeout)
            .await
        {
            Ok(()) => Ok(()),
            Err(e) => {
                // don't count the time spent waiting for lock below, and also in walreceiver.status(), towards the wait_lsn_time_histo
                drop(_timer);
                let walreceiver_status = self.walreceiver_status();
                Err(anyhow::Error::new(e).context({
                    format!(
                        "Timed out while waiting for WAL record at LSN {} to arrive, last_record_lsn {} disk consistent LSN={}, WalReceiver status: {}",
                        lsn,
                        self.get_last_record_lsn(),
                        self.get_disk_consistent_lsn(),
                        walreceiver_status,
                    )
                }))
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
    pub fn check_lsn_is_in_scope(
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
    pub async fn freeze_and_flush(&self) -> anyhow::Result<()> {
        self.freeze_inmem_layer(false).await;
        self.flush_frozen_layers_and_wait().await
    }

    /// Outermost timeline compaction operation; downloads needed layers.
    pub(crate) async fn compact(
        self: &Arc<Self>,
        cancel: &CancellationToken,
        flags: EnumSet<CompactFlags>,
        ctx: &RequestContext,
    ) -> Result<(), CompactionError> {
        let _g = self.compaction_lock.lock().await;

        // this wait probably never needs any "long time spent" logging, because we already nag if
        // compaction task goes over it's period (20s) which is quite often in production.
        let _permit = match super::tasks::concurrent_background_tasks_rate_limit(
            BackgroundLoopKind::Compaction,
            ctx,
            cancel,
        )
        .await
        {
            Ok(permit) => permit,
            Err(RateLimitError::Cancelled) => return Ok(()),
        };

        let last_record_lsn = self.get_last_record_lsn();

        // Last record Lsn could be zero in case the timeline was just created
        if !last_record_lsn.is_valid() {
            warn!("Skipping compaction for potentially just initialized timeline, it has invalid last record lsn: {last_record_lsn}");
            return Ok(());
        }

        // High level strategy for compaction / image creation:
        //
        // 1. First, calculate the desired "partitioning" of the
        // currently in-use key space. The goal is to partition the
        // key space into roughly fixed-size chunks, but also take into
        // account any existing image layers, and try to align the
        // chunk boundaries with the existing image layers to avoid
        // too much churn. Also try to align chunk boundaries with
        // relation boundaries.  In principle, we don't know about
        // relation boundaries here, we just deal with key-value
        // pairs, and the code in pgdatadir_mapping.rs knows how to
        // map relations into key-value pairs. But in practice we know
        // that 'field6' is the block number, and the fields 1-5
        // identify a relation. This is just an optimization,
        // though.
        //
        // 2. Once we know the partitioning, for each partition,
        // decide if it's time to create a new image layer. The
        // criteria is: there has been too much "churn" since the last
        // image layer? The "churn" is fuzzy concept, it's a
        // combination of too many delta files, or too much WAL in
        // total in the delta file. Or perhaps: if creating an image
        // file would allow to delete some older files.
        //
        // 3. After that, we compact all level0 delta files if there
        // are too many of them.  While compacting, we also garbage
        // collect any page versions that are no longer needed because
        // of the new image layers we created in step 2.
        //
        // TODO: This high level strategy hasn't been implemented yet.
        // Below are functions compact_level0() and create_image_layers()
        // but they are a bit ad hoc and don't quite work like it's explained
        // above. Rewrite it.

        // Is the timeline being deleted?
        if self.is_stopping() {
            trace!("Dropping out of compaction on timeline shutdown");
            return Err(CompactionError::ShuttingDown);
        }

        let target_file_size = self.get_checkpoint_distance();

        // Define partitioning schema if needed

        // FIXME: the match should only cover repartitioning, not the next steps
        match self
            .repartition(
                self.get_last_record_lsn(),
                self.get_compaction_target_size(),
                flags,
                ctx,
            )
            .await
        {
            Ok((partitioning, lsn)) => {
                // Disables access_stats updates, so that the files we read remain candidates for eviction after we're done with them
                let image_ctx = RequestContextBuilder::extend(ctx)
                    .access_stats_behavior(AccessStatsBehavior::Skip)
                    .build();

                // 2. Compact
                let timer = self.metrics.compact_time_histo.start_timer();
                self.compact_level0(target_file_size, ctx).await?;
                timer.stop_and_record();

                // 3. Create new image layers for partitions that have been modified
                // "enough".
                let layers = self
                    .create_image_layers(&partitioning, lsn, false, &image_ctx)
                    .await
                    .map_err(anyhow::Error::from)?;
                if let Some(remote_client) = &self.remote_client {
                    for layer in layers {
                        remote_client.schedule_layer_file_upload(layer)?;
                    }
                }

                if let Some(remote_client) = &self.remote_client {
                    // should any new image layer been created, not uploading index_part will
                    // result in a mismatch between remote_physical_size and layermap calculated
                    // size, which will fail some tests, but should not be an issue otherwise.
                    remote_client.schedule_index_upload_for_file_changes()?;
                }
            }
            Err(err) => {
                // no partitioning? This is normal, if the timeline was just created
                // as an empty timeline. Also in unit tests, when we use the timeline
                // as a simple key-value store, ignoring the datadir layout. Log the
                // error but continue.
                //
                // Suppress error when it's due to cancellation
                if !self.cancel.is_cancelled() {
                    error!("could not compact, repartitioning keyspace failed: {err:?}");
                }
            }
        };

        Ok(())
    }

    /// Mutate the timeline with a [`TimelineWriter`].
    pub async fn writer(&self) -> TimelineWriter<'_> {
        TimelineWriter {
            tl: self,
            _write_guard: self.write_lock.lock().await,
        }
    }

    /// Check if more than 'checkpoint_distance' of WAL has been accumulated in
    /// the in-memory layer, and initiate flushing it if so.
    ///
    /// Also flush after a period of time without new data -- it helps
    /// safekeepers to regard pageserver as caught up and suspend activity.
    pub async fn check_checkpoint_distance(self: &Arc<Timeline>) -> anyhow::Result<()> {
        let last_lsn = self.get_last_record_lsn();
        let open_layer_size = {
            let guard = self.layers.read().await;
            let layers = guard.layer_map();
            let Some(open_layer) = layers.open_layer.as_ref() else {
                return Ok(());
            };
            open_layer.size().await?
        };
        let last_freeze_at = self.last_freeze_at.load();
        let last_freeze_ts = *(self.last_freeze_ts.read().unwrap());
        let distance = last_lsn.widening_sub(last_freeze_at);
        // Checkpointing the open layer can be triggered by layer size or LSN range.
        // S3 has a 5 GB limit on the size of one upload (without multi-part upload), and
        // we want to stay below that with a big margin.  The LSN distance determines how
        // much WAL the safekeepers need to store.
        if distance >= self.get_checkpoint_distance().into()
            || open_layer_size > self.get_checkpoint_distance()
            || (distance > 0 && last_freeze_ts.elapsed() >= self.get_checkpoint_timeout())
        {
            info!(
                "check_checkpoint_distance {}, layer size {}, elapsed since last flush {:?}",
                distance,
                open_layer_size,
                last_freeze_ts.elapsed()
            );

            self.freeze_inmem_layer(true).await;
            self.last_freeze_at.store(last_lsn);
            *(self.last_freeze_ts.write().unwrap()) = Instant::now();

            // Wake up the layer flusher
            self.flush_frozen_layers();
        }
        Ok(())
    }

    pub fn activate(
        self: &Arc<Self>,
        broker_client: BrokerClientChannel,
        background_jobs_can_start: Option<&completion::Barrier>,
        ctx: &RequestContext,
    ) {
        self.spawn_initial_logical_size_computation_task(ctx);
        self.launch_wal_receiver(ctx, broker_client);
        self.set_state(TimelineState::Active);
        self.launch_eviction_task(background_jobs_can_start);
    }

    /// Graceful shutdown, may do a lot of I/O as we flush any open layers to disk and then
    /// also to remote storage.  This method can easily take multiple seconds for a busy timeline.
    ///
    /// While we are flushing, we continue to accept read I/O.
    #[instrument(skip_all, fields(timeline_id=%self.timeline_id))]
    pub(crate) async fn flush_and_shutdown(&self) {
        debug_assert_current_span_has_tenant_and_timeline_id();

        // Stop ingesting data, so that we are not still writing to an InMemoryLayer while
        // trying to flush
        tracing::debug!("Waiting for WalReceiverManager...");
        task_mgr::shutdown_tasks(
            Some(TaskKind::WalReceiverManager),
            Some(self.tenant_shard_id),
            Some(self.timeline_id),
        )
        .await;

        // Since we have shut down WAL ingest, we should not let anyone start waiting for the LSN to advance
        self.last_record_lsn.shutdown();

        // now all writers to InMemory layer are gone, do the final flush if requested
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
                    if let Err(e) = client.shutdown().await {
                        // Non-fatal.  Shutdown is infallible.  Failures to flush just mean that
                        // we have some extra WAL replay to do next time the timeline starts.
                        warn!("failed to flush to remote storage: {e:#}");
                    }
                }
            }
            Err(e) => {
                // Non-fatal.  Shutdown is infallible.  Failures to flush just mean that
                // we have some extra WAL replay to do next time the timeline starts.
                warn!("failed to freeze and flush: {e:#}");
            }
        }

        self.shutdown().await;
    }

    /// Shut down immediately, without waiting for any open layers to flush to disk.  This is a subset of
    /// the graceful [`Timeline::flush_and_shutdown`] function.
    pub(crate) async fn shutdown(&self) {
        // Signal any subscribers to our cancellation token to drop out
        tracing::debug!("Cancelling CancellationToken");
        self.cancel.cancel();

        // Page request handlers might be waiting for LSN to advance: they do not respect Timeline::cancel
        // while doing so.
        self.last_record_lsn.shutdown();

        // Shut down the layer flush task before the remote client, as one depends on the other
        task_mgr::shutdown_tasks(
            Some(TaskKind::LayerFlushTask),
            Some(self.tenant_shard_id),
            Some(self.timeline_id),
        )
        .await;

        // Shut down remote timeline client: this gracefully moves its metadata into its Stopping state in
        // case our caller wants to use that for a deletion
        if let Some(remote_client) = self.remote_client.as_ref() {
            match remote_client.stop() {
                Ok(()) => {}
                Err(StopError::QueueUninitialized) => {
                    // Shutting down during initialization is legal
                }
            }
        }

        tracing::debug!("Waiting for tasks...");

        task_mgr::shutdown_tasks(None, Some(self.tenant_shard_id), Some(self.timeline_id)).await;

        // Finally wait until any gate-holders are complete
        self.gate.close().await;
    }

    pub fn set_state(&self, new_state: TimelineState) {
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

    pub fn set_broken(&self, reason: String) {
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

    pub fn current_state(&self) -> TimelineState {
        self.state.borrow().clone()
    }

    pub fn is_broken(&self) -> bool {
        matches!(&*self.state.borrow(), TimelineState::Broken { .. })
    }

    pub fn is_active(&self) -> bool {
        self.current_state() == TimelineState::Active
    }

    pub fn is_stopping(&self) -> bool {
        self.current_state() == TimelineState::Stopping
    }

    pub fn subscribe_for_state_updates(&self) -> watch::Receiver<TimelineState> {
        self.state.subscribe()
    }

    pub async fn wait_to_become_active(
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

    pub async fn layer_map_info(&self, reset: LayerAccessStatsReset) -> LayerMapInfo {
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
    pub async fn download_layer(&self, layer_file_name: &str) -> anyhow::Result<Option<bool>> {
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
    pub async fn evict_layer(&self, layer_file_name: &str) -> anyhow::Result<Option<bool>> {
        let _gate = self
            .gate
            .enter()
            .map_err(|_| anyhow::anyhow!("Shutting down"))?;

        let Some(local_layer) = self.find_layer(layer_file_name).await else {
            return Ok(None);
        };

        let rtc = self
            .remote_client
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("remote storage not configured; cannot evict"))?;

        match local_layer.evict_and_wait(rtc).await {
            Ok(()) => Ok(Some(true)),
            Err(EvictionError::NotFound) => Ok(Some(false)),
            Err(EvictionError::Downloaded) => Ok(Some(false)),
        }
    }
}

/// Number of times we will compute partition within a checkpoint distance.
const REPARTITION_FREQ_IN_CHECKPOINT_DISTANCE: u64 = 10;

// Private functions
impl Timeline {
    fn get_checkpoint_distance(&self) -> u64 {
        let tenant_conf = self.tenant_conf.read().unwrap().tenant_conf;
        tenant_conf
            .checkpoint_distance
            .unwrap_or(self.conf.default_tenant_conf.checkpoint_distance)
    }

    fn get_checkpoint_timeout(&self) -> Duration {
        let tenant_conf = self.tenant_conf.read().unwrap().tenant_conf;
        tenant_conf
            .checkpoint_timeout
            .unwrap_or(self.conf.default_tenant_conf.checkpoint_timeout)
    }

    fn get_compaction_target_size(&self) -> u64 {
        let tenant_conf = self.tenant_conf.read().unwrap().tenant_conf;
        tenant_conf
            .compaction_target_size
            .unwrap_or(self.conf.default_tenant_conf.compaction_target_size)
    }

    fn get_compaction_threshold(&self) -> usize {
        let tenant_conf = self.tenant_conf.read().unwrap().tenant_conf;
        tenant_conf
            .compaction_threshold
            .unwrap_or(self.conf.default_tenant_conf.compaction_threshold)
    }

    fn get_image_creation_threshold(&self) -> usize {
        let tenant_conf = self.tenant_conf.read().unwrap().tenant_conf;
        tenant_conf
            .image_creation_threshold
            .unwrap_or(self.conf.default_tenant_conf.image_creation_threshold)
    }

    fn get_eviction_policy(&self) -> EvictionPolicy {
        let tenant_conf = self.tenant_conf.read().unwrap().tenant_conf;
        tenant_conf
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

    fn get_gc_feedback(&self) -> bool {
        let tenant_conf = &self.tenant_conf.read().unwrap().tenant_conf;
        tenant_conf
            .gc_feedback
            .unwrap_or(self.conf.default_tenant_conf.gc_feedback)
    }

    pub(super) fn tenant_conf_updated(&self) {
        // NB: Most tenant conf options are read by background loops, so,
        // changes will automatically be picked up.

        // The threshold is embedded in the metric. So, we need to update it.
        {
            let new_threshold = Self::get_evictions_low_residence_duration_metric_threshold(
                &self.tenant_conf.read().unwrap().tenant_conf,
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
        tenant_conf: Arc<RwLock<AttachedTenantConf>>,
        metadata: &TimelineMetadata,
        ancestor: Option<Arc<Timeline>>,
        timeline_id: TimelineId,
        tenant_shard_id: TenantShardId,
        generation: Generation,
        shard_identity: ShardIdentity,
        walredo_mgr: Arc<super::WalRedoManager>,
        resources: TimelineResources,
        pg_version: u32,
        state: TimelineState,
        cancel: CancellationToken,
    ) -> Arc<Self> {
        let disk_consistent_lsn = metadata.disk_consistent_lsn();
        let (state, _) = watch::channel(state);

        let (layer_flush_start_tx, _) = tokio::sync::watch::channel(0);
        let (layer_flush_done_tx, _) = tokio::sync::watch::channel((0, Ok(())));

        let tenant_conf_guard = tenant_conf.read().unwrap();

        let evictions_low_residence_duration_metric_threshold =
            Self::get_evictions_low_residence_duration_metric_threshold(
                &tenant_conf_guard.tenant_conf,
                &conf.default_tenant_conf,
            );
        drop(tenant_conf_guard);

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
                layers: Arc::new(tokio::sync::RwLock::new(LayerManager::create())),
                wanted_image_layers: Mutex::new(None),

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

                flush_loop_state: Mutex::new(FlushLoopState::NotStarted),

                layer_flush_start_tx,
                layer_flush_done_tx,

                write_lock: tokio::sync::Mutex::new(()),

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
                partitioning: Mutex::new((KeyPartitioning::new(), Lsn(0))),
                repartition_threshold: 0,

                last_received_wal: Mutex::new(None),
                rel_size_cache: RwLock::new(HashMap::new()),

                download_all_remote_layers_task_info: RwLock::new(None),

                state,

                eviction_task_timeline_state: tokio::sync::Mutex::new(
                    EvictionTaskTimelineState::default(),
                ),
                delete_progress: Arc::new(tokio::sync::Mutex::new(DeleteTimelineFlow::default())),

                cancel,
                gate: Gate::new(format!("Timeline<{tenant_shard_id}/{timeline_id}>")),

                compaction_lock: tokio::sync::Mutex::default(),
                gc_lock: tokio::sync::Mutex::default(),
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

        let tenant_conf_guard = self.tenant_conf.read().unwrap();
        let wal_connect_timeout = tenant_conf_guard
            .tenant_conf
            .walreceiver_connect_timeout
            .unwrap_or(self.conf.default_tenant_conf.walreceiver_connect_timeout);
        let lagging_wal_timeout = tenant_conf_guard
            .tenant_conf
            .lagging_wal_timeout
            .unwrap_or(self.conf.default_tenant_conf.lagging_wal_timeout);
        let max_lsn_wal_lag = tenant_conf_guard
            .tenant_conf
            .max_lsn_wal_lag
            .unwrap_or(self.conf.default_tenant_conf.max_lsn_wal_lag);
        drop(tenant_conf_guard);

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
                        Discovered::Metadata | Discovered::IgnoredBackup => {
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
            .instrument(info_span!(parent: None, "initial_size_calculation", tenant_id=%self.tenant_shard_id.tenant_id, timeline_id=%self.timeline_id)),
        );
    }

    async fn initial_logical_size_calculation_task(
        self: Arc<Self>,
        initial_part_end: Lsn,
        skip_concurrency_limiter: CancellationToken,
        cancel: CancellationToken,
        background_ctx: RequestContext,
    ) {
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
                let wait_for_permit = super::tasks::concurrent_background_tasks_rate_limit(
                    BackgroundLoopKind::InitialLogicalSizeCalculation,
                    background_ctx,
                    &cancel,
                );

                use crate::metrics::initial_logical_size::StartCircumstances;
                let (_maybe_permit, circumstances) = tokio::select! {
                    res = wait_for_permit => {
                        match res {
                            Ok(permit) => (Some(permit), StartCircumstances::AfterBackgroundTasksRateLimit),
                            Err(RateLimitError::Cancelled) => {
                                return Err(BackgroundCalculationError::Cancelled);
                            }
                        }
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

    pub fn spawn_ondemand_logical_size_calculation(
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
        span::debug_assert_current_span_has_tenant_and_timeline_id();

        let _guard = self.gate.enter();

        let self_calculation = Arc::clone(self);

        let mut calculation = pin!(async {
            let ctx = ctx.attached_child();
            self_calculation
                .calculate_logical_size(lsn, cause, &ctx)
                .await
        });

        tokio::select! {
            res = &mut calculation => { res }
            _ = self.cancel.cancelled() => {
                debug!("cancelling logical size calculation for timeline shutdown");
                calculation.await
            }
            _ = task_mgr::shutdown_watcher() => {
                debug!("cancelling logical size calculation for task shutdown");
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
    pub async fn calculate_logical_size(
        &self,
        up_to_lsn: Lsn,
        cause: LogicalSizeCalculationCause,
        ctx: &RequestContext,
    ) -> Result<u64, CalculateLogicalSizeError> {
        info!(
            "Calculating logical size for timeline {} at {}",
            self.timeline_id, up_to_lsn
        );
        // These failpoints are used by python tests to ensure that we don't delete
        // the timeline while the logical size computation is ongoing.
        // The first failpoint is used to make this function pause.
        // Then the python test initiates timeline delete operation in a thread.
        // It waits for a few seconds, then arms the second failpoint and disables
        // the first failpoint. The second failpoint prints an error if the timeline
        // delete code has deleted the on-disk state while we're still running here.
        // It shouldn't do that. If it does it anyway, the error will be caught
        // by the test suite, highlighting the problem.
        fail::fail_point!("timeline-calculate-logical-size-pause");
        fail::fail_point!("timeline-calculate-logical-size-check-dir-exists", |_| {
            if !self
                .conf
                .metadata_path(&self.tenant_shard_id, &self.timeline_id)
                .exists()
            {
                error!("timeline-calculate-logical-size-pre metadata file does not exist")
            }
            // need to return something
            Ok(0)
        });
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
        let eviction_info = self.get_local_layers_for_disk_usage_eviction().await;

        let remote_client = match &self.remote_client {
            Some(c) => c,
            None => return None,
        };

        let layer_file_names = eviction_info
            .resident_layers
            .iter()
            .map(|l| l.layer.layer_desc().filename())
            .collect::<Vec<_>>();

        let decorated = match remote_client.get_layers_metadata(layer_file_names) {
            Ok(d) => d,
            Err(_) => {
                // Getting metadata only fails on Timeline in bad state.
                return None;
            }
        };

        let heatmap_layers = std::iter::zip(
            eviction_info.resident_layers.into_iter(),
            decorated.into_iter(),
        )
        .filter_map(|(layer, remote_info)| {
            remote_info.map(|remote_info| {
                HeatMapLayer::new(
                    layer.layer.layer_desc().filename(),
                    IndexLayerMetadata::from(remote_info),
                    layer.last_activity_ts,
                )
            })
        });

        Some(HeatMapTimeline::new(
            self.timeline_id,
            heatmap_layers.collect(),
        ))
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
        let mut prev_lsn = Lsn(u64::MAX);

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
                    if prev_lsn <= cont_lsn {
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
                    prev_lsn = cont_lsn;
                }
                ValueReconstructResult::Missing => {
                    return Err(layer_traversal_error(
                        if cfg!(test) {
                            format!(
                                "could not find data for key {} at LSN {}, for request at LSN {}\n{}",
                                key, cont_lsn, request_lsn, std::backtrace::Backtrace::force_capture(),
                            )
                        } else {
                            format!(
                                "could not find data for key {} at LSN {}, for request at LSN {}",
                                key, cont_lsn, request_lsn
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
                let ancestor = match timeline.get_ancestor_timeline() {
                    Ok(timeline) => timeline,
                    Err(e) => return Err(PageReconstructError::from(e)),
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
                        return Err(PageReconstructError::AncestorStopping(ancestor.timeline_id));
                    }
                    Err(state) => {
                        return Err(PageReconstructError::Other(anyhow::anyhow!(
                            "Timeline {} will not become active. Current state: {:?}",
                            ancestor.timeline_id,
                            &state,
                        )));
                    }
                }
                ancestor
                    .wait_lsn(timeline.ancestor_lsn, ctx)
                    .await
                    .with_context(|| {
                        format!(
                            "wait for lsn {} on ancestor timeline_id={}",
                            timeline.ancestor_lsn, ancestor.timeline_id
                        )
                    })?;

                timeline_owned = ancestor;
                timeline = &*timeline_owned;
                prev_lsn = Lsn(u64::MAX);
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
                        Box::new({
                            let open_layer = Arc::clone(open_layer);
                            move || open_layer.traversal_id()
                        }),
                    ));
                    continue 'outer;
                }
            }
            for frozen_layer in layers.frozen_layers.iter().rev() {
                let start_lsn = frozen_layer.get_lsn_range().start;
                if cont_lsn > start_lsn {
                    //info!("CHECKING for {} at {} on frozen layer {}", key, cont_lsn, frozen_layer.filename().display());
                    let lsn_floor = max(cached_lsn + 1, start_lsn);
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
                        Box::new({
                            let frozen_layer = Arc::clone(frozen_layer);
                            move || frozen_layer.traversal_id()
                        }),
                    ));
                    continue 'outer;
                }
            }

            if let Some(SearchResult { lsn_floor, layer }) = layers.search(key, cont_lsn) {
                let layer = guard.get_from_desc(&layer);
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

    async fn put_value(
        &self,
        key: Key,
        lsn: Lsn,
        val: &Value,
        ctx: &RequestContext,
    ) -> anyhow::Result<()> {
        //info!("PUT: key {} at {}", key, lsn);
        let layer = self.get_layer_for_write(lsn).await?;
        layer.put_value(key, lsn, val, ctx).await?;
        Ok(())
    }

    async fn put_values(
        &self,
        values: &HashMap<Key, Vec<(Lsn, Value)>>,
        ctx: &RequestContext,
    ) -> anyhow::Result<()> {
        // Pick the first LSN in the batch to get the layer to write to.
        for lsns in values.values() {
            if let Some((lsn, _)) = lsns.first() {
                let layer = self.get_layer_for_write(*lsn).await?;
                layer.put_values(values, ctx).await?;
                break;
            }
        }
        Ok(())
    }

    async fn put_tombstones(&self, tombstones: &[(Range<Key>, Lsn)]) -> anyhow::Result<()> {
        if let Some((_, lsn)) = tombstones.first() {
            let layer = self.get_layer_for_write(*lsn).await?;
            layer.put_tombstones(tombstones).await?;
        }
        Ok(())
    }

    pub(crate) fn finish_write(&self, new_lsn: Lsn) {
        assert!(new_lsn.is_aligned());

        self.metrics.last_record_gauge.set(new_lsn.0 as i64);
        self.last_record_lsn.advance(new_lsn);
    }

    async fn freeze_inmem_layer(&self, write_lock_held: bool) {
        // Freeze the current open in-memory layer. It will be written to disk on next
        // iteration.
        let _write_guard = if write_lock_held {
            None
        } else {
            Some(self.write_lock.lock().await)
        };
        let mut guard = self.layers.write().await;
        guard
            .try_freeze_in_memory_layer(self.get_last_record_lsn(), &self.last_freeze_at)
            .await;
    }

    /// Layer flusher task's main loop.
    async fn flush_loop(
        self: &Arc<Self>,
        mut layer_flush_start_rx: tokio::sync::watch::Receiver<u64>,
        ctx: &RequestContext,
    ) {
        info!("started flush loop");
        loop {
            tokio::select! {
                _ = self.cancel.cancelled() => {
                    info!("shutting down layer flush task");
                    break;
                },
                _ = task_mgr::shutdown_watcher() => {
                    info!("shutting down layer flush task");
                    break;
                },
                _ = layer_flush_start_rx.changed() => {}
            }

            trace!("waking up");
            let timer = self.metrics.flush_time_histo.start_timer();
            let flush_counter = *layer_flush_start_rx.borrow();
            let result = loop {
                if self.cancel.is_cancelled() {
                    info!("dropping out of flush loop for timeline shutdown");
                    // Note: we do not bother transmitting into [`layer_flush_done_tx`], because
                    // anyone waiting on that will respect self.cancel as well: they will stop
                    // waiting at the same time we as drop out of this loop.
                    return;
                }

                let layer_to_flush = {
                    let guard = self.layers.read().await;
                    guard.layer_map().frozen_layers.front().cloned()
                    // drop 'layers' lock to allow concurrent reads and writes
                };
                let Some(layer_to_flush) = layer_to_flush else {
                    break Ok(());
                };
                match self.flush_frozen_layer(layer_to_flush, ctx).await {
                    Ok(()) => {}
                    Err(FlushLayerError::Cancelled) => {
                        info!("dropping out of flush loop for timeline shutdown");
                        return;
                    }
                    err @ Err(
                        FlushLayerError::Other(_) | FlushLayerError::PageReconstructError(_),
                    ) => {
                        error!("could not flush frozen layer: {err:?}");
                        break err;
                    }
                }
            };
            // Notify any listeners that we're done
            let _ = self
                .layer_flush_done_tx
                .send_replace((flush_counter, result));

            timer.stop_and_record();
        }
    }

    async fn flush_frozen_layers_and_wait(&self) -> anyhow::Result<()> {
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

        self.layer_flush_start_tx.send_modify(|counter| {
            my_flush_request = *counter + 1;
            *counter = my_flush_request;
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
        self.layer_flush_start_tx.send_modify(|val| *val += 1);
    }

    /// Flush one frozen in-memory layer to disk, as a new delta layer.
    #[instrument(skip_all, fields(tenant_id=%self.tenant_shard_id.tenant_id, shard_id = %self.tenant_shard_id.shard_slug(), timeline_id=%self.timeline_id, layer=%frozen_layer))]
    async fn flush_frozen_layer(
        self: &Arc<Self>,
        frozen_layer: Arc<InMemoryLayer>,
        ctx: &RequestContext,
    ) -> Result<(), FlushLayerError> {
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
        let old_disk_consistent_lsn = self.disk_consistent_lsn.load();

        // The new on-disk layers are now in the layer map. We can remove the
        // in-memory layer from the map now. The flushed layer is stored in
        // the mapping in `create_delta_layer`.
        let metadata = {
            let mut guard = self.layers.write().await;

            if self.cancel.is_cancelled() {
                return Err(FlushLayerError::Cancelled);
            }

            guard.finish_flush_l0_layer(delta_layer_to_add.as_ref(), &frozen_layer, &self.metrics);

            if disk_consistent_lsn != old_disk_consistent_lsn {
                assert!(disk_consistent_lsn > old_disk_consistent_lsn);
                self.disk_consistent_lsn.store(disk_consistent_lsn);

                // Schedule remote uploads that will reflect our new disk_consistent_lsn
                Some(self.schedule_uploads(disk_consistent_lsn, layers_to_upload)?)
            } else {
                None
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

        // Update the metadata file, with new 'disk_consistent_lsn'
        //
        // TODO: This perhaps should be done in 'flush_frozen_layers', after flushing
        // *all* the layers, to avoid fsyncing the file multiple times.

        // If we updated our disk_consistent_lsn, persist the updated metadata to local disk.
        if let Some(metadata) = metadata {
            save_metadata(
                self.conf,
                &self.tenant_shard_id,
                &self.timeline_id,
                &metadata,
            )
            .await
            .context("save_metadata")?;
        }
        Ok(())
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

    async fn update_metadata_file(
        &self,
        disk_consistent_lsn: Lsn,
        layers_to_upload: impl IntoIterator<Item = ResidentLayer>,
    ) -> anyhow::Result<()> {
        let metadata = self.schedule_uploads(disk_consistent_lsn, layers_to_upload)?;

        save_metadata(
            self.conf,
            &self.tenant_shard_id,
            &self.timeline_id,
            &metadata,
        )
        .await
        .context("save_metadata")?;

        Ok(())
    }

    // Write out the given frozen in-memory layer as a new L0 delta file. This L0 file will not be tracked
    // in layer map immediately. The caller is responsible to put it into the layer map.
    async fn create_delta_layer(
        self: &Arc<Self>,
        frozen_layer: &Arc<InMemoryLayer>,
        ctx: &RequestContext,
    ) -> anyhow::Result<ResidentLayer> {
        let span = tracing::info_span!("blocking");
        let new_delta: ResidentLayer = tokio::task::spawn_blocking({
            let self_clone = Arc::clone(self);
            let frozen_layer = Arc::clone(frozen_layer);
            let ctx = ctx.attached_child();
            move || {
                // Write it out
                // Keep this inside `spawn_blocking` and `Handle::current`
                // as long as the write path is still sync and the read impl
                // is still not fully async. Otherwise executor threads would
                // be blocked.
                let _g = span.entered();
                let new_delta =
                    Handle::current().block_on(frozen_layer.write_to_disk(&self_clone, &ctx))?;
                let new_delta_path = new_delta.local_path().to_owned();

                // Sync it to disk.
                //
                // We must also fsync the timeline dir to ensure the directory entries for
                // new layer files are durable.
                //
                // NB: timeline dir must be synced _after_ the file contents are durable.
                // So, two separate fsyncs are required, they mustn't be batched.
                //
                // TODO: If we're running inside 'flush_frozen_layers' and there are multiple
                // files to flush, the fsync overhead can be reduces as follows:
                // 1. write them all to temporary file names
                // 2. fsync them
                // 3. rename to the final name
                // 4. fsync the parent directory.
                // Note that (1),(2),(3) today happen inside write_to_disk().
                //
                // FIXME: the writer already fsyncs all data, only rename needs to be fsynced here
                par_fsync::par_fsync(&[new_delta_path]).context("fsync of delta layer")?;
                par_fsync::par_fsync(&[self_clone
                    .conf
                    .timeline_path(&self_clone.tenant_shard_id, &self_clone.timeline_id)])
                .context("fsync of timeline dir")?;

                anyhow::Ok(new_delta)
            }
        })
        .await
        .context("spawn_blocking")
        .and_then(|x| x)?;

        Ok(new_delta)
    }

    async fn repartition(
        &self,
        lsn: Lsn,
        partition_size: u64,
        flags: EnumSet<CompactFlags>,
        ctx: &RequestContext,
    ) -> anyhow::Result<(KeyPartitioning, Lsn)> {
        {
            let partitioning_guard = self.partitioning.lock().unwrap();
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
        }
        let keyspace = self.collect_keyspace(lsn, ctx).await?;
        let partitioning = keyspace.partition(partition_size);

        let mut partitioning_guard = self.partitioning.lock().unwrap();
        if lsn > partitioning_guard.1 {
            *partitioning_guard = (partitioning, lsn);
        } else {
            warn!("Concurrent repartitioning of keyspace. This unexpected, but probably harmless");
        }
        Ok((partitioning_guard.0.clone(), partitioning_guard.1))
    }

    // Is it time to create a new image layer for the given partition?
    async fn time_for_new_image_layer(
        &self,
        partition: &KeySpace,
        lsn: Lsn,
    ) -> anyhow::Result<bool> {
        let threshold = self.get_image_creation_threshold();

        let guard = self.layers.read().await;
        let layers = guard.layer_map();

        let mut max_deltas = 0;
        {
            let wanted_image_layers = self.wanted_image_layers.lock().unwrap();
            if let Some((cutoff_lsn, wanted)) = &*wanted_image_layers {
                let img_range =
                    partition.ranges.first().unwrap().start..partition.ranges.last().unwrap().end;
                if wanted.overlaps(&img_range) {
                    //
                    // gc_timeline only pays attention to image layers that are older than the GC cutoff,
                    // but create_image_layers creates image layers at last-record-lsn.
                    // So it's possible that gc_timeline wants a new image layer to be created for a key range,
                    // but the range is already covered by image layers at more recent LSNs. Before we
                    // create a new image layer, check if the range is already covered at more recent LSNs.
                    if !layers
                        .image_layer_exists(&img_range, &(Lsn::min(lsn, *cutoff_lsn)..lsn + 1))?
                    {
                        debug!(
                            "Force generation of layer {}-{} wanted by GC, cutoff={}, lsn={})",
                            img_range.start, img_range.end, cutoff_lsn, lsn
                        );
                        return Ok(true);
                    }
                }
            }
        }

        for part_range in &partition.ranges {
            let image_coverage = layers.image_coverage(part_range, lsn)?;
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
                        layers.count_deltas(&img_range, &(img_lsn..lsn), Some(threshold))?;

                    max_deltas = max_deltas.max(num_deltas);
                    if num_deltas >= threshold {
                        debug!(
                            "key range {}-{}, has {} deltas on this timeline in LSN range {}..{}",
                            img_range.start, img_range.end, num_deltas, img_lsn, lsn
                        );
                        return Ok(true);
                    }
                }
            }
        }

        debug!(
            max_deltas,
            "none of the partitioned ranges had >= {threshold} deltas"
        );
        Ok(false)
    }

    #[tracing::instrument(skip_all, fields(%lsn, %force))]
    async fn create_image_layers(
        self: &Arc<Timeline>,
        partitioning: &KeyPartitioning,
        lsn: Lsn,
        force: bool,
        ctx: &RequestContext,
    ) -> Result<Vec<ResidentLayer>, PageReconstructError> {
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
            start = img_range.end;
            if force || self.time_for_new_image_layer(partition, lsn).await? {
                let mut image_layer_writer = ImageLayerWriter::new(
                    self.conf,
                    self.timeline_id,
                    self.tenant_shard_id,
                    &img_range,
                    lsn,
                )
                .await?;

                fail_point!("image-layer-writer-fail-before-finish", |_| {
                    Err(PageReconstructError::Other(anyhow::anyhow!(
                        "failpoint image-layer-writer-fail-before-finish"
                    )))
                });
                for range in &partition.ranges {
                    let mut key = range.start;
                    while key < range.end {
                        let img = match self.get(key, lsn, ctx).await {
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
                                if is_rel_fsm_block_key(key) || is_rel_vm_block_key(key) {
                                    warn!("could not reconstruct FSM or VM key {key}, filling with zeros: {err:?}");
                                    ZERO_PAGE.clone()
                                } else {
                                    return Err(err);
                                }
                            }
                        };
                        image_layer_writer.put_image(key, &img).await?;
                        key = key.next();
                    }
                }
                let image_layer = image_layer_writer.finish(self).await?;
                image_layers.push(image_layer);
            }
        }
        // All layers that the GC wanted us to create have now been created.
        //
        // It's possible that another GC cycle happened while we were compacting, and added
        // something new to wanted_image_layers, and we now clear that before processing it.
        // That's OK, because the next GC iteration will put it back in.
        *self.wanted_image_layers.lock().unwrap() = None;

        // Sync the new layer to disk before adding it to the layer map, to make sure
        // we don't garbage collect something based on the new layer, before it has
        // reached the disk.
        //
        // We must also fsync the timeline dir to ensure the directory entries for
        // new layer files are durable
        //
        // Compaction creates multiple image layers. It would be better to create them all
        // and fsync them all in parallel.
        let all_paths = image_layers
            .iter()
            .map(|layer| layer.local_path().to_owned())
            .collect::<Vec<_>>();

        par_fsync::par_fsync_async(&all_paths)
            .await
            .context("fsync of newly created layer files")?;

        par_fsync::par_fsync_async(&[self
            .conf
            .timeline_path(&self.tenant_shard_id, &self.timeline_id)])
        .await
        .context("fsync of timeline dir")?;

        let mut guard = self.layers.write().await;

        // FIXME: we could add the images to be uploaded *before* returning from here, but right
        // now they are being scheduled outside of write lock
        guard.track_new_image_layers(&image_layers, &self.metrics);
        drop_wlock(guard);
        timer.stop_and_record();

        Ok(image_layers)
    }
}

#[derive(Default)]
struct CompactLevel0Phase1Result {
    new_layers: Vec<ResidentLayer>,
    deltas_to_compact: Vec<Layer>,
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
    pub fn till_now(&self) -> DurationRecorder {
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
    pub fn into_recorded(self) -> Option<RecordedDuration> {
        match self {
            DurationRecorder::NotStarted => None,
            DurationRecorder::Recorded(recorded, _) => Some(recorded),
        }
    }
}

#[derive(Default)]
struct CompactLevel0Phase1StatsBuilder {
    version: Option<u64>,
    tenant_id: Option<TenantShardId>,
    timeline_id: Option<TimelineId>,
    read_lock_acquisition_micros: DurationRecorder,
    read_lock_held_spawn_blocking_startup_micros: DurationRecorder,
    read_lock_held_key_sort_micros: DurationRecorder,
    read_lock_held_prerequisites_micros: DurationRecorder,
    read_lock_held_compute_holes_micros: DurationRecorder,
    read_lock_drop_micros: DurationRecorder,
    write_layer_files_micros: DurationRecorder,
    level0_deltas_count: Option<usize>,
    new_deltas_count: Option<usize>,
    new_deltas_size: Option<u64>,
}

#[derive(serde::Serialize)]
struct CompactLevel0Phase1Stats {
    version: u64,
    tenant_id: TenantShardId,
    timeline_id: TimelineId,
    read_lock_acquisition_micros: RecordedDuration,
    read_lock_held_spawn_blocking_startup_micros: RecordedDuration,
    read_lock_held_key_sort_micros: RecordedDuration,
    read_lock_held_prerequisites_micros: RecordedDuration,
    read_lock_held_compute_holes_micros: RecordedDuration,
    read_lock_drop_micros: RecordedDuration,
    write_layer_files_micros: RecordedDuration,
    level0_deltas_count: usize,
    new_deltas_count: usize,
    new_deltas_size: u64,
}

impl TryFrom<CompactLevel0Phase1StatsBuilder> for CompactLevel0Phase1Stats {
    type Error = anyhow::Error;

    fn try_from(value: CompactLevel0Phase1StatsBuilder) -> Result<Self, Self::Error> {
        Ok(Self {
            version: value.version.ok_or_else(|| anyhow!("version not set"))?,
            tenant_id: value
                .tenant_id
                .ok_or_else(|| anyhow!("tenant_id not set"))?,
            timeline_id: value
                .timeline_id
                .ok_or_else(|| anyhow!("timeline_id not set"))?,
            read_lock_acquisition_micros: value
                .read_lock_acquisition_micros
                .into_recorded()
                .ok_or_else(|| anyhow!("read_lock_acquisition_micros not set"))?,
            read_lock_held_spawn_blocking_startup_micros: value
                .read_lock_held_spawn_blocking_startup_micros
                .into_recorded()
                .ok_or_else(|| anyhow!("read_lock_held_spawn_blocking_startup_micros not set"))?,
            read_lock_held_key_sort_micros: value
                .read_lock_held_key_sort_micros
                .into_recorded()
                .ok_or_else(|| anyhow!("read_lock_held_key_sort_micros not set"))?,
            read_lock_held_prerequisites_micros: value
                .read_lock_held_prerequisites_micros
                .into_recorded()
                .ok_or_else(|| anyhow!("read_lock_held_prerequisites_micros not set"))?,
            read_lock_held_compute_holes_micros: value
                .read_lock_held_compute_holes_micros
                .into_recorded()
                .ok_or_else(|| anyhow!("read_lock_held_compute_holes_micros not set"))?,
            read_lock_drop_micros: value
                .read_lock_drop_micros
                .into_recorded()
                .ok_or_else(|| anyhow!("read_lock_drop_micros not set"))?,
            write_layer_files_micros: value
                .write_layer_files_micros
                .into_recorded()
                .ok_or_else(|| anyhow!("write_layer_files_micros not set"))?,
            level0_deltas_count: value
                .level0_deltas_count
                .ok_or_else(|| anyhow!("level0_deltas_count not set"))?,
            new_deltas_count: value
                .new_deltas_count
                .ok_or_else(|| anyhow!("new_deltas_count not set"))?,
            new_deltas_size: value
                .new_deltas_size
                .ok_or_else(|| anyhow!("new_deltas_size not set"))?,
        })
    }
}

impl Timeline {
    /// Level0 files first phase of compaction, explained in the [`Self::compact`] comment.
    async fn compact_level0_phase1(
        self: &Arc<Self>,
        guard: tokio::sync::OwnedRwLockReadGuard<LayerManager>,
        mut stats: CompactLevel0Phase1StatsBuilder,
        target_file_size: u64,
        ctx: &RequestContext,
    ) -> Result<CompactLevel0Phase1Result, CompactionError> {
        stats.read_lock_held_spawn_blocking_startup_micros =
            stats.read_lock_acquisition_micros.till_now(); // set by caller
        let layers = guard.layer_map();
        let level0_deltas = layers.get_level0_deltas()?;
        let mut level0_deltas = level0_deltas
            .into_iter()
            .map(|x| guard.get_from_desc(&x))
            .collect_vec();
        stats.level0_deltas_count = Some(level0_deltas.len());
        // Only compact if enough layers have accumulated.
        let threshold = self.get_compaction_threshold();
        if level0_deltas.is_empty() || level0_deltas.len() < threshold {
            debug!(
                level0_deltas = level0_deltas.len(),
                threshold, "too few deltas to compact"
            );
            return Ok(CompactLevel0Phase1Result::default());
        }

        // This failpoint is used together with `test_duplicate_layers` integration test.
        // It returns the compaction result exactly the same layers as input to compaction.
        // We want to ensure that this will not cause any problem when updating the layer map
        // after the compaction is finished.
        //
        // Currently, there are two rare edge cases that will cause duplicated layers being
        // inserted.
        // 1. The compaction job is inturrupted / did not finish successfully. Assume we have file 1, 2, 3, 4, which
        //    is compacted to 5, but the page server is shut down, next time we start page server we will get a layer
        //    map containing 1, 2, 3, 4, and 5, whereas 5 has the same content as 4. If we trigger L0 compation at this
        //    point again, it is likely that we will get a file 6 which has the same content and the key range as 5,
        //    and this causes an overwrite. This is acceptable because the content is the same, and we should do a
        //    layer replace instead of the normal remove / upload process.
        // 2. The input workload pattern creates exactly n files that are sorted, non-overlapping and is of target file
        //    size length. Compaction will likely create the same set of n files afterwards.
        //
        // This failpoint is a superset of both of the cases.
        if cfg!(feature = "testing") {
            let active = (|| {
                ::fail::fail_point!("compact-level0-phase1-return-same", |_| true);
                false
            })();

            if active {
                let mut new_layers = Vec::with_capacity(level0_deltas.len());
                for delta in &level0_deltas {
                    // we are just faking these layers as being produced again for this failpoint
                    new_layers.push(
                        delta
                            .download_and_keep_resident()
                            .await
                            .context("download layer for failpoint")?,
                    );
                }
                tracing::info!("compact-level0-phase1-return-same"); // so that we can check if we hit the failpoint
                return Ok(CompactLevel0Phase1Result {
                    new_layers,
                    deltas_to_compact: level0_deltas,
                });
            }
        }

        // Gather the files to compact in this iteration.
        //
        // Start with the oldest Level 0 delta file, and collect any other
        // level 0 files that form a contiguous sequence, such that the end
        // LSN of previous file matches the start LSN of the next file.
        //
        // Note that if the files don't form such a sequence, we might
        // "compact" just a single file. That's a bit pointless, but it allows
        // us to get rid of the level 0 file, and compact the other files on
        // the next iteration. This could probably made smarter, but such
        // "gaps" in the sequence of level 0 files should only happen in case
        // of a crash, partial download from cloud storage, or something like
        // that, so it's not a big deal in practice.
        level0_deltas.sort_by_key(|l| l.layer_desc().lsn_range.start);
        let mut level0_deltas_iter = level0_deltas.iter();

        let first_level0_delta = level0_deltas_iter.next().unwrap();
        let mut prev_lsn_end = first_level0_delta.layer_desc().lsn_range.end;
        let mut deltas_to_compact = Vec::with_capacity(level0_deltas.len());

        deltas_to_compact.push(first_level0_delta.download_and_keep_resident().await?);
        for l in level0_deltas_iter {
            let lsn_range = &l.layer_desc().lsn_range;

            if lsn_range.start != prev_lsn_end {
                break;
            }
            deltas_to_compact.push(l.download_and_keep_resident().await?);
            prev_lsn_end = lsn_range.end;
        }
        let lsn_range = Range {
            start: deltas_to_compact
                .first()
                .unwrap()
                .layer_desc()
                .lsn_range
                .start,
            end: deltas_to_compact.last().unwrap().layer_desc().lsn_range.end,
        };

        info!(
            "Starting Level0 compaction in LSN range {}-{} for {} layers ({} deltas in total)",
            lsn_range.start,
            lsn_range.end,
            deltas_to_compact.len(),
            level0_deltas.len()
        );

        for l in deltas_to_compact.iter() {
            info!("compact includes {l}");
        }

        // We don't need the original list of layers anymore. Drop it so that
        // we don't accidentally use it later in the function.
        drop(level0_deltas);

        stats.read_lock_held_prerequisites_micros = stats
            .read_lock_held_spawn_blocking_startup_micros
            .till_now();

        // Determine N largest holes where N is number of compacted layers.
        let max_holes = deltas_to_compact.len();
        let last_record_lsn = self.get_last_record_lsn();
        let min_hole_range = (target_file_size / page_cache::PAGE_SZ as u64) as i128;
        let min_hole_coverage_size = 3; // TODO: something more flexible?

        // min-heap (reserve space for one more element added before eviction)
        let mut heap: BinaryHeap<Hole> = BinaryHeap::with_capacity(max_holes + 1);
        let mut prev: Option<Key> = None;

        let mut all_keys = Vec::new();

        for l in deltas_to_compact.iter() {
            all_keys.extend(l.load_keys(ctx).await?);
        }

        // FIXME: should spawn_blocking the rest of this function

        // The current stdlib sorting implementation is designed in a way where it is
        // particularly fast where the slice is made up of sorted sub-ranges.
        all_keys.sort_by_key(|DeltaEntry { key, lsn, .. }| (*key, *lsn));

        stats.read_lock_held_key_sort_micros = stats.read_lock_held_prerequisites_micros.till_now();

        for &DeltaEntry { key: next_key, .. } in all_keys.iter() {
            if let Some(prev_key) = prev {
                // just first fast filter
                if next_key.to_i128() - prev_key.to_i128() >= min_hole_range {
                    let key_range = prev_key..next_key;
                    // Measuring hole by just subtraction of i128 representation of key range boundaries
                    // has not so much sense, because largest holes will corresponds field1/field2 changes.
                    // But we are mostly interested to eliminate holes which cause generation of excessive image layers.
                    // That is why it is better to measure size of hole as number of covering image layers.
                    let coverage_size = layers.image_coverage(&key_range, last_record_lsn)?.len();
                    if coverage_size >= min_hole_coverage_size {
                        heap.push(Hole {
                            key_range,
                            coverage_size,
                        });
                        if heap.len() > max_holes {
                            heap.pop(); // remove smallest hole
                        }
                    }
                }
            }
            prev = Some(next_key.next());
        }
        stats.read_lock_held_compute_holes_micros = stats.read_lock_held_key_sort_micros.till_now();
        drop_rlock(guard);
        stats.read_lock_drop_micros = stats.read_lock_held_compute_holes_micros.till_now();
        let mut holes = heap.into_vec();
        holes.sort_unstable_by_key(|hole| hole.key_range.start);
        let mut next_hole = 0; // index of next hole in holes vector

        // This iterator walks through all key-value pairs from all the layers
        // we're compacting, in key, LSN order.
        let all_values_iter = all_keys.iter();

        // This iterator walks through all keys and is needed to calculate size used by each key
        let mut all_keys_iter = all_keys
            .iter()
            .map(|DeltaEntry { key, lsn, size, .. }| (*key, *lsn, *size))
            .coalesce(|mut prev, cur| {
                // Coalesce keys that belong to the same key pair.
                // This ensures that compaction doesn't put them
                // into different layer files.
                // Still limit this by the target file size,
                // so that we keep the size of the files in
                // check.
                if prev.0 == cur.0 && prev.2 < target_file_size {
                    prev.2 += cur.2;
                    Ok(prev)
                } else {
                    Err((prev, cur))
                }
            });

        // Merge the contents of all the input delta layers into a new set
        // of delta layers, based on the current partitioning.
        //
        // We split the new delta layers on the key dimension. We iterate through the key space, and for each key, check if including the next key to the current output layer we're building would cause the layer to become too large. If so, dump the current output layer and start new one.
        // It's possible that there is a single key with so many page versions that storing all of them in a single layer file
        // would be too large. In that case, we also split on the LSN dimension.
        //
        // LSN
        //  ^
        //  |
        //  | +-----------+            +--+--+--+--+
        //  | |           |            |  |  |  |  |
        //  | +-----------+            |  |  |  |  |
        //  | |           |            |  |  |  |  |
        //  | +-----------+     ==>    |  |  |  |  |
        //  | |           |            |  |  |  |  |
        //  | +-----------+            |  |  |  |  |
        //  | |           |            |  |  |  |  |
        //  | +-----------+            +--+--+--+--+
        //  |
        //  +--------------> key
        //
        //
        // If one key (X) has a lot of page versions:
        //
        // LSN
        //  ^
        //  |                                 (X)
        //  | +-----------+            +--+--+--+--+
        //  | |           |            |  |  |  |  |
        //  | +-----------+            |  |  +--+  |
        //  | |           |            |  |  |  |  |
        //  | +-----------+     ==>    |  |  |  |  |
        //  | |           |            |  |  +--+  |
        //  | +-----------+            |  |  |  |  |
        //  | |           |            |  |  |  |  |
        //  | +-----------+            +--+--+--+--+
        //  |
        //  +--------------> key
        // TODO: this actually divides the layers into fixed-size chunks, not
        // based on the partitioning.
        //
        // TODO: we should also opportunistically materialize and
        // garbage collect what we can.
        let mut new_layers = Vec::new();
        let mut prev_key: Option<Key> = None;
        let mut writer: Option<DeltaLayerWriter> = None;
        let mut key_values_total_size = 0u64;
        let mut dup_start_lsn: Lsn = Lsn::INVALID; // start LSN of layer containing values of the single key
        let mut dup_end_lsn: Lsn = Lsn::INVALID; // end LSN of layer containing values of the single key

        for &DeltaEntry {
            key, lsn, ref val, ..
        } in all_values_iter
        {
            let value = val.load(ctx).await?;
            let same_key = prev_key.map_or(false, |prev_key| prev_key == key);
            // We need to check key boundaries once we reach next key or end of layer with the same key
            if !same_key || lsn == dup_end_lsn {
                let mut next_key_size = 0u64;
                let is_dup_layer = dup_end_lsn.is_valid();
                dup_start_lsn = Lsn::INVALID;
                if !same_key {
                    dup_end_lsn = Lsn::INVALID;
                }
                // Determine size occupied by this key. We stop at next key or when size becomes larger than target_file_size
                for (next_key, next_lsn, next_size) in all_keys_iter.by_ref() {
                    next_key_size = next_size;
                    if key != next_key {
                        if dup_end_lsn.is_valid() {
                            // We are writting segment with duplicates:
                            // place all remaining values of this key in separate segment
                            dup_start_lsn = dup_end_lsn; // new segments starts where old stops
                            dup_end_lsn = lsn_range.end; // there are no more values of this key till end of LSN range
                        }
                        break;
                    }
                    key_values_total_size += next_size;
                    // Check if it is time to split segment: if total keys size is larger than target file size.
                    // We need to avoid generation of empty segments if next_size > target_file_size.
                    if key_values_total_size > target_file_size && lsn != next_lsn {
                        // Split key between multiple layers: such layer can contain only single key
                        dup_start_lsn = if dup_end_lsn.is_valid() {
                            dup_end_lsn // new segment with duplicates starts where old one stops
                        } else {
                            lsn // start with the first LSN for this key
                        };
                        dup_end_lsn = next_lsn; // upper LSN boundary is exclusive
                        break;
                    }
                }
                // handle case when loop reaches last key: in this case dup_end is non-zero but dup_start is not set.
                if dup_end_lsn.is_valid() && !dup_start_lsn.is_valid() {
                    dup_start_lsn = dup_end_lsn;
                    dup_end_lsn = lsn_range.end;
                }
                if writer.is_some() {
                    let written_size = writer.as_mut().unwrap().size();
                    let contains_hole =
                        next_hole < holes.len() && key >= holes[next_hole].key_range.end;
                    // check if key cause layer overflow or contains hole...
                    if is_dup_layer
                        || dup_end_lsn.is_valid()
                        || written_size + key_values_total_size > target_file_size
                        || contains_hole
                    {
                        // ... if so, flush previous layer and prepare to write new one
                        new_layers.push(
                            writer
                                .take()
                                .unwrap()
                                .finish(prev_key.unwrap().next(), self)
                                .await?,
                        );
                        writer = None;

                        if contains_hole {
                            // skip hole
                            next_hole += 1;
                        }
                    }
                }
                // Remember size of key value because at next iteration we will access next item
                key_values_total_size = next_key_size;
            }
            if writer.is_none() {
                // Create writer if not initiaized yet
                writer = Some(
                    DeltaLayerWriter::new(
                        self.conf,
                        self.timeline_id,
                        self.tenant_shard_id,
                        key,
                        if dup_end_lsn.is_valid() {
                            // this is a layer containing slice of values of the same key
                            debug!("Create new dup layer {}..{}", dup_start_lsn, dup_end_lsn);
                            dup_start_lsn..dup_end_lsn
                        } else {
                            debug!("Create new layer {}..{}", lsn_range.start, lsn_range.end);
                            lsn_range.clone()
                        },
                    )
                    .await?,
                );
            }

            fail_point!("delta-layer-writer-fail-before-finish", |_| {
                Err(CompactionError::Other(anyhow::anyhow!(
                    "failpoint delta-layer-writer-fail-before-finish"
                )))
            });

            writer.as_mut().unwrap().put_value(key, lsn, value).await?;

            if !new_layers.is_empty() {
                fail_point!("after-timeline-compacted-first-L1");
            }

            prev_key = Some(key);
        }
        if let Some(writer) = writer {
            new_layers.push(writer.finish(prev_key.unwrap().next(), self).await?);
        }

        // Sync layers
        if !new_layers.is_empty() {
            // Print a warning if the created layer is larger than double the target size
            // Add two pages for potential overhead. This should in theory be already
            // accounted for in the target calculation, but for very small targets,
            // we still might easily hit the limit otherwise.
            let warn_limit = target_file_size * 2 + page_cache::PAGE_SZ as u64 * 2;
            for layer in new_layers.iter() {
                if layer.layer_desc().file_size > warn_limit {
                    warn!(
                        %layer,
                        "created delta file of size {} larger than double of target of {target_file_size}", layer.layer_desc().file_size
                    );
                }
            }

            // FIXME: the writer already fsyncs all data, only rename needs to be fsynced here
            let layer_paths: Vec<Utf8PathBuf> = new_layers
                .iter()
                .map(|l| l.local_path().to_owned())
                .collect();

            // Fsync all the layer files and directory using multiple threads to
            // minimize latency.
            par_fsync::par_fsync_async(&layer_paths)
                .await
                .context("fsync all new layers")?;

            let timeline_dir = self
                .conf
                .timeline_path(&self.tenant_shard_id, &self.timeline_id);

            par_fsync::par_fsync_async(&[timeline_dir])
                .await
                .context("fsync of timeline dir")?;
        }

        stats.write_layer_files_micros = stats.read_lock_drop_micros.till_now();
        stats.new_deltas_count = Some(new_layers.len());
        stats.new_deltas_size = Some(new_layers.iter().map(|l| l.layer_desc().file_size).sum());

        match TryInto::<CompactLevel0Phase1Stats>::try_into(stats)
            .and_then(|stats| serde_json::to_string(&stats).context("serde_json::to_string"))
        {
            Ok(stats_json) => {
                info!(
                    stats_json = stats_json.as_str(),
                    "compact_level0_phase1 stats available"
                )
            }
            Err(e) => {
                warn!("compact_level0_phase1 stats failed to serialize: {:#}", e);
            }
        }

        Ok(CompactLevel0Phase1Result {
            new_layers,
            deltas_to_compact: deltas_to_compact
                .into_iter()
                .map(|x| x.drop_eviction_guard())
                .collect::<Vec<_>>(),
        })
    }

    ///
    /// Collect a bunch of Level 0 layer files, and compact and reshuffle them as
    /// as Level 1 files.
    ///
    async fn compact_level0(
        self: &Arc<Self>,
        target_file_size: u64,
        ctx: &RequestContext,
    ) -> Result<(), CompactionError> {
        let CompactLevel0Phase1Result {
            new_layers,
            deltas_to_compact,
        } = {
            let phase1_span = info_span!("compact_level0_phase1");
            let ctx = ctx.attached_child();
            let mut stats = CompactLevel0Phase1StatsBuilder {
                version: Some(2),
                tenant_id: Some(self.tenant_shard_id),
                timeline_id: Some(self.timeline_id),
                ..Default::default()
            };

            let begin = tokio::time::Instant::now();
            let phase1_layers_locked = Arc::clone(&self.layers).read_owned().await;
            let now = tokio::time::Instant::now();
            stats.read_lock_acquisition_micros =
                DurationRecorder::Recorded(RecordedDuration(now - begin), now);
            self.compact_level0_phase1(phase1_layers_locked, stats, target_file_size, &ctx)
                .instrument(phase1_span)
                .await?
        };

        if new_layers.is_empty() && deltas_to_compact.is_empty() {
            // nothing to do
            return Ok(());
        }

        let mut guard = self.layers.write().await;

        let mut duplicated_layers = HashSet::new();

        let mut insert_layers = Vec::with_capacity(new_layers.len());

        for l in &new_layers {
            if guard.contains(l.as_ref()) {
                // expected in tests
                tracing::error!(layer=%l, "duplicated L1 layer");

                // good ways to cause a duplicate: we repeatedly error after taking the writelock
                // `guard`  on self.layers. as of writing this, there are no error returns except
                // for compact_level0_phase1 creating an L0, which does not happen in practice
                // because we have not implemented L0 => L0 compaction.
                duplicated_layers.insert(l.layer_desc().key());
            } else if LayerMap::is_l0(l.layer_desc()) {
                return Err(CompactionError::Other(anyhow!("compaction generates a L0 layer file as output, which will cause infinite compaction.")));
            } else {
                insert_layers.push(l.clone());
            }
        }

        let remove_layers = {
            let mut deltas_to_compact = deltas_to_compact;
            // only remove those inputs which were not outputs
            deltas_to_compact.retain(|l| !duplicated_layers.contains(&l.layer_desc().key()));
            deltas_to_compact
        };

        // deletion will happen later, the layer file manager calls garbage_collect_on_drop
        guard.finish_compact_l0(&remove_layers, &insert_layers, &self.metrics);

        if let Some(remote_client) = self.remote_client.as_ref() {
            remote_client.schedule_compaction_update(&remove_layers, &new_layers)?;
        }

        drop_wlock(guard);

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
                        debug!("future({})", lsn);
                        cutoff_horizon
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
        let _g = self.gc_lock.lock().await;
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
        let mut wanted_image_layers = KeySpaceRandomAccum::default();

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
                .image_layer_exists(&l.get_key_range(), &(l.get_lsn_range().end..new_gc_cutoff))?
            {
                debug!("keeping {} because it is the latest layer", l.filename());
                // Collect delta key ranges that need image layers to allow garbage
                // collecting the layers.
                // It is not so obvious whether we need to propagate information only about
                // delta layers. Image layers can form "stairs" preventing old image from been deleted.
                // But image layers are in any case less sparse than delta layers. Also we need some
                // protection from replacing recent image layers with new one after each GC iteration.
                if self.get_gc_feedback() && l.is_incremental() && !LayerMap::is_l0(&l) {
                    wanted_image_layers.add_range(l.get_key_range());
                }
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
        self.wanted_image_layers
            .lock()
            .unwrap()
            .replace((new_gc_cutoff, wanted_image_layers.to_keyspace()));

        if !layers_to_remove.is_empty() {
            // Persist the new GC cutoff value in the metadata file, before
            // we actually remove anything.
            //
            // This does not in fact have any effect as we no longer consider local metadata unless
            // running without remote storage.
            //
            // This unconditionally schedules also an index_part.json update, even though, we will
            // be doing one a bit later with the unlinked gc'd layers.
            //
            // TODO: remove when implementing <https://github.com/neondatabase/neon/issues/4099>.
            self.update_metadata_file(self.disk_consistent_lsn.load(), None)
                .await?;

            let gc_layers = layers_to_remove
                .iter()
                .map(|x| guard.get_from_desc(x))
                .collect::<Vec<Layer>>();

            result.layers_removed = gc_layers.len() as u64;

            if let Some(remote_client) = self.remote_client.as_ref() {
                remote_client.schedule_gc_update(&gc_layers)?;
            }

            guard.finish_gc_timeline(&gc_layers);

            if result.layers_removed != 0 {
                fail_point!("after-timeline-gc-removed-layers");
            }

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
                    .request_redo(key, request_lsn, data.img, data.records, self.pg_version)
                    .await
                    .context("Failed to reconstruct a page image:")
                {
                    Ok(img) => img,
                    Err(e) => return Err(PageReconstructError::from(e)),
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

    pub fn get_download_all_remote_layers_task_info(&self) -> Option<DownloadRemoteLayersTaskInfo> {
        self.download_all_remote_layers_task_info
            .read()
            .unwrap()
            .clone()
    }
}

pub(crate) struct DiskUsageEvictionInfo {
    /// Timeline's largest layer (remote or resident)
    pub max_layer_size: Option<u64>,
    /// Timeline's resident layers
    pub resident_layers: Vec<LocalLayerInfoForDiskUsageEviction>,
}

pub(crate) struct LocalLayerInfoForDiskUsageEviction {
    pub layer: Layer,
    pub last_activity_ts: SystemTime,
}

impl std::fmt::Debug for LocalLayerInfoForDiskUsageEviction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // format the tv_sec, tv_nsec into rfc3339 in case someone is looking at it
        // having to allocate a string to this is bad, but it will rarely be formatted
        let ts = chrono::DateTime::<chrono::Utc>::from(self.last_activity_ts);
        let ts = ts.to_rfc3339_opts(chrono::SecondsFormat::Nanos, true);
        struct DisplayIsDebug<'a, T>(&'a T);
        impl<'a, T: std::fmt::Display> std::fmt::Debug for DisplayIsDebug<'a, T> {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "{}", self.0)
            }
        }
        f.debug_struct("LocalLayerInfoForDiskUsageEviction")
            .field("layer", &DisplayIsDebug(&self.layer))
            .field("last_activity", &ts)
            .finish()
    }
}

impl LocalLayerInfoForDiskUsageEviction {
    pub fn file_size(&self) -> u64 {
        self.layer.layer_desc().file_size
    }
}

impl Timeline {
    /// Returns non-remote layers for eviction.
    pub(crate) async fn get_local_layers_for_disk_usage_eviction(&self) -> DiskUsageEvictionInfo {
        let guard = self.layers.read().await;
        let layers = guard.layer_map();

        let mut max_layer_size: Option<u64> = None;
        let mut resident_layers = Vec::new();

        for l in layers.iter_historic_layers() {
            let file_size = l.file_size();
            max_layer_size = max_layer_size.map_or(Some(file_size), |m| Some(m.max(file_size)));

            let l = guard.get_from_desc(&l);

            let l = match l.keep_resident().await {
                Ok(Some(l)) => l,
                Ok(None) => continue,
                Err(e) => {
                    // these should not happen, but we cannot make them statically impossible right
                    // now.
                    tracing::warn!(layer=%l, "failed to keep the layer resident: {e:#}");
                    continue;
                }
            };

            let last_activity_ts = l.access_stats().latest_activity().unwrap_or_else(|| {
                // We only use this fallback if there's an implementation error.
                // `latest_activity` already does rate-limited warn!() log.
                debug!(layer=%l, "last_activity returns None, using SystemTime::now");
                SystemTime::now()
            });

            resident_layers.push(LocalLayerInfoForDiskUsageEviction {
                layer: l.drop_eviction_guard(),
                last_activity_ts,
            });
        }

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

/// Various functions to mutate the timeline.
// TODO Currently, Deref is used to allow easy access to read methods from this trait.
// This is probably considered a bad practice in Rust and should be fixed eventually,
// but will cause large code changes.
pub struct TimelineWriter<'a> {
    tl: &'a Timeline,
    _write_guard: tokio::sync::MutexGuard<'a, ()>,
}

impl Deref for TimelineWriter<'_> {
    type Target = Timeline;

    fn deref(&self) -> &Self::Target {
        self.tl
    }
}

impl<'a> TimelineWriter<'a> {
    /// Put a new page version that can be constructed from a WAL record
    ///
    /// This will implicitly extend the relation, if the page is beyond the
    /// current end-of-file.
    pub async fn put(
        &self,
        key: Key,
        lsn: Lsn,
        value: &Value,
        ctx: &RequestContext,
    ) -> anyhow::Result<()> {
        self.tl.put_value(key, lsn, value, ctx).await
    }

    pub(crate) async fn put_batch(
        &self,
        batch: &HashMap<Key, Vec<(Lsn, Value)>>,
        ctx: &RequestContext,
    ) -> anyhow::Result<()> {
        self.tl.put_values(batch, ctx).await
    }

    pub(crate) async fn delete_batch(&self, batch: &[(Range<Key>, Lsn)]) -> anyhow::Result<()> {
        self.tl.put_tombstones(batch).await
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

        let ctx = any_context();
        let tenant = harness.try_load(&ctx).await.unwrap();
        let timeline = tenant
            .create_test_timeline(TimelineId::generate(), Lsn(0x10), 14, &ctx)
            .await
            .unwrap();

        let rtc = timeline
            .remote_client
            .clone()
            .expect("just configured this");

        let layer = find_some_layer(&timeline).await;
        let layer = layer
            .keep_resident()
            .await
            .expect("no download => no downloading errors")
            .expect("should had been resident")
            .drop_eviction_guard();

        let first = async { layer.evict_and_wait(&rtc).await };
        let second = async { layer.evict_and_wait(&rtc).await };

        let (first, second) = tokio::join!(first, second);

        let res = layer.keep_resident().await;
        assert!(matches!(res, Ok(None)), "{res:?}");

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

    fn any_context() -> crate::context::RequestContext {
        use crate::context::*;
        use crate::task_mgr::*;
        RequestContext::new(TaskKind::UnitTest, DownloadBehavior::Error)
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

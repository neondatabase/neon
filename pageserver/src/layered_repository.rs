//!
//! Zenith repository implementation that keeps old data in files on disk, and
//! the recent changes in memory. See layered_repository/*_layer.rs files.
//! The functions here are responsible for locating the correct layer for the
//! get/put call, tracing timeline branching history as needed.
//!
//! The files are stored in the .neon/tenants/<tenantid>/timelines/<timelineid>
//! directory. See layered_repository/README for how the files are managed.
//! In addition to the layer files, there is a metadata file in the same
//! directory that contains information about the timeline, in particular its
//! parent timeline, and the last LSN that has been written to disk.
//!

use anyhow::{anyhow, bail, ensure, Context, Result};
use bytes::Bytes;
use fail::fail_point;
use itertools::Itertools;
use lazy_static::lazy_static;
use tracing::*;

use std::cmp::{max, min, Ordering};
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::collections::{BTreeSet, HashSet};
use std::fs;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::num::NonZeroU64;
use std::ops::{Bound::Included, Deref, Range};
use std::path::{Path, PathBuf};
use std::sync::atomic::{self, AtomicBool};
use std::sync::{Arc, Mutex, MutexGuard, RwLock, RwLockReadGuard, TryLockError};
use std::time::{Duration, Instant, SystemTime};

use self::metadata::{metadata_path, TimelineMetadata, METADATA_FILE_NAME};
use crate::config::PageServerConf;
use crate::keyspace::KeySpace;
use crate::storage_sync::index::RemoteIndex;
use crate::tenant_config::{TenantConf, TenantConfOpt};

use crate::repository::{
    GcResult, Repository, RepositoryTimeline, Timeline, TimelineSyncStatusUpdate, TimelineWriter,
};
use crate::repository::{Key, Value};
use crate::tenant_mgr;
use crate::thread_mgr;
use crate::virtual_file::VirtualFile;
use crate::walreceiver::IS_WAL_RECEIVER;
use crate::walredo::WalRedoManager;
use crate::CheckpointConfig;
use crate::{page_cache, storage_sync};

use metrics::{
    register_histogram_vec, register_int_counter, register_int_counter_vec, register_int_gauge_vec,
    Histogram, HistogramVec, IntCounter, IntCounterVec, IntGauge, IntGaugeVec,
};
use toml_edit;
use utils::{
    crashsafe_dir,
    lsn::{AtomicLsn, Lsn, RecordLsn},
    seqwait::SeqWait,
    zid::{ZTenantId, ZTimelineId},
};

mod blob_io;
pub mod block_io;
mod delta_layer;
mod disk_btree;
pub(crate) mod ephemeral_file;
mod filename;
mod image_layer;
mod inmemory_layer;
mod layer_map;
pub mod metadata;
mod par_fsync;
mod storage_layer;

use crate::pgdatadir_mapping::LsnForTimestamp;
use delta_layer::{DeltaLayer, DeltaLayerWriter};
use ephemeral_file::is_ephemeral_file;
use filename::{DeltaFileName, ImageFileName};
use image_layer::{ImageLayer, ImageLayerWriter};
use inmemory_layer::InMemoryLayer;
use layer_map::LayerMap;
use layer_map::SearchResult;
use postgres_ffi::xlog_utils::to_pg_timestamp;
use storage_layer::{Layer, ValueReconstructResult, ValueReconstructState};

// re-export this function so that page_cache.rs can use it.
pub use crate::layered_repository::ephemeral_file::writeback as writeback_ephemeral_file;

// Metrics collected on operations on the storage repository.
lazy_static! {
    static ref STORAGE_TIME: HistogramVec = register_histogram_vec!(
        "pageserver_storage_operations_seconds",
        "Time spent on storage operations",
        &["operation", "tenant_id", "timeline_id"]
    )
    .expect("failed to define a metric");
}

// Metrics collected on operations on the storage repository.
lazy_static! {
    static ref RECONSTRUCT_TIME: HistogramVec = register_histogram_vec!(
        "pageserver_getpage_reconstruct_seconds",
        "Time spent in reconstruct_value",
        &["tenant_id", "timeline_id"]
    )
    .expect("failed to define a metric");
}

lazy_static! {
    static ref MATERIALIZED_PAGE_CACHE_HIT: IntCounterVec = register_int_counter_vec!(
        "pageserver_materialized_cache_hits_total",
        "Number of cache hits from materialized page cache",
        &["tenant_id", "timeline_id"]
    )
    .expect("failed to define a metric");
    static ref WAIT_LSN_TIME: HistogramVec = register_histogram_vec!(
        "pageserver_wait_lsn_seconds",
        "Time spent waiting for WAL to arrive",
        &["tenant_id", "timeline_id"]
    )
    .expect("failed to define a metric");
}

lazy_static! {
    static ref LAST_RECORD_LSN: IntGaugeVec = register_int_gauge_vec!(
        "pageserver_last_record_lsn",
        "Last record LSN grouped by timeline",
        &["tenant_id", "timeline_id"]
    )
    .expect("failed to define a metric");
}

// Metrics for cloud upload. These metrics reflect data uploaded to cloud storage,
// or in testing they estimate how much we would upload if we did.
lazy_static! {
    static ref NUM_PERSISTENT_FILES_CREATED: IntCounter = register_int_counter!(
        "pageserver_created_persistent_files_total",
        "Number of files created that are meant to be uploaded to cloud storage",
    )
    .expect("failed to define a metric");
    static ref PERSISTENT_BYTES_WRITTEN: IntCounter = register_int_counter!(
        "pageserver_written_persistent_bytes_total",
        "Total bytes written that are meant to be uploaded to cloud storage",
    )
    .expect("failed to define a metric");
}

/// Parts of the `.neon/tenants/<tenantid>/timelines/<timelineid>` directory prefix.
pub const TIMELINES_SEGMENT_NAME: &str = "timelines";

///
/// Repository consists of multiple timelines. Keep them in a hash table.
///
pub struct LayeredRepository {
    // Global pageserver config parameters
    pub conf: &'static PageServerConf,

    // Overridden tenant-specific config parameters.
    // We keep TenantConfOpt sturct here to preserve the information
    // about parameters that are not set.
    // This is necessary to allow global config updates.
    tenant_conf: Arc<RwLock<TenantConfOpt>>,

    tenant_id: ZTenantId,
    timelines: Mutex<HashMap<ZTimelineId, LayeredTimelineEntry>>,
    // This mutex prevents creation of new timelines during GC.
    // Adding yet another mutex (in addition to `timelines`) is needed because holding
    // `timelines` mutex during all GC iteration (especially with enforced checkpoint)
    // may block for a long time `get_timeline`, `get_timelines_state`,... and other operations
    // with timelines, which in turn may cause dropping replication connection, expiration of wait_for_lsn
    // timeout...
    gc_cs: Mutex<()>,
    walredo_mgr: Arc<dyn WalRedoManager + Send + Sync>,

    // provides access to timeline data sitting in the remote storage
    // supposed to be used for retrieval of remote consistent lsn in walreceiver
    remote_index: RemoteIndex,

    /// Makes every timeline to backup their files to remote storage.
    upload_layers: bool,
}

/// Public interface
impl Repository for LayeredRepository {
    type Timeline = LayeredTimeline;

    fn get_timeline(&self, timelineid: ZTimelineId) -> Option<RepositoryTimeline<Self::Timeline>> {
        let timelines = self.timelines.lock().unwrap();
        self.get_timeline_internal(timelineid, &timelines)
            .map(RepositoryTimeline::from)
    }

    fn get_timeline_load(&self, timelineid: ZTimelineId) -> Result<Arc<LayeredTimeline>> {
        let mut timelines = self.timelines.lock().unwrap();
        match self.get_timeline_load_internal(timelineid, &mut timelines)? {
            Some(local_loaded_timeline) => Ok(local_loaded_timeline),
            None => anyhow::bail!(
                "cannot get local timeline: unknown timeline id: {}",
                timelineid
            ),
        }
    }

    fn list_timelines(&self) -> Vec<(ZTimelineId, RepositoryTimeline<Self::Timeline>)> {
        self.timelines
            .lock()
            .unwrap()
            .iter()
            .map(|(timeline_id, timeline_entry)| {
                (
                    *timeline_id,
                    RepositoryTimeline::from(timeline_entry.clone()),
                )
            })
            .collect()
    }

    fn create_empty_timeline(
        &self,
        timelineid: ZTimelineId,
        initdb_lsn: Lsn,
    ) -> Result<Arc<LayeredTimeline>> {
        let mut timelines = self.timelines.lock().unwrap();

        // Create the timeline directory, and write initial metadata to file.
        crashsafe_dir::create_dir_all(self.conf.timeline_path(&timelineid, &self.tenant_id))?;

        let metadata = TimelineMetadata::new(Lsn(0), None, None, Lsn(0), initdb_lsn, initdb_lsn);
        Self::save_metadata(self.conf, timelineid, self.tenant_id, &metadata, true)?;

        let timeline = LayeredTimeline::new(
            self.conf,
            Arc::clone(&self.tenant_conf),
            metadata,
            None,
            timelineid,
            self.tenant_id,
            Arc::clone(&self.walredo_mgr),
            self.upload_layers,
        );
        timeline.layers.write().unwrap().next_open_layer_at = Some(initdb_lsn);

        let timeline = Arc::new(timeline);
        let r = timelines.insert(
            timelineid,
            LayeredTimelineEntry::Loaded(Arc::clone(&timeline)),
        );
        ensure!(
            r.is_none(),
            "assertion failure, inserted duplicate timeline"
        );
        Ok(timeline)
    }

    /// Branch a timeline
    fn branch_timeline(&self, src: ZTimelineId, dst: ZTimelineId, start_lsn: Lsn) -> Result<()> {
        // We need to hold this lock to prevent GC from starting at the same time. GC scans the directory to learn
        // about timelines, so otherwise a race condition is possible, where we create new timeline and GC
        // concurrently removes data that is needed by the new timeline.
        let _gc_cs = self.gc_cs.lock().unwrap();

        let mut timelines = self.timelines.lock().unwrap();
        let src_timeline = self
            .get_timeline_load_internal(src, &mut timelines)
            // message about timeline being remote is one .context up in the stack
            .context("failed to load timeline for branching")?
            .ok_or_else(|| anyhow::anyhow!("unknown timeline id: {}", &src))?;
        let latest_gc_cutoff_lsn = src_timeline.get_latest_gc_cutoff_lsn();
        src_timeline
            .check_lsn_is_in_scope(start_lsn, &latest_gc_cutoff_lsn)
            .context("invalid branch start lsn")?;

        let RecordLsn {
            last: src_last,
            prev: src_prev,
        } = src_timeline.get_last_record_rlsn();

        // Use src_prev from the source timeline only if we branched at the last record.
        let dst_prev = if src_last == start_lsn {
            Some(src_prev)
        } else {
            None
        };

        // create a new timeline directory
        let timelinedir = self.conf.timeline_path(&dst, &self.tenant_id);

        crashsafe_dir::create_dir(&timelinedir)?;

        // Create the metadata file, noting the ancestor of the new timeline.
        // There is initially no data in it, but all the read-calls know to look
        // into the ancestor.
        let metadata = TimelineMetadata::new(
            start_lsn,
            dst_prev,
            Some(src),
            start_lsn,
            *src_timeline.latest_gc_cutoff_lsn.read().unwrap(),
            src_timeline.initdb_lsn,
        );
        crashsafe_dir::create_dir_all(self.conf.timeline_path(&dst, &self.tenant_id))?;
        Self::save_metadata(self.conf, dst, self.tenant_id, &metadata, true)?;
        timelines.insert(dst, LayeredTimelineEntry::Unloaded { id: dst, metadata });

        info!("branched timeline {} from {} at {}", dst, src, start_lsn);

        Ok(())
    }

    /// Public entry point to GC. All the logic is in the private
    /// gc_iteration_internal function, this public facade just wraps it for
    /// metrics collection.
    fn gc_iteration(
        &self,
        target_timelineid: Option<ZTimelineId>,
        horizon: u64,
        pitr: Duration,
        checkpoint_before_gc: bool,
    ) -> Result<GcResult> {
        let timeline_str = target_timelineid
            .map(|x| x.to_string())
            .unwrap_or_else(|| "-".to_string());

        STORAGE_TIME
            .with_label_values(&["gc", &self.tenant_id.to_string(), &timeline_str])
            .observe_closure_duration(|| {
                self.gc_iteration_internal(target_timelineid, horizon, pitr, checkpoint_before_gc)
            })
    }

    fn compaction_iteration(&self) -> Result<()> {
        // Scan through the hashmap and collect a list of all the timelines,
        // while holding the lock. Then drop the lock and actually perform the
        // compactions.  We don't want to block everything else while the
        // compaction runs.
        let timelines = self.timelines.lock().unwrap();
        let timelines_to_compact = timelines
            .iter()
            .map(|(timelineid, timeline)| (*timelineid, timeline.clone()))
            .collect::<Vec<_>>();
        drop(timelines);

        for (timelineid, timeline) in &timelines_to_compact {
            let _entered =
                info_span!("compact", timeline = %timelineid, tenant = %self.tenant_id).entered();
            match timeline {
                LayeredTimelineEntry::Loaded(timeline) => {
                    timeline.compact()?;
                }
                LayeredTimelineEntry::Unloaded { .. } => {
                    debug!("Cannot compact remote timeline {}", timelineid)
                }
            }
        }

        Ok(())
    }

    ///
    /// Flush all in-memory data to disk.
    ///
    /// Used at shutdown.
    ///
    fn checkpoint(&self) -> Result<()> {
        // Scan through the hashmap and collect a list of all the timelines,
        // while holding the lock. Then drop the lock and actually perform the
        // checkpoints. We don't want to block everything else while the
        // checkpoint runs.
        let timelines = self.timelines.lock().unwrap();
        let timelines_to_compact = timelines
            .iter()
            // filter to get only loaded timelines
            .filter_map(|(timelineid, entry)| match entry {
                LayeredTimelineEntry::Loaded(timeline) => Some((timelineid, timeline)),
                LayeredTimelineEntry::Unloaded { .. } => {
                    debug!("Skipping checkpoint for unloaded timeline {}", timelineid);
                    None
                }
            })
            .map(|(timelineid, timeline)| (*timelineid, timeline.clone()))
            .collect::<Vec<_>>();
        drop(timelines);

        for (timelineid, timeline) in &timelines_to_compact {
            let _entered =
                info_span!("checkpoint", timeline = %timelineid, tenant = %self.tenant_id)
                    .entered();
            timeline.checkpoint(CheckpointConfig::Flush)?;
        }

        Ok(())
    }

    fn detach_timeline(&self, timeline_id: ZTimelineId) -> anyhow::Result<()> {
        let mut timelines = self.timelines.lock().unwrap();
        // check no child timelines, because detach will remove files, which will brake child branches
        // FIXME this can still be violated because we do not guarantee
        //   that all ancestors are downloaded/attached to the same pageserver
        let num_children = timelines
            .iter()
            .filter(|(_, entry)| entry.ancestor_timeline_id() == Some(timeline_id))
            .count();

        ensure!(
            num_children == 0,
            "Cannot detach timeline which has child timelines"
        );

        ensure!(
            timelines.remove(&timeline_id).is_some(),
            "Cannot detach timeline {timeline_id} that is not available locally"
        );
        Ok(())
    }

    fn apply_timeline_remote_sync_status_update(
        &self,
        timeline_id: ZTimelineId,
        timeline_sync_status_update: TimelineSyncStatusUpdate,
    ) -> Result<()> {
        debug!(
            "apply_timeline_remote_sync_status_update timeline_id: {} update: {:?}",
            timeline_id, timeline_sync_status_update
        );
        match timeline_sync_status_update {
            TimelineSyncStatusUpdate::Downloaded => {
                match self.timelines.lock().unwrap().entry(timeline_id) {
                    Entry::Occupied(_) => bail!("We completed a download for a timeline that already exists in repository. This is a bug."),
                    Entry::Vacant(entry) => {
                        // we need to get metadata of a timeline, another option is to pass it along with Downloaded status
                        let metadata = load_metadata(self.conf, timeline_id, self.tenant_id).context("failed to load local metadata")?;
                        // finally we make newly downloaded timeline visible to repository
                        entry.insert(LayeredTimelineEntry::Unloaded { id: timeline_id, metadata, })
                    },
                };
            }
        }
        Ok(())
    }

    fn get_remote_index(&self) -> &RemoteIndex {
        &self.remote_index
    }
}

#[derive(Clone)]
enum LayeredTimelineEntry {
    Loaded(Arc<LayeredTimeline>),
    Unloaded {
        id: ZTimelineId,
        metadata: TimelineMetadata,
    },
}

impl LayeredTimelineEntry {
    fn timeline_id(&self) -> ZTimelineId {
        match self {
            LayeredTimelineEntry::Loaded(timeline) => timeline.timeline_id,
            LayeredTimelineEntry::Unloaded { id, .. } => *id,
        }
    }

    fn ancestor_timeline_id(&self) -> Option<ZTimelineId> {
        match self {
            LayeredTimelineEntry::Loaded(timeline) => {
                timeline.ancestor_timeline.as_ref().map(|t| t.timeline_id())
            }
            LayeredTimelineEntry::Unloaded { metadata, .. } => metadata.ancestor_timeline(),
        }
    }

    fn ancestor_lsn(&self) -> Lsn {
        match self {
            LayeredTimelineEntry::Loaded(timeline) => timeline.ancestor_lsn,
            LayeredTimelineEntry::Unloaded { metadata, .. } => metadata.ancestor_lsn(),
        }
    }

    fn ensure_loaded(&self) -> anyhow::Result<&Arc<LayeredTimeline>> {
        match self {
            LayeredTimelineEntry::Loaded(timeline) => Ok(timeline),
            LayeredTimelineEntry::Unloaded { .. } => {
                anyhow::bail!("timeline is unloaded")
            }
        }
    }
}

impl From<LayeredTimelineEntry> for RepositoryTimeline<LayeredTimeline> {
    fn from(entry: LayeredTimelineEntry) -> Self {
        match entry {
            LayeredTimelineEntry::Loaded(timeline) => RepositoryTimeline::Loaded(timeline as _),
            LayeredTimelineEntry::Unloaded { metadata, .. } => {
                RepositoryTimeline::Unloaded { metadata }
            }
        }
    }
}

/// Private functions
impl LayeredRepository {
    pub fn get_checkpoint_distance(&self) -> u64 {
        let tenant_conf = self.tenant_conf.read().unwrap();
        tenant_conf
            .checkpoint_distance
            .unwrap_or(self.conf.default_tenant_conf.checkpoint_distance)
    }

    pub fn get_compaction_target_size(&self) -> u64 {
        let tenant_conf = self.tenant_conf.read().unwrap();
        tenant_conf
            .compaction_target_size
            .unwrap_or(self.conf.default_tenant_conf.compaction_target_size)
    }

    pub fn get_compaction_period(&self) -> Duration {
        let tenant_conf = self.tenant_conf.read().unwrap();
        tenant_conf
            .compaction_period
            .unwrap_or(self.conf.default_tenant_conf.compaction_period)
    }

    pub fn get_compaction_threshold(&self) -> usize {
        let tenant_conf = self.tenant_conf.read().unwrap();
        tenant_conf
            .compaction_threshold
            .unwrap_or(self.conf.default_tenant_conf.compaction_threshold)
    }

    pub fn get_gc_horizon(&self) -> u64 {
        let tenant_conf = self.tenant_conf.read().unwrap();
        tenant_conf
            .gc_horizon
            .unwrap_or(self.conf.default_tenant_conf.gc_horizon)
    }

    pub fn get_gc_period(&self) -> Duration {
        let tenant_conf = self.tenant_conf.read().unwrap();
        tenant_conf
            .gc_period
            .unwrap_or(self.conf.default_tenant_conf.gc_period)
    }

    pub fn get_image_creation_threshold(&self) -> usize {
        let tenant_conf = self.tenant_conf.read().unwrap();
        tenant_conf
            .image_creation_threshold
            .unwrap_or(self.conf.default_tenant_conf.image_creation_threshold)
    }

    pub fn get_pitr_interval(&self) -> Duration {
        let tenant_conf = self.tenant_conf.read().unwrap();
        tenant_conf
            .pitr_interval
            .unwrap_or(self.conf.default_tenant_conf.pitr_interval)
    }

    pub fn get_wal_receiver_connect_timeout(&self) -> Duration {
        let tenant_conf = self.tenant_conf.read().unwrap();
        tenant_conf
            .walreceiver_connect_timeout
            .unwrap_or(self.conf.default_tenant_conf.walreceiver_connect_timeout)
    }

    pub fn get_lagging_wal_timeout(&self) -> Duration {
        let tenant_conf = self.tenant_conf.read().unwrap();
        tenant_conf
            .lagging_wal_timeout
            .unwrap_or(self.conf.default_tenant_conf.lagging_wal_timeout)
    }

    pub fn get_max_lsn_wal_lag(&self) -> NonZeroU64 {
        let tenant_conf = self.tenant_conf.read().unwrap();
        tenant_conf
            .max_lsn_wal_lag
            .unwrap_or(self.conf.default_tenant_conf.max_lsn_wal_lag)
    }

    pub fn update_tenant_config(&self, new_tenant_conf: TenantConfOpt) -> Result<()> {
        let mut tenant_conf = self.tenant_conf.write().unwrap();

        tenant_conf.update(&new_tenant_conf);

        LayeredRepository::persist_tenant_config(self.conf, self.tenant_id, *tenant_conf)?;
        Ok(())
    }

    // Implementation of the public `get_timeline` function.
    // Differences from the public:
    //  * interface in that the caller must already hold the mutex on the 'timelines' hashmap.
    fn get_timeline_internal(
        &self,
        timelineid: ZTimelineId,
        timelines: &HashMap<ZTimelineId, LayeredTimelineEntry>,
    ) -> Option<LayeredTimelineEntry> {
        timelines.get(&timelineid).cloned()
    }

    // Implementation of the public `get_timeline_load` function.
    // Differences from the public:
    //  * interface in that the caller must already hold the mutex on the 'timelines' hashmap.
    fn get_timeline_load_internal(
        &self,
        timelineid: ZTimelineId,
        timelines: &mut HashMap<ZTimelineId, LayeredTimelineEntry>,
    ) -> anyhow::Result<Option<Arc<LayeredTimeline>>> {
        match timelines.get(&timelineid) {
            Some(entry) => match entry {
                LayeredTimelineEntry::Loaded(local_timeline) => {
                    debug!("timeline {} found loaded into memory", &timelineid);
                    return Ok(Some(Arc::clone(local_timeline)));
                }
                LayeredTimelineEntry::Unloaded { .. } => {}
            },
            None => {
                debug!("timeline {} not found", &timelineid);
                return Ok(None);
            }
        };
        debug!(
            "timeline {} found on a local disk, but not loaded into the memory, loading",
            &timelineid
        );
        let timeline = self.load_local_timeline(timelineid, timelines)?;
        let was_loaded = timelines.insert(
            timelineid,
            LayeredTimelineEntry::Loaded(Arc::clone(&timeline)),
        );
        ensure!(
            was_loaded.is_none()
                || matches!(was_loaded, Some(LayeredTimelineEntry::Unloaded { .. })),
            "assertion failure, inserted wrong timeline in an incorrect state"
        );
        Ok(Some(timeline))
    }

    fn load_local_timeline(
        &self,
        timeline_id: ZTimelineId,
        timelines: &mut HashMap<ZTimelineId, LayeredTimelineEntry>,
    ) -> anyhow::Result<Arc<LayeredTimeline>> {
        let metadata = load_metadata(self.conf, timeline_id, self.tenant_id)
            .context("failed to load metadata")?;
        let disk_consistent_lsn = metadata.disk_consistent_lsn();

        let ancestor = metadata
            .ancestor_timeline()
            .map(|ancestor_timeline_id| {
                trace!("loading {timeline_id}'s ancestor {}", &ancestor_timeline_id);
                self.get_timeline_load_internal(ancestor_timeline_id, timelines)
            })
            .transpose()
            .context("cannot load ancestor timeline")?
            .flatten()
            .map(LayeredTimelineEntry::Loaded);
        let _enter = info_span!("loading local timeline").entered();

        let timeline = LayeredTimeline::new(
            self.conf,
            Arc::clone(&self.tenant_conf),
            metadata,
            ancestor,
            timeline_id,
            self.tenant_id,
            Arc::clone(&self.walredo_mgr),
            self.upload_layers,
        );
        timeline
            .load_layer_map(disk_consistent_lsn)
            .context("failed to load layermap")?;

        Ok(Arc::new(timeline))
    }

    pub fn new(
        conf: &'static PageServerConf,
        tenant_conf: TenantConfOpt,
        walredo_mgr: Arc<dyn WalRedoManager + Send + Sync>,
        tenant_id: ZTenantId,
        remote_index: RemoteIndex,
        upload_layers: bool,
    ) -> LayeredRepository {
        LayeredRepository {
            tenant_id,
            conf,
            tenant_conf: Arc::new(RwLock::new(tenant_conf)),
            timelines: Mutex::new(HashMap::new()),
            gc_cs: Mutex::new(()),
            walredo_mgr,
            remote_index,
            upload_layers,
        }
    }

    /// Locate and load config
    pub fn load_tenant_config(
        conf: &'static PageServerConf,
        tenantid: ZTenantId,
    ) -> anyhow::Result<TenantConfOpt> {
        let target_config_path = TenantConf::path(conf, tenantid);

        info!("load tenantconf from {}", target_config_path.display());

        // FIXME If the config file is not found, assume that we're attaching
        // a detached tenant and config is passed via attach command.
        // https://github.com/neondatabase/neon/issues/1555
        if !target_config_path.exists() {
            info!(
                "Zenith tenant config is not found in {}",
                target_config_path.display()
            );
            return Ok(Default::default());
        }

        // load and parse file
        let config = fs::read_to_string(target_config_path)?;

        let toml = config.parse::<toml_edit::Document>()?;

        let mut tenant_conf: TenantConfOpt = Default::default();
        for (key, item) in toml.iter() {
            match key {
                "tenant_config" => {
                    tenant_conf = PageServerConf::parse_toml_tenant_conf(item)?;
                }
                _ => bail!("unrecognized pageserver option '{}'", key),
            }
        }

        Ok(tenant_conf)
    }

    pub fn persist_tenant_config(
        conf: &'static PageServerConf,
        tenantid: ZTenantId,
        tenant_conf: TenantConfOpt,
    ) -> anyhow::Result<()> {
        let _enter = info_span!("saving tenantconf").entered();
        let target_config_path = TenantConf::path(conf, tenantid);
        info!("save tenantconf to {}", target_config_path.display());

        let mut conf_content = r#"# This file contains a specific per-tenant's config.
#  It is read in case of pageserver restart.

[tenant_config]
"#
        .to_string();

        // Convert the config to a toml file.
        conf_content += &toml_edit::easy::to_string(&tenant_conf)?;

        fs::write(&target_config_path, conf_content).with_context(|| {
            format!(
                "Failed to write config file into path '{}'",
                target_config_path.display()
            )
        })
    }

    /// Save timeline metadata to file
    pub fn save_metadata(
        conf: &'static PageServerConf,
        timelineid: ZTimelineId,
        tenantid: ZTenantId,
        data: &TimelineMetadata,
        first_save: bool,
    ) -> Result<()> {
        let _enter = info_span!("saving metadata").entered();
        let path = metadata_path(conf, timelineid, tenantid);
        // use OpenOptions to ensure file presence is consistent with first_save
        let mut file = VirtualFile::open_with_options(
            &path,
            OpenOptions::new().write(true).create_new(first_save),
        )?;

        let metadata_bytes = data.to_bytes().context("Failed to get metadata bytes")?;

        if file.write(&metadata_bytes)? != metadata_bytes.len() {
            bail!("Could not write all the metadata bytes in a single call");
        }
        file.sync_all()?;

        // fsync the parent directory to ensure the directory entry is durable
        if first_save {
            let timeline_dir = File::open(
                &path
                    .parent()
                    .expect("Metadata should always have a parent dir"),
            )?;
            timeline_dir.sync_all()?;
        }

        Ok(())
    }

    //
    // How garbage collection works:
    //
    //                    +--bar------------->
    //                   /
    //             +----+-----foo---------------->
    //            /
    // ----main--+-------------------------->
    //                \
    //                 +-----baz-------->
    //
    //
    // 1. Grab a mutex to prevent new timelines from being created
    // 2. Scan all timelines, and on each timeline, make note of the
    //    all the points where other timelines have been branched off.
    //    We will refrain from removing page versions at those LSNs.
    // 3. For each timeline, scan all layer files on the timeline.
    //    Remove all files for which a newer file exists and which
    //    don't cover any branch point LSNs.
    //
    // TODO:
    // - if a relation has a non-incremental persistent layer on a child branch, then we
    //   don't need to keep that in the parent anymore. But currently
    //   we do.
    fn gc_iteration_internal(
        &self,
        target_timelineid: Option<ZTimelineId>,
        horizon: u64,
        pitr: Duration,
        checkpoint_before_gc: bool,
    ) -> Result<GcResult> {
        let _span_guard =
            info_span!("gc iteration", tenant = %self.tenant_id, timeline = ?target_timelineid)
                .entered();
        let mut totals: GcResult = Default::default();
        let now = Instant::now();

        // grab mutex to prevent new timelines from being created here.
        let _gc_cs = self.gc_cs.lock().unwrap();

        // Scan all timelines. For each timeline, remember the timeline ID and
        // the branch point where it was created.
        let mut all_branchpoints: BTreeSet<(ZTimelineId, Lsn)> = BTreeSet::new();
        let mut timeline_ids = Vec::new();
        let mut timelines = self.timelines.lock().unwrap();

        for (timeline_id, timeline_entry) in timelines.iter() {
            timeline_ids.push(*timeline_id);

            // This is unresolved question for now, how to do gc in presence of remote timelines
            // especially when this is combined with branching.
            // Somewhat related: https://github.com/zenithdb/zenith/issues/999
            if let Some(ancestor_timeline_id) = &timeline_entry.ancestor_timeline_id() {
                // If target_timeline is specified, we only need to know branchpoints of its children
                if let Some(timelineid) = target_timelineid {
                    if ancestor_timeline_id == &timelineid {
                        all_branchpoints
                            .insert((*ancestor_timeline_id, timeline_entry.ancestor_lsn()));
                    }
                }
                // Collect branchpoints for all timelines
                else {
                    all_branchpoints.insert((*ancestor_timeline_id, timeline_entry.ancestor_lsn()));
                }
            }
        }

        // Ok, we now know all the branch points.
        // Perform GC for each timeline.
        for timelineid in timeline_ids.into_iter() {
            if thread_mgr::is_shutdown_requested() {
                // We were requested to shut down. Stop and return with the progress we
                // made.
                break;
            }

            // Timeline is known to be local and loaded.
            let timeline = self
                .get_timeline_load_internal(timelineid, &mut *timelines)?
                .expect("checked above that timeline is local and loaded");

            // If target_timeline is specified, only GC it
            if let Some(target_timelineid) = target_timelineid {
                if timelineid != target_timelineid {
                    continue;
                }
            }

            if let Some(cutoff) = timeline.get_last_record_lsn().checked_sub(horizon) {
                drop(timelines);
                let branchpoints: Vec<Lsn> = all_branchpoints
                    .range((
                        Included((timelineid, Lsn(0))),
                        Included((timelineid, Lsn(u64::MAX))),
                    ))
                    .map(|&x| x.1)
                    .collect();

                // If requested, force flush all in-memory layers to disk first,
                // so that they too can be garbage collected. That's
                // used in tests, so we want as deterministic results as possible.
                if checkpoint_before_gc {
                    timeline.checkpoint(CheckpointConfig::Forced)?;
                    info!("timeline {} checkpoint_before_gc done", timelineid);
                }
                timeline.update_gc_info(branchpoints, cutoff, pitr);
                let result = timeline.gc()?;

                totals += result;
                timelines = self.timelines.lock().unwrap();
            }
        }

        totals.elapsed = now.elapsed();
        Ok(totals)
    }

    pub fn tenant_id(&self) -> ZTenantId {
        self.tenant_id
    }
}

pub struct LayeredTimeline {
    conf: &'static PageServerConf,
    tenant_conf: Arc<RwLock<TenantConfOpt>>,

    tenant_id: ZTenantId,
    timeline_id: ZTimelineId,

    layers: RwLock<LayerMap>,

    last_freeze_at: AtomicLsn,

    // WAL redo manager
    walredo_mgr: Arc<dyn WalRedoManager + Sync + Send>,

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
    ancestor_timeline: Option<LayeredTimelineEntry>,
    ancestor_lsn: Lsn,

    // Metrics
    reconstruct_time_histo: Histogram,
    materialized_page_cache_hit_counter: IntCounter,
    flush_time_histo: Histogram,
    compact_time_histo: Histogram,
    create_images_time_histo: Histogram,
    last_record_gauge: IntGauge,
    wait_lsn_time_histo: Histogram,

    /// If `true`, will backup its files that appear after each checkpointing to the remote storage.
    upload_layers: AtomicBool,

    /// Ensures layers aren't frozen by checkpointer between
    /// [`LayeredTimeline::get_layer_for_write`] and layer reads.
    /// Locked automatically by [`LayeredTimelineWriter`] and checkpointer.
    /// Must always be acquired before the layer map/individual layer lock
    /// to avoid deadlock.
    write_lock: Mutex<()>,

    /// Used to ensure that there is only one thread
    layer_flush_lock: Mutex<()>,

    // Prevent concurrent compactions.
    // Compactions are normally performed by one thread. But compaction can also be manually
    // requested by admin (that's used in tests). These forced compactions run in a different
    // thread and could be triggered at the same time as a normal, timed compaction.
    compaction_cs: Mutex<()>,

    // Needed to ensure that we can't create a branch at a point that was already garbage collected
    latest_gc_cutoff_lsn: RwLock<Lsn>,

    // List of child timelines and their branch points. This is needed to avoid
    // garbage collecting data that is still needed by the child timelines.
    gc_info: RwLock<GcInfo>,

    // It may change across major versions so for simplicity
    // keep it after running initdb for a timeline.
    // It is needed in checks when we want to error on some operations
    // when they are requested for pre-initdb lsn.
    // It can be unified with latest_gc_cutoff_lsn under some "first_valid_lsn",
    // though lets keep them both for better error visibility.
    initdb_lsn: Lsn,
}

///
/// Information about how much history needs to be retained, needed by
/// Garbage Collection.
///
struct GcInfo {
    /// Specific LSNs that are needed.
    ///
    /// Currently, this includes all points where child branches have
    /// been forked off from. In the future, could also include
    /// explicit user-defined snapshot points.
    retain_lsns: Vec<Lsn>,

    /// In addition to 'retain_lsns', keep everything newer than this
    /// point.
    ///
    /// This is calculated by subtracting 'gc_horizon' setting from
    /// last-record LSN
    ///
    /// FIXME: is this inclusive or exclusive?
    cutoff: Lsn,

    /// In addition to 'retain_lsns', keep everything newer than 'SystemTime::now()'
    /// minus 'pitr_interval'
    ///
    pitr: Duration,
}

/// Public interface functions
impl Timeline for LayeredTimeline {
    fn get_ancestor_lsn(&self) -> Lsn {
        self.ancestor_lsn
    }

    fn get_ancestor_timeline_id(&self) -> Option<ZTimelineId> {
        self.ancestor_timeline
            .as_ref()
            .map(LayeredTimelineEntry::timeline_id)
    }

    /// Wait until WAL has been received up to the given LSN.
    fn wait_lsn(&self, lsn: Lsn) -> anyhow::Result<()> {
        // This should never be called from the WAL receiver thread, because that could lead
        // to a deadlock.
        ensure!(
            !IS_WAL_RECEIVER.with(|c| c.get()),
            "wait_lsn called by WAL receiver thread"
        );

        self.wait_lsn_time_histo.observe_closure_duration(
            || self.last_record_lsn
                .wait_for_timeout(lsn, self.conf.wait_lsn_timeout)
                .with_context(|| {
                    format!(
                        "Timed out while waiting for WAL record at LSN {} to arrive, last_record_lsn {} disk consistent LSN={}",
                        lsn, self.get_last_record_lsn(), self.get_disk_consistent_lsn()
                    )
                }))?;

        Ok(())
    }

    fn get_latest_gc_cutoff_lsn(&self) -> RwLockReadGuard<Lsn> {
        self.latest_gc_cutoff_lsn.read().unwrap()
    }

    /// Look up the value with the given a key
    fn get(&self, key: Key, lsn: Lsn) -> Result<Bytes> {
        debug_assert!(lsn <= self.get_last_record_lsn());

        // Check the page cache. We will get back the most recent page with lsn <= `lsn`.
        // The cached image can be returned directly if there is no WAL between the cached image
        // and requested LSN. The cached image can also be used to reduce the amount of WAL needed
        // for redo.
        let cached_page_img = match self.lookup_cached_page(&key, lsn) {
            Some((cached_lsn, cached_img)) => {
                match cached_lsn.cmp(&lsn) {
                    Ordering::Less => {} // there might be WAL between cached_lsn and lsn, we need to check
                    Ordering::Equal => return Ok(cached_img), // exact LSN match, return the image
                    Ordering::Greater => panic!(), // the returned lsn should never be after the requested lsn
                }
                Some((cached_lsn, cached_img))
            }
            None => None,
        };

        let mut reconstruct_state = ValueReconstructState {
            records: Vec::new(),
            img: cached_page_img,
        };

        self.get_reconstruct_data(key, lsn, &mut reconstruct_state)?;

        self.reconstruct_time_histo
            .observe_closure_duration(|| self.reconstruct_value(key, lsn, reconstruct_state))
    }

    /// Public entry point for checkpoint(). All the logic is in the private
    /// checkpoint_internal function, this public facade just wraps it for
    /// metrics collection.
    fn checkpoint(&self, cconf: CheckpointConfig) -> anyhow::Result<()> {
        match cconf {
            CheckpointConfig::Flush => {
                self.freeze_inmem_layer(false);
                self.flush_frozen_layers(true)
            }
            CheckpointConfig::Forced => {
                self.freeze_inmem_layer(false);
                self.flush_frozen_layers(true)?;
                self.compact()
            }
        }
    }

    ///
    /// Validate lsn against initdb_lsn and latest_gc_cutoff_lsn.
    ///
    fn check_lsn_is_in_scope(
        &self,
        lsn: Lsn,
        latest_gc_cutoff_lsn: &RwLockReadGuard<Lsn>,
    ) -> Result<()> {
        ensure!(
            lsn >= **latest_gc_cutoff_lsn,
            "LSN {} is earlier than latest GC horizon {} (we might've already garbage collected needed data)",
            lsn,
            **latest_gc_cutoff_lsn,
        );
        Ok(())
    }

    fn get_last_record_lsn(&self) -> Lsn {
        self.last_record_lsn.load().last
    }

    fn get_prev_record_lsn(&self) -> Lsn {
        self.last_record_lsn.load().prev
    }

    fn get_last_record_rlsn(&self) -> RecordLsn {
        self.last_record_lsn.load()
    }

    fn get_disk_consistent_lsn(&self) -> Lsn {
        self.disk_consistent_lsn.load()
    }

    fn writer<'a>(&'a self) -> Box<dyn TimelineWriter + 'a> {
        Box::new(LayeredTimelineWriter {
            tl: self,
            _write_guard: self.write_lock.lock().unwrap(),
        })
    }
}

impl LayeredTimeline {
    fn get_checkpoint_distance(&self) -> u64 {
        let tenant_conf = self.tenant_conf.read().unwrap();
        tenant_conf
            .checkpoint_distance
            .unwrap_or(self.conf.default_tenant_conf.checkpoint_distance)
    }

    fn get_compaction_target_size(&self) -> u64 {
        let tenant_conf = self.tenant_conf.read().unwrap();
        tenant_conf
            .compaction_target_size
            .unwrap_or(self.conf.default_tenant_conf.compaction_target_size)
    }

    fn get_compaction_threshold(&self) -> usize {
        let tenant_conf = self.tenant_conf.read().unwrap();
        tenant_conf
            .compaction_threshold
            .unwrap_or(self.conf.default_tenant_conf.compaction_threshold)
    }

    fn get_image_creation_threshold(&self) -> usize {
        let tenant_conf = self.tenant_conf.read().unwrap();
        tenant_conf
            .image_creation_threshold
            .unwrap_or(self.conf.default_tenant_conf.image_creation_threshold)
    }

    /// Open a Timeline handle.
    ///
    /// Loads the metadata for the timeline into memory, but not the layer map.
    #[allow(clippy::too_many_arguments)]
    fn new(
        conf: &'static PageServerConf,
        tenant_conf: Arc<RwLock<TenantConfOpt>>,
        metadata: TimelineMetadata,
        ancestor: Option<LayeredTimelineEntry>,
        timeline_id: ZTimelineId,
        tenant_id: ZTenantId,
        walredo_mgr: Arc<dyn WalRedoManager + Send + Sync>,
        upload_layers: bool,
    ) -> LayeredTimeline {
        let reconstruct_time_histo = RECONSTRUCT_TIME
            .get_metric_with_label_values(&[&tenant_id.to_string(), &timeline_id.to_string()])
            .unwrap();
        let materialized_page_cache_hit_counter = MATERIALIZED_PAGE_CACHE_HIT
            .get_metric_with_label_values(&[&tenant_id.to_string(), &timeline_id.to_string()])
            .unwrap();
        let flush_time_histo = STORAGE_TIME
            .get_metric_with_label_values(&[
                "layer flush",
                &tenant_id.to_string(),
                &timeline_id.to_string(),
            ])
            .unwrap();
        let compact_time_histo = STORAGE_TIME
            .get_metric_with_label_values(&[
                "compact",
                &tenant_id.to_string(),
                &timeline_id.to_string(),
            ])
            .unwrap();
        let create_images_time_histo = STORAGE_TIME
            .get_metric_with_label_values(&[
                "create images",
                &tenant_id.to_string(),
                &timeline_id.to_string(),
            ])
            .unwrap();
        let last_record_gauge = LAST_RECORD_LSN
            .get_metric_with_label_values(&[&tenant_id.to_string(), &timeline_id.to_string()])
            .unwrap();
        let wait_lsn_time_histo = WAIT_LSN_TIME
            .get_metric_with_label_values(&[&tenant_id.to_string(), &timeline_id.to_string()])
            .unwrap();

        LayeredTimeline {
            conf,
            tenant_conf,
            timeline_id,
            tenant_id,
            layers: RwLock::new(LayerMap::default()),

            walredo_mgr,

            // initialize in-memory 'last_record_lsn' from 'disk_consistent_lsn'.
            last_record_lsn: SeqWait::new(RecordLsn {
                last: metadata.disk_consistent_lsn(),
                prev: metadata.prev_record_lsn().unwrap_or(Lsn(0)),
            }),
            disk_consistent_lsn: AtomicLsn::new(metadata.disk_consistent_lsn().0),

            last_freeze_at: AtomicLsn::new(metadata.disk_consistent_lsn().0),

            ancestor_timeline: ancestor,
            ancestor_lsn: metadata.ancestor_lsn(),

            reconstruct_time_histo,
            materialized_page_cache_hit_counter,
            flush_time_histo,
            compact_time_histo,
            create_images_time_histo,
            last_record_gauge,
            wait_lsn_time_histo,

            upload_layers: AtomicBool::new(upload_layers),

            write_lock: Mutex::new(()),
            layer_flush_lock: Mutex::new(()),
            compaction_cs: Mutex::new(()),

            gc_info: RwLock::new(GcInfo {
                retain_lsns: Vec::new(),
                cutoff: Lsn(0),
                pitr: Duration::ZERO,
            }),

            latest_gc_cutoff_lsn: RwLock::new(metadata.latest_gc_cutoff_lsn()),
            initdb_lsn: metadata.initdb_lsn(),
        }
    }

    ///
    /// Scan the timeline directory to populate the layer map.
    /// Returns all timeline-related files that were found and loaded.
    ///
    fn load_layer_map(&self, disk_consistent_lsn: Lsn) -> anyhow::Result<()> {
        let mut layers = self.layers.write().unwrap();
        let mut num_layers = 0;

        // Scan timeline directory and create ImageFileName and DeltaFilename
        // structs representing all files on disk
        let timeline_path = self.conf.timeline_path(&self.timeline_id, &self.tenant_id);

        for direntry in fs::read_dir(timeline_path)? {
            let direntry = direntry?;
            let fname = direntry.file_name();
            let fname = fname.to_string_lossy();

            if let Some(imgfilename) = ImageFileName::parse_str(&fname) {
                // create an ImageLayer struct for each image file.
                if imgfilename.lsn > disk_consistent_lsn {
                    warn!(
                        "found future image layer {} on timeline {} disk_consistent_lsn is {}",
                        imgfilename, self.timeline_id, disk_consistent_lsn
                    );

                    rename_to_backup(direntry.path())?;
                    continue;
                }

                let layer =
                    ImageLayer::new(self.conf, self.timeline_id, self.tenant_id, &imgfilename);

                trace!("found layer {}", layer.filename().display());
                layers.insert_historic(Arc::new(layer));
                num_layers += 1;
            } else if let Some(deltafilename) = DeltaFileName::parse_str(&fname) {
                // Create a DeltaLayer struct for each delta file.
                // The end-LSN is exclusive, while disk_consistent_lsn is
                // inclusive. For example, if disk_consistent_lsn is 100, it is
                // OK for a delta layer to have end LSN 101, but if the end LSN
                // is 102, then it might not have been fully flushed to disk
                // before crash.
                if deltafilename.lsn_range.end > disk_consistent_lsn + 1 {
                    warn!(
                        "found future delta layer {} on timeline {} disk_consistent_lsn is {}",
                        deltafilename, self.timeline_id, disk_consistent_lsn
                    );

                    rename_to_backup(direntry.path())?;
                    continue;
                }

                let layer =
                    DeltaLayer::new(self.conf, self.timeline_id, self.tenant_id, &deltafilename);

                trace!("found layer {}", layer.filename().display());
                layers.insert_historic(Arc::new(layer));
                num_layers += 1;
            } else if fname == METADATA_FILE_NAME || fname.ends_with(".old") {
                // ignore these
            } else if is_ephemeral_file(&fname) {
                // Delete any old ephemeral files
                trace!("deleting old ephemeral file in timeline dir: {}", fname);
                fs::remove_file(direntry.path())?;
            } else {
                warn!("unrecognized filename in timeline dir: {}", fname);
            }
        }

        layers.next_open_layer_at = Some(Lsn(disk_consistent_lsn.0) + 1);

        info!(
            "loaded layer map with {} layers at {}",
            num_layers, disk_consistent_lsn
        );

        Ok(())
    }

    ///
    /// Get a handle to a Layer for reading.
    ///
    /// The returned Layer might be from an ancestor timeline, if the
    /// segment hasn't been updated on this timeline yet.
    ///
    /// This function takes the current timeline's locked LayerMap as an argument,
    /// so callers can avoid potential race conditions.
    fn get_reconstruct_data(
        &self,
        key: Key,
        request_lsn: Lsn,
        reconstruct_state: &mut ValueReconstructState,
    ) -> anyhow::Result<()> {
        // Start from the current timeline.
        let mut timeline_owned;
        let mut timeline = self;

        // For debugging purposes, collect the path of layers that we traversed
        // through. It's included in the error message if we fail to find the key.
        let mut traversal_path: Vec<(ValueReconstructResult, Lsn, Arc<dyn Layer>)> = Vec::new();

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
            // The function should have updated 'state'
            //info!("CALLED for {} at {}: {:?} with {} records, cached {}", key, cont_lsn, result, reconstruct_state.records.len(), cached_lsn);
            match result {
                ValueReconstructResult::Complete => return Ok(()),
                ValueReconstructResult::Continue => {
                    // If we reached an earlier cached page image, we're done.
                    if cont_lsn == cached_lsn + 1 {
                        self.materialized_page_cache_hit_counter.inc_by(1);
                        return Ok(());
                    }
                    if prev_lsn <= cont_lsn {
                        // Didn't make any progress in last iteration. Error out to avoid
                        // getting stuck in the loop.
                        return layer_traversal_error(format!(
                            "could not find layer with more data for key {} at LSN {}, request LSN {}, ancestor {}",
                            key,
                            Lsn(cont_lsn.0 - 1),
                            request_lsn,
                            timeline.ancestor_lsn
                        ), traversal_path);
                    }
                    prev_lsn = cont_lsn;
                }
                ValueReconstructResult::Missing => {
                    return layer_traversal_error(
                        format!(
                            "could not find data for key {} at LSN {}, for request at LSN {}",
                            key, cont_lsn, request_lsn
                        ),
                        traversal_path,
                    );
                }
            }

            // Recurse into ancestor if needed
            if Lsn(cont_lsn.0 - 1) <= timeline.ancestor_lsn {
                trace!(
                    "going into ancestor {}, cont_lsn is {}",
                    timeline.ancestor_lsn,
                    cont_lsn
                );
                let ancestor = timeline.get_ancestor_timeline()?;
                timeline_owned = ancestor;
                timeline = &*timeline_owned;
                prev_lsn = Lsn(u64::MAX);
                continue;
            }

            let layers = timeline.layers.read().unwrap();

            // Check the open and frozen in-memory layers first, in order from newest
            // to oldest.
            if let Some(open_layer) = &layers.open_layer {
                let start_lsn = open_layer.get_lsn_range().start;
                if cont_lsn > start_lsn {
                    //info!("CHECKING for {} at {} on open layer {}", key, cont_lsn, open_layer.filename().display());
                    // Get all the data needed to reconstruct the page version from this layer.
                    // But if we have an older cached page image, no need to go past that.
                    let lsn_floor = max(cached_lsn + 1, start_lsn);
                    result = open_layer.get_value_reconstruct_data(
                        key,
                        lsn_floor..cont_lsn,
                        reconstruct_state,
                    )?;
                    cont_lsn = lsn_floor;
                    traversal_path.push((result, cont_lsn, open_layer.clone()));
                    continue;
                }
            }
            for frozen_layer in layers.frozen_layers.iter().rev() {
                let start_lsn = frozen_layer.get_lsn_range().start;
                if cont_lsn > start_lsn {
                    //info!("CHECKING for {} at {} on frozen layer {}", key, cont_lsn, frozen_layer.filename().display());
                    let lsn_floor = max(cached_lsn + 1, start_lsn);
                    result = frozen_layer.get_value_reconstruct_data(
                        key,
                        lsn_floor..cont_lsn,
                        reconstruct_state,
                    )?;
                    cont_lsn = lsn_floor;
                    traversal_path.push((result, cont_lsn, frozen_layer.clone()));
                    continue 'outer;
                }
            }

            if let Some(SearchResult { lsn_floor, layer }) = layers.search(key, cont_lsn)? {
                //info!("CHECKING for {} at {} on historic layer {}", key, cont_lsn, layer.filename().display());

                let lsn_floor = max(cached_lsn + 1, lsn_floor);
                result = layer.get_value_reconstruct_data(
                    key,
                    lsn_floor..cont_lsn,
                    reconstruct_state,
                )?;
                cont_lsn = lsn_floor;
                traversal_path.push((result, cont_lsn, layer));
            } else if timeline.ancestor_timeline.is_some() {
                // Nothing on this timeline. Traverse to parent
                result = ValueReconstructResult::Continue;
                cont_lsn = Lsn(timeline.ancestor_lsn.0 + 1);
            } else {
                // Nothing found
                result = ValueReconstructResult::Missing;
            }
        }
    }

    fn lookup_cached_page(&self, key: &Key, lsn: Lsn) -> Option<(Lsn, Bytes)> {
        let cache = page_cache::get();

        // FIXME: It's pointless to check the cache for things that are not 8kB pages.
        // We should look at the key to determine if it's a cacheable object
        let (lsn, read_guard) =
            cache.lookup_materialized_page(self.tenant_id, self.timeline_id, key, lsn)?;
        let img = Bytes::from(read_guard.to_vec());
        Some((lsn, img))
    }

    fn get_ancestor_timeline(&self) -> Result<Arc<LayeredTimeline>> {
        let ancestor = self
            .ancestor_timeline
            .as_ref()
            .with_context(|| {
                format!(
                    "Ancestor is missing. Timeline id: {} Ancestor id {:?}",
                    self.timeline_id,
                    self.get_ancestor_timeline_id(),
                )
            })?
            .ensure_loaded()
            .with_context(|| {
                format!(
                    "Ancestor timeline is not loaded. Timeline id: {} Ancestor id {:?}",
                    self.timeline_id,
                    self.get_ancestor_timeline_id(),
                )
            })?;
        Ok(Arc::clone(ancestor))
    }

    ///
    /// Get a handle to the latest layer for appending.
    ///
    fn get_layer_for_write(&self, lsn: Lsn) -> anyhow::Result<Arc<InMemoryLayer>> {
        let mut layers = self.layers.write().unwrap();

        ensure!(lsn.is_aligned());

        let last_record_lsn = self.get_last_record_lsn();
        ensure!(
            lsn > last_record_lsn,
            "cannot modify relation after advancing last_record_lsn (incoming_lsn={}, last_record_lsn={})",
            lsn,
            last_record_lsn,
        );

        // Do we have a layer open for writing already?
        let layer;
        if let Some(open_layer) = &layers.open_layer {
            if open_layer.get_lsn_range().start > lsn {
                bail!("unexpected open layer in the future");
            }

            layer = Arc::clone(open_layer);
        } else {
            // No writeable layer yet. Create one.
            let start_lsn = layers.next_open_layer_at.unwrap();

            trace!(
                "creating layer for write at {}/{} for record at {}",
                self.timeline_id,
                start_lsn,
                lsn
            );
            let new_layer =
                InMemoryLayer::create(self.conf, self.timeline_id, self.tenant_id, start_lsn)?;
            let layer_rc = Arc::new(new_layer);

            layers.open_layer = Some(Arc::clone(&layer_rc));
            layers.next_open_layer_at = None;

            layer = layer_rc;
        }
        Ok(layer)
    }

    fn put_value(&self, key: Key, lsn: Lsn, val: Value) -> Result<()> {
        //info!("PUT: key {} at {}", key, lsn);
        let layer = self.get_layer_for_write(lsn)?;
        layer.put_value(key, lsn, val)?;
        Ok(())
    }

    fn put_tombstone(&self, key_range: Range<Key>, lsn: Lsn) -> Result<()> {
        let layer = self.get_layer_for_write(lsn)?;
        layer.put_tombstone(key_range, lsn)?;

        Ok(())
    }

    fn finish_write(&self, new_lsn: Lsn) {
        assert!(new_lsn.is_aligned());

        self.last_record_gauge.set(new_lsn.0 as i64);
        self.last_record_lsn.advance(new_lsn);
    }

    fn freeze_inmem_layer(&self, write_lock_held: bool) {
        // Freeze the current open in-memory layer. It will be written to disk on next
        // iteration.
        let _write_guard = if write_lock_held {
            None
        } else {
            Some(self.write_lock.lock().unwrap())
        };
        let mut layers = self.layers.write().unwrap();
        if let Some(open_layer) = &layers.open_layer {
            let open_layer_rc = Arc::clone(open_layer);
            // Does this layer need freezing?
            let end_lsn = Lsn(self.get_last_record_lsn().0 + 1);
            open_layer.freeze(end_lsn);

            // The layer is no longer open, update the layer map to reflect this.
            // We will replace it with on-disk historics below.
            layers.frozen_layers.push_back(open_layer_rc);
            layers.open_layer = None;
            layers.next_open_layer_at = Some(end_lsn);
            self.last_freeze_at.store(end_lsn);
        }
        drop(layers);
    }

    ///
    /// Check if more than 'checkpoint_distance' of WAL has been accumulated
    /// in the in-memory layer, and initiate flushing it if so.
    ///
    pub fn check_checkpoint_distance(self: &Arc<LayeredTimeline>) -> Result<()> {
        let last_lsn = self.get_last_record_lsn();

        // Has more than 'checkpoint_distance' of WAL been accumulated?
        let distance = last_lsn.widening_sub(self.last_freeze_at.load());
        if distance >= self.get_checkpoint_distance().into() {
            // Yes. Freeze the current in-memory layer.
            self.freeze_inmem_layer(true);
            self.last_freeze_at.store(last_lsn);

            // Launch a thread to flush the frozen layer to disk, unless
            // a thread was already running. (If the thread was running
            // at the time that we froze the layer, it must've seen the
            // the layer we just froze before it exited; see comments
            // in flush_frozen_layers())
            if let Ok(guard) = self.layer_flush_lock.try_lock() {
                drop(guard);
                let self_clone = Arc::clone(self);
                thread_mgr::spawn(
                    thread_mgr::ThreadKind::LayerFlushThread,
                    Some(self.tenant_id),
                    Some(self.timeline_id),
                    "layer flush thread",
                    false,
                    move || self_clone.flush_frozen_layers(false),
                )?;
            }
        }
        Ok(())
    }

    /// Flush all frozen layers to disk.
    ///
    /// Only one thread at a time can be doing layer-flushing for a
    /// given timeline. If 'wait' is true, and another thread is
    /// currently doing the flushing, this function will wait for it
    /// to finish. If 'wait' is false, this function will return
    /// immediately instead.
    fn flush_frozen_layers(&self, wait: bool) -> Result<()> {
        let flush_lock_guard = if wait {
            self.layer_flush_lock.lock().unwrap()
        } else {
            match self.layer_flush_lock.try_lock() {
                Ok(guard) => guard,
                Err(TryLockError::WouldBlock) => return Ok(()),
                Err(TryLockError::Poisoned(err)) => panic!("{:?}", err),
            }
        };

        let timer = self.flush_time_histo.start_timer();

        loop {
            let layers = self.layers.read().unwrap();
            if let Some(frozen_layer) = layers.frozen_layers.front() {
                let frozen_layer = Arc::clone(frozen_layer);
                drop(layers); // to allow concurrent reads and writes
                self.flush_frozen_layer(frozen_layer)?;
            } else {
                // Drop the 'layer_flush_lock' *before* 'layers'. That
                // way, if you freeze a layer, and then call
                // flush_frozen_layers(false), it is guaranteed that
                // if another thread was busy flushing layers and the
                // call therefore returns immediately, the other
                // thread will have seen the newly-frozen layer and
                // will flush that too (assuming no errors).
                drop(flush_lock_guard);
                drop(layers);
                break;
            }
        }

        timer.stop_and_record();

        Ok(())
    }

    /// Flush one frozen in-memory layer to disk, as a new delta layer.
    fn flush_frozen_layer(&self, frozen_layer: Arc<InMemoryLayer>) -> Result<()> {
        let new_delta = frozen_layer.write_to_disk()?;
        let new_delta_path = new_delta.path();

        // Sync the new layer to disk.
        //
        // We must also fsync the timeline dir to ensure the directory entries for
        // new layer files are durable
        //
        // TODO: If we're running inside 'flush_frozen_layers' and there are multiple
        // files to flush, it might be better to first write them all, and then fsync
        // them all in parallel.
        par_fsync::par_fsync(&[
            new_delta_path.clone(),
            self.conf.timeline_path(&self.timeline_id, &self.tenant_id),
        ])?;
        fail_point!("flush-frozen-before-sync");

        // Finally, replace the frozen in-memory layer with the new on-disk layer
        {
            let mut layers = self.layers.write().unwrap();
            let l = layers.frozen_layers.pop_front();

            // Only one thread may call this function at a time (for this
            // timeline). If two threads tried to flush the same frozen
            // layer to disk at the same time, that would not work.
            assert!(Arc::ptr_eq(&l.unwrap(), &frozen_layer));

            // Add the new delta layer to the LayerMap
            layers.insert_historic(Arc::new(new_delta));

            // release lock on 'layers'
        }

        // Update the metadata file, with new 'disk_consistent_lsn'
        //
        // TODO: This perhaps should be done in 'flush_frozen_layers', after flushing
        // *all* the layers, to avoid fsyncing the file multiple times.
        let disk_consistent_lsn = Lsn(frozen_layer.get_lsn_range().end.0 - 1);
        fail_point!("checkpoint-after-sync");

        // If we were able to advance 'disk_consistent_lsn', save it the metadata file.
        // After crash, we will restart WAL streaming and processing from that point.
        let old_disk_consistent_lsn = self.disk_consistent_lsn.load();
        if disk_consistent_lsn != old_disk_consistent_lsn {
            assert!(disk_consistent_lsn > old_disk_consistent_lsn);

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

            let ancestor_timelineid = self
                .ancestor_timeline
                .as_ref()
                .map(LayeredTimelineEntry::timeline_id);

            let metadata = TimelineMetadata::new(
                disk_consistent_lsn,
                ondisk_prev_record_lsn,
                ancestor_timelineid,
                self.ancestor_lsn,
                *self.latest_gc_cutoff_lsn.read().unwrap(),
                self.initdb_lsn,
            );

            fail_point!("checkpoint-before-saving-metadata", |x| bail!(
                "{}",
                x.unwrap()
            ));

            LayeredRepository::save_metadata(
                self.conf,
                self.timeline_id,
                self.tenant_id,
                &metadata,
                false,
            )?;

            NUM_PERSISTENT_FILES_CREATED.inc_by(1);
            PERSISTENT_BYTES_WRITTEN.inc_by(new_delta_path.metadata()?.len());

            if self.upload_layers.load(atomic::Ordering::Relaxed) {
                storage_sync::schedule_layer_upload(
                    self.tenant_id,
                    self.timeline_id,
                    HashSet::from([new_delta_path]),
                    Some(metadata),
                );
            }

            // Also update the in-memory copy
            self.disk_consistent_lsn.store(disk_consistent_lsn);
        }

        Ok(())
    }

    pub fn compact(&self) -> Result<()> {
        //
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
        let _compaction_cs = self.compaction_cs.lock().unwrap();

        let target_file_size = self.get_checkpoint_distance();

        // Define partitioning schema if needed
        if let Ok(pgdir) =
            tenant_mgr::get_local_timeline_with_load(self.tenant_id, self.timeline_id)
        {
            let (partitioning, lsn) = pgdir.repartition(
                self.get_last_record_lsn(),
                self.get_compaction_target_size(),
            )?;
            let timer = self.create_images_time_histo.start_timer();
            // 2. Create new image layers for partitions that have been modified
            // "enough".
            let mut layer_paths_to_upload = HashSet::with_capacity(partitioning.parts.len());
            for part in partitioning.parts.iter() {
                if self.time_for_new_image_layer(part, lsn)? {
                    let new_path = self.create_image_layer(part, lsn)?;
                    layer_paths_to_upload.insert(new_path);
                }
            }
            if self.upload_layers.load(atomic::Ordering::Relaxed) {
                storage_sync::schedule_layer_upload(
                    self.tenant_id,
                    self.timeline_id,
                    layer_paths_to_upload,
                    None,
                );
            }
            timer.stop_and_record();

            // 3. Compact
            let timer = self.compact_time_histo.start_timer();
            self.compact_level0(target_file_size)?;
            timer.stop_and_record();
        } else {
            debug!("Could not compact because no partitioning specified yet");
        }

        Ok(())
    }

    // Is it time to create a new image layer for the given partition?
    fn time_for_new_image_layer(&self, partition: &KeySpace, lsn: Lsn) -> Result<bool> {
        let layers = self.layers.read().unwrap();

        for part_range in &partition.ranges {
            let image_coverage = layers.image_coverage(part_range, lsn)?;
            for (img_range, last_img) in image_coverage {
                let img_lsn = if let Some(last_img) = last_img {
                    last_img.get_lsn_range().end
                } else {
                    Lsn(0)
                };

                let num_deltas = layers.count_deltas(&img_range, &(img_lsn..lsn))?;

                debug!(
                    "range {}-{}, has {} deltas on this timeline",
                    img_range.start, img_range.end, num_deltas
                );
                if num_deltas >= self.get_image_creation_threshold() {
                    return Ok(true);
                }
            }
        }

        Ok(false)
    }

    fn create_image_layer(&self, partition: &KeySpace, lsn: Lsn) -> anyhow::Result<PathBuf> {
        let img_range =
            partition.ranges.first().unwrap().start..partition.ranges.last().unwrap().end;
        let mut image_layer_writer =
            ImageLayerWriter::new(self.conf, self.timeline_id, self.tenant_id, &img_range, lsn)?;

        for range in &partition.ranges {
            let mut key = range.start;
            while key < range.end {
                let img = self.get(key, lsn)?;
                image_layer_writer.put_image(key, &img)?;
                key = key.next();
            }
        }
        let image_layer = image_layer_writer.finish()?;

        // Sync the new layer to disk before adding it to the layer map, to make sure
        // we don't garbage collect something based on the new layer, before it has
        // reached the disk.
        //
        // We must also fsync the timeline dir to ensure the directory entries for
        // new layer files are durable
        //
        // Compaction creates multiple image layers. It would be better to create them all
        // and fsync them all in parallel.
        par_fsync::par_fsync(&[
            image_layer.path(),
            self.conf.timeline_path(&self.timeline_id, &self.tenant_id),
        ])?;

        // FIXME: Do we need to do something to upload it to remote storage here?

        let mut layers = self.layers.write().unwrap();
        let new_path = image_layer.path();
        layers.insert_historic(Arc::new(image_layer));
        drop(layers);

        Ok(new_path)
    }

    ///
    /// Collect a bunch of Level 0 layer files, and compact and reshuffle them as
    /// as Level 1 files.
    ///
    fn compact_level0(&self, target_file_size: u64) -> Result<()> {
        let layers = self.layers.read().unwrap();
        let mut level0_deltas = layers.get_level0_deltas()?;
        drop(layers);

        // Only compact if enough layers have accumulated.
        if level0_deltas.is_empty() || level0_deltas.len() < self.get_compaction_threshold() {
            return Ok(());
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
        level0_deltas.sort_by_key(|l| l.get_lsn_range().start);
        let mut level0_deltas_iter = level0_deltas.iter();

        let first_level0_delta = level0_deltas_iter.next().unwrap();
        let mut prev_lsn_end = first_level0_delta.get_lsn_range().end;
        let mut deltas_to_compact = vec![Arc::clone(first_level0_delta)];
        for l in level0_deltas_iter {
            let lsn_range = l.get_lsn_range();

            if lsn_range.start != prev_lsn_end {
                break;
            }
            deltas_to_compact.push(Arc::clone(l));
            prev_lsn_end = lsn_range.end;
        }
        let lsn_range = Range {
            start: deltas_to_compact.first().unwrap().get_lsn_range().start,
            end: deltas_to_compact.last().unwrap().get_lsn_range().end,
        };

        info!(
            "Starting Level0 compaction in LSN range {}-{} for {} layers ({} deltas in total)",
            lsn_range.start,
            lsn_range.end,
            deltas_to_compact.len(),
            level0_deltas.len()
        );
        for l in deltas_to_compact.iter() {
            info!("compact includes {}", l.filename().display());
        }
        // We don't need the original list of layers anymore. Drop it so that
        // we don't accidentally use it later in the function.
        drop(level0_deltas);

        // This iterator walks through all key-value pairs from all the layers
        // we're compacting, in key, LSN order.
        let all_values_iter = deltas_to_compact
            .iter()
            .map(|l| l.iter())
            .kmerge_by(|a, b| {
                if let Ok((a_key, a_lsn, _)) = a {
                    if let Ok((b_key, b_lsn, _)) = b {
                        match a_key.cmp(b_key) {
                            Ordering::Less => true,
                            Ordering::Equal => a_lsn <= b_lsn,
                            Ordering::Greater => false,
                        }
                    } else {
                        false
                    }
                } else {
                    true
                }
            });

        // Merge the contents of all the input delta layers into a new set
        // of delta layers, based on the current partitioning.
        //
        // TODO: this actually divides the layers into fixed-size chunks, not
        // based on the partitioning.
        //
        // TODO: we should also opportunistically materialize and
        // garbage collect what we can.
        let mut new_layers = Vec::new();
        let mut prev_key: Option<Key> = None;
        let mut writer: Option<DeltaLayerWriter> = None;
        for x in all_values_iter {
            let (key, lsn, value) = x?;

            if let Some(prev_key) = prev_key {
                if key != prev_key && writer.is_some() {
                    let size = writer.as_mut().unwrap().size();
                    if size > target_file_size {
                        new_layers.push(writer.take().unwrap().finish(prev_key.next())?);
                        writer = None;
                    }
                }
            }

            if writer.is_none() {
                writer = Some(DeltaLayerWriter::new(
                    self.conf,
                    self.timeline_id,
                    self.tenant_id,
                    key,
                    lsn_range.clone(),
                )?);
            }

            writer.as_mut().unwrap().put_value(key, lsn, value)?;
            prev_key = Some(key);
        }
        if let Some(writer) = writer {
            new_layers.push(writer.finish(prev_key.unwrap().next())?);
        }

        // Sync layers
        if !new_layers.is_empty() {
            let mut layer_paths: Vec<PathBuf> = new_layers.iter().map(|l| l.path()).collect();

            // also sync the directory
            layer_paths.push(self.conf.timeline_path(&self.timeline_id, &self.tenant_id));

            // Fsync all the layer files and directory using multiple threads to
            // minimize latency.
            par_fsync::par_fsync(&layer_paths)?;

            layer_paths.pop().unwrap();
        }

        let mut layers = self.layers.write().unwrap();
        let mut new_layer_paths = HashSet::with_capacity(new_layers.len());
        for l in new_layers {
            new_layer_paths.insert(l.path());
            layers.insert_historic(Arc::new(l));
        }

        // Now that we have reshuffled the data to set of new delta layers, we can
        // delete the old ones
        let mut layer_paths_do_delete = HashSet::with_capacity(deltas_to_compact.len());
        for l in deltas_to_compact {
            l.delete()?;
            if let Some(path) = l.local_path() {
                layer_paths_do_delete.insert(path);
            }
            layers.remove_historic(l);
        }
        drop(layers);

        if self.upload_layers.load(atomic::Ordering::Relaxed) {
            storage_sync::schedule_layer_upload(
                self.tenant_id,
                self.timeline_id,
                new_layer_paths,
                None,
            );
            storage_sync::schedule_layer_delete(
                self.tenant_id,
                self.timeline_id,
                layer_paths_do_delete,
            );
        }

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
    /// The caller specifies how much history is needed with the two arguments:
    ///
    /// retain_lsns: keep a version of each page at these LSNs
    /// cutoff: also keep everything newer than this LSN
    ///
    /// The 'retain_lsns' list is currently used to prevent removing files that
    /// are needed by child timelines. In the future, the user might be able to
    /// name additional points in time to retain. The caller is responsible for
    /// collecting that information.
    ///
    /// The 'cutoff' point is used to retain recent versions that might still be
    /// needed by read-only nodes. (As of this writing, the caller just passes
    /// the latest LSN subtracted by a constant, and doesn't do anything smart
    /// to figure out what read-only nodes might actually need.)
    ///
    fn update_gc_info(&self, retain_lsns: Vec<Lsn>, cutoff: Lsn, pitr: Duration) {
        let mut gc_info = self.gc_info.write().unwrap();
        gc_info.retain_lsns = retain_lsns;
        gc_info.cutoff = cutoff;
        gc_info.pitr = pitr;
    }

    ///
    /// Garbage collect layer files on a timeline that are no longer needed.
    ///
    /// Currently, we don't make any attempt at removing unneeded page versions
    /// within a layer file. We can only remove the whole file if it's fully
    /// obsolete.
    ///
    fn gc(&self) -> Result<GcResult> {
        let now = SystemTime::now();
        let mut result: GcResult = Default::default();
        let disk_consistent_lsn = self.get_disk_consistent_lsn();

        let _compaction_cs = self.compaction_cs.lock().unwrap();

        let gc_info = self.gc_info.read().unwrap();
        let retain_lsns = &gc_info.retain_lsns;
        let cutoff = min(gc_info.cutoff, disk_consistent_lsn);
        let pitr = gc_info.pitr;

        // Calculate pitr cutoff point.
        // If we cannot determine a cutoff LSN, be conservative and don't GC anything.
        let mut pitr_cutoff_lsn: Lsn = *self.get_latest_gc_cutoff_lsn();

        if let Ok(timeline) =
            tenant_mgr::get_local_timeline_with_load(self.tenant_id, self.timeline_id)
        {
            // First, calculate pitr_cutoff_timestamp and then convert it to LSN.
            // If we don't have enough data to convert to LSN,
            // play safe and don't remove any layers.
            if let Some(pitr_cutoff_timestamp) = now.checked_sub(pitr) {
                let pitr_timestamp = to_pg_timestamp(pitr_cutoff_timestamp);

                match timeline.find_lsn_for_timestamp(pitr_timestamp)? {
                    LsnForTimestamp::Present(lsn) => pitr_cutoff_lsn = lsn,
                    LsnForTimestamp::Future(lsn) => {
                        debug!("future({})", lsn);
                        pitr_cutoff_lsn = cutoff;
                    }
                    LsnForTimestamp::Past(lsn) => {
                        debug!("past({})", lsn);
                    }
                }
                debug!("pitr_cutoff_lsn = {:?}", pitr_cutoff_lsn)
            }
        } else if cfg!(test) {
            // We don't have local timeline in mocked cargo tests.
            // So, just ignore pitr_interval setting in this case.
            pitr_cutoff_lsn = cutoff;
        }

        let new_gc_cutoff = Lsn::min(cutoff, pitr_cutoff_lsn);

        // Nothing to GC. Return early.
        if *self.get_latest_gc_cutoff_lsn() >= new_gc_cutoff {
            info!(
                "Nothing to GC for timeline {}. cutoff_lsn {}",
                self.timeline_id, new_gc_cutoff
            );
            result.elapsed = now.elapsed()?;
            return Ok(result);
        }

        let _enter = info_span!("garbage collection", timeline = %self.timeline_id, tenant = %self.tenant_id, cutoff = %cutoff).entered();

        // We need to ensure that no one branches at a point before latest_gc_cutoff_lsn.
        // See branch_timeline() for details.
        *self.latest_gc_cutoff_lsn.write().unwrap() = new_gc_cutoff;

        info!("GC starting");

        debug!("retain_lsns: {:?}", retain_lsns);

        let mut layers_to_remove = Vec::new();

        // Scan all on-disk layers in the timeline.
        //
        // Garbage collect the layer if all conditions are satisfied:
        // 1. it is older than cutoff LSN;
        // 2. it is older than PITR interval;
        // 3. it doesn't need to be retained for 'retain_lsns';
        // 4. newer on-disk image layers cover the layer's whole key range
        //
        let mut layers = self.layers.write().unwrap();
        'outer: for l in layers.iter_historic_layers() {
            // This layer is in the process of being flushed to disk.
            // It will be swapped out of the layer map, replaced with
            // on-disk layers containing the same data.
            // We can't GC it, as it's not on disk. We can't remove it
            // from the layer map yet, as it would make its data
            // inaccessible.
            if l.is_in_memory() {
                continue;
            }

            result.layers_total += 1;

            // 1. Is it newer than cutoff point?
            if l.get_lsn_range().end > cutoff {
                debug!(
                    "keeping {} because it's newer than cutoff {}",
                    l.filename().display(),
                    cutoff
                );
                result.layers_needed_by_cutoff += 1;
                continue 'outer;
            }

            // 2. It is newer than PiTR cutoff point?
            if l.get_lsn_range().end > pitr_cutoff_lsn {
                debug!(
                    "keeping {} because it's newer than pitr_cutoff_lsn {}",
                    l.filename().display(),
                    pitr_cutoff_lsn
                );
                result.layers_needed_by_pitr += 1;
                continue 'outer;
            }

            // 3. Is it needed by a child branch?
            // NOTE With that we would keep data that
            // might be referenced by child branches forever.
            // We can track this in child timeline GC and delete parent layers when
            // they are no longer needed. This might be complicated with long inheritance chains.
            for retain_lsn in retain_lsns {
                // start_lsn is inclusive
                if &l.get_lsn_range().start <= retain_lsn {
                    debug!(
                        "keeping {} because it's still might be referenced by child branch forked at {} is_dropped: xx is_incremental: {}",
                        l.filename().display(),
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
                debug!(
                    "keeping {} because it is the latest layer",
                    l.filename().display()
                );
                result.layers_not_updated += 1;
                continue 'outer;
            }

            // We didn't find any reason to keep this file, so remove it.
            debug!(
                "garbage collecting {} is_dropped: xx is_incremental: {}",
                l.filename().display(),
                l.is_incremental(),
            );
            layers_to_remove.push(Arc::clone(l));
        }

        // Actually delete the layers from disk and remove them from the map.
        // (couldn't do this in the loop above, because you cannot modify a collection
        // while iterating it. BTreeMap::retain() would be another option)
        let mut layer_paths_to_delete = HashSet::with_capacity(layers_to_remove.len());
        for doomed_layer in layers_to_remove {
            doomed_layer.delete()?;
            if let Some(path) = doomed_layer.local_path() {
                layer_paths_to_delete.insert(path);
            }
            layers.remove_historic(doomed_layer);
            result.layers_removed += 1;
        }

        if self.upload_layers.load(atomic::Ordering::Relaxed) {
            storage_sync::schedule_layer_delete(
                self.tenant_id,
                self.timeline_id,
                layer_paths_to_delete,
            );
        }

        result.elapsed = now.elapsed()?;
        Ok(result)
    }

    ///
    /// Reconstruct a value, using the given base image and WAL records in 'data'.
    ///
    fn reconstruct_value(
        &self,
        key: Key,
        request_lsn: Lsn,
        mut data: ValueReconstructState,
    ) -> Result<Bytes> {
        // Perform WAL redo if needed
        data.records.reverse();

        // If we have a page image, and no WAL, we're all set
        if data.records.is_empty() {
            if let Some((img_lsn, img)) = &data.img {
                trace!(
                    "found page image for key {} at {}, no WAL redo required",
                    key,
                    img_lsn
                );
                Ok(img.clone())
            } else {
                bail!("base image for {} at {} not found", key, request_lsn);
            }
        } else {
            // We need to do WAL redo.
            //
            // If we don't have a base image, then the oldest WAL record better initialize
            // the page
            if data.img.is_none() && !data.records.first().unwrap().1.will_init() {
                bail!(
                    "Base image for {} at {} not found, but got {} WAL records",
                    key,
                    request_lsn,
                    data.records.len()
                );
            } else {
                let base_img = if let Some((_lsn, img)) = data.img {
                    trace!(
                        "found {} WAL records and a base image for {} at {}, performing WAL redo",
                        data.records.len(),
                        key,
                        request_lsn
                    );
                    Some(img)
                } else {
                    trace!("found {} WAL records that will init the page for {} at {}, performing WAL redo", data.records.len(), key, request_lsn);
                    None
                };

                let last_rec_lsn = data.records.last().unwrap().0;

                let img =
                    self.walredo_mgr
                        .request_redo(key, request_lsn, base_img, data.records)?;

                if img.len() == page_cache::PAGE_SZ {
                    let cache = page_cache::get();
                    cache.memorize_materialized_page(
                        self.tenant_id,
                        self.timeline_id,
                        key,
                        last_rec_lsn,
                        &img,
                    );
                }

                Ok(img)
            }
        }
    }
}

/// Helper function for get_reconstruct_data() to add the path of layers traversed
/// to an error, as anyhow context information.
fn layer_traversal_error(
    msg: String,
    path: Vec<(ValueReconstructResult, Lsn, Arc<dyn Layer>)>,
) -> anyhow::Result<()> {
    // We want the original 'msg' to be the outermost context. The outermost context
    // is the most high-level information, which also gets propagated to the client.
    let mut msg_iter = path
        .iter()
        .map(|(r, c, l)| {
            format!(
                "layer traversal: result {:?}, cont_lsn {}, layer: {}",
                r,
                c,
                l.filename().display()
            )
        })
        .chain(std::iter::once(msg));
    // Construct initial message from the first traversed layer
    let err = anyhow!(msg_iter.next().unwrap());

    // Append all subsequent traversals, and the error message 'msg', as contexts.
    Err(msg_iter.fold(err, |err, msg| err.context(msg)))
}

struct LayeredTimelineWriter<'a> {
    tl: &'a LayeredTimeline,
    _write_guard: MutexGuard<'a, ()>,
}

impl Deref for LayeredTimelineWriter<'_> {
    type Target = dyn Timeline;

    fn deref(&self) -> &Self::Target {
        self.tl
    }
}

impl<'a> TimelineWriter<'_> for LayeredTimelineWriter<'a> {
    fn put(&self, key: Key, lsn: Lsn, value: Value) -> Result<()> {
        self.tl.put_value(key, lsn, value)
    }

    fn delete(&self, key_range: Range<Key>, lsn: Lsn) -> Result<()> {
        self.tl.put_tombstone(key_range, lsn)
    }

    ///
    /// Remember the (end of) last valid WAL record remembered in the timeline.
    ///
    fn finish_write(&self, new_lsn: Lsn) {
        self.tl.finish_write(new_lsn);
    }
}

/// Dump contents of a layer file to stdout.
pub fn dump_layerfile_from_path(path: &Path, verbose: bool) -> Result<()> {
    use std::os::unix::fs::FileExt;

    // All layer files start with a two-byte "magic" value, to identify the kind of
    // file.
    let file = File::open(path)?;
    let mut header_buf = [0u8; 2];
    file.read_exact_at(&mut header_buf, 0)?;

    match u16::from_be_bytes(header_buf) {
        crate::IMAGE_FILE_MAGIC => ImageLayer::new_for_path(path, file)?.dump(verbose)?,
        crate::DELTA_FILE_MAGIC => DeltaLayer::new_for_path(path, file)?.dump(verbose)?,
        magic => bail!("unrecognized magic identifier: {:?}", magic),
    }

    Ok(())
}

/// Add a suffix to a layer file's name: .{num}.old
/// Uses the first available num (starts at 0)
fn rename_to_backup(path: PathBuf) -> anyhow::Result<()> {
    let filename = path
        .file_name()
        .ok_or_else(|| anyhow!("Path {} don't have a file name", path.display()))?
        .to_string_lossy();
    let mut new_path = path.clone();

    for i in 0u32.. {
        new_path.set_file_name(format!("{}.{}.old", filename, i));
        if !new_path.exists() {
            std::fs::rename(&path, &new_path)?;
            return Ok(());
        }
    }

    bail!("couldn't find an unused backup number for {:?}", path)
}

pub fn load_metadata(
    conf: &'static PageServerConf,
    timeline_id: ZTimelineId,
    tenant_id: ZTenantId,
) -> anyhow::Result<TimelineMetadata> {
    let metadata_path = metadata_path(conf, timeline_id, tenant_id);
    let metadata_bytes = std::fs::read(&metadata_path).with_context(|| {
        format!(
            "Failed to read metadata bytes from path {}",
            metadata_path.display()
        )
    })?;
    TimelineMetadata::from_bytes(&metadata_bytes).with_context(|| {
        format!(
            "Failed to parse metadata bytes from path {}",
            metadata_path.display()
        )
    })
}

///
/// Tests that are specific to the layered storage format.
///
/// There are more unit tests in repository.rs that work through the
/// Repository interface and are expected to work regardless of the
/// file format and directory layout. The test here are more low level.
///
#[cfg(test)]
pub mod tests {
    use super::*;
    use crate::keyspace::KeySpaceAccum;
    use crate::repository::repo_harness::*;
    use rand::{thread_rng, Rng};

    #[test]
    fn corrupt_metadata() -> Result<()> {
        const TEST_NAME: &str = "corrupt_metadata";
        let harness = RepoHarness::create(TEST_NAME)?;
        let repo = harness.load();

        repo.create_empty_timeline(TIMELINE_ID, Lsn(0))?;
        drop(repo);

        let metadata_path = harness.timeline_path(&TIMELINE_ID).join(METADATA_FILE_NAME);

        assert!(metadata_path.is_file());

        let mut metadata_bytes = std::fs::read(&metadata_path)?;
        assert_eq!(metadata_bytes.len(), 512);
        metadata_bytes[8] ^= 1;
        std::fs::write(metadata_path, metadata_bytes)?;

        let err = harness.try_load().err().expect("should fail");
        assert_eq!(err.to_string(), "failed to load local metadata");

        let mut found_error_message = false;
        let mut err_source = err.source();
        while let Some(source) = err_source {
            if source.to_string() == "metadata checksum mismatch" {
                found_error_message = true;
                break;
            }
            err_source = source.source();
        }
        assert!(
            found_error_message,
            "didn't find the corrupted metadata error"
        );

        Ok(())
    }

    // Target file size in the unit tests. In production, the target
    // file size is much larger, maybe 1 GB. But a small size makes it
    // much faster to exercise all the logic for creating the files,
    // garbage collection, compaction etc.
    pub const TEST_FILE_SIZE: u64 = 4 * 1024 * 1024;

    #[test]
    fn test_images() -> Result<()> {
        let repo = RepoHarness::create("test_images")?.load();
        let tline = repo.create_empty_timeline(TIMELINE_ID, Lsn(0))?;

        #[allow(non_snake_case)]
        let TEST_KEY: Key = Key::from_hex("112222222233333333444444445500000001").unwrap();

        let writer = tline.writer();
        writer.put(TEST_KEY, Lsn(0x10), Value::Image(TEST_IMG("foo at 0x10")))?;
        writer.finish_write(Lsn(0x10));
        drop(writer);

        tline.checkpoint(CheckpointConfig::Forced)?;
        tline.compact()?;

        let writer = tline.writer();
        writer.put(TEST_KEY, Lsn(0x20), Value::Image(TEST_IMG("foo at 0x20")))?;
        writer.finish_write(Lsn(0x20));
        drop(writer);

        tline.checkpoint(CheckpointConfig::Forced)?;
        tline.compact()?;

        let writer = tline.writer();
        writer.put(TEST_KEY, Lsn(0x30), Value::Image(TEST_IMG("foo at 0x30")))?;
        writer.finish_write(Lsn(0x30));
        drop(writer);

        tline.checkpoint(CheckpointConfig::Forced)?;
        tline.compact()?;

        let writer = tline.writer();
        writer.put(TEST_KEY, Lsn(0x40), Value::Image(TEST_IMG("foo at 0x40")))?;
        writer.finish_write(Lsn(0x40));
        drop(writer);

        tline.checkpoint(CheckpointConfig::Forced)?;
        tline.compact()?;

        assert_eq!(tline.get(TEST_KEY, Lsn(0x10))?, TEST_IMG("foo at 0x10"));
        assert_eq!(tline.get(TEST_KEY, Lsn(0x1f))?, TEST_IMG("foo at 0x10"));
        assert_eq!(tline.get(TEST_KEY, Lsn(0x20))?, TEST_IMG("foo at 0x20"));
        assert_eq!(tline.get(TEST_KEY, Lsn(0x30))?, TEST_IMG("foo at 0x30"));
        assert_eq!(tline.get(TEST_KEY, Lsn(0x40))?, TEST_IMG("foo at 0x40"));

        Ok(())
    }

    //
    // Insert 1000 key-value pairs with increasing keys, checkpoint,
    // repeat 50 times.
    //
    #[test]
    fn test_bulk_insert() -> Result<()> {
        let repo = RepoHarness::create("test_bulk_insert")?.load();
        let tline = repo.create_empty_timeline(TIMELINE_ID, Lsn(0))?;

        let mut lsn = Lsn(0x10);

        let mut keyspace = KeySpaceAccum::new();

        let mut test_key = Key::from_hex("012222222233333333444444445500000000").unwrap();
        let mut blknum = 0;
        for _ in 0..50 {
            for _ in 0..10000 {
                test_key.field6 = blknum;
                let writer = tline.writer();
                writer.put(
                    test_key,
                    lsn,
                    Value::Image(TEST_IMG(&format!("{} at {}", blknum, lsn))),
                )?;
                writer.finish_write(lsn);
                drop(writer);

                keyspace.add_key(test_key);

                lsn = Lsn(lsn.0 + 0x10);
                blknum += 1;
            }

            let cutoff = tline.get_last_record_lsn();

            tline.update_gc_info(Vec::new(), cutoff, Duration::ZERO);
            tline.checkpoint(CheckpointConfig::Forced)?;
            tline.compact()?;
            tline.gc()?;
        }

        Ok(())
    }

    #[test]
    fn test_random_updates() -> Result<()> {
        let repo = RepoHarness::create("test_random_updates")?.load();
        let tline = repo.create_empty_timeline(TIMELINE_ID, Lsn(0))?;

        const NUM_KEYS: usize = 1000;

        let mut test_key = Key::from_hex("012222222233333333444444445500000000").unwrap();

        let mut keyspace = KeySpaceAccum::new();

        // Track when each page was last modified. Used to assert that
        // a read sees the latest page version.
        let mut updated = [Lsn(0); NUM_KEYS];

        let mut lsn = Lsn(0);
        #[allow(clippy::needless_range_loop)]
        for blknum in 0..NUM_KEYS {
            lsn = Lsn(lsn.0 + 0x10);
            test_key.field6 = blknum as u32;
            let writer = tline.writer();
            writer.put(
                test_key,
                lsn,
                Value::Image(TEST_IMG(&format!("{} at {}", blknum, lsn))),
            )?;
            writer.finish_write(lsn);
            updated[blknum] = lsn;
            drop(writer);

            keyspace.add_key(test_key);
        }

        for _ in 0..50 {
            for _ in 0..NUM_KEYS {
                lsn = Lsn(lsn.0 + 0x10);
                let blknum = thread_rng().gen_range(0..NUM_KEYS);
                test_key.field6 = blknum as u32;
                let writer = tline.writer();
                writer.put(
                    test_key,
                    lsn,
                    Value::Image(TEST_IMG(&format!("{} at {}", blknum, lsn))),
                )?;
                writer.finish_write(lsn);
                drop(writer);
                updated[blknum] = lsn;
            }

            // Read all the blocks
            for (blknum, last_lsn) in updated.iter().enumerate() {
                test_key.field6 = blknum as u32;
                assert_eq!(
                    tline.get(test_key, lsn)?,
                    TEST_IMG(&format!("{} at {}", blknum, last_lsn))
                );
            }

            // Perform a cycle of checkpoint, compaction, and GC
            println!("checkpointing {}", lsn);
            let cutoff = tline.get_last_record_lsn();
            tline.update_gc_info(Vec::new(), cutoff, Duration::ZERO);
            tline.checkpoint(CheckpointConfig::Forced)?;
            tline.compact()?;
            tline.gc()?;
        }

        Ok(())
    }

    #[test]
    fn test_traverse_branches() -> Result<()> {
        let repo = RepoHarness::create("test_traverse_branches")?.load();
        let mut tline = repo.create_empty_timeline(TIMELINE_ID, Lsn(0))?;

        const NUM_KEYS: usize = 1000;

        let mut test_key = Key::from_hex("012222222233333333444444445500000000").unwrap();

        let mut keyspace = KeySpaceAccum::new();

        // Track when each page was last modified. Used to assert that
        // a read sees the latest page version.
        let mut updated = [Lsn(0); NUM_KEYS];

        let mut lsn = Lsn(0);
        #[allow(clippy::needless_range_loop)]
        for blknum in 0..NUM_KEYS {
            lsn = Lsn(lsn.0 + 0x10);
            test_key.field6 = blknum as u32;
            let writer = tline.writer();
            writer.put(
                test_key,
                lsn,
                Value::Image(TEST_IMG(&format!("{} at {}", blknum, lsn))),
            )?;
            writer.finish_write(lsn);
            updated[blknum] = lsn;
            drop(writer);

            keyspace.add_key(test_key);
        }

        let mut tline_id = TIMELINE_ID;
        for _ in 0..50 {
            let new_tline_id = ZTimelineId::generate();
            repo.branch_timeline(tline_id, new_tline_id, lsn)?;
            tline = repo.get_timeline_load(new_tline_id)?;
            tline_id = new_tline_id;

            for _ in 0..NUM_KEYS {
                lsn = Lsn(lsn.0 + 0x10);
                let blknum = thread_rng().gen_range(0..NUM_KEYS);
                test_key.field6 = blknum as u32;
                let writer = tline.writer();
                writer.put(
                    test_key,
                    lsn,
                    Value::Image(TEST_IMG(&format!("{} at {}", blknum, lsn))),
                )?;
                println!("updating {} at {}", blknum, lsn);
                writer.finish_write(lsn);
                drop(writer);
                updated[blknum] = lsn;
            }

            // Read all the blocks
            for (blknum, last_lsn) in updated.iter().enumerate() {
                test_key.field6 = blknum as u32;
                assert_eq!(
                    tline.get(test_key, lsn)?,
                    TEST_IMG(&format!("{} at {}", blknum, last_lsn))
                );
            }

            // Perform a cycle of checkpoint, compaction, and GC
            println!("checkpointing {}", lsn);
            let cutoff = tline.get_last_record_lsn();
            tline.update_gc_info(Vec::new(), cutoff, Duration::ZERO);
            tline.checkpoint(CheckpointConfig::Forced)?;
            tline.compact()?;
            tline.gc()?;
        }

        Ok(())
    }

    #[test]
    fn test_traverse_ancestors() -> Result<()> {
        let repo = RepoHarness::create("test_traverse_ancestors")?.load();
        let mut tline = repo.create_empty_timeline(TIMELINE_ID, Lsn(0))?;

        const NUM_KEYS: usize = 100;
        const NUM_TLINES: usize = 50;

        let mut test_key = Key::from_hex("012222222233333333444444445500000000").unwrap();
        // Track page mutation lsns across different timelines.
        let mut updated = [[Lsn(0); NUM_KEYS]; NUM_TLINES];

        let mut lsn = Lsn(0);
        let mut tline_id = TIMELINE_ID;

        #[allow(clippy::needless_range_loop)]
        for idx in 0..NUM_TLINES {
            let new_tline_id = ZTimelineId::generate();
            repo.branch_timeline(tline_id, new_tline_id, lsn)?;
            tline = repo.get_timeline_load(new_tline_id)?;
            tline_id = new_tline_id;

            for _ in 0..NUM_KEYS {
                lsn = Lsn(lsn.0 + 0x10);
                let blknum = thread_rng().gen_range(0..NUM_KEYS);
                test_key.field6 = blknum as u32;
                let writer = tline.writer();
                writer.put(
                    test_key,
                    lsn,
                    Value::Image(TEST_IMG(&format!("{} {} at {}", idx, blknum, lsn))),
                )?;
                println!("updating [{}][{}] at {}", idx, blknum, lsn);
                writer.finish_write(lsn);
                drop(writer);
                updated[idx][blknum] = lsn;
            }
        }

        // Read pages from leaf timeline across all ancestors.
        for (idx, lsns) in updated.iter().enumerate() {
            for (blknum, lsn) in lsns.iter().enumerate() {
                // Skip empty mutations.
                if lsn.0 == 0 {
                    continue;
                }
                println!("chekcking [{}][{}] at {}", idx, blknum, lsn);
                test_key.field6 = blknum as u32;
                assert_eq!(
                    tline.get(test_key, *lsn)?,
                    TEST_IMG(&format!("{} {} at {}", idx, blknum, lsn))
                );
            }
        }
        Ok(())
    }
}

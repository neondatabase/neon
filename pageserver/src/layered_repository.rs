//!
//! Timeline repository implementation that keeps old data in files on disk, and
//! the recent changes in memory. See layered_repository/*_layer.rs files.
//! The functions here are responsible for locating the correct layer for the
//! get/put call, walking back the timeline branching history as needed.
//!
//! The files are stored in the .neon/tenants/<tenantid>/timelines/<timelineid>
//! directory. See docs/pageserver-storage.md for how the files are managed.
//! In addition to the layer files, there is a metadata file in the same
//! directory that contains information about the timeline, in particular its
//! parent timeline, and the last LSN that has been written to disk.
//!

use anyhow::{bail, ensure, Context, Result};
use tracing::*;

use std::cmp::min;
use std::collections::hash_map::Entry;
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::fs;
use std::fs::File;
use std::num::NonZeroU64;
use std::ops::Bound::Included;
use std::path::Path;
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

use self::metadata::{metadata_path, TimelineMetadata};
use crate::config::PageServerConf;
use crate::storage_sync::index::RemoteIndex;
use crate::tenant_config::{TenantConf, TenantConfOpt};

use crate::repository::{GcResult, RepositoryTimeline};
use crate::thread_mgr;
use crate::walredo::WalRedoManager;
use crate::CheckpointConfig;

use toml_edit;
use utils::{
    crashsafe_dir,
    lsn::{Lsn, RecordLsn},
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

mod timeline;

use storage_layer::Layer;
use timeline::LayeredTimelineEntry;

pub use timeline::Timeline;

// re-export this function so that page_cache.rs can use it.
pub use crate::layered_repository::ephemeral_file::writeback as writeback_ephemeral_file;

// re-export for use in storage_sync.rs
pub use crate::layered_repository::timeline::save_metadata;

// re-export for use in walreceiver
pub use crate::layered_repository::timeline::WalReceiverInfo;

/// Parts of the `.neon/tenants/<tenantid>/timelines/<timelineid>` directory prefix.
pub const TIMELINES_SEGMENT_NAME: &str = "timelines";

///
/// Repository consists of multiple timelines. Keep them in a hash table.
///
pub struct Repository {
    // Global pageserver config parameters
    pub conf: &'static PageServerConf,

    // Allows us to gracefully cancel operations that edit the directory
    // that backs this layered repository. Usage:
    //
    // Use `let _guard = file_lock.try_read()` while writing any files.
    // Use `let _guard = file_lock.write().unwrap()` to wait for all writes to finish.
    //
    // TODO try_read this lock during checkpoint as well to prevent race
    //      between checkpoint and detach/delete.
    // TODO try_read this lock for all gc/compaction operations, not just
    //      ones scheduled by the tenant task manager.
    pub file_lock: RwLock<()>,

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

/// A repository corresponds to one .neon directory. One repository holds multiple
/// timelines, forked off from the same initial call to 'initdb'.
impl Repository {
    /// Get Timeline handle for given zenith timeline ID.
    /// This function is idempotent. It doesn't change internal state in any way.
    pub fn get_timeline(&self, timelineid: ZTimelineId) -> Option<RepositoryTimeline<Timeline>> {
        let timelines = self.timelines.lock().unwrap();
        self.get_timeline_internal(timelineid, &timelines)
            .map(RepositoryTimeline::from)
    }

    /// Get Timeline handle for locally available timeline. Load it into memory if it is not loaded.
    pub fn get_timeline_load(&self, timelineid: ZTimelineId) -> Result<Arc<Timeline>> {
        let mut timelines = self.timelines.lock().unwrap();
        match self.get_timeline_load_internal(timelineid, &mut timelines)? {
            Some(local_loaded_timeline) => Ok(local_loaded_timeline),
            None => anyhow::bail!(
                "cannot get local timeline: unknown timeline id: {}",
                timelineid
            ),
        }
    }

    /// Lists timelines the repository contains.
    /// Up to repository's implementation to omit certain timelines that ar not considered ready for use.
    pub fn list_timelines(&self) -> Vec<(ZTimelineId, RepositoryTimeline<Timeline>)> {
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

    /// Create a new, empty timeline. The caller is responsible for loading data into it
    /// Initdb lsn is provided for timeline impl to be able to perform checks for some operations against it.
    pub fn create_empty_timeline(
        &self,
        timeline_id: ZTimelineId,
        initdb_lsn: Lsn,
    ) -> Result<Arc<Timeline>> {
        let mut timelines = self.timelines.lock().unwrap();
        let vacant_timeline_entry = match timelines.entry(timeline_id) {
            Entry::Occupied(_) => bail!("Timeline already exists"),
            Entry::Vacant(vacant_entry) => vacant_entry,
        };

        let timeline_path = self.conf.timeline_path(&timeline_id, &self.tenant_id);
        if timeline_path.exists() {
            bail!("Timeline directory already exists, but timeline is missing in repository map. This is a bug.")
        }

        // Create the timeline directory, and write initial metadata to file.
        crashsafe_dir::create_dir_all(timeline_path)?;

        let metadata = TimelineMetadata::new(Lsn(0), None, None, Lsn(0), initdb_lsn, initdb_lsn);
        timeline::save_metadata(self.conf, timeline_id, self.tenant_id, &metadata, true)?;

        let timeline = Timeline::new(
            self.conf,
            Arc::clone(&self.tenant_conf),
            metadata,
            None,
            timeline_id,
            self.tenant_id,
            Arc::clone(&self.walredo_mgr),
            self.upload_layers,
        );
        timeline.layers.write().unwrap().next_open_layer_at = Some(initdb_lsn);

        // Insert if not exists
        let timeline = Arc::new(timeline);
        vacant_timeline_entry.insert(LayeredTimelineEntry::Loaded(Arc::clone(&timeline)));

        Ok(timeline)
    }

    /// Branch a timeline
    pub fn branch_timeline(
        &self,
        src: ZTimelineId,
        dst: ZTimelineId,
        start_lsn: Option<Lsn>,
    ) -> Result<()> {
        // We need to hold this lock to prevent GC from starting at the same time. GC scans the directory to learn
        // about timelines, so otherwise a race condition is possible, where we create new timeline and GC
        // concurrently removes data that is needed by the new timeline.
        let _gc_cs = self.gc_cs.lock().unwrap();

        // In order for the branch creation task to not wait for GC/compaction,
        // we need to make sure that the starting LSN of the child branch is not out of scope midway by
        //
        // 1. holding the GC lock to prevent overwritting timeline's GC data
        // 2. checking both the latest GC cutoff LSN and latest GC info of the source timeline
        //
        // Step 2 is to avoid initializing the new branch using data removed by past GC iterations
        // or in-queue GC iterations.

        let mut timelines = self.timelines.lock().unwrap();
        let src_timeline = self
            .get_timeline_load_internal(src, &mut timelines)
            // message about timeline being remote is one .context up in the stack
            .context("failed to load timeline for branching")?
            .ok_or_else(|| anyhow::anyhow!("unknown timeline id: {}", &src))?;

        let latest_gc_cutoff_lsn = src_timeline.get_latest_gc_cutoff_lsn();

        // If no start LSN is specified, we branch the new timeline from the source timeline's last record LSN
        let start_lsn = start_lsn.unwrap_or_else(|| {
            let lsn = src_timeline.get_last_record_lsn();
            info!("branching timeline {dst} from timeline {src} at last record LSN: {lsn}");
            lsn
        });

        // Check if the starting LSN is out of scope because it is less than
        // 1. the latest GC cutoff LSN or
        // 2. the planned GC cutoff LSN, which is from an in-queue GC iteration.
        src_timeline
            .check_lsn_is_in_scope(start_lsn, &latest_gc_cutoff_lsn)
            .context(format!(
                "invalid branch start lsn: less than latest GC cutoff {latest_gc_cutoff_lsn}"
            ))?;
        {
            let gc_info = src_timeline.gc_info.read().unwrap();
            let cutoff = min(gc_info.pitr_cutoff, gc_info.horizon_cutoff);
            if start_lsn < cutoff {
                bail!(format!(
                    "invalid branch start lsn: less than planned GC cutoff {cutoff}"
                ));
            }
        }

        // Determine prev-LSN for the new timeline. We can only determine it if
        // the timeline was branched at the current end of the source timeline.
        let RecordLsn {
            last: src_last,
            prev: src_prev,
        } = src_timeline.get_last_record_rlsn();
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
        timeline::save_metadata(self.conf, dst, self.tenant_id, &metadata, true)?;
        timelines.insert(dst, LayeredTimelineEntry::Unloaded { id: dst, metadata });

        info!("branched timeline {} from {} at {}", dst, src, start_lsn);

        Ok(())
    }

    /// perform one garbage collection iteration, removing old data files from disk.
    /// this function is periodically called by gc thread.
    /// also it can be explicitly requested through page server api 'do_gc' command.
    ///
    /// 'timelineid' specifies the timeline to GC, or None for all.
    /// `horizon` specifies delta from last lsn to preserve all object versions (pitr interval).
    /// `checkpoint_before_gc` parameter is used to force compaction of storage before GC
    /// to make tests more deterministic.
    /// TODO Do we still need it or we can call checkpoint explicitly in tests where needed?
    pub fn gc_iteration(
        &self,
        target_timeline_id: Option<ZTimelineId>,
        horizon: u64,
        pitr: Duration,
        checkpoint_before_gc: bool,
    ) -> Result<GcResult> {
        let timeline_str = target_timeline_id
            .map(|x| x.to_string())
            .unwrap_or_else(|| "-".to_string());

        timeline::STORAGE_TIME
            .with_label_values(&["gc", &self.tenant_id.to_string(), &timeline_str])
            .observe_closure_duration(|| {
                self.gc_iteration_internal(target_timeline_id, horizon, pitr, checkpoint_before_gc)
            })
    }

    /// Perform one compaction iteration.
    /// This function is periodically called by compactor thread.
    /// Also it can be explicitly requested per timeline through page server
    /// api's 'compact' command.
    pub fn compaction_iteration(&self) -> Result<()> {
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

    /// Flush all in-memory data to disk.
    ///
    /// Used at graceful shutdown.
    ///
    pub fn checkpoint(&self) -> Result<()> {
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

    /// Removes timeline-related in-memory data
    pub fn delete_timeline(&self, timeline_id: ZTimelineId) -> anyhow::Result<()> {
        // in order to be retriable detach needs to be idempotent
        // (or at least to a point that each time the detach is called it can make progress)
        let mut timelines = self.timelines.lock().unwrap();

        // Ensure that there are no child timelines **attached to that pageserver**,
        // because detach removes files, which will break child branches
        let children_exist = timelines
            .iter()
            .any(|(_, entry)| entry.ancestor_timeline_id() == Some(timeline_id));

        ensure!(
            !children_exist,
            "Cannot detach timeline which has child timelines"
        );
        let timeline_entry = match timelines.entry(timeline_id) {
            Entry::Occupied(e) => e,
            Entry::Vacant(_) => bail!("timeline not found"),
        };

        let layer_removal_guard = timeline_entry.get().layer_removal_guard()?;

        let local_timeline_directory = self.conf.timeline_path(&timeline_id, &self.tenant_id);
        std::fs::remove_dir_all(&local_timeline_directory).with_context(|| {
            format!(
                "Failed to remove local timeline directory '{}'",
                local_timeline_directory.display()
            )
        })?;
        info!("detach removed files");

        drop(layer_removal_guard);
        timeline_entry.remove();

        Ok(())
    }

    /// Updates timeline based on the `TimelineSyncStatusUpdate`, received from the remote storage synchronization.
    /// See [`crate::remote_storage`] for more details about the synchronization.
    pub fn attach_timeline(&self, timeline_id: ZTimelineId) -> Result<()> {
        debug!("attach timeline_id: {}", timeline_id,);
        match self.timelines.lock().unwrap().entry(timeline_id) {
            Entry::Occupied(_) => bail!("We completed a download for a timeline that already exists in repository. This is a bug."),
            Entry::Vacant(entry) => {
                // we need to get metadata of a timeline, another option is to pass it along with Downloaded status
                let metadata = load_metadata(self.conf, timeline_id, self.tenant_id).context("failed to load local metadata")?;
                // finally we make newly downloaded timeline visible to repository
                entry.insert(LayeredTimelineEntry::Unloaded { id: timeline_id, metadata })
            },
        };
        Ok(())
    }

    /// Allows to retrieve remote timeline index from the tenant. Used in walreceiver to grab remote consistent lsn.
    pub fn get_remote_index(&self) -> &RemoteIndex {
        &self.remote_index
    }
}

/// Private functions
impl Repository {
    pub fn get_checkpoint_distance(&self) -> u64 {
        let tenant_conf = self.tenant_conf.read().unwrap();
        tenant_conf
            .checkpoint_distance
            .unwrap_or(self.conf.default_tenant_conf.checkpoint_distance)
    }

    pub fn get_checkpoint_timeout(&self) -> Duration {
        let tenant_conf = self.tenant_conf.read().unwrap();
        tenant_conf
            .checkpoint_timeout
            .unwrap_or(self.conf.default_tenant_conf.checkpoint_timeout)
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

        Repository::persist_tenant_config(self.conf, self.tenant_id, *tenant_conf)?;
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
    ) -> anyhow::Result<Option<Arc<Timeline>>> {
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
    ) -> anyhow::Result<Arc<Timeline>> {
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

        let timeline = Timeline::new(
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
    ) -> Repository {
        Repository {
            tenant_id,
            file_lock: RwLock::new(()),
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
                "tenant config not found in {}",
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
    // 1. Grab 'gc_cs' mutex to prevent new timelines from being created
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
        target_timeline_id: Option<ZTimelineId>,
        horizon: u64,
        pitr: Duration,
        checkpoint_before_gc: bool,
    ) -> Result<GcResult> {
        let _span_guard =
            info_span!("gc iteration", tenant = %self.tenant_id, timeline = ?target_timeline_id)
                .entered();
        let mut totals: GcResult = Default::default();
        let now = Instant::now();

        // grab mutex to prevent new timelines from being created here.
        let gc_cs = self.gc_cs.lock().unwrap();

        let timelines = self.timelines.lock().unwrap();

        // Scan all timelines. For each timeline, remember the timeline ID and
        // the branch point where it was created.
        let mut all_branchpoints: BTreeSet<(ZTimelineId, Lsn)> = BTreeSet::new();
        let timeline_ids = {
            if let Some(target_timeline_id) = target_timeline_id.as_ref() {
                if timelines.get(target_timeline_id).is_none() {
                    bail!("gc target timeline does not exist")
                }
            };

            timelines
                .iter()
                .map(|(timeline_id, timeline_entry)| {
                    // This is unresolved question for now, how to do gc in presence of remote timelines
                    // especially when this is combined with branching.
                    // Somewhat related: https://github.com/zenithdb/zenith/issues/999
                    if let Some(ancestor_timeline_id) = &timeline_entry.ancestor_timeline_id() {
                        // If target_timeline is specified, we only need to know branchpoints of its children
                        if let Some(timelineid) = target_timeline_id {
                            if ancestor_timeline_id == &timelineid {
                                all_branchpoints
                                    .insert((*ancestor_timeline_id, timeline_entry.ancestor_lsn()));
                            }
                        }
                        // Collect branchpoints for all timelines
                        else {
                            all_branchpoints
                                .insert((*ancestor_timeline_id, timeline_entry.ancestor_lsn()));
                        }
                    }

                    *timeline_id
                })
                .collect::<Vec<_>>()
        };
        drop(timelines);

        // Ok, we now know all the branch points.
        // Update the GC information for each timeline.
        let mut gc_timelines = Vec::with_capacity(timeline_ids.len());
        for timeline_id in timeline_ids {
            // Timeline is known to be local and loaded.
            let timeline = self.get_timeline_load(timeline_id)?;

            // If target_timeline is specified, ignore all other timelines
            if let Some(target_timelineid) = target_timeline_id {
                if timeline_id != target_timelineid {
                    continue;
                }
            }

            if let Some(cutoff) = timeline.get_last_record_lsn().checked_sub(horizon) {
                let branchpoints: Vec<Lsn> = all_branchpoints
                    .range((
                        Included((timeline_id, Lsn(0))),
                        Included((timeline_id, Lsn(u64::MAX))),
                    ))
                    .map(|&x| x.1)
                    .collect();
                timeline.update_gc_info(branchpoints, cutoff, pitr)?;

                gc_timelines.push(timeline);
            }
        }
        drop(gc_cs);

        // Perform GC for each timeline.
        //
        // Note that we don't hold the GC lock here because we don't want
        // to delay the branch creation task, which requires the GC lock.
        // A timeline GC iteration can be slow because it may need to wait for
        // compaction (both require `layer_removal_cs` lock),
        // but the GC iteration can run concurrently with branch creation.
        //
        // See comments in [`LayeredRepository::branch_timeline`] for more information
        // about why branch creation task can run concurrently with timeline's GC iteration.
        for timeline in gc_timelines {
            if thread_mgr::is_shutdown_requested() {
                // We were requested to shut down. Stop and return with the progress we
                // made.
                break;
            }

            // If requested, force flush all in-memory layers to disk first,
            // so that they too can be garbage collected. That's
            // used in tests, so we want as deterministic results as possible.
            if checkpoint_before_gc {
                timeline.checkpoint(CheckpointConfig::Forced)?;
                info!(
                    "timeline {} checkpoint_before_gc done",
                    timeline.timeline_id
                );
            }

            let result = timeline.gc()?;
            totals += result;
        }

        totals.elapsed = now.elapsed();
        Ok(totals)
    }

    pub fn tenant_id(&self) -> ZTenantId {
        self.tenant_id
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
        crate::IMAGE_FILE_MAGIC => {
            image_layer::ImageLayer::new_for_path(path, file)?.dump(verbose)?
        }
        crate::DELTA_FILE_MAGIC => {
            delta_layer::DeltaLayer::new_for_path(path, file)?.dump(verbose)?
        }
        magic => bail!("unrecognized magic identifier: {:?}", magic),
    }

    Ok(())
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
    use super::metadata::METADATA_FILE_NAME;
    use super::*;
    use crate::keyspace::KeySpaceAccum;
    use crate::repository::repo_harness::*;
    use crate::repository::{Key, Value};
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
        writer.put(TEST_KEY, Lsn(0x10), &Value::Image(TEST_IMG("foo at 0x10")))?;
        writer.finish_write(Lsn(0x10));
        drop(writer);

        tline.checkpoint(CheckpointConfig::Forced)?;
        tline.compact()?;

        let writer = tline.writer();
        writer.put(TEST_KEY, Lsn(0x20), &Value::Image(TEST_IMG("foo at 0x20")))?;
        writer.finish_write(Lsn(0x20));
        drop(writer);

        tline.checkpoint(CheckpointConfig::Forced)?;
        tline.compact()?;

        let writer = tline.writer();
        writer.put(TEST_KEY, Lsn(0x30), &Value::Image(TEST_IMG("foo at 0x30")))?;
        writer.finish_write(Lsn(0x30));
        drop(writer);

        tline.checkpoint(CheckpointConfig::Forced)?;
        tline.compact()?;

        let writer = tline.writer();
        writer.put(TEST_KEY, Lsn(0x40), &Value::Image(TEST_IMG("foo at 0x40")))?;
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
                    &Value::Image(TEST_IMG(&format!("{} at {}", blknum, lsn))),
                )?;
                writer.finish_write(lsn);
                drop(writer);

                keyspace.add_key(test_key);

                lsn = Lsn(lsn.0 + 0x10);
                blknum += 1;
            }

            let cutoff = tline.get_last_record_lsn();

            tline.update_gc_info(Vec::new(), cutoff, Duration::ZERO)?;
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
                &Value::Image(TEST_IMG(&format!("{} at {}", blknum, lsn))),
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
                    &Value::Image(TEST_IMG(&format!("{} at {}", blknum, lsn))),
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
            tline.update_gc_info(Vec::new(), cutoff, Duration::ZERO)?;
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
                &Value::Image(TEST_IMG(&format!("{} at {}", blknum, lsn))),
            )?;
            writer.finish_write(lsn);
            updated[blknum] = lsn;
            drop(writer);

            keyspace.add_key(test_key);
        }

        let mut tline_id = TIMELINE_ID;
        for _ in 0..50 {
            let new_tline_id = ZTimelineId::generate();
            repo.branch_timeline(tline_id, new_tline_id, Some(lsn))?;
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
                    &Value::Image(TEST_IMG(&format!("{} at {}", blknum, lsn))),
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
            tline.update_gc_info(Vec::new(), cutoff, Duration::ZERO)?;
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
            repo.branch_timeline(tline_id, new_tline_id, Some(lsn))?;
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
                    &Value::Image(TEST_IMG(&format!("{} {} at {}", idx, blknum, lsn))),
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

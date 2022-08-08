//!
//! Timeline repository implementation that keeps old data in files on disk, and
//! the recent changes in memory. See layered_repository/*_layer.rs files.
//! The functions here are responsible for locating the correct layer for the
//! get/put call, walking back the timeline branching history as needed.
//!
//! The files are stored in the .neon/tenants/<tenantid>/timelines/<timelineid>
//! directory. See layered_repository/README for how the files are managed.
//! In addition to the layer files, there is a metadata file in the same
//! directory that contains information about the timeline, in particular its
//! parent timeline, and the last LSN that has been written to disk.
//!

use anyhow::{anyhow, bail, ensure, Context, Result};
use tracing::*;

use std::cmp::min;
use std::collections::hash_map::Entry;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::fs;
use std::fs::File;
use std::ops::Bound::Included;
use std::path::Path;
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

use tokio::task::JoinHandle;

use self::metadata::TimelineMetadata;
use crate::config::PageServerConf;
use crate::tenant_config::{TenantConf, TenantConfOpt};

use crate::repository::{GcResult, Repository, Timeline};
use crate::thread_mgr;
use crate::walredo::{PostgresRedoManager, WalRedoManager};
use crate::CheckpointConfig;

use crate::storage_sync::create_remote_timeline_client;
use crate::storage_sync::download::list_remote_timelines;
use crate::storage_sync::index::IndexPart;

use serde::{Deserialize, Serialize};
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
use timeline::LayeredTimeline;

// re-export this function so that page_cache.rs can use it.
pub use crate::layered_repository::ephemeral_file::writeback as writeback_ephemeral_file;

// re-export for use in storage_sync.rs
pub use crate::layered_repository::timeline::save_metadata;

// re-export for use in tenant_mgr.rs
pub use crate::layered_repository::timeline::load_metadata;

// re-export for use in walreceiver
pub use crate::layered_repository::timeline::WalReceiverInfo;

/// Parts of the `.neon/tenants/<tenantid>/timelines/<timelineid>` directory prefix.
pub const TIMELINES_SEGMENT_NAME: &str = "timelines";

///
/// Each tenant has a state, which indicates whether it's active and ready to
/// process requests, or if it's loading or being stopped.
///
/// At pageserver startup, the `init_tenant_mgr` function scans the local 'tenants'
/// directory, and calls `spawn_load` for each tenant that it finds there.
/// `spawn_load` creates a LayeredRepository object for the tenant, initially
/// in Loading state, and launches a background task to load all the rest of the
/// in-memory structures for all the tenant's timelines into memory. When the
/// background task finishes, it sets the state to Active.
///
/// Attaching works similarly. When an Attach command is received, a LayeredRepository
/// struct is created in state Attaching, and a background task is launched. The
/// background task downloads all the data for the tenant from remote storage, and
/// when it's done, it sets the state to Active.
///
///   Loading ---->
///                        Active   ----> Stopping   ---> (dropped)
///   Attaching ---->
///
/// In Loading or Attaching state, some of the data can still be missing from
/// the in-memory structures, or from local disk, so GetPage requests cannot be
/// processed yet. When a new connection from the compute node comes in, the
/// code in `page_service.rs` will first look up the tenant in `tenant_mgr.rs`,
/// and then check the state of the tenant. If it's still Loading or Attaching,
/// the request will block and wait for the tenant to become Active.
///
/// Tenant shutdown happens in reverse to loading. The state is first set to
/// Stopping. After that, we signal all threads and tokio tasks that are
/// operating on the tenant, wait for them to stop (see `shutdown_threads` in
/// `thread_mgr.rs`). After all the threads/tasks have stopped, the
/// LayeredRepository struct can be dropped.
///
/// Tenant shutdown happens in two cases: when a tenant is detached, or when
/// the whole pageserver is shut down. The only difference is that o Detach,
/// all the files are also deleted from the local disk.
///
/// To recap, to access a timeline, i.e. to execute get() requests
/// on it:
/// 1. register the thread / task in 'thread_mgr.rs` to associate it with the
///    tenant and timeline
/// 2. look up the tenant's LayeredRepository object
/// 3. check that it's in Active state. Wait or error out if it's not
/// 4. look up the timeline's LayeredTimeline object
/// 5. call get()
///
/// You can keep the reference LayeredTimeline, and use it for as many get()
/// calls as you want, as long as the thread/task is registered with it. You
/// should react to shutdown-requests, while registered, otherwise
/// shutdown/detach will be blocked.
///
/// Shutdown sequence:
///
/// 1. change state to Stopping
/// 2. signal all registered threads/tasks to stop
/// 3. remove the tenant
///
/// There's one more state: Broken. If an error happens when a tenant is loaded
/// at pageserver startup, or when a tenant is attached, the tenant is marked as
/// Broken. It acts as a "tombstone", and trying to access a Broken tenant
/// returns an error.
///
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TenantState {
    // This tenant is being loaded from local disk
    Loading,

    // This tenant is being downloaded from cloud storage.
    Attaching,

    // This tenant exists on local disk, and the layer map has been loaded into memory.
    // The local disk might have some newer files that don't exist in cloud storage yet.
    Active,

    // This tenant exists on local disk, and the layer map has been loaded into memory.
    // The local disk might have some newer files that don't exist in cloud storage yet.
    // The tenant cannot be accessed anymore for any reason, but graceful shutdown.
    Stopping,

    // Something went wrong loading the tenant state
    Broken,
}

impl std::fmt::Display for TenantState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Loading => f.write_str("Loading"),
            Self::Attaching => f.write_str("Attaching"),
            Self::Active => f.write_str("Active"),
            Self::Stopping => f.write_str("Stopping"),
            Self::Broken => f.write_str("Broken"),
        }
    }
}

///
/// Repository consists of multiple timelines. Keep them in a hash table.
///
pub struct LayeredRepository {
    // Global pageserver config parameters
    pub conf: &'static PageServerConf,

    // Current state of the tenant. The timelines should only be accessed on
    // Active tenants. If a tenant is Stopping, it's OK to continue processing
    // current requests, as long as the current thread/task is registered with
    // the tenant/timeline in thread_mgr.rs. But as soon as all the currently
    // registered threads/tasks have finished, the tenant will go away.
    pub state: tokio::sync::watch::Sender<TenantState>,

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
    timelines: Mutex<HashMap<ZTimelineId, Arc<LayeredTimeline>>>,
    // This mutex prevents creation of new timelines during GC.
    // Adding yet another mutex (in addition to `timelines`) is needed because holding
    // `timelines` mutex during all GC iteration (especially with enforced checkpoint)
    // may block for a long time `get_timeline`, `get_timelines_state`,... and other operations
    // with timelines, which in turn may cause dropping replication connection, expiration of wait_for_lsn
    // timeout...
    gc_cs: Mutex<()>,
    walredo_mgr: Arc<dyn WalRedoManager + Send + Sync>,

    /// Makes every timeline to backup their files to remote storage.
    upload_layers: bool,
}

/// Public interface
impl Repository for LayeredRepository {
    type Timeline = LayeredTimeline;

    fn get_timeline(&self, timeline_id: ZTimelineId) -> Option<Arc<Self::Timeline>> {
        let timelines = self.timelines.lock().unwrap();
        timelines.get(&timeline_id).map(Arc::clone)
    }

    fn list_timelines(&self) -> Vec<Arc<Self::Timeline>> {
        self.timelines
            .lock()
            .unwrap()
            .values()
            .map(Arc::clone)
            .collect()
    }

    ///
    /// This is used to create the initial 'main' timeline during bootstrapping,
    /// or when importing a new base backup. The caller is expected to load an
    /// initial image of the datadir to the new timeline after this.
    ///
    /// NB: this doesn't launch the WAL receiver, because some callers don't want it.
    /// Call `timeline.launch_wal_receiver()` to launch it.
    ///
    fn create_empty_timeline(
        &self,
        timeline_id: ZTimelineId,
        initdb_lsn: Lsn,
    ) -> Result<Arc<LayeredTimeline>> {
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

        let remote_client = if self.upload_layers {
            let remote_client =
                create_remote_timeline_client(self.conf, self.tenant_id, timeline_id)?;
            remote_client.init_queue(&std::collections::HashSet::new(), Lsn(0));
            Some(remote_client)
        } else {
            None
        };

        let timeline = LayeredTimeline::new(
            self.conf,
            Arc::clone(&self.tenant_conf),
            &metadata,
            None,
            timeline_id,
            self.tenant_id,
            Arc::clone(&self.walredo_mgr),
            remote_client,
        );
        timeline.layers.write().unwrap().next_open_layer_at = Some(initdb_lsn);

        // Insert it to the hash map
        vacant_timeline_entry.insert(Arc::clone(&timeline));

        Ok(timeline)
    }

    ///
    /// Branch an existing timeline
    ///
    /// NB: this doesn't launch the WAL receiver, because some callers don't want it.
    /// Call `timeline.launch_wal_receiver()` to launch it.
    ///
    fn branch_timeline(
        &self,
        src: ZTimelineId,
        dst: ZTimelineId,
        start_lsn: Option<Lsn>,
    ) -> Result<Arc<LayeredTimeline>> {
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
        let src_timeline = Arc::clone(
            timelines
                .get(&src)
                // message about timeline being remote is one .context up in the stack
                .context("could not find source timeline {src}")?,
        );

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

        let remote_client = if self.upload_layers {
            let remote_client = create_remote_timeline_client(self.conf, self.tenant_id, dst)?;
            remote_client.init_queue(&HashSet::new(), start_lsn);
            Some(remote_client)
        } else {
            None
        };

        let new_timeline = LayeredTimeline::new(
            self.conf,
            Arc::clone(&self.tenant_conf),
            &metadata,
            Some(Arc::clone(&src_timeline)),
            dst,
            self.tenant_id,
            Arc::clone(&self.walredo_mgr),
            remote_client,
        );

        // Initialize the layer map. This will check the filesystem and find zero files.
        new_timeline
            .load_layer_map(start_lsn)
            .context("failed to load layermap")?;

        new_timeline.init_logical_size()?;

        timelines.insert(dst, Arc::clone(&new_timeline));

        info!("branched timeline {} from {} at {}", dst, src, start_lsn);

        Ok(new_timeline)
    }

    fn delete_timeline(&self, timeline_id: ZTimelineId) -> anyhow::Result<()> {
        // Start with the shutdown of timeline tasks (this shuts down the walreceiver)
        // It is important that we do not take locks here, and do not check whether the timeline exists
        // because if we hold tenants_state::write_tenants() while awaiting for the threads to join
        // we cannot create new timelines and tenants, and that can take quite some time,
        // it can even become stuck due to a bug making whole pageserver unavailable for some operations
        // so this is the way how we deal with concurrent delete requests: shutdown everythig, wait for confirmation
        // and then try to actually remove timeline from inmemory state and this is the point when concurrent requests
        // will synchronize and either fail with the not found error or succeed

        //let (sender, receiver) = std::sync::mpsc::channel::<()>();

        // FIXME
        //tenants_state::try_send_timeline_update(LocalTimelineUpdate::Detach {
        //    id: ZTenantTimelineId::new(tenant_id, timeline_id),
        //    join_confirmation_sender: sender,
        //});

        debug!("waiting for wal receiver to shutdown");
        //let _ = receiver.recv();
        debug!("wal receiver shutdown confirmed");
        debug!("waiting for threads to shutdown");
        thread_mgr::shutdown_threads(None, Some(self.tenant_id), Some(timeline_id));
        debug!("thread shutdown completed");

        // in order to be retriable detach needs to be idempotent
        // (or at least to a point that each time the detach is called it can make progress)
        let mut timelines = self.timelines.lock().unwrap();

        ensure!(timelines.contains_key(&timeline_id), "timeline not found");

        // Ensure that the timeline has no children
        let children_exist = timelines
            .values()
            .any(|t| t.get_ancestor_timeline_id() == Some(timeline_id));
        ensure!(
            !children_exist,
            "Cannot delete timeline which has child timelines"
        );

        timelines.remove(&timeline_id);

        let local_timeline_directory = self.conf.timeline_path(&timeline_id, &self.tenant_id);
        std::fs::remove_dir_all(&local_timeline_directory).with_context(|| {
            format!(
                "Failed to remove local timeline directory '{}'",
                local_timeline_directory.display()
            )
        })?;

        Ok(())
    }

    /// Public entry point to GC. All the logic is in the private
    /// gc_iteration_internal function, this public facade just wraps it for
    /// metrics collection.
    fn gc_iteration(
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
            timeline.compact()?;
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
            .map(|(&timeline_id, timeline)| (timeline_id, Arc::clone(timeline)))
            .collect::<Vec<(ZTimelineId, Arc<LayeredTimeline>)>>();
        drop(timelines);

        for (timelineid, timeline) in &timelines_to_compact {
            let _entered =
                info_span!("checkpoint", timeline = %timelineid, tenant = %self.tenant_id)
                    .entered();
            timeline.checkpoint(CheckpointConfig::Flush)?;
        }

        Ok(())
    }
}

/// Private functions
impl LayeredRepository {
    ///
    /// Initialize a new tenant. The tenant ID better not be in use in
    /// the remote storage.
    ///
    pub fn create(
        conf: &'static PageServerConf,
        tenant_conf: TenantConfOpt,
        tenant_id: ZTenantId,
        wal_redo_manager: Arc<dyn WalRedoManager + Send + Sync>,
    ) -> Result<LayeredRepository> {
        let repo_dir = conf.tenant_path(&tenant_id);
        ensure!(
            !repo_dir.exists(),
            "cannot create new tenant repo: '{}' directory already exists",
            tenant_id
        );

        // top-level dir may exist if we are creating it through CLI
        crashsafe_dir::create_dir_all(&repo_dir)
            .with_context(|| format!("could not create directory {}", repo_dir.display()))?;
        crashsafe_dir::create_dir(conf.timelines_path(&tenant_id))?;
        info!("created directory structure in {}", repo_dir.display());

        // Save tenant's config
        LayeredRepository::persist_tenant_config(conf, tenant_id, tenant_conf)?;

        // TODO: Should we upload something to remote storage? Otherwise there's no
        // trace of the new tenant in remote storage. The `tenant_conf` maybe?

        Ok(LayeredRepository::new(
            TenantState::Active,
            conf,
            tenant_conf,
            wal_redo_manager,
            tenant_id,
            conf.remote_storage_config.is_some(),
        ))
    }

    ///
    /// Attach a tenant that's available in cloud storage.
    ///
    /// This returns quickly, after just creating the in-memory object
    /// LayeredRepository struct.  On return, the tenant is most likely still in
    /// Attaching state, and it will become online in the background. You can
    /// use wait_until_active() to wait for the download to complete.
    ///
    pub fn spawn_attach(
        conf: &'static PageServerConf,
        tenant_id: ZTenantId,
    ) -> Result<Arc<LayeredRepository>> {
        // FIXME: where to get tenant config when attaching? This will just fill in
        // the defaults
        let tenant_conf = Self::load_tenant_config(conf, tenant_id)?;

        let wal_redo_manager = Arc::new(PostgresRedoManager::new(conf, tenant_id));
        let repo = Arc::new(LayeredRepository::new(
            TenantState::Attaching,
            conf,
            tenant_conf,
            wal_redo_manager,
            tenant_id,
            conf.remote_storage_config.is_some(),
        ));

        // Do all the hard work in the background
        let repo_clone = Arc::clone(&repo);
        tokio::spawn(async move {
            match repo_clone.attach_tenant().await {
                Ok(_) => {}
                Err(e) => {
                    error!("error attaching tenant: {:?}", e);
                }
            }
        });

        Ok(repo)
    }

    ///
    /// Background task that downloads all data for a tenant and brings it to Active state.
    ///
    async fn attach_tenant(self: &Arc<LayeredRepository>) -> Result<()> {
        // Get list of remote timelines
        // download index files for every tenant timeline
        let remote_timelines = list_remote_timelines(self.conf, self.tenant_id).await?;

        info!(
            "tenant {} contains {} timelines",
            self.tenant_id,
            remote_timelines.len()
        );

        let mut timeline_ancestors: Vec<(ZTimelineId, Option<ZTimelineId>)> = Vec::new();
        let mut index_parts: HashMap<ZTimelineId, &IndexPart> = HashMap::new();
        for (timeline_id, index_part) in remote_timelines.iter() {
            let remote_metadata = TimelineMetadata::from_bytes(&index_part.metadata_bytes)
                .with_context(|| {
                    format!(
                        "Failed to parse metadata file from remote storage for tenant {}",
                        self.tenant_id
                    )
                })?;
            timeline_ancestors.push((*timeline_id, remote_metadata.ancestor_timeline()));
            index_parts.insert(*timeline_id, index_part);
        }

        let sorted_timelines = tree_sort_timelines(&timeline_ancestors)?;

        let mut attached_timelines = Vec::new();

        // For every timeline, download every remote layer file that's missing locally
        for timeline_id in sorted_timelines {
            info!("downloading timeline {}", timeline_id);
            let index_part = index_parts.get(&timeline_id).unwrap();
            tokio::fs::create_dir_all(self.conf.timeline_path(&timeline_id, &self.tenant_id))
                .await
                .context("Failed to create new timeline directory")?;

            // Download everything
            let remote_metadata = TimelineMetadata::from_bytes(&index_part.metadata_bytes)
                .with_context(|| {
                    format!(
                        "Failed to parse metadata file from remote storage for tenant {}",
                        self.tenant_id
                    )
                })?;

            let remote_client =
                create_remote_timeline_client(self.conf, self.tenant_id, timeline_id)?;

            let ancestor = if let Some(ancestor_id) = remote_metadata.ancestor_timeline() {
                let timelines = self.timelines.lock().unwrap();
                Some(Arc::clone(timelines.get(&ancestor_id)
                    .ok_or_else(|| anyhow!("cannot find ancestor timeline {ancestor_id} for timeline {timeline_id}"))?))
            } else {
                None
            };

            let timeline = LayeredTimeline::new(
                self.conf,
                Arc::clone(&self.tenant_conf),
                &remote_metadata,
                ancestor,
                timeline_id,
                self.tenant_id,
                Arc::clone(&self.walredo_mgr),
                Some(remote_client),
            );

            // Initialize the layer map, based on all the files we now have on local disk.
            timeline
                .load_layer_map(remote_metadata.disk_consistent_lsn())
                .context("failed to load layermap")?;

            // Download everything from remote storage to local disk
            timeline.reconcile_with_remote(Some(index_part)).await?;

            info!("calculating initial size of {}", timeline.timeline_id);
            timeline.init_logical_size()?;

            let mut timelines = self.timelines.lock().unwrap();
            timelines.insert(timeline_id, Arc::clone(&timeline));
            drop(timelines);

            attached_timelines.push(timeline);
        }

        info!("tenant {} attach complete", self.tenant_id);

        // Spawn gc and compaction loops. The loops will shut themselves
        // down when they notice that the tenant is inactive.
        // TODO maybe use tokio::sync::watch instead?
        crate::tenant_tasks::start_background_loops(self);

        // We're ready for business.
        // FIXME: Check if the state has changed to Stopping while we were downloading stuff
        self.update_state(TenantState::Active);

        for timeline in attached_timelines.iter() {
            timeline.launch_wal_receiver()?;
        }

        Ok(())
    }

    ///
    /// Load a tenant that's available on local disk
    ///
    /// This is used at pageserver startup, to rebuild the in-memory
    /// structures from on-disk state.
    ///
    /// If the loading fails for some reason, the LayeredRepository will go into Broken
    /// state.
    ///
    /// TODO: initiate synchronization with remote storage
    ///
    pub fn spawn_load(
        conf: &'static PageServerConf,
        tenant_id: ZTenantId,
    ) -> Result<(Arc<LayeredRepository>, JoinHandle<()>)> {
        // FIXME: also go into Broken state if this fails
        let tenant_conf = Self::load_tenant_config(conf, tenant_id)?;

        let wal_redo_manager = Arc::new(PostgresRedoManager::new(conf, tenant_id));
        let repo = LayeredRepository::new(
            TenantState::Loading,
            conf,
            tenant_conf,
            wal_redo_manager,
            tenant_id,
            conf.remote_storage_config.is_some(),
        );
        let repo = Arc::new(repo);

        // Do all the hard work in a background task
        let repo_clone = Arc::clone(&repo);
        let handle = tokio::spawn(async move {
            match repo_clone.load_tenant().await {
                Ok(()) => {}
                Err(err) => {
                    repo_clone.update_state(TenantState::Broken);
                    error!("could not load tenant {}: {:?}", tenant_id, err);
                }
            }
        });

        info!("spawned load of {} into background", tenant_id);

        Ok((repo, handle))
    }

    ///
    /// Background task to load in-memory data structures for this tenant, from
    /// files on disk. Used at pageserver startup.
    ///
    async fn load_tenant(self: &Arc<LayeredRepository>) -> Result<()> {
        info!("loading tenant task {}", self.tenant_id);

        // Load in-memory state to reflect the local files on disk
        //
        // Scan the directory, peek into the metadata file of each timeline, and
        // collect a list of timelines and their ancestors.
        let mut timelines_to_load: Vec<(ZTimelineId, Option<ZTimelineId>)> = Vec::new();
        let timelines_dir = self.conf.timelines_path(&self.tenant_id);
        for entry in std::fs::read_dir(&timelines_dir).with_context(|| {
            format!(
                "Failed to list timelines directory for tenant {}",
                self.tenant_id
            )
        })? {
            let entry = entry?;
            let file_name = entry.file_name();
            if let Ok(timeline_id) = file_name
                .to_str()
                .unwrap_or_default()
                .parse::<ZTimelineId>()
            {
                let metadata = load_metadata(self.conf, timeline_id, self.tenant_id)
                    .context("failed to load metadata")?;
                let ancestor_id = metadata.ancestor_timeline();

                timelines_to_load.push((timeline_id, ancestor_id));
            } else {
                // A file or directory that doesn't look like a timeline ID
                warn!(
                    "unexpected file or directory in timelines directory: {}",
                    file_name.to_string_lossy()
                );
            }
        }

        // Sort the array of timeline IDs into tree-order, so that parent comes before
        // all its children.
        let sorted_timelines = tree_sort_timelines(&timelines_to_load)?;

        let mut loaded_timelines: Vec<Arc<LayeredTimeline>> = Vec::new();
        for &timeline_id in sorted_timelines.iter() {
            let timeline = self.load_timeline(timeline_id).await?;

            let mut timelines = self.timelines.lock().unwrap();
            timelines.insert(timeline_id, Arc::clone(&timeline));
            info!(
                "inserted timeline {} for tenant {}",
                timeline_id, self.tenant_id
            );
            loaded_timelines.push(timeline);
        }

        // FIXME: Currently, we assume that the local state is *ahead* or equal to the state in
        // remote storage. If it's not, we should try to catch up using the data from remote
        // storage.
        //let remote_timelines = list_remote_timelines(self.conf, self.tenant_id).await?;

        // Spawn gc and compaction loops. The loops will shut themselves
        // down when they notice that the tenant is inactive.
        // TODO maybe use tokio::sync::watch instead?
        crate::tenant_tasks::start_background_loops(self);

        // We're ready for business.
        // FIXME: Check if the state has changed to Stopping while we were downloading stuff
        self.update_state(TenantState::Active);

        info!("tenant {} loaded successfully", self.tenant_id);

        // Launch WAL receivers
        for timeline in loaded_timelines {
            timeline.launch_wal_receiver()?;
        }

        Ok(())
    }

    fn load_local_timeline(&self, timeline_id: ZTimelineId) -> Result<Arc<LayeredTimeline>> {
        // FIXME
        //let _enter =
        //    info_span!("loading timeline state from disk", timeline = %timeline_id).entered();
        let metadata = load_metadata(self.conf, timeline_id, self.tenant_id)
            .context("failed to load metadata")?;
        let disk_consistent_lsn = metadata.disk_consistent_lsn();

        let ancestor = if let Some(ancestor_timeline_id) = metadata.ancestor_timeline() {
            let ancestor_timeline = self.get_timeline(ancestor_timeline_id)
                .ok_or_else(|| anyhow!("cannot find ancestor timeline {ancestor_timeline_id} for timeline {timeline_id}"))?;
            Some(ancestor_timeline)
        } else {
            None
        };

        let remote_client = if self.upload_layers {
            Some(create_remote_timeline_client(
                self.conf,
                self.tenant_id,
                timeline_id,
            )?)
        } else {
            None
        };

        // TODO: Launch background task to start uploading anything that's not present in
        // remote storage yet

        let timeline = LayeredTimeline::new(
            self.conf,
            Arc::clone(&self.tenant_conf),
            &metadata,
            ancestor,
            timeline_id,
            self.tenant_id,
            Arc::clone(&self.walredo_mgr),
            remote_client,
        );

        // Scan the timeline directory, and load information about all the layer files
        // into memory
        timeline
            .load_layer_map(disk_consistent_lsn)
            .context("failed to load layermap")?;
        Ok(timeline)
    }

    /// Subroutine of `load_tenant`, to load an individual timeline
    ///
    /// NB: The parent is assumed to be already loaded!
    async fn load_timeline(&self, timeline_id: ZTimelineId) -> Result<Arc<LayeredTimeline>> {
        let timeline = self.load_local_timeline(timeline_id)?;

        if self.upload_layers {
            // Reconcile local state with remote storage, downloading anything that's
            // missing locally, and scheduling uploads for anything that's missing
            // in remote storage.
            timeline.reconcile_with_remote(None).await?;
        }

        timeline.init_logical_size()?;

        Ok(timeline)
    }

    /// Detach tenant.
    ///
    /// This removes all in-memory data for the tenant, as well as all local files.
    /// The tenant still remains in remote storage, and can be re-attached later,
    /// or to a different pageserver.
    ///
    /// Returns a JoinHandle that you can use to wait for the detach operation to finish.
    pub fn spawn_detach(self: &Arc<LayeredRepository>) -> Result<JoinHandle<Result<()>>> {
        let old_state = self.state.send_replace(TenantState::Stopping);
        if old_state == TenantState::Stopping {
            bail!("already stopping");
        }

        // Do all the hard work in the background
        let repo_clone = Arc::clone(self);
        Ok(tokio::spawn(
            async move { repo_clone.detach_tenant().await },
        ))
    }

    async fn detach_tenant(&self) -> Result<()> {
        // FIXME: Should we wait for all in-progress uploads to finish first? Maybe with
        // a timeout?

        // shutdown the tenant and timeline threads: gc, compaction, page service threads)
        // FIXME: should we keep the layer flushing active until we have shut down WAL
        // receivers
        // FIXME: does thread_mgr::shutdown_threads also shut down the WAL receiver?
        thread_mgr::shutdown_threads(None, Some(self.tenant_id), None);

        let timelines: Vec<Arc<LayeredTimeline>> =
            self.timelines.lock().unwrap().values().cloned().collect();
        for timeline in timelines.iter() {
            timeline.shutdown().await?;
        }

        // Now there is nothing actively accessing the tenant or its timelines, we can
        // delete local files.
        //
        // Start by deleting the index files, so that if we crash, we will not be fooled to
        // to think that the tenant data is valid on disk.
        // FIXME: this isn't bulletproof either, if we crash after deleting some of the index
        // files, but not all.
        let timelines = self.timelines.lock().unwrap();
        for (&timeline_id, _timeline) in timelines.iter() {
            timeline::delete_metadata(self.conf, timeline_id, self.tenant_id)?;
        }

        // If removal fails there will be no way to successfully retry detach,
        // because tenant no longer exists in in memory map. And it needs to be removed from it
        // before we remove files because it contains references to repository
        // which references ephemeral files which are deleted on drop. So if we keep these references
        // code will attempt to remove files which no longer exist. This can be fixed by having shutdown
        // mechanism for repository that will clean temporary data to avoid any references to ephemeral files
        let local_tenant_directory = self.conf.tenant_path(&self.tenant_id);
        std::fs::remove_dir_all(&local_tenant_directory).with_context(|| {
            format!(
                "Failed to remove local tenant directory '{}'",
                local_tenant_directory.display()
            )
        })?;

        Ok(())
    }

    /// Called on pageserver shutdown
    pub async fn shutdown(&self) -> Result<()> {
        let old_state = self.state.send_replace(TenantState::Stopping);
        if old_state == TenantState::Stopping {
            bail!("already stopping");
        }
        // shutdown the tenant and timeline threads: gc, compaction, page service threads)
        thread_mgr::shutdown_threads(None, Some(self.tenant_id), None);

        // FIXME: does thread_mgr::shutdown_threads also shut down the WAL receiver?

        let timelines: Vec<Arc<LayeredTimeline>> =
            self.timelines.lock().unwrap().values().cloned().collect();
        for timeline in timelines.iter() {
            timeline.shutdown().await?;
        }

        Ok(())
    }

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

    pub fn update_tenant_config(&self, new_tenant_conf: TenantConfOpt) -> Result<()> {
        let mut tenant_conf = self.tenant_conf.write().unwrap();

        tenant_conf.update(&new_tenant_conf);

        LayeredRepository::persist_tenant_config(self.conf, self.tenant_id, *tenant_conf)?;
        Ok(())
    }

    pub fn new(
        state: TenantState,
        conf: &'static PageServerConf,
        tenant_conf: TenantConfOpt,
        walredo_mgr: Arc<dyn WalRedoManager + Send + Sync>,
        tenant_id: ZTenantId,
        upload_layers: bool,
    ) -> LayeredRepository {
        let (watch_sender, _) = tokio::sync::watch::channel(state);
        LayeredRepository {
            state: watch_sender,
            tenant_id,
            file_lock: RwLock::new(()),
            conf,
            tenant_conf: Arc::new(RwLock::new(tenant_conf)),
            timelines: Mutex::new(HashMap::new()),
            gc_cs: Mutex::new(()),
            walredo_mgr,
            upload_layers,
        }
    }

    /// Locate and load config
    pub fn load_tenant_config(
        conf: &'static PageServerConf,
        tenant_id: ZTenantId,
    ) -> anyhow::Result<TenantConfOpt> {
        let target_config_path = TenantConf::path(conf, tenant_id);

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
        tenant_id: ZTenantId,
        tenant_conf: TenantConfOpt,
    ) -> anyhow::Result<()> {
        let _enter = info_span!("saving tenantconf").entered();
        let target_config_path = TenantConf::path(conf, tenant_id);
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

        info!("gc_iteration for tenant {}", self.tenant_id);

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
                .map(|(timeline_id, timeline)| {
                    // This is unresolved question for now, how to do gc in presence of remote timelines
                    // especially when this is combined with branching.
                    // Somewhat related: https://github.com/neondatabase/neon/issues/999
                    if let Some(ancestor_timeline_id) = &timeline.get_ancestor_timeline_id() {
                        // If target_timeline is specified, we only need to know branchpoints of its children
                        if let Some(timelineid) = target_timeline_id {
                            if ancestor_timeline_id == &timelineid {
                                all_branchpoints
                                    .insert((*ancestor_timeline_id, timeline.get_ancestor_lsn()));
                            }
                        }
                        // Collect branchpoints for all timelines
                        else {
                            all_branchpoints
                                .insert((*ancestor_timeline_id, timeline.get_ancestor_lsn()));
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
            if let Some(timeline) = self.get_timeline(timeline_id) {
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
            } else {
                // The timeline was deleted, while we were busy GC'ing other timelines
                // It could happen, but should be rare. Print a message to the log,
                // so that if it happens more frequently than we expect, we might notice.
                info!("timeline {timeline_id} could not be GC'd, becuase it concurrently deleted");
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

    pub async fn wait_until_active(&self) -> Result<()> {
        let mut receiver = self.state.subscribe();
        loop {
            let value = *receiver.borrow();
            match value {
                TenantState::Active => {
                    return Ok(());
                }
                TenantState::Loading | TenantState::Attaching => {
                    // Wait
                    info!(
                        "waiting for tenant {} to become active, current state: {}",
                        self.tenant_id, value
                    );
                    receiver.changed().await?;
                    continue;
                }
                TenantState::Stopping => {
                    bail!("tenant is being shut down");
                }
                TenantState::Broken => {
                    bail!("tenant is in broken state");
                }
            }
        }
    }

    fn update_state(&self, state: TenantState) {
        self.state.send_replace(state);
    }

    pub fn get_state(&self) -> TenantState {
        *self.state.borrow()
    }
}

///
/// Given a Vec of timelines and their ancestors (timeline_id, ancestor_id),
/// perform a topological sort, so that the parent of each timeline comes
/// before the children.
///
fn tree_sort_timelines(
    timelines: &[(ZTimelineId, Option<ZTimelineId>)],
) -> Result<Vec<ZTimelineId>> {
    let mut result = Vec::new();

    let mut now: Vec<ZTimelineId> = Vec::new();
    // (ancestor, children)
    let mut later: HashMap<ZTimelineId, Vec<ZTimelineId>> = HashMap::new();

    for &(timeline_id, ancestor_id) in timelines.iter() {
        if let Some(ancestor_id) = ancestor_id {
            let children = later.entry(ancestor_id).or_default();
            children.push(timeline_id);
        } else {
            now.push(timeline_id);
        }
    }

    while let Some(timeline_id) = now.pop() {
        result.push(timeline_id);
        // All children of this can be loaded now
        if let Some(mut children) = later.remove(&timeline_id) {
            now.append(&mut children);
        }
    }

    // All timelines should be visited now. Unless there were timelines with missing ancestors.
    if !later.is_empty() {
        for (missing_id, orphan_ids) in later.iter() {
            for orphan_id in orphan_ids.iter() {
                error!("could not load timeline {orphan_id} because its ancestor timeline {missing_id} could not be loaded");
            }
        }
        bail!("could not load tenant because some timelines are missing ancestors");
    }

    Ok(result)
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

#[cfg(test)]
pub mod repo_harness {
    use bytes::{Bytes, BytesMut};
    use once_cell::sync::Lazy;
    use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};
    use std::{fs, path::PathBuf};

    use crate::repository::Key;
    use crate::walrecord::ZenithWalRecord;
    use crate::RepositoryImpl;
    use crate::{
        config::PageServerConf,
        layered_repository::LayeredRepository,
        walredo::{WalRedoError, WalRedoManager},
    };

    use super::*;
    use crate::tenant_config::{TenantConf, TenantConfOpt};
    use hex_literal::hex;
    use utils::zid::ZTenantId;

    pub const TIMELINE_ID: ZTimelineId =
        ZTimelineId::from_array(hex!("11223344556677881122334455667788"));
    pub const NEW_TIMELINE_ID: ZTimelineId =
        ZTimelineId::from_array(hex!("AA223344556677881122334455667788"));

    /// Convenience function to create a page image with given string as the only content
    #[allow(non_snake_case)]
    pub fn TEST_IMG(s: &str) -> Bytes {
        let mut buf = BytesMut::new();
        buf.extend_from_slice(s.as_bytes());
        buf.resize(64, 0);

        buf.freeze()
    }

    static LOCK: Lazy<RwLock<()>> = Lazy::new(|| RwLock::new(()));

    impl From<TenantConf> for TenantConfOpt {
        fn from(tenant_conf: TenantConf) -> Self {
            Self {
                checkpoint_distance: Some(tenant_conf.checkpoint_distance),
                compaction_target_size: Some(tenant_conf.compaction_target_size),
                compaction_period: Some(tenant_conf.compaction_period),
                compaction_threshold: Some(tenant_conf.compaction_threshold),
                gc_horizon: Some(tenant_conf.gc_horizon),
                gc_period: Some(tenant_conf.gc_period),
                image_creation_threshold: Some(tenant_conf.image_creation_threshold),
                pitr_interval: Some(tenant_conf.pitr_interval),
                walreceiver_connect_timeout: Some(tenant_conf.walreceiver_connect_timeout),
                lagging_wal_timeout: Some(tenant_conf.lagging_wal_timeout),
                max_lsn_wal_lag: Some(tenant_conf.max_lsn_wal_lag),
            }
        }
    }

    pub struct RepoHarness<'a> {
        pub conf: &'static PageServerConf,
        pub tenant_conf: TenantConf,
        pub tenant_id: ZTenantId,

        pub lock_guard: (
            Option<RwLockReadGuard<'a, ()>>,
            Option<RwLockWriteGuard<'a, ()>>,
        ),
    }

    impl<'a> RepoHarness<'a> {
        pub fn create(test_name: &'static str) -> Result<Self> {
            Self::create_internal(test_name, false)
        }
        pub fn create_exclusive(test_name: &'static str) -> Result<Self> {
            Self::create_internal(test_name, true)
        }
        fn create_internal(test_name: &'static str, exclusive: bool) -> Result<Self> {
            let lock_guard = if exclusive {
                (None, Some(LOCK.write().unwrap()))
            } else {
                (Some(LOCK.read().unwrap()), None)
            };

            let repo_dir = PageServerConf::test_repo_dir(test_name);
            let _ = fs::remove_dir_all(&repo_dir);
            fs::create_dir_all(&repo_dir)?;

            let conf = PageServerConf::dummy_conf(repo_dir);
            // Make a static copy of the config. This can never be free'd, but that's
            // OK in a test.
            let conf: &'static PageServerConf = Box::leak(Box::new(conf));

            let tenant_conf = TenantConf::dummy_conf();

            let tenant_id = ZTenantId::generate();
            fs::create_dir_all(conf.tenant_path(&tenant_id))?;
            fs::create_dir_all(conf.timelines_path(&tenant_id))?;

            Ok(Self {
                conf,
                tenant_conf,
                tenant_id,
                lock_guard,
            })
        }

        pub fn load(&self) -> RepositoryImpl {
            self.try_load().expect("failed to load test repo")
        }

        pub fn try_load(&self) -> Result<RepositoryImpl> {
            let walredo_mgr = Arc::new(TestRedoManager);

            let repo = LayeredRepository::new(
                crate::layered_repository::TenantState::Active,
                self.conf,
                TenantConfOpt::from(self.tenant_conf),
                walredo_mgr,
                self.tenant_id,
                false,
            );
            // populate repo with locally available timelines
            for timeline_dir_entry in fs::read_dir(self.conf.timelines_path(&self.tenant_id))
                .expect("should be able to read timelines dir")
            {
                let timeline_dir_entry = timeline_dir_entry.unwrap();
                let timeline_id: ZTimelineId = timeline_dir_entry
                    .path()
                    .file_name()
                    .unwrap()
                    .to_string_lossy()
                    .parse()
                    .unwrap();

                let timeline = repo.load_local_timeline(timeline_id)?;
                timeline.init_logical_size()?;

                // Insert it to the hash map
                repo.timelines.lock().unwrap().insert(timeline_id, timeline);
            }

            Ok(repo)
        }

        pub fn timeline_path(&self, timeline_id: &ZTimelineId) -> PathBuf {
            self.conf.timeline_path(timeline_id, &self.tenant_id)
        }
    }

    // Mock WAL redo manager that doesn't do much
    pub struct TestRedoManager;

    impl WalRedoManager for TestRedoManager {
        fn request_redo(
            &self,
            key: Key,
            lsn: Lsn,
            base_img: Option<Bytes>,
            records: Vec<(Lsn, ZenithWalRecord)>,
        ) -> Result<Bytes, WalRedoError> {
            let s = format!(
                "redo for {} to get to {}, with {} and {} records",
                key,
                lsn,
                if base_img.is_some() {
                    "base image"
                } else {
                    "no base image"
                },
                records.len()
            );
            println!("{}", s);

            Ok(TEST_IMG(&s))
        }
    }
}

///
/// Tests that should work the same with any Repository/Timeline implementation.
///
#[allow(clippy::bool_assert_comparison)]
#[cfg(test)]
mod repo_tests {
    use super::repo_harness::*;
    use super::*;
    use crate::pgdatadir_mapping::create_test_timeline;
    use crate::pgdatadir_mapping::DatadirTimeline;
    use crate::repository::{Key, Value};
    use bytes::BytesMut;
    use hex_literal::hex;
    use once_cell::sync::Lazy;

    static TEST_KEY: Lazy<Key> =
        Lazy::new(|| Key::from_slice(&hex!("112222222233333333444444445500000001")));

    pub fn assert_current_logical_size(timeline: &LayeredTimeline, lsn: Lsn) {
        let incremental = timeline.get_current_logical_size();
        let non_incremental = timeline
            .get_current_logical_size_non_incremental(lsn)
            .unwrap();
        assert_eq!(incremental, non_incremental);
    }

    #[test]
    fn test_basic() -> Result<()> {
        let mut repo = RepoHarness::create("test_basic")?.load();
        let tline = create_test_timeline(&mut repo, TIMELINE_ID)?;

        let writer = tline.writer();
        writer.put(*TEST_KEY, Lsn(0x10), &Value::Image(TEST_IMG("foo at 0x10")))?;
        writer.finish_write(Lsn(0x10));
        drop(writer);

        let writer = tline.writer();
        writer.put(*TEST_KEY, Lsn(0x20), &Value::Image(TEST_IMG("foo at 0x20")))?;
        writer.finish_write(Lsn(0x20));
        drop(writer);

        assert_eq!(tline.get(*TEST_KEY, Lsn(0x10))?, TEST_IMG("foo at 0x10"));
        assert_eq!(tline.get(*TEST_KEY, Lsn(0x1f))?, TEST_IMG("foo at 0x10"));
        assert_eq!(tline.get(*TEST_KEY, Lsn(0x20))?, TEST_IMG("foo at 0x20"));

        Ok(())
    }

    #[test]
    fn no_duplicate_timelines() -> Result<()> {
        let repo = RepoHarness::create("no_duplicate_timelines")?.load();
        let _ = repo.create_empty_timeline(TIMELINE_ID, Lsn(0))?;

        match repo.create_empty_timeline(TIMELINE_ID, Lsn(0)) {
            Ok(_) => panic!("duplicate timeline creation should fail"),
            Err(e) => assert_eq!(e.to_string(), "Timeline already exists"),
        }

        Ok(())
    }

    /// Convenience function to create a page image with given string as the only content
    pub fn test_value(s: &str) -> Value {
        let mut buf = BytesMut::new();
        buf.extend_from_slice(s.as_bytes());
        Value::Image(buf.freeze())
    }

    ///
    /// Test branch creation
    ///
    #[test]
    fn test_branch() -> Result<()> {
        let mut repo = RepoHarness::create("test_branch")?.load();
        let tline = create_test_timeline(&mut repo, TIMELINE_ID)?;
        tline.init_logical_size()?;
        let writer = tline.writer();
        use std::str::from_utf8;

        #[allow(non_snake_case)]
        let TEST_KEY_A: Key = Key::from_hex("112222222233333333444444445500000001").unwrap();
        #[allow(non_snake_case)]
        let TEST_KEY_B: Key = Key::from_hex("112222222233333333444444445500000002").unwrap();

        // Insert a value on the timeline
        writer.put(TEST_KEY_A, Lsn(0x20), &test_value("foo at 0x20"))?;
        writer.put(TEST_KEY_B, Lsn(0x20), &test_value("foobar at 0x20"))?;
        writer.finish_write(Lsn(0x20));

        writer.put(TEST_KEY_A, Lsn(0x30), &test_value("foo at 0x30"))?;
        writer.finish_write(Lsn(0x30));
        writer.put(TEST_KEY_A, Lsn(0x40), &test_value("foo at 0x40"))?;
        writer.finish_write(Lsn(0x40));

        assert_current_logical_size(&tline, Lsn(0x40));

        // Branch the history, modify relation differently on the new timeline
        repo.branch_timeline(TIMELINE_ID, NEW_TIMELINE_ID, Some(Lsn(0x30)))?;
        let newtline = repo
            .get_timeline(NEW_TIMELINE_ID)
            .expect("Should have a local timeline");
        let new_writer = newtline.writer();
        new_writer.put(TEST_KEY_A, Lsn(0x40), &test_value("bar at 0x40"))?;
        new_writer.finish_write(Lsn(0x40));

        // Check page contents on both branches
        assert_eq!(
            from_utf8(&tline.get(TEST_KEY_A, Lsn(0x40))?)?,
            "foo at 0x40"
        );
        assert_eq!(
            from_utf8(&newtline.get(TEST_KEY_A, Lsn(0x40))?)?,
            "bar at 0x40"
        );
        assert_eq!(
            from_utf8(&newtline.get(TEST_KEY_B, Lsn(0x40))?)?,
            "foobar at 0x20"
        );

        //assert_current_logical_size(&tline, Lsn(0x40));

        Ok(())
    }

    fn make_some_layers<T: Timeline>(tline: &T, start_lsn: Lsn) -> Result<()> {
        let mut lsn = start_lsn;
        #[allow(non_snake_case)]
        {
            let writer = tline.writer();
            // Create a relation on the timeline
            writer.put(
                *TEST_KEY,
                lsn,
                &Value::Image(TEST_IMG(&format!("foo at {}", lsn))),
            )?;
            writer.finish_write(lsn);
            lsn += 0x10;
            writer.put(
                *TEST_KEY,
                lsn,
                &Value::Image(TEST_IMG(&format!("foo at {}", lsn))),
            )?;
            writer.finish_write(lsn);
            lsn += 0x10;
        }
        tline.checkpoint(CheckpointConfig::Forced)?;
        {
            let writer = tline.writer();
            writer.put(
                *TEST_KEY,
                lsn,
                &Value::Image(TEST_IMG(&format!("foo at {}", lsn))),
            )?;
            writer.finish_write(lsn);
            lsn += 0x10;
            writer.put(
                *TEST_KEY,
                lsn,
                &Value::Image(TEST_IMG(&format!("foo at {}", lsn))),
            )?;
            writer.finish_write(lsn);
        }
        tline.checkpoint(CheckpointConfig::Forced)
    }

    #[test]
    fn test_prohibit_branch_creation_on_garbage_collected_data() -> Result<()> {
        let repo =
            RepoHarness::create("test_prohibit_branch_creation_on_garbage_collected_data")?.load();
        let tline = repo.create_empty_timeline(TIMELINE_ID, Lsn(0))?;
        make_some_layers(tline.as_ref(), Lsn(0x20))?;

        // this removes layers before lsn 40 (50 minus 10), so there are two remaining layers, image and delta for 31-50
        // FIXME: this doesn't actually remove any layer currently, given how the checkpointing
        // and compaction works. But it does set the 'cutoff' point so that the cross check
        // below should fail.
        repo.gc_iteration(Some(TIMELINE_ID), 0x10, Duration::ZERO, false)?;

        // try to branch at lsn 25, should fail because we already garbage collected the data
        match repo.branch_timeline(TIMELINE_ID, NEW_TIMELINE_ID, Some(Lsn(0x25))) {
            Ok(_) => panic!("branching should have failed"),
            Err(err) => {
                assert!(err.to_string().contains("invalid branch start lsn"));
                assert!(err
                    .source()
                    .unwrap()
                    .to_string()
                    .contains("we might've already garbage collected needed data"))
            }
        }

        Ok(())
    }

    #[test]
    fn test_prohibit_branch_creation_on_pre_initdb_lsn() -> Result<()> {
        let repo = RepoHarness::create("test_prohibit_branch_creation_on_pre_initdb_lsn")?.load();

        repo.create_empty_timeline(TIMELINE_ID, Lsn(0x50))?;
        // try to branch at lsn 0x25, should fail because initdb lsn is 0x50
        match repo.branch_timeline(TIMELINE_ID, NEW_TIMELINE_ID, Some(Lsn(0x25))) {
            Ok(_) => panic!("branching should have failed"),
            Err(err) => {
                assert!(&err.to_string().contains("invalid branch start lsn"));
                assert!(&err
                    .source()
                    .unwrap()
                    .to_string()
                    .contains("is earlier than latest GC horizon"));
            }
        }

        Ok(())
    }

    /*
    // FIXME: This currently fails to error out. Calling GC doesn't currently
    // remove the old value, we'd need to work a little harder
    #[test]
    fn test_prohibit_get_for_garbage_collected_data() -> Result<()> {
        let repo =
            RepoHarness::create("test_prohibit_get_for_garbage_collected_data")?
            .load();

        let tline = repo.create_empty_timeline(TIMELINE_ID, Lsn(0))?;
        make_some_layers(tline.as_ref(), Lsn(0x20))?;

        repo.gc_iteration(Some(TIMELINE_ID), 0x10, Duration::ZERO, false)?;
        let latest_gc_cutoff_lsn = tline.get_latest_gc_cutoff_lsn();
        assert!(*latest_gc_cutoff_lsn > Lsn(0x25));
        match tline.get(*TEST_KEY, Lsn(0x25)) {
            Ok(_) => panic!("request for page should have failed"),
            Err(err) => assert!(err.to_string().contains("not found at")),
        }
        Ok(())
    }
     */

    #[test]
    fn test_retain_data_in_parent_which_is_needed_for_child() -> Result<()> {
        let mut repo =
            RepoHarness::create("test_retain_data_in_parent_which_is_needed_for_child")?.load();
        let tline = create_test_timeline(&mut repo, TIMELINE_ID)?;
        make_some_layers(tline.as_ref(), Lsn(0x20))?;

        repo.branch_timeline(TIMELINE_ID, NEW_TIMELINE_ID, Some(Lsn(0x40)))?;
        let newtline = repo
            .get_timeline(NEW_TIMELINE_ID)
            .expect("Should have a local timeline");
        // this removes layers before lsn 40 (50 minus 10), so there are two remaining layers, image and delta for 31-50
        repo.gc_iteration(Some(TIMELINE_ID), 0x10, Duration::ZERO, false)?;
        assert!(newtline.get(*TEST_KEY, Lsn(0x25)).is_ok());

        Ok(())
    }
    #[test]
    fn test_parent_keeps_data_forever_after_branching() -> Result<()> {
        let mut repo =
            RepoHarness::create("test_parent_keeps_data_forever_after_branching")?.load();
        let tline = create_test_timeline(&mut repo, TIMELINE_ID)?;
        make_some_layers(tline.as_ref(), Lsn(0x20))?;

        repo.branch_timeline(TIMELINE_ID, NEW_TIMELINE_ID, Some(Lsn(0x40)))?;
        let newtline = repo
            .get_timeline(NEW_TIMELINE_ID)
            .expect("Should have a local timeline");

        make_some_layers(newtline.as_ref(), Lsn(0x60))?;

        // run gc on parent
        repo.gc_iteration(Some(TIMELINE_ID), 0x10, Duration::ZERO, false)?;

        // Check that the data is still accessible on the branch.
        assert_eq!(
            newtline.get(*TEST_KEY, Lsn(0x50))?,
            TEST_IMG(&format!("foo at {}", Lsn(0x40)))
        );

        Ok(())
    }
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
    use super::repo_harness::*;
    use super::*;
    use crate::keyspace::KeySpaceAccum;
    use crate::pgdatadir_mapping::create_test_timeline;
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
        assert_eq!(err.to_string(), "failed to load metadata");

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
        let mut repo = RepoHarness::create("test_traverse_branches")?.load();
        let mut tline = create_test_timeline(&mut repo, TIMELINE_ID)?;

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
            tline = repo.get_timeline(new_tline_id).unwrap();
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
        let mut repo = RepoHarness::create("test_traverse_ancestors")?.load();
        let mut tline = create_test_timeline(&mut repo, TIMELINE_ID)?;

        const NUM_KEYS: usize = 100;
        const NUM_TLINES: usize = 50;

        let mut test_key = Key::from_hex("012222222233333333444444445500000000").unwrap();
        // Track page mutation lsns across different timelines.
        let mut updated = [[Lsn(0); NUM_KEYS]; NUM_TLINES];

        let mut lsn = Lsn(0x10);
        let mut tline_id = TIMELINE_ID;

        #[allow(clippy::needless_range_loop)]
        for idx in 0..NUM_TLINES {
            let new_tline_id = ZTimelineId::generate();
            repo.branch_timeline(tline_id, new_tline_id, Some(lsn))?;
            tline = repo.get_timeline(new_tline_id).unwrap();
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

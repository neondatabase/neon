//!
//! Timeline repository implementation that keeps old data in files on disk, and
//! the recent changes in memory. See tenant/*_layer.rs files.
//! The functions here are responsible for locating the correct layer for the
//! get/put call, walking back the timeline branching history as needed.
//!
//! The files are stored in the .neon/tenants/<tenant_id>/timelines/<timeline_id>
//! directory. See docs/pageserver-storage.md for how the files are managed.
//! In addition to the layer files, there is a metadata file in the same
//! directory that contains information about the timeline, in particular its
//! parent timeline, and the last LSN that has been written to disk.
//!

use anyhow::{anyhow, bail, ensure, Context, Result};
use tokio::sync::watch;
use tracing::*;
use utils::crashsafe_dir::path_with_suffix_extension;

use std::cmp::min;
use std::collections::hash_map::Entry;
use std::collections::{BTreeSet, HashMap};
use std::fs;
use std::fs::File;
use std::fs::OpenOptions;
use std::io::Write;
use std::ops::Bound::Included;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::process::Stdio;
use std::sync::Arc;
use std::sync::{Mutex, MutexGuard, RwLock};
use std::time::{Duration, Instant};

use self::metadata::TimelineMetadata;
use crate::config::PageServerConf;
use crate::import_datadir;
use crate::metrics::{remove_tenant_metrics, STORAGE_TIME};
use crate::repository::GcResult;
use crate::task_mgr::{self, TaskKind};
use crate::tenant_config::TenantConfOpt;
use crate::virtual_file::VirtualFile;
use crate::walredo::{PostgresRedoManager, WalRedoManager};
use crate::{CheckpointConfig, TEMP_FILE_SUFFIX};
pub use pageserver_api::models::TenantState;

use crate::storage_sync::create_remote_timeline_client;
use crate::storage_sync::index::IndexPart;
use crate::storage_sync::list_remote_timelines;
use remote_storage::GenericRemoteStorage;

use toml_edit;
use utils::{
    crashsafe_dir,
    id::{TenantId, TimelineId},
    lsn::{Lsn, RecordLsn},
};

mod blob_io;
pub mod block_io;
mod delta_layer;
mod disk_btree;
pub(crate) mod ephemeral_file;
pub mod filename;
mod image_layer;
mod inmemory_layer;
pub mod layer_map;

pub mod metadata;
mod par_fsync;
mod remote_layer;
mod storage_layer;

mod timeline;

use storage_layer::Layer;

pub use timeline::{retry_get, retry_get_with_timeout};
pub use timeline::{PageReconstructError, Timeline};

// re-export this function so that page_cache.rs can use it.
pub use crate::tenant::ephemeral_file::writeback as writeback_ephemeral_file;

// re-export for use in storage_sync.rs
pub use crate::tenant::metadata::{load_metadata, save_metadata};

// re-export for use in walreceiver
pub use crate::tenant::timeline::WalReceiverInfo;

/// Parts of the `.neon/tenants/<tenant_id>/timelines/<timeline_id>` directory prefix.
pub const TIMELINES_SEGMENT_NAME: &str = "timelines";

///
/// Tenant consists of multiple timelines. Keep them in a hash table.
///
pub struct Tenant {
    // Global pageserver config parameters
    pub conf: &'static PageServerConf,

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
    /// Stopping. After that, we signal all tokio tasks that are
    /// operating on the tenant, wait for them to stop (see `shutdown_tasks` in
    /// `task_mgr.rs`). After all the tasks have stopped, the
    /// Repository struct can be dropped.
    ///
    /// Tenant shutdown happens in two cases: when a tenant is detached, or when
    /// the whole pageserver is shut down. The only difference is that o Detach,
    /// all the files are also deleted from the local disk.
    ///
    /// To recap, to access a timeline, i.e. to execute get() requests
    /// on it:
    /// 1. register the task in 'task_mgr.rs` to associate it with the
    ///    tenant and timeline
    /// 2. look up the tenant's LayeredRepository object
    /// 3. check that it's in Active state. Wait or error out if it's not
    /// 4. look up the timeline's LayeredTimeline object
    /// 5. call get()
    ///
    /// You can keep the reference LayeredTimeline, and use it for as many get()
    /// calls as you want, as long as the task is registered with it. You
    /// should react to shutdown-requests, while registered, otherwise
    /// shutdown/detach will be blocked.
    ///
    /// Shutdown sequence:
    ///
    /// 1. change state to Stopping
    /// 2. signal all registered tasks to stop
    /// 3. remove the tenant
    ///
    /// There's one more state: Broken. If an error happens when a tenant is loaded
    /// at pageserver startup, or when a tenant is attached, the tenant is marked as
    /// Broken. It acts as a "tombstone", and trying to access a Broken tenant
    /// returns an error.
    ///
    // Current state of the tenant. The timelines should only be accessed on
    // Active tenants. If a tenant is Stopping, it's OK to continue processing
    // current requests, as long as the current task is registered with
    // the tenant/timeline in task_mgr.rs. But as soon as all the currently
    // registered tasks have finished, the tenant will go away.
    pub state: watch::Sender<TenantState>,

    // Overridden tenant-specific config parameters.
    // We keep TenantConfOpt sturct here to preserve the information
    // about parameters that are not set.
    // This is necessary to allow global config updates.
    tenant_conf: Arc<RwLock<TenantConfOpt>>,

    tenant_id: TenantId,
    timelines: Mutex<HashMap<TimelineId, Arc<Timeline>>>,
    // This mutex prevents creation of new timelines during GC.
    // Adding yet another mutex (in addition to `timelines`) is needed because holding
    // `timelines` mutex during all GC iteration (especially with enforced checkpoint)
    // may block for a long time `get_timeline`, `get_timelines_state`,... and other operations
    // with timelines, which in turn may cause dropping replication connection, expiration of wait_for_lsn
    // timeout...
    gc_cs: Mutex<()>,
    walredo_mgr: Arc<dyn WalRedoManager + Send + Sync>,

    // provides access to timeline data sitting in the remote storage
    remote_storage: Option<GenericRemoteStorage>,
}

//
// Constructor functions
//
impl Tenant {
    pub fn create_empty_tenant(
        conf: &'static PageServerConf,
        tenant_conf: TenantConfOpt,
        tenant_id: TenantId,
        remote_storage: Option<&GenericRemoteStorage>,
    ) -> Result<Arc<Tenant>> {
        let wal_redo_manager = Arc::new(PostgresRedoManager::new(conf, tenant_id));

        let target_tenant_directory = conf.tenant_path(&tenant_id);
        anyhow::ensure!(
            !target_tenant_directory.exists(),
            "cannot create new tenant dir: '{tenant_id}' directory already exists",
        );

        let temporary_tenant_dir =
            path_with_suffix_extension(&target_tenant_directory, TEMP_FILE_SUFFIX);
        debug!(
            "Creating temporary directory structure in {}",
            temporary_tenant_dir.display()
        );

        let temporary_tenant_timelines_dir = rebase_directory(
            &conf.timelines_path(&tenant_id),
            &target_tenant_directory,
            &temporary_tenant_dir,
        )?;
        let temporary_tenant_config_path = rebase_directory(
            &conf.tenant_config_path(tenant_id),
            &target_tenant_directory,
            &temporary_tenant_dir,
        )?;

        // top-level dir may exist if we are creating it through CLI
        crashsafe_dir::create_dir_all(&temporary_tenant_dir).with_context(|| {
            format!(
                "could not create temporary tenant directory {}",
                temporary_tenant_dir.display()
            )
        })?;
        // first, create a config in the top-level temp directory, fsync the file
        Tenant::persist_tenant_config(&temporary_tenant_config_path, &tenant_conf, true)?;
        // then, create a subdirectory in the top-level temp directory, fsynced
        crashsafe_dir::create_dir(&temporary_tenant_timelines_dir).with_context(|| {
            format!(
                "could not create temporary tenant timelines directory {}",
                temporary_tenant_timelines_dir.display()
            )
        })?;

        fail::fail_point!("tenant-creation-before-tmp-rename", |_| {
            anyhow::bail!("failpoint tenant-creation-before-tmp-rename");
        });

        // move-rename tmp directory with all files synced into a permanent directory, fsync its parent
        fs::rename(&temporary_tenant_dir, &target_tenant_directory).with_context(|| {
            format!(
                "failed to move temporary tenant directory {} into the permanent one {}",
                temporary_tenant_dir.display(),
                target_tenant_directory.display()
            )
        })?;
        let target_dir_parent = target_tenant_directory.parent().with_context(|| {
            format!(
                "Failed to get tenant dir parent for {}",
                target_tenant_directory.display()
            )
        })?;
        fs::File::open(target_dir_parent)?.sync_all()?;

        info!(
            "created tenant directory structure in {}",
            target_tenant_directory.display()
        );

        let tenant = Arc::new(Tenant::new(
            TenantState::Active,
            conf,
            tenant_conf,
            wal_redo_manager,
            tenant_id,
            remote_storage.cloned(),
        ));
        //tenant.activate(false);

        Ok(tenant)
    }

    ///
    /// Attach a tenant that's available in cloud storage.
    ///
    /// This returns quickly, after just creating the in-memory object
    /// Tenant struct and launching a background task to download
    /// the remote index files.  On return, the tenant is most likely still in
    /// Attaching state, and it will become Active once the background task
    /// finishes. You can use wait_until_active() to wait for the task to
    /// complete.
    ///
    pub fn spawn_attach(
        conf: &'static PageServerConf,
        tenant_id: TenantId,
        remote_storage: &GenericRemoteStorage,
    ) -> Result<Arc<Tenant>> {
        // FIXME: where to get tenant config when attaching? This will just fill in
        // the defaults
        let tenant_conf = Self::load_tenant_config(conf, tenant_id)?;

        let wal_redo_manager = Arc::new(PostgresRedoManager::new(conf, tenant_id));
        let tenant = Arc::new(Tenant::new(
            TenantState::Attaching,
            conf,
            tenant_conf,
            wal_redo_manager,
            tenant_id,
            Some(remote_storage.clone()),
        ));

        // Do all the hard work in the background
        let tenant_clone = Arc::clone(&tenant);

        task_mgr::spawn(
            &tokio::runtime::Handle::current(),
            TaskKind::Attach,
            Some(tenant_id),
            None,
            "attach tenant",
            false,
            async move {
                match tenant_clone.attach_tenant().await {
                    Ok(_) => {}
                    Err(e) => {
                        tenant_clone.update_state(TenantState::Broken);
                        error!("error attaching tenant: {:?}", e);
                    }
                }
                Ok(())
            },
        );
        Ok(tenant)
    }

    ///
    /// Background task that downloads all data for a tenant and brings it to Active state.
    ///
    #[instrument(skip(self), fields(tenant_id=%self.tenant_id))]
    async fn attach_tenant(self: &Arc<Tenant>) -> Result<()> {
        // Get list of remote timelines
        // download index files for every tenant timeline
        info!(
            "attach: listing remote timelines for tenant {}",
            self.tenant_id
        );

        let remote_storage = self
            .remote_storage
            .as_ref()
            .ok_or_else(|| anyhow!("cannot attach without remote storage"))?;

        let remote_timelines =
            list_remote_timelines(remote_storage, self.conf, self.tenant_id).await?;

        info!(
            "tenant {} contains {} timelines",
            self.tenant_id,
            remote_timelines.len()
        );

        let mut timeline_ancestors: HashMap<TimelineId, TimelineMetadata> = HashMap::new();
        let mut index_parts: HashMap<TimelineId, &IndexPart> = HashMap::new();
        for (timeline_id, index_part) in remote_timelines.iter() {
            let remote_metadata = index_part.parse_metadata().with_context(|| {
                format!(
                    "Failed to parse metadata file from remote storage for tenant {}",
                    self.tenant_id
                )
            })?;
            timeline_ancestors.insert(*timeline_id, remote_metadata);
            index_parts.insert(*timeline_id, index_part);
        }

        let sorted_timelines = tree_sort_timelines(timeline_ancestors)?;

        let mut attached_timelines = Vec::new();

        // For every timeline, download the metadata file, scan the local directory,
        // and build a layer map that contains an entry for each remote and local
        // layer file.
        for (timeline_id, _metadata) in sorted_timelines {
            info!("downloading index file for timeline {}", timeline_id);
            let index_part = index_parts.get(&timeline_id).unwrap();
            tokio::fs::create_dir_all(self.conf.timeline_path(&timeline_id, &self.tenant_id))
                .await
                .context("Failed to create new timeline directory")?;

            let remote_metadata = index_part.parse_metadata().with_context(|| {
                format!(
                    "Failed to parse metadata file from remote storage for tenant {}",
                    self.tenant_id
                )
            })?;

            let remote_client = create_remote_timeline_client(
                remote_storage,
                self.conf,
                self.tenant_id,
                timeline_id,
            )?;

            let ancestor = if let Some(ancestor_id) = remote_metadata.ancestor_timeline() {
                let timelines = self.timelines.lock().unwrap();
                Some(Arc::clone(timelines.get(&ancestor_id)
                    .ok_or_else(|| anyhow!("cannot find ancestor timeline {ancestor_id} for timeline {timeline_id}"))?))
            } else {
                None
            };

            let timeline = Timeline::new(
                self.conf,
                Arc::clone(&self.tenant_conf),
                &remote_metadata,
                ancestor,
                timeline_id,
                self.tenant_id,
                Arc::clone(&self.walredo_mgr),
                remote_metadata.pg_version(),
                Some(remote_client),
            );

            // Initialize the layer map, based on all the files we now have on local disk.
            timeline
                .load_layer_map(remote_metadata.disk_consistent_lsn())
                .context("failed to load layermap")?;

            // Fill in RemoteLayers for layers that don't exist locally.
            timeline
                .reconcile_with_remote(Some(index_part), true)
                .await?;

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
    /// structures from on-disk state. This is similar to attaching a tenant,
    /// but the index files already exist on local disk, as well as some layer
    /// files.
    ///
    /// If the loading fails for some reason, the Tenant will go into Broken
    /// state.
    ///
    #[instrument(skip(conf, remote_storage), fields(tenant_id=%tenant_id))]
    pub fn spawn_load(
        conf: &'static PageServerConf,
        tenant_id: TenantId,
        remote_storage: Option<&GenericRemoteStorage>,
    ) -> Result<Arc<Tenant>> {
        // FIXME: also go into Broken state if this fails
        let tenant_conf = Self::load_tenant_config(conf, tenant_id)?;

        let wal_redo_manager = Arc::new(PostgresRedoManager::new(conf, tenant_id));
        let tenant = Tenant::new(
            TenantState::Loading,
            conf,
            tenant_conf,
            wal_redo_manager,
            tenant_id,
            remote_storage.cloned(),
        );
        let tenant = Arc::new(tenant);

        // Do all the hard work in a background task
        let tenant_clone = Arc::clone(&tenant);

        let _ = task_mgr::spawn(
            &tokio::runtime::Handle::current(),
            TaskKind::InitialLoad,
            Some(tenant_id),
            None,
            "initial tenant load",
            false,
            async move {
                match tenant_clone.load_tenant().await {
                    Ok(()) => {}
                    Err(err) => {
                        tenant_clone.update_state(TenantState::Broken);
                        error!("could not load tenant {tenant_id}: {err:?}");
                    }
                }
                info!("initial load for tenant {tenant_id} finished!");
                Ok(())
            },
        );

        info!("spawned load of {} into background", tenant_id);

        Ok(tenant)
    }

    ///
    /// Background task to load in-memory data structures for this tenant, from
    /// files on disk. Used at pageserver startup.
    ///
    #[instrument(skip(self), fields(tenant_id=%self.tenant_id))]
    async fn load_tenant(self: &Arc<Tenant>) -> Result<()> {
        info!("loading tenant task {}", self.tenant_id);

        // Load in-memory state to reflect the local files on disk
        //
        // Scan the directory, peek into the metadata file of each timeline, and
        // collect a list of timelines and their ancestors.
        let mut timelines_to_load: HashMap<TimelineId, TimelineMetadata> = HashMap::new();
        let timelines_dir = self.conf.timelines_path(&self.tenant_id);
        for entry in std::fs::read_dir(&timelines_dir).with_context(|| {
            format!(
                "Failed to list timelines directory for tenant {}",
                self.tenant_id
            )
        })? {
            let entry = entry?;
            let timeline_dir = entry.path();

            if crate::is_temporary(&timeline_dir) {
                info!(
                    "Found temporary timeline directory, removing: {}",
                    timeline_dir.display()
                );
                if let Err(e) = std::fs::remove_dir_all(&timeline_dir) {
                    error!(
                        "Failed to remove temporary directory '{}': {:?}",
                        timeline_dir.display(),
                        e
                    );
                }
            } else {
                let file_name = entry.file_name();
                if let Ok(timeline_id) =
                    file_name.to_str().unwrap_or_default().parse::<TimelineId>()
                {
                    let metadata = load_metadata(self.conf, timeline_id, self.tenant_id)
                        .context("failed to load metadata")?;
                    timelines_to_load.insert(timeline_id, metadata);
                } else {
                    // A file or directory that doesn't look like a timeline ID
                    warn!(
                        "unexpected file or directory in timelines directory: {}",
                        file_name.to_string_lossy()
                    );
                }
            }
        }

        // Sort the array of timeline IDs into tree-order, so that parent comes before
        // all its children.
        let sorted_timelines = tree_sort_timelines(timelines_to_load)?;

        let mut loaded_timelines: Vec<Arc<Timeline>> = Vec::new();
        for (timeline_id, _metadata) in sorted_timelines.iter() {
            let timeline = self.load_timeline(*timeline_id).await?;

            let mut timelines = self.timelines.lock().unwrap();
            timelines.insert(*timeline_id, Arc::clone(&timeline));
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

    fn load_local_timeline(&self, timeline_id: TimelineId) -> Result<Arc<Timeline>> {
        let _enter =
            info_span!("loading timeline state from disk", timeline = %timeline_id).entered();
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

        let remote_client = if let Some(remote_storage) = &self.remote_storage {
            Some(create_remote_timeline_client(
                remote_storage,
                self.conf,
                self.tenant_id,
                timeline_id,
            )?)
        } else {
            None
        };

        // TODO: Launch background task to start uploading anything that's not present in
        // remote storage yet

        let timeline = Timeline::new(
            self.conf,
            Arc::clone(&self.tenant_conf),
            &metadata,
            ancestor,
            timeline_id,
            self.tenant_id,
            Arc::clone(&self.walredo_mgr),
            metadata.pg_version(),
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
    #[instrument(skip(self), fields(tenant_id=%self.tenant_id, timeline_id=%timeline_id))]
    async fn load_timeline(&self, timeline_id: TimelineId) -> Result<Arc<Timeline>> {
        let timeline = self.load_local_timeline(timeline_id)?;

        if self.remote_storage.is_some() {
            // Reconcile local state with remote storage, downloading anything that's
            // missing locally, and scheduling uploads for anything that's missing
            // in remote storage.
            timeline.reconcile_with_remote(None, false).await?;
        }

        Ok(timeline)
    }
}

// Functions on an existing Tenant
impl Tenant {
    pub fn tenant_id(&self) -> TenantId {
        self.tenant_id
    }

    /// Get Timeline handle for given Neon timeline ID. Returns None if the timeline
    /// does not exist. Note: In Loading or Attaching state, this will also return None
    /// if the timeline's information hasn't been loaded into memory yet.
    ///
    /// This function is idempotent. It doesn't change internal state in any way.
    pub fn get_timeline(&self, timeline_id: TimelineId) -> Option<Arc<Timeline>> {
        self.timelines.lock().unwrap().get(&timeline_id).cloned()
    }

    /// Lists timelines the tenant contains.
    /// Up to tenant's implementation to omit certain timelines that ar not considered ready for use.
    pub fn list_timelines(&self) -> Vec<Arc<Timeline>> {
        self.timelines
            .lock()
            .unwrap()
            .values()
            .map(Arc::clone)
            .collect()
    }

    /// This is used to create the initial 'main' timeline during bootstrapping,
    /// or when importing a new base backup. The caller is expected to load an
    /// initial image of the datadir to the new timeline after this.
    ///
    /// NB: this doesn't launch the WAL receiver, because some callers don't want it.
    /// Call `timeline.launch_wal_receiver()` to launch it.
    pub fn create_empty_timeline(
        &self,
        new_timeline_id: TimelineId,
        initdb_lsn: Lsn,
        pg_version: u32,
    ) -> Result<Arc<Timeline>> {
        // XXX: keep the lock to avoid races during timeline creation
        let mut timelines = self.timelines.lock().unwrap();

        anyhow::ensure!(
            timelines.get(&new_timeline_id).is_none(),
            "Timeline {new_timeline_id} already exists"
        );

        let timeline_path = self.conf.timeline_path(&new_timeline_id, &self.tenant_id);
        if timeline_path.exists() {
            bail!("Timeline directory already exists, but timeline is missing in repository map. This is a bug.")
        }

        let new_metadata = TimelineMetadata::new(
            Lsn(0),
            None,
            None,
            Lsn(0),
            initdb_lsn,
            initdb_lsn,
            pg_version,
        );
        let new_timeline =
            self.create_initialized_timeline(new_timeline_id, new_metadata, &mut timelines)?;
        new_timeline.layers.write().unwrap().next_open_layer_at = Some(initdb_lsn);

        Ok(new_timeline)
    }

    /// Create a new timeline.
    ///
    /// Returns the new timeline ID and reference to its Timeline object.
    ///
    /// If the caller specified the timeline ID to use (`new_timeline_id`), and timeline with
    /// the same timeline ID already exists, returns None. If `new_timeline_id` is not given,
    /// a new unique ID is generated.
    pub async fn create_timeline(
        &self,
        new_timeline_id: TimelineId,
        ancestor_timeline_id: Option<TimelineId>,
        mut ancestor_start_lsn: Option<Lsn>,
        pg_version: u32,
    ) -> Result<Option<Arc<Timeline>>> {
        if self
            .conf
            .timeline_path(&new_timeline_id, &self.tenant_id)
            .exists()
        {
            debug!("timeline {new_timeline_id} already exists");
            return Ok(None);
        }

        let new_timeline = match ancestor_timeline_id {
            Some(ancestor_timeline_id) => {
                let ancestor_timeline = self
                    .get_timeline(ancestor_timeline_id)
                    .context("Cannot branch off the timeline that's not present in pageserver")?;

                if let Some(lsn) = ancestor_start_lsn.as_mut() {
                    // Wait for the WAL to arrive and be processed on the parent branch up
                    // to the requested branch point. The repository code itself doesn't
                    // require it, but if we start to receive WAL on the new timeline,
                    // decoding the new WAL might need to look up previous pages, relation
                    // sizes etc. and that would get confused if the previous page versions
                    // are not in the repository yet.
                    *lsn = lsn.align();
                    ancestor_timeline.wait_lsn(*lsn).await?;

                    let ancestor_ancestor_lsn = ancestor_timeline.get_ancestor_lsn();
                    if ancestor_ancestor_lsn > *lsn {
                        // can we safely just branch from the ancestor instead?
                        bail!(
                            "invalid start lsn {} for ancestor timeline {}: less than timeline ancestor lsn {}",
                            lsn,
                            ancestor_timeline_id,
                            ancestor_ancestor_lsn,
                        );
                    }
                }

                self.branch_timeline(ancestor_timeline_id, new_timeline_id, ancestor_start_lsn)?
            }
            None => self.bootstrap_timeline(new_timeline_id, pg_version).await?,
        };

        // Have added new timeline into the tenant, now its background tasks are needed.
        new_timeline.launch_wal_receiver()?;

        Ok(Some(new_timeline))
    }

    /// perform one garbage collection iteration, removing old data files from disk.
    /// this function is periodically called by gc task.
    /// also it can be explicitly requested through page server api 'do_gc' command.
    ///
    /// 'target_timeline_id' specifies the timeline to GC, or None for all.
    /// `horizon` specifies delta from last lsn to preserve all object versions (pitr interval).
    /// `checkpoint_before_gc` parameter is used to force compaction of storage before GC
    /// to make tests more deterministic.
    /// TODO Do we still need it or we can call checkpoint explicitly in tests where needed?
    pub async fn gc_iteration(
        &self,
        target_timeline_id: Option<TimelineId>,
        horizon: u64,
        pitr: Duration,
        checkpoint_before_gc: bool,
    ) -> Result<GcResult> {
        let timeline_str = target_timeline_id
            .map(|x| x.to_string())
            .unwrap_or_else(|| "-".to_string());

        {
            let _timer = STORAGE_TIME
                .with_label_values(&["gc", &self.tenant_id.to_string(), &timeline_str])
                .start_timer();
            self.gc_iteration_internal(target_timeline_id, horizon, pitr, checkpoint_before_gc)
                .await
        }
    }

    /// Perform one compaction iteration.
    /// This function is periodically called by compactor task.
    /// Also it can be explicitly requested per timeline through page server
    /// api's 'compact' command.
    pub async fn compaction_iteration(&self) -> Result<()> {
        // Scan through the hashmap and collect a list of all the timelines,
        // while holding the lock. Then drop the lock and actually perform the
        // compactions.  We don't want to block everything else while the
        // compaction runs.
        let timelines_to_compact = {
            let timelines = self.timelines.lock().unwrap();
            timelines.values().map(Arc::clone).collect::<Vec<_>>()
        };

        for timeline in &timelines_to_compact {
            timeline.compact().await?;
        }

        Ok(())
    }

    /// Flush all in-memory data to disk.
    ///
    /// Used at graceful shutdown.
    ///
    pub async fn checkpoint(&self) -> Result<()> {
        // Scan through the hashmap and collect a list of all the timelines,
        // while holding the lock. Then drop the lock and actually perform the
        // checkpoints. We don't want to block everything else while the
        // checkpoint runs.
        let timelines_to_compact = {
            let timelines = self.timelines.lock().unwrap();
            timelines.values().map(Arc::clone).collect::<Vec<_>>()
        };

        for timeline in &timelines_to_compact {
            timeline.checkpoint(CheckpointConfig::Flush).await?;
        }

        Ok(())
    }

    /// Removes timeline-related in-memory data
    #[instrument(skip(self), fields(tenant_id = %self.tenant_id, timeline_id=%timeline_id))]
    pub async fn delete_timeline(&self, timeline_id: TimelineId) -> anyhow::Result<()> {
        // Start with the shutdown of timeline tasks (this shuts down the walreceiver)
        // It is important that we do not take locks here, and do not check whether the timeline exists
        // because if we hold tenants_state::write_tenants() while awaiting for the tasks to join
        // we cannot create new timelines and tenants, and that can take quite some time,
        // it can even become stuck due to a bug making whole pageserver unavailable for some operations
        // so this is the way how we deal with concurrent delete requests: shutdown everythig, wait for confirmation
        // and then try to actually remove timeline from inmemory state and this is the point when concurrent requests
        // will synchronize and either fail with the not found error or succeed

        debug!("waiting for wal receiver to shutdown");
        task_mgr::shutdown_tasks(
            Some(TaskKind::WalReceiverManager),
            Some(self.tenant_id),
            Some(timeline_id),
        )
        .await;
        debug!("wal receiver shutdown confirmed");

        // FIXME: nothing stops the tasks from starting again while we're not holding locks

        info!("waiting for timeline tasks to shutdown");
        task_mgr::shutdown_tasks(None, Some(self.tenant_id), Some(timeline_id)).await;
        info!("timeline task shutdown completed");

        // in order to be retriable detach needs to be idempotent
        // (or at least to a point that each time the detach is called it can make progress)
        let mut timelines = self.timelines.lock().unwrap();

        // Ensure that there are no child timelines **attached to that pageserver**,
        // because detach removes files, which will break child branches
        let children_exist = timelines
            .iter()
            .any(|(_, entry)| entry.get_ancestor_timeline_id() == Some(timeline_id));

        ensure!(
            !children_exist,
            "Cannot delete timeline which has child timelines"
        );
        let timeline_entry = match timelines.entry(timeline_id) {
            Entry::Occupied(e) => e,
            Entry::Vacant(_) => bail!("timeline not found"),
        };
        let local_timeline_directory = self.conf.timeline_path(&timeline_id, &self.tenant_id);
        std::fs::remove_dir_all(&local_timeline_directory).with_context(|| {
            format!(
                "Failed to remove local timeline directory '{}'",
                local_timeline_directory.display()
            )
        })?;
        info!("timeline files removed");

        timeline_entry.remove();

        Ok(())
    }

    /// Detach tenant.
    ///
    /// This removes all in-memory data for the tenant, as well as all local files.
    /// The tenant still remains in remote storage, and can be re-attached later,
    /// or to a different pageserver.
    ///
    /// Returns a JoinHandle that you can use to wait for the detach operation to finish.
    #[instrument(skip(self), fields(tenant_id=%self.tenant_id))]
    pub async fn detach_tenant(&self) -> Result<()> {
        let old_state = self.state.send_replace(TenantState::Stopping);
        if old_state == TenantState::Stopping {
            bail!("already stopping");
        }

        // FIXME: Should we wait for all in-progress uploads to finish first? Maybe with
        // a timeout?

        // shutdown the tenant and timeline tasks: gc, compaction, page service tasks)
        // FIXME: should we keep the layer flushing active until we have shut down WAL
        // receivers
        // FIXME: does task_mgr::shutdown_tasks also shut down the WAL receiver?
        task_mgr::shutdown_tasks(Some(TaskKind::GarbageCollector), Some(self.tenant_id), None)
            .await;
        task_mgr::shutdown_tasks(Some(TaskKind::Compaction), Some(self.tenant_id), None).await;
        task_mgr::shutdown_tasks(
            Some(TaskKind::LibpqEndpointListener),
            Some(self.tenant_id),
            None,
        )
        .await;

        let timelines: Vec<Arc<Timeline>> =
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
        for timeline in timelines.iter() {
            metadata::delete_metadata(self.conf, timeline.timeline_id, self.tenant_id)?;
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
    #[instrument(skip(self), fields(tenant_id=%self.tenant_id))]
    pub async fn shutdown(&self) -> Result<()> {
        let old_state = self.state.send_replace(TenantState::Stopping);
        if old_state == TenantState::Stopping {
            bail!("already stopping");
        }
        // shutdown the tenant and timeline tasks: gc, compaction, page service tasks)
        task_mgr::shutdown_tasks(Some(TaskKind::GarbageCollector), Some(self.tenant_id), None)
            .await;
        task_mgr::shutdown_tasks(Some(TaskKind::Compaction), Some(self.tenant_id), None).await;
        task_mgr::shutdown_tasks(
            Some(TaskKind::LibpqEndpointListener),
            Some(self.tenant_id),
            None,
        )
        .await;

        // FIXME: does task_mgr::shutdown_tasks also shut down the WAL receiver?

        let timelines: Vec<Arc<Timeline>> =
            self.timelines.lock().unwrap().values().cloned().collect();
        for timeline in timelines.iter() {
            timeline.shutdown().await?;
        }
        info!("tenant shutdown complete");

        Ok(())
    }

    pub fn current_state(&self) -> TenantState {
        *self.state.borrow()
    }

    pub fn is_active(&self) -> bool {
        matches!(self.current_state(), TenantState::Active { .. })
    }
}

/// Given a Vec of timelines and their ancestors (timeline_id, ancestor_id),
/// perform a topological sort, so that the parent of each timeline comes
/// before the children.
fn tree_sort_timelines(
    timelines: HashMap<TimelineId, TimelineMetadata>,
) -> Result<Vec<(TimelineId, TimelineMetadata)>> {
    let mut result = Vec::with_capacity(timelines.len());

    let mut now = Vec::with_capacity(timelines.len());
    // (ancestor, children)
    let mut later: HashMap<TimelineId, Vec<(TimelineId, TimelineMetadata)>> =
        HashMap::with_capacity(timelines.len());

    for (timeline_id, metadata) in timelines {
        if let Some(ancestor_id) = metadata.ancestor_timeline() {
            let children = later.entry(ancestor_id).or_default();
            children.push((timeline_id, metadata));
        } else {
            now.push((timeline_id, metadata));
        }
    }

    while let Some((timeline_id, metadata)) = now.pop() {
        result.push((timeline_id, metadata));
        // All children of this can be loaded now
        if let Some(mut children) = later.remove(&timeline_id) {
            now.append(&mut children);
        }
    }

    // All timelines should be visited now. Unless there were timelines with missing ancestors.
    if !later.is_empty() {
        for (missing_id, orphan_ids) in later {
            for (orphan_id, _) in orphan_ids {
                error!("could not load timeline {orphan_id} because its ancestor timeline {missing_id} could not be loaded");
            }
        }
        bail!("could not load tenant because some timelines are missing ancestors");
    }

    Ok(result)
}

/// Private functions
impl Tenant {
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

    pub fn update_tenant_config(&self, new_tenant_conf: TenantConfOpt) -> Result<()> {
        let mut tenant_conf = self.tenant_conf.write().unwrap();
        tenant_conf.update(&new_tenant_conf);
        Tenant::persist_tenant_config(
            &self.conf.tenant_config_path(self.tenant_id),
            &tenant_conf,
            false,
        )?;
        Ok(())
    }

    fn initialize_new_timeline(
        &self,
        new_timeline_id: TimelineId,
        new_metadata: &TimelineMetadata,
        ancestor: Option<Arc<Timeline>>,
    ) -> anyhow::Result<Arc<Timeline>> {
        if let Some(ancestor_timeline_id) = new_metadata.ancestor_timeline() {
            anyhow::ensure!(
                ancestor.is_some(),
                "Timeline's {new_timeline_id} ancestor {ancestor_timeline_id} was not found"
            )
        }

        let remote_client = if let Some(remote_storage) = &self.remote_storage {
            let remote_client = create_remote_timeline_client(
                remote_storage,
                self.conf,
                self.tenant_id,
                new_timeline_id,
            )?;
            remote_client.init_upload_queue_empty(new_metadata);
            Some(remote_client)
        } else {
            None
        };

        let new_disk_consistent_lsn = new_metadata.disk_consistent_lsn();
        let pg_version = new_metadata.pg_version();
        let new_timeline = Timeline::new(
            self.conf,
            Arc::clone(&self.tenant_conf),
            new_metadata,
            ancestor,
            new_timeline_id,
            self.tenant_id,
            Arc::clone(&self.walredo_mgr),
            pg_version,
            remote_client,
        );

        new_timeline
            .load_layer_map(new_disk_consistent_lsn)
            .context("failed to load layermap")?;

        Ok(new_timeline)
    }

    fn new(
        state: TenantState,
        conf: &'static PageServerConf,
        tenant_conf: TenantConfOpt,
        walredo_mgr: Arc<dyn WalRedoManager + Send + Sync>,
        tenant_id: TenantId,
        remote_storage: Option<GenericRemoteStorage>,
    ) -> Tenant {
        let (state, _) = watch::channel(state);
        Tenant {
            tenant_id,
            conf,
            tenant_conf: Arc::new(RwLock::new(tenant_conf)),
            timelines: Mutex::new(HashMap::new()),
            gc_cs: Mutex::new(()),
            walredo_mgr,
            remote_storage,
            state,
        }
    }

    /// Locate and load config
    pub fn load_tenant_config(
        conf: &'static PageServerConf,
        tenant_id: TenantId,
    ) -> anyhow::Result<TenantConfOpt> {
        let target_config_path = conf.tenant_config_path(tenant_id);
        let target_config_display = target_config_path.display();

        info!("loading tenantconf from {target_config_display}");

        // FIXME If the config file is not found, assume that we're attaching
        // a detached tenant and config is passed via attach command.
        // https://github.com/neondatabase/neon/issues/1555
        if !target_config_path.exists() {
            info!("tenant config not found in {target_config_display}");
            return Ok(TenantConfOpt::default());
        }

        // load and parse file
        let config = fs::read_to_string(&target_config_path).with_context(|| {
            format!("Failed to load config from path '{target_config_display}'")
        })?;

        let toml = config.parse::<toml_edit::Document>().with_context(|| {
            format!("Failed to parse config from file '{target_config_display}' as toml file")
        })?;

        let mut tenant_conf = TenantConfOpt::default();
        for (key, item) in toml.iter() {
            match key {
                "tenant_config" => {
                    tenant_conf = PageServerConf::parse_toml_tenant_conf(item).with_context(|| {
                        format!("Failed to parse config from file '{target_config_display}' as pageserver config")
                    })?;
                }
                _ => bail!("config file {target_config_display} has unrecognized pageserver option '{key}'"),

            }
        }

        Ok(tenant_conf)
    }

    pub fn persist_tenant_config(
        target_config_path: &Path,
        tenant_conf: &TenantConfOpt,
        first_save: bool,
    ) -> anyhow::Result<()> {
        let _enter = info_span!("saving tenantconf").entered();
        info!("persisting tenantconf to {}", target_config_path.display());

        // TODO this will prepend comments endlessly
        let mut conf_content = r#"# This file contains a specific per-tenant's config.
#  It is read in case of pageserver restart.

[tenant_config]
"#
        .to_string();

        // Convert the config to a toml file.
        conf_content += &toml_edit::easy::to_string(tenant_conf)?;

        let mut target_config_file = VirtualFile::open_with_options(
            target_config_path,
            OpenOptions::new().write(true).create_new(first_save),
        )?;

        target_config_file
            .write(conf_content.as_bytes())
            .context("Failed to write toml bytes into file")
            .and_then(|_| {
                target_config_file
                    .sync_all()
                    .context("Faile to fsync config file")
            })
            .with_context(|| {
                format!(
                    "Failed to write config file into path '{}'",
                    target_config_path.display()
                )
            })?;

        // fsync the parent directory to ensure the directory entry is durable
        if first_save {
            target_config_path
                .parent()
                .context("Config file does not have a parent")
                .and_then(|target_config_parent| {
                    File::open(target_config_parent).context("Failed to open config parent")
                })
                .and_then(|tenant_dir| {
                    tenant_dir
                        .sync_all()
                        .context("Failed to fsync config parent")
                })
                .with_context(|| {
                    format!(
                        "Failed to fsync on first save for config {}",
                        target_config_path.display()
                    )
                })?;
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
    #[instrument(skip(self), fields(tenant_id = %self.tenant_id, timeline_id = ?target_timeline_id))]
    async fn gc_iteration_internal(
        &self,
        target_timeline_id: Option<TimelineId>,
        horizon: u64,
        pitr: Duration,
        checkpoint_before_gc: bool,
    ) -> Result<GcResult> {
        let mut totals: GcResult = Default::default();
        let now = Instant::now();

        let gc_timelines = {
            // grab mutex to prevent new timelines from being created here.
            let _gc_cs = self.gc_cs.lock().unwrap();

            let timelines = self.timelines.lock().unwrap();

            // Scan all timelines. For each timeline, remember the timeline ID and
            // the branch point where it was created.
            let mut all_branchpoints: BTreeSet<(TimelineId, Lsn)> = BTreeSet::new();
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
                            if let Some(timeline_id) = target_timeline_id {
                                if ancestor_timeline_id == &timeline_id {
                                    all_branchpoints.insert((
                                        *ancestor_timeline_id,
                                        timeline.get_ancestor_lsn(),
                                    ));
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
                    if let Some(target_timeline_id) = target_timeline_id {
                        if timeline_id != target_timeline_id {
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
                    info!(
                        "timeline {timeline_id} could not be GC'd, becuase it concurrently deleted"
                    );
                }
            }
            gc_timelines
        };

        // Perform GC for each timeline.
        //
        // Note that we don't hold the GC lock here because we don't want
        // to delay the branch creation task, which requires the GC lock.
        // A timeline GC iteration can be slow because it may need to wait for
        // compaction (both require `layer_removal_cs` lock),
        // but the GC iteration can run concurrently with branch creation.
        //
        // See comments in [`Tenant::branch_timeline`] for more information
        // about why branch creation task can run concurrently with timeline's GC iteration.
        for timeline in gc_timelines {
            if task_mgr::is_shutdown_requested() {
                // We were requested to shut down. Stop and return with the progress we
                // made.
                break;
            }

            // If requested, force flush all in-memory layers to disk first,
            // so that they too can be garbage collected. That's
            // used in tests, so we want as deterministic results as possible.
            if checkpoint_before_gc {
                timeline.checkpoint(CheckpointConfig::Forced).await?;
                info!(
                    "timeline {} checkpoint_before_gc done",
                    timeline.timeline_id
                );
            }

            let result = timeline.gc().await?;
            totals += result;
        }

        totals.elapsed = now.elapsed();
        Ok(totals)
    }

    /// Branch an existing timeline
    ///
    /// NB: this doesn't launch the WAL receiver, because some callers don't want it.
    /// Call `timeline.launch_wal_receiver()` to launch it.
    fn branch_timeline(
        &self,
        src: TimelineId,
        dst: TimelineId,
        start_lsn: Option<Lsn>,
    ) -> Result<Arc<Timeline>> {
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

        // XXX: keep the lock to avoid races during timeline creation
        let mut timelines = self.timelines.lock().unwrap();
        let src_timeline = timelines
            .get(&src)
            // message about timeline being remote is one .context up in the stack
            .ok_or_else(|| anyhow::anyhow!("unknown timeline id: {src}"))?;

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
                "invalid branch start lsn: less than latest GC cutoff {}",
                *latest_gc_cutoff_lsn,
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

        // Create the metadata file, noting the ancestor of the new timeline.
        // There is initially no data in it, but all the read-calls know to look
        // into the ancestor.
        let metadata = TimelineMetadata::new(
            start_lsn,
            dst_prev,
            Some(src),
            start_lsn,
            *src_timeline.latest_gc_cutoff_lsn.read(), // FIXME: should we hold onto this guard longer?
            src_timeline.initdb_lsn,
            src_timeline.pg_version,
        );
        let new_timeline = self.create_initialized_timeline(dst, metadata, &mut timelines)?;
        info!("branched timeline {dst} from {src} at {start_lsn}");

        Ok(new_timeline)
    }

    /// - run initdb to init temporary instance and get bootstrap data
    /// - after initialization complete, remove the temp dir.
    async fn bootstrap_timeline(
        &self,
        timeline_id: TimelineId,
        pg_version: u32,
    ) -> Result<Arc<Timeline>> {
        // create a `tenant/{tenant_id}/timelines/basebackup-{timeline_id}.{TEMP_FILE_SUFFIX}/`
        // temporary directory for basebackup files for the given timeline.
        let initdb_path = path_with_suffix_extension(
            self.conf
                .timelines_path(&self.tenant_id)
                .join(format!("basebackup-{timeline_id}")),
            TEMP_FILE_SUFFIX,
        );

        // Init temporarily repo to get bootstrap data
        run_initdb(self.conf, &initdb_path, pg_version)?;
        let pgdata_path = initdb_path;

        let lsn = import_datadir::get_lsn_from_controlfile(&pgdata_path)?.align();

        // Import the contents of the data directory at the initial checkpoint
        // LSN, and any WAL after that.
        // Initdb lsn will be equal to last_record_lsn which will be set after import.
        // Because we know it upfront avoid having an option or dummy zero value by passing it to create_empty_timeline.
        let timeline = self.create_empty_timeline(timeline_id, lsn, pg_version)?;

        let import_result = self
            .import_timeline_from_pgdata(&pgdata_path, timeline.as_ref(), lsn)
            .await;

        // Remove temp dir. We don't need it anymore
        if let Err(err) = fs::remove_dir_all(&pgdata_path) {
            error!(
                "could not remove temporary initdb dir {}: {:?}",
                pgdata_path.display(),
                err
            );
        }

        if let Err(err) = import_result {
            // If the import failed, try to undo our changes
            //
            // FIXME: This only works in some cases. For one, we might crash before we
            // have a chance to fully clean up. See https://github.com/neondatabase/neon/pull/2489
            // for an attempt to fix this.
            error!(
                "could not import freshly initdb'd cluster into timeline: {:?}",
                err
            );
            self.timelines.lock().unwrap().remove(&timeline_id);
            info!("waiting for timeline tasks to shutdown");
            task_mgr::shutdown_tasks(None, Some(self.tenant_id), Some(timeline_id)).await;

            let local_timeline_directory = self.conf.timeline_path(&timeline_id, &self.tenant_id);
            if let Err(err) = std::fs::remove_dir_all(&local_timeline_directory) {
                error!(
                    "Could not remove incomplete timeline directory '{}': {:?}",
                    local_timeline_directory.display(),
                    err
                );
            }
            Err(err)
        } else {
            info!(
                "created root timeline {} timeline.lsn {}",
                timeline.timeline_id,
                timeline.get_last_record_lsn()
            );
            Ok(timeline)
        }
    }

    async fn import_timeline_from_pgdata(
        &self,
        pgdata_path: &Path,
        timeline: &Timeline,
        lsn: Lsn,
    ) -> Result<()> {
        import_datadir::import_timeline_from_postgres_datadir(pgdata_path, timeline, lsn)?;

        fail::fail_point!("before-checkpoint-new-timeline", |_| {
            bail!("failpoint before-checkpoint-new-timeline");
        });

        timeline.checkpoint(CheckpointConfig::Forced).await?;

        Ok(())
    }

    fn create_initialized_timeline(
        &self,
        new_timeline_id: TimelineId,
        new_metadata: TimelineMetadata,
        timelines: &mut MutexGuard<HashMap<TimelineId, Arc<Timeline>>>,
    ) -> Result<Arc<Timeline>> {
        crashsafe_dir::create_dir_all(self.conf.timeline_path(&new_timeline_id, &self.tenant_id))
            .with_context(|| {
            format!(
                "Failed to create timeline {}/{} directory",
                new_timeline_id, self.tenant_id
            )
        })?;
        save_metadata(
            self.conf,
            new_timeline_id,
            self.tenant_id,
            &new_metadata,
            true,
        )
        .with_context(|| {
            format!(
                "Failed to create timeline {}/{} metadata",
                new_timeline_id, self.tenant_id
            )
        })?;

        let ancestor = new_metadata
            .ancestor_timeline()
            .and_then(|ancestor_timeline_id| timelines.get(&ancestor_timeline_id))
            .cloned();
        let new_timeline = self
            .initialize_new_timeline(new_timeline_id, &new_metadata, ancestor)
            .with_context(|| {
                format!(
                    "Failed to initialize timeline {}/{}",
                    new_timeline_id, self.tenant_id
                )
            })?;

        match timelines.entry(new_timeline_id) {
            Entry::Occupied(_) => bail!(
                "Found freshly initialized timeline {} in the tenant map",
                new_timeline_id
            ),
            Entry::Vacant(v) => {
                v.insert(Arc::clone(&new_timeline));
            }
        }

        Ok(new_timeline)
    }

    /// If the tenant is Loading or Attaching, wait for it to become Active.
    /// Returns Ok if the tenant was Active or it became Active, and Err if it
    /// became Broken or Stopping.
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
                        "waiting for tenant {} to become active, current state: {:?}",
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

/// Create the cluster temporarily in 'initdbpath' directory inside the repository
/// to get bootstrap data for timeline initialization.
fn run_initdb(
    conf: &'static PageServerConf,
    initdb_target_dir: &Path,
    pg_version: u32,
) -> Result<()> {
    let initdb_bin_path = conf.pg_bin_dir(pg_version).join("initdb");
    let initdb_lib_dir = conf.pg_lib_dir(pg_version);
    info!(
        "running {} in {}, libdir: {}",
        initdb_bin_path.display(),
        initdb_target_dir.display(),
        initdb_lib_dir.display(),
    );

    let initdb_output = Command::new(initdb_bin_path)
        .args(&["-D", &initdb_target_dir.to_string_lossy()])
        .args(&["-U", &conf.superuser])
        .args(&["-E", "utf8"])
        .arg("--no-instructions")
        // This is only used for a temporary installation that is deleted shortly after,
        // so no need to fsync it
        .arg("--no-sync")
        .env_clear()
        .env("LD_LIBRARY_PATH", &initdb_lib_dir)
        .env("DYLD_LIBRARY_PATH", &initdb_lib_dir)
        .stdout(Stdio::null())
        .output()
        .context("failed to execute initdb")?;
    if !initdb_output.status.success() {
        bail!(
            "initdb failed: '{}'",
            String::from_utf8_lossy(&initdb_output.stderr)
        );
    }

    Ok(())
}

fn rebase_directory(original_path: &Path, base: &Path, new_base: &Path) -> anyhow::Result<PathBuf> {
    let relative_path = original_path.strip_prefix(base).with_context(|| {
        format!(
            "Failed to strip base prefix '{}' off path '{}'",
            base.display(),
            original_path.display()
        )
    })?;
    Ok(new_base.join(relative_path))
}

impl Drop for Tenant {
    fn drop(&mut self) {
        remove_tenant_metrics(&self.tenant_id);
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

#[cfg(test)]
pub mod harness {
    use bytes::{Bytes, BytesMut};
    use once_cell::sync::Lazy;
    use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};
    use std::{fs, path::PathBuf};

    use crate::{
        config::PageServerConf,
        repository::Key,
        walrecord::NeonWalRecord,
        walredo::{WalRedoError, WalRedoManager},
    };

    use super::*;
    use crate::tenant_config::{TenantConf, TenantConfOpt};
    use hex_literal::hex;
    use utils::id::TenantId;

    pub const TIMELINE_ID: TimelineId =
        TimelineId::from_array(hex!("11223344556677881122334455667788"));
    pub const NEW_TIMELINE_ID: TimelineId =
        TimelineId::from_array(hex!("AA223344556677881122334455667788"));

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
                checkpoint_timeout: Some(tenant_conf.checkpoint_timeout),
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

    pub struct TenantHarness<'a> {
        pub conf: &'static PageServerConf,
        pub tenant_conf: TenantConf,
        pub tenant_id: TenantId,

        pub lock_guard: (
            Option<RwLockReadGuard<'a, ()>>,
            Option<RwLockWriteGuard<'a, ()>>,
        ),
    }

    impl<'a> TenantHarness<'a> {
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

            let tenant_id = TenantId::generate();
            fs::create_dir_all(conf.tenant_path(&tenant_id))?;
            fs::create_dir_all(conf.timelines_path(&tenant_id))?;

            Ok(Self {
                conf,
                tenant_conf,
                tenant_id,
                lock_guard,
            })
        }

        pub fn load(&self) -> Tenant {
            self.try_load().expect("failed to load test tenant")
        }

        pub fn try_load(&self) -> Result<Tenant> {
            let walredo_mgr = Arc::new(TestRedoManager);

            let tenant = Tenant::new(
                TenantState::Active,
                self.conf,
                TenantConfOpt::from(self.tenant_conf),
                walredo_mgr,
                self.tenant_id,
                None,
            );
            // populate tenant with locally available timelines
            for timeline_dir_entry in fs::read_dir(self.conf.timelines_path(&self.tenant_id))
                .expect("should be able to read timelines dir")
            {
                let timeline_dir_entry = timeline_dir_entry?;
                let timeline_id: TimelineId = timeline_dir_entry
                    .path()
                    .file_name()
                    .unwrap()
                    .to_string_lossy()
                    .parse()?;

                let timeline = tenant.load_local_timeline(timeline_id)?;

                // Insert it to the hash map
                tenant
                    .timelines
                    .lock()
                    .unwrap()
                    .insert(timeline_id, timeline);
            }
            Ok(tenant)
        }

        pub fn timeline_path(&self, timeline_id: &TimelineId) -> PathBuf {
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
            records: Vec<(Lsn, NeonWalRecord)>,
            _pg_version: u32,
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::METADATA_FILE_NAME;
    use crate::keyspace::KeySpaceAccum;
    use crate::pgdatadir_mapping::create_test_timeline;
    use crate::repository::{Key, Value};
    use crate::tenant::harness::*;
    use crate::DEFAULT_PG_VERSION;
    use bytes::BytesMut;
    use hex_literal::hex;
    use once_cell::sync::Lazy;
    use rand::{thread_rng, Rng};

    static TEST_KEY: Lazy<Key> =
        Lazy::new(|| Key::from_slice(&hex!("112222222233333333444444445500000001")));

    pub async fn assert_current_logical_size(timeline: &Timeline, lsn: Lsn) {
        let incremental = timeline.get_current_logical_size().unwrap();
        let non_incremental = timeline
            .get_current_logical_size_non_incremental(lsn)
            .await
            .unwrap();
        assert_eq!(incremental, non_incremental);
    }

    #[test]
    fn test_basic() -> Result<()> {
        let tenant = TenantHarness::create("test_basic")?.load();
        let tline = create_test_timeline(&tenant, TIMELINE_ID, DEFAULT_PG_VERSION)?;

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
        let tenant = TenantHarness::create("no_duplicate_timelines")?.load();
        let _ = tenant.create_empty_timeline(TIMELINE_ID, Lsn(0), DEFAULT_PG_VERSION)?;

        match tenant.create_empty_timeline(TIMELINE_ID, Lsn(0), DEFAULT_PG_VERSION) {
            Ok(_) => panic!("duplicate timeline creation should fail"),
            Err(e) => assert_eq!(
                e.to_string(),
                format!("Timeline {TIMELINE_ID} already exists")
            ),
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
    #[tokio::test]
    async fn test_branch() -> Result<()> {
        let tenant = TenantHarness::create("test_branch")?.load();
        let tline = create_test_timeline(&tenant, TIMELINE_ID, DEFAULT_PG_VERSION)?;
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

        assert_current_logical_size(&tline, Lsn(0x40)).await;

        // Branch the history, modify relation differently on the new timeline
        tenant.branch_timeline(TIMELINE_ID, NEW_TIMELINE_ID, Some(Lsn(0x30)))?;
        let newtline = tenant
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

        //assert_current_logical_size(&tline, Lsn(0x40)).await;

        Ok(())
    }

    async fn make_some_layers(tline: &Timeline, start_lsn: Lsn) -> Result<()> {
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
        tline.checkpoint(CheckpointConfig::Forced).await?;
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
        tline.checkpoint(CheckpointConfig::Forced).await
    }

    #[tokio::test]
    async fn test_prohibit_branch_creation_on_garbage_collected_data() -> Result<()> {
        let tenant =
            TenantHarness::create("test_prohibit_branch_creation_on_garbage_collected_data")?
                .load();
        let tline = tenant.create_empty_timeline(TIMELINE_ID, Lsn(0), DEFAULT_PG_VERSION)?;
        make_some_layers(tline.as_ref(), Lsn(0x20)).await?;

        // this removes layers before lsn 40 (50 minus 10), so there are two remaining layers, image and delta for 31-50
        // FIXME: this doesn't actually remove any layer currently, given how the checkpointing
        // and compaction works. But it does set the 'cutoff' point so that the cross check
        // below should fail.
        tenant
            .gc_iteration(Some(TIMELINE_ID), 0x10, Duration::ZERO, false)
            .await?;

        // try to branch at lsn 25, should fail because we already garbage collected the data
        match tenant.branch_timeline(TIMELINE_ID, NEW_TIMELINE_ID, Some(Lsn(0x25))) {
            Ok(_) => panic!("branching should have failed"),
            Err(err) => {
                assert!(err.to_string().contains("invalid branch start lsn"));
                assert!(err.chain().any(|e| e
                    .to_string()
                    .contains("we might've already garbage collected needed data")))
            }
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_prohibit_branch_creation_on_pre_initdb_lsn() -> Result<()> {
        let tenant =
            TenantHarness::create("test_prohibit_branch_creation_on_pre_initdb_lsn")?.load();

        tenant.create_empty_timeline(TIMELINE_ID, Lsn(0x50), DEFAULT_PG_VERSION)?;
        // try to branch at lsn 0x25, should fail because initdb lsn is 0x50
        match tenant.branch_timeline(TIMELINE_ID, NEW_TIMELINE_ID, Some(Lsn(0x25))) {
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
    #[tokio::test]
    async fn test_prohibit_get_for_garbage_collected_data() -> Result<()> {
        let repo =
            RepoHarness::create("test_prohibit_get_for_garbage_collected_data")?
            .load();

        let tline = repo.create_empty_timeline(TIMELINE_ID, Lsn(0), DEFAULT_PG_VERSION)?;
        make_some_layers(tline.as_ref(), Lsn(0x20)).await?;

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

    #[tokio::test]
    async fn test_retain_data_in_parent_which_is_needed_for_child() -> Result<()> {
        let tenant =
            TenantHarness::create("test_retain_data_in_parent_which_is_needed_for_child")?.load();
        let tline = create_test_timeline(&tenant, TIMELINE_ID, DEFAULT_PG_VERSION)?;
        make_some_layers(tline.as_ref(), Lsn(0x20)).await?;

        tenant.branch_timeline(TIMELINE_ID, NEW_TIMELINE_ID, Some(Lsn(0x40)))?;
        let newtline = tenant
            .get_timeline(NEW_TIMELINE_ID)
            .expect("Should have a local timeline");
        // this removes layers before lsn 40 (50 minus 10), so there are two remaining layers, image and delta for 31-50
        tenant
            .gc_iteration(Some(TIMELINE_ID), 0x10, Duration::ZERO, false)
            .await?;
        assert!(newtline.get(*TEST_KEY, Lsn(0x25)).is_ok());

        Ok(())
    }
    #[tokio::test]
    async fn test_parent_keeps_data_forever_after_branching() -> Result<()> {
        let tenant =
            TenantHarness::create("test_parent_keeps_data_forever_after_branching")?.load();
        let tline = create_test_timeline(&tenant, TIMELINE_ID, DEFAULT_PG_VERSION)?;
        make_some_layers(tline.as_ref(), Lsn(0x20)).await?;

        tenant.branch_timeline(TIMELINE_ID, NEW_TIMELINE_ID, Some(Lsn(0x40)))?;
        let newtline = tenant
            .get_timeline(NEW_TIMELINE_ID)
            .expect("Should have a local timeline");

        make_some_layers(newtline.as_ref(), Lsn(0x60)).await?;

        // run gc on parent
        tenant
            .gc_iteration(Some(TIMELINE_ID), 0x10, Duration::ZERO, false)
            .await?;

        // Check that the data is still accessible on the branch.
        assert_eq!(
            newtline.get(*TEST_KEY, Lsn(0x50))?,
            TEST_IMG(&format!("foo at {}", Lsn(0x40)))
        );

        Ok(())
    }

    #[test]
    fn corrupt_metadata() -> Result<()> {
        const TEST_NAME: &str = "corrupt_metadata";
        let harness = TenantHarness::create(TEST_NAME)?;
        let tenant = harness.load();

        tenant.create_empty_timeline(TIMELINE_ID, Lsn(0), DEFAULT_PG_VERSION)?;
        drop(tenant);

        let metadata_path = harness.timeline_path(&TIMELINE_ID).join(METADATA_FILE_NAME);

        assert!(metadata_path.is_file());

        let mut metadata_bytes = std::fs::read(&metadata_path)?;
        assert_eq!(metadata_bytes.len(), 512);
        metadata_bytes[8] ^= 1;
        std::fs::write(metadata_path, metadata_bytes)?;

        let err = harness.try_load().err().expect("should fail");
        assert!(err.chain().any(|e| e
            .to_string()
            .starts_with("Failed to parse metadata bytes from path")));

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

    #[tokio::test]
    async fn test_images() -> Result<()> {
        let tenant = TenantHarness::create("test_images")?.load();
        let tline = tenant.create_empty_timeline(TIMELINE_ID, Lsn(0), DEFAULT_PG_VERSION)?;

        let writer = tline.writer();
        writer.put(*TEST_KEY, Lsn(0x10), &Value::Image(TEST_IMG("foo at 0x10")))?;
        writer.finish_write(Lsn(0x10));
        drop(writer);

        tline.checkpoint(CheckpointConfig::Forced).await?;
        tline.compact().await?;

        let writer = tline.writer();
        writer.put(*TEST_KEY, Lsn(0x20), &Value::Image(TEST_IMG("foo at 0x20")))?;
        writer.finish_write(Lsn(0x20));
        drop(writer);

        tline.checkpoint(CheckpointConfig::Forced).await?;
        tline.compact().await?;

        let writer = tline.writer();
        writer.put(*TEST_KEY, Lsn(0x30), &Value::Image(TEST_IMG("foo at 0x30")))?;
        writer.finish_write(Lsn(0x30));
        drop(writer);

        tline.checkpoint(CheckpointConfig::Forced).await?;
        tline.compact().await?;

        let writer = tline.writer();
        writer.put(*TEST_KEY, Lsn(0x40), &Value::Image(TEST_IMG("foo at 0x40")))?;
        writer.finish_write(Lsn(0x40));
        drop(writer);

        tline.checkpoint(CheckpointConfig::Forced).await?;
        tline.compact().await?;

        assert_eq!(tline.get(*TEST_KEY, Lsn(0x10))?, TEST_IMG("foo at 0x10"));
        assert_eq!(tline.get(*TEST_KEY, Lsn(0x1f))?, TEST_IMG("foo at 0x10"));
        assert_eq!(tline.get(*TEST_KEY, Lsn(0x20))?, TEST_IMG("foo at 0x20"));
        assert_eq!(tline.get(*TEST_KEY, Lsn(0x30))?, TEST_IMG("foo at 0x30"));
        assert_eq!(tline.get(*TEST_KEY, Lsn(0x40))?, TEST_IMG("foo at 0x40"));

        Ok(())
    }

    //
    // Insert 1000 key-value pairs with increasing keys, checkpoint,
    // repeat 50 times.
    //
    #[tokio::test]
    async fn test_bulk_insert() -> Result<()> {
        let tenant = TenantHarness::create("test_bulk_insert")?.load();
        let tline = tenant.create_empty_timeline(TIMELINE_ID, Lsn(0), DEFAULT_PG_VERSION)?;

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
            tline.checkpoint(CheckpointConfig::Forced).await?;
            tline.compact().await?;
            tline.gc().await?;
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_random_updates() -> Result<()> {
        let tenant = TenantHarness::create("test_random_updates")?.load();
        let tline = tenant.create_empty_timeline(TIMELINE_ID, Lsn(0), DEFAULT_PG_VERSION)?;

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
            tline.checkpoint(CheckpointConfig::Forced).await?;
            tline.compact().await?;
            tline.gc().await?;
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_traverse_branches() -> Result<()> {
        let tenant = TenantHarness::create("test_traverse_branches")?.load();
        let mut tline = create_test_timeline(&tenant, TIMELINE_ID, DEFAULT_PG_VERSION)?;

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
            let new_tline_id = TimelineId::generate();
            tenant.branch_timeline(tline_id, new_tline_id, Some(lsn))?;
            tline = tenant
                .get_timeline(new_tline_id)
                .expect("Should have the branched timeline");
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
            tline.checkpoint(CheckpointConfig::Forced).await?;
            tline.compact().await?;
            tline.gc().await?;
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_traverse_ancestors() -> Result<()> {
        let tenant = TenantHarness::create("test_traverse_ancestors")?.load();
        let mut tline = create_test_timeline(&tenant, TIMELINE_ID, DEFAULT_PG_VERSION)?;

        const NUM_KEYS: usize = 100;
        const NUM_TLINES: usize = 50;

        let mut test_key = Key::from_hex("012222222233333333444444445500000000").unwrap();
        // Track page mutation lsns across different timelines.
        let mut updated = [[Lsn(0); NUM_KEYS]; NUM_TLINES];

        let mut lsn = Lsn(0x10);
        let mut tline_id = TIMELINE_ID;

        #[allow(clippy::needless_range_loop)]
        for idx in 0..NUM_TLINES {
            let new_tline_id = TimelineId::generate();
            tenant.branch_timeline(tline_id, new_tline_id, Some(lsn))?;
            tline = tenant
                .get_timeline(new_tline_id)
                .expect("Should have the branched timeline");
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
                println!("checking [{idx}][{blknum}] at {lsn}");
                test_key.field6 = blknum as u32;
                assert_eq!(
                    tline.get(test_key, *lsn)?,
                    TEST_IMG(&format!("{idx} {blknum} at {lsn}"))
                );
            }
        }
        Ok(())
    }
}

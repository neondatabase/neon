//! This module acts as a switchboard to access different repositories managed by this
//! page server.

use crate::config::PageServerConf;
use crate::layered_repository::LayeredRepository;
use crate::pgdatadir_mapping::DatadirTimeline;
use crate::repository::{Repository, TimelineSyncStatusUpdate};
use crate::storage_sync::index::RemoteIndex;
use crate::storage_sync::{self, LocalTimelineInitStatus, SyncStartupData};
use crate::tenant_config::TenantConfOpt;
use crate::thread_mgr;
use crate::thread_mgr::ThreadKind;
use crate::timelines;
use crate::timelines::CreateRepo;
use crate::walredo::PostgresRedoManager;
use crate::{DatadirTimelineImpl, RepositoryImpl};
use anyhow::{bail, Context};
use serde::{Deserialize, Serialize};
use serde_with::{serde_as, DisplayFromStr};
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use tracing::*;

use utils::zid::{ZTenantId, ZTimelineId};

mod tenants_state {
    use std::{
        collections::HashMap,
        sync::{RwLock, RwLockReadGuard, RwLockWriteGuard},
    };

    use utils::zid::ZTenantId;

    use crate::tenant_mgr::Tenant;

    lazy_static::lazy_static! {
        static ref TENANTS: RwLock<HashMap<ZTenantId, Tenant>> = RwLock::new(HashMap::new());
    }

    pub(super) fn read_tenants() -> RwLockReadGuard<'static, HashMap<ZTenantId, Tenant>> {
        TENANTS
            .read()
            .expect("Failed to read() tenants lock, it got poisoned")
    }

    pub(super) fn write_tenants() -> RwLockWriteGuard<'static, HashMap<ZTenantId, Tenant>> {
        TENANTS
            .write()
            .expect("Failed to write() tenants lock, it got poisoned")
    }
}

struct Tenant {
    state: TenantState,
    /// Contains in-memory state, including the timeline that might not yet flushed on disk or loaded form disk.
    repo: Arc<RepositoryImpl>,
    /// Timelines, located locally in the pageserver's datadir.
    /// Timelines can entirely be removed entirely by the `detach` operation only.
    ///
    /// Local timelines have more metadata that's loaded into memory,
    /// that is located in the `repo.timelines` field, [`crate::layered_repository::LayeredTimelineEntry`].
    local_timelines: HashMap<ZTimelineId, Arc<DatadirTimelineImpl>>,
}

#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Eq)]
pub enum TenantState {
    // All data for this tenant is complete on local disk, but we haven't loaded the Repository,
    // Timeline and Layer structs into memory yet, so it cannot be accessed yet.
    //Ready,
    // This tenant exists on local disk, and the layer map has been loaded into memory.
    // The local disk might have some newer files that don't exist in cloud storage yet.
    Active,
    // Tenant is active, but there is no walreceiver connection.
    Idle,
    // This tenant exists on local disk, and the layer map has been loaded into memory.
    // The local disk might have some newer files that don't exist in cloud storage yet.
    // The tenant cannot be accessed anymore for any reason, but graceful shutdown.
    Stopping,
}

impl fmt::Display for TenantState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TenantState::Active => f.write_str("Active"),
            TenantState::Idle => f.write_str("Idle"),
            TenantState::Stopping => f.write_str("Stopping"),
        }
    }
}

/// Initialize repositories with locally available timelines.
/// Timelines that are only partially available locally (remote storage has more data than this pageserver)
/// are scheduled for download and added to the repository once download is completed.
pub fn init_tenant_mgr(conf: &'static PageServerConf) -> anyhow::Result<RemoteIndex> {
    let SyncStartupData {
        remote_index,
        local_timeline_init_statuses,
    } = storage_sync::start_local_timeline_sync(conf)
        .context("Failed to set up local files sync with external storage")?;
    init_local_repositories(conf, local_timeline_init_statuses, &remote_index)?;
    Ok(remote_index)
}

/// Updates tenants' repositories, changing their timelines state in memory.
pub fn apply_timeline_sync_status_updates(
    conf: &'static PageServerConf,
    remote_index: &RemoteIndex,
    sync_status_updates: HashMap<ZTenantId, HashMap<ZTimelineId, TimelineSyncStatusUpdate>>,
) {
    if sync_status_updates.is_empty() {
        debug!("no sync status updates to apply");
        return;
    }
    info!(
        "Applying sync status updates for {} timelines",
        sync_status_updates.len()
    );
    debug!("Sync status updates: {sync_status_updates:?}");

    for (tenant_id, status_updates) in sync_status_updates {
        let repo = match load_local_repo(conf, tenant_id, remote_index) {
            Ok(repo) => repo,
            Err(e) => {
                error!("Failed to load repo for tenant {tenant_id} Error: {e:?}",);
                continue;
            }
        };
        match apply_timeline_remote_sync_status_updates(&repo, status_updates) {
            Ok(()) => info!("successfully applied sync status updates for tenant {tenant_id}"),
            Err(e) => error!(
                "Failed to apply timeline sync timeline status updates for tenant {tenant_id}: {e:?}"
            ),
        }
    }
}

///
/// Shut down all tenants. This runs as part of pageserver shutdown.
///
pub fn shutdown_all_tenants() {
    let mut m = tenants_state::write_tenants();
    let mut tenantids = Vec::new();
    for (tenantid, tenant) in m.iter_mut() {
        tenant.state = TenantState::Stopping;
        tenantids.push(*tenantid)
    }
    drop(m);

    thread_mgr::shutdown_threads(Some(ThreadKind::WalReceiver), None, None);
    thread_mgr::shutdown_threads(Some(ThreadKind::GarbageCollector), None, None);
    thread_mgr::shutdown_threads(Some(ThreadKind::Compactor), None, None);

    // Ok, no background threads running anymore. Flush any remaining data in
    // memory to disk.
    //
    // We assume that any incoming connections that might request pages from
    // the repository have already been terminated by the caller, so there
    // should be no more activity in any of the repositories.
    //
    // On error, log it but continue with the shutdown for other tenants.
    for tenant_id in tenantids {
        debug!("shutdown tenant {tenant_id}");
        match get_repository_for_tenant(tenant_id) {
            Ok(repo) => {
                if let Err(err) = repo.checkpoint() {
                    error!("Could not checkpoint tenant {tenant_id} during shutdown: {err:?}");
                }
            }
            Err(err) => {
                error!("Could not get repository for tenant {tenant_id} during shutdown: {err:?}");
            }
        }
    }
}

pub fn create_tenant_repository(
    conf: &'static PageServerConf,
    tenant_conf: TenantConfOpt,
    tenant_id: ZTenantId,
    remote_index: RemoteIndex,
) -> anyhow::Result<Option<ZTenantId>> {
    match tenants_state::write_tenants().entry(tenant_id) {
        Entry::Occupied(_) => {
            debug!("tenant {tenant_id} already exists");
            Ok(None)
        }
        Entry::Vacant(v) => {
            let wal_redo_manager = Arc::new(PostgresRedoManager::new(conf, tenant_id));
            let repo = timelines::create_repo(
                conf,
                tenant_conf,
                tenant_id,
                CreateRepo::Real {
                    wal_redo_manager,
                    remote_index,
                },
            )?;
            v.insert(Tenant {
                state: TenantState::Idle,
                repo,
                local_timelines: HashMap::new(),
            });
            Ok(Some(tenant_id))
        }
    }
}

pub fn update_tenant_config(
    tenant_conf: TenantConfOpt,
    tenant_id: ZTenantId,
) -> anyhow::Result<()> {
    info!("configuring tenant {tenant_id}");
    let repo = get_repository_for_tenant(tenant_id)?;

    repo.update_tenant_config(tenant_conf)?;
    Ok(())
}

pub fn get_tenant_state(tenantid: ZTenantId) -> Option<TenantState> {
    Some(tenants_state::read_tenants().get(&tenantid)?.state)
}

///
/// Change the state of a tenant to Active and launch its compactor and GC
/// threads. If the tenant was already in Active state or Stopping, does nothing.
///
pub fn activate_tenant(tenant_id: ZTenantId) -> anyhow::Result<()> {
    let mut m = tenants_state::write_tenants();
    let tenant = m
        .get_mut(&tenant_id)
        .with_context(|| format!("Tenant not found for id {tenant_id}"))?;

    info!("activating tenant {tenant_id}");

    match tenant.state {
        // If the tenant is already active, nothing to do.
        TenantState::Active => {}

        // If it's Idle, launch the compactor and GC threads
        TenantState::Idle => {
            thread_mgr::spawn(
                ThreadKind::Compactor,
                Some(tenant_id),
                None,
                "Compactor thread",
                false,
                move || crate::tenant_threads::compact_loop(tenant_id),
            )?;

            let gc_spawn_result = thread_mgr::spawn(
                ThreadKind::GarbageCollector,
                Some(tenant_id),
                None,
                "GC thread",
                false,
                move || crate::tenant_threads::gc_loop(tenant_id),
            )
            .map(|_| ())
            .with_context(|| format!("Failed to launch GC thread for tenant {tenant_id}"));

            if let Err(e) = &gc_spawn_result {
                error!("Failed to start GC thread for tenant {tenant_id}, stopping its checkpointer thread: {e:?}");
                thread_mgr::shutdown_threads(Some(ThreadKind::Compactor), Some(tenant_id), None);
                return gc_spawn_result;
            }
            tenant.state = TenantState::Active;
        }

        TenantState::Stopping => {
            // don't re-activate it if it's being stopped
        }
    }
    Ok(())
}

pub fn get_repository_for_tenant(tenant_id: ZTenantId) -> anyhow::Result<Arc<RepositoryImpl>> {
    let m = tenants_state::read_tenants();
    let tenant = m
        .get(&tenant_id)
        .with_context(|| format!("Tenant {tenant_id} not found"))?;

    Ok(Arc::clone(&tenant.repo))
}

/// Retrieves local timeline for tenant.
/// Loads it into memory if it is not already loaded.
pub fn get_local_timeline_with_load(
    tenant_id: ZTenantId,
    timeline_id: ZTimelineId,
) -> anyhow::Result<Arc<DatadirTimelineImpl>> {
    let mut m = tenants_state::write_tenants();
    let tenant = m
        .get_mut(&tenant_id)
        .with_context(|| format!("Tenant {tenant_id} not found"))?;

    if let Some(page_tline) = tenant.local_timelines.get(&timeline_id) {
        return Ok(Arc::clone(page_tline));
    }

    let page_tline = new_local_timeline(&tenant.repo, timeline_id)
        .with_context(|| format!("Failed to create new local timeline for tenant {tenant_id}"))?;
    tenant
        .local_timelines
        .insert(timeline_id, Arc::clone(&page_tline));
    Ok(page_tline)
}

pub fn detach_timeline(
    conf: &'static PageServerConf,
    tenant_id: ZTenantId,
    timeline_id: ZTimelineId,
) -> anyhow::Result<()> {
    // shutdown the timeline threads (this shuts down the walreceiver)
    thread_mgr::shutdown_threads(None, Some(tenant_id), Some(timeline_id));

    match tenants_state::write_tenants().get_mut(&tenant_id) {
        Some(tenant) => {
            tenant
                .repo
                .detach_timeline(timeline_id)
                .context("Failed to detach inmem tenant timeline")?;
            tenant.local_timelines.remove(&timeline_id);
        }
        None => bail!("Tenant {tenant_id} not found in local tenant state"),
    }

    let local_timeline_directory = conf.timeline_path(&timeline_id, &tenant_id);
    std::fs::remove_dir_all(&local_timeline_directory).with_context(|| {
        format!(
            "Failed to remove local timeline directory '{}'",
            local_timeline_directory.display()
        )
    })?;

    Ok(())
}

fn new_local_timeline(
    repo: &RepositoryImpl,
    timeline_id: ZTimelineId,
) -> anyhow::Result<Arc<DatadirTimeline<LayeredRepository>>> {
    let inmem_timeline = repo.get_timeline_load(timeline_id).with_context(|| {
        format!("Inmem timeline {timeline_id} not found in tenant's repository")
    })?;
    let repartition_distance = repo.get_checkpoint_distance() / 10;
    let page_tline = Arc::new(DatadirTimelineImpl::new(
        inmem_timeline,
        repartition_distance,
    ));
    page_tline.init_logical_size()?;
    Ok(page_tline)
}

#[serde_as]
#[derive(Serialize, Deserialize, Clone)]
pub struct TenantInfo {
    #[serde_as(as = "DisplayFromStr")]
    pub id: ZTenantId,
    pub state: TenantState,
}

pub fn list_tenants() -> Vec<TenantInfo> {
    tenants_state::read_tenants()
        .iter()
        .map(|(id, tenant)| TenantInfo {
            id: *id,
            state: tenant.state,
        })
        .collect()
}

pub fn get_tenant_by_id(id: ZTenantId) -> Option<TenantInfo> {
    tenants_state::read_tenants()
        .get(&id)
        .map(|tenant| TenantInfo {
            id,
            state: tenant.state,
        })
}

fn init_local_repositories(
    conf: &'static PageServerConf,
    local_timeline_init_statuses: HashMap<ZTenantId, HashMap<ZTimelineId, LocalTimelineInitStatus>>,
    remote_index: &RemoteIndex,
) -> anyhow::Result<(), anyhow::Error> {
    for (tenant_id, local_timeline_init_statuses) in local_timeline_init_statuses {
        // initialize local tenant
        let repo = load_local_repo(conf, tenant_id, remote_index)
            .with_context(|| format!("Failed to load repo for tenant {tenant_id}"))?;

        let mut status_updates = HashMap::with_capacity(local_timeline_init_statuses.len());
        for (timeline_id, init_status) in local_timeline_init_statuses {
            match init_status {
                LocalTimelineInitStatus::LocallyComplete => {
                    debug!("timeline {timeline_id} for tenant {tenant_id} is locally complete, registering it in repository");
                    status_updates.insert(timeline_id, TimelineSyncStatusUpdate::Downloaded);
                }
                LocalTimelineInitStatus::NeedsSync => {
                    debug!(
                        "timeline {tenant_id} for tenant {timeline_id} needs sync, \
                         so skipped for adding into repository until sync is finished"
                    );
                }
            }
        }

        // Lets fail here loudly to be on the safe side.
        // XXX: It may be a better api to actually distinguish between repository startup
        //   and processing of newly downloaded timelines.
        apply_timeline_remote_sync_status_updates(&repo, status_updates)
            .with_context(|| format!("Failed to bootstrap timelines for tenant {tenant_id}"))?
    }
    Ok(())
}

fn apply_timeline_remote_sync_status_updates(
    repo: &LayeredRepository,
    status_updates: HashMap<ZTimelineId, TimelineSyncStatusUpdate>,
) -> anyhow::Result<()> {
    let mut registration_queue = Vec::with_capacity(status_updates.len());

    // first need to register the in-mem representations, to avoid missing ancestors during the local disk data registration
    for (timeline_id, status_update) in status_updates {
        repo.apply_timeline_remote_sync_status_update(timeline_id, status_update)
            .with_context(|| {
                format!("Failed to load timeline {timeline_id} into in-memory repository")
            })?;
        match status_update {
            TimelineSyncStatusUpdate::Downloaded => registration_queue.push(timeline_id),
        }
    }

    for timeline_id in registration_queue {
        let tenant_id = repo.tenant_id();
        match tenants_state::write_tenants().get_mut(&tenant_id) {
            Some(tenant) => match tenant.local_timelines.entry(timeline_id) {
                Entry::Occupied(_) => {
                    bail!("Local timeline {timeline_id} already registered")
                }
                Entry::Vacant(v) => {
                    v.insert(new_local_timeline(repo, timeline_id).with_context(|| {
                        format!("Failed to register new local timeline for tenant {tenant_id}")
                    })?);
                }
            },
            None => bail!(
                "Tenant {} not found in local tenant state",
                repo.tenant_id()
            ),
        }
    }

    Ok(())
}

// Sets up wal redo manager and repository for tenant. Reduces code duplication.
// Used during pageserver startup, or when new tenant is attached to pageserver.
fn load_local_repo(
    conf: &'static PageServerConf,
    tenant_id: ZTenantId,
    remote_index: &RemoteIndex,
) -> anyhow::Result<Arc<RepositoryImpl>> {
    let mut m = tenants_state::write_tenants();
    let tenant = m.entry(tenant_id).or_insert_with(|| {
        // Set up a WAL redo manager, for applying WAL records.
        let walredo_mgr = PostgresRedoManager::new(conf, tenant_id);

        // Set up an object repository, for actual data storage.
        let repo: Arc<LayeredRepository> = Arc::new(LayeredRepository::new(
            conf,
            TenantConfOpt::default(),
            Arc::new(walredo_mgr),
            tenant_id,
            remote_index.clone(),
            conf.remote_storage_config.is_some(),
        ));
        Tenant {
            state: TenantState::Idle,
            repo,
            local_timelines: HashMap::new(),
        }
    });

    // Restore tenant config
    let tenant_conf = LayeredRepository::load_tenant_config(conf, tenant_id)?;
    tenant.repo.update_tenant_config(tenant_conf)?;

    Ok(Arc::clone(&tenant.repo))
}

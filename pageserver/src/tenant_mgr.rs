//! This module acts as a switchboard to access different repositories managed by this
//! page server.

use crate::config::PageServerConf;
use crate::layered_repository::LayeredRepository;
use crate::remote_storage::RemoteTimelineIndex;
use crate::repository::{Repository, Timeline, TimelineSyncStatusUpdate};
use crate::thread_mgr;
use crate::thread_mgr::ThreadKind;
use crate::timelines;
use crate::timelines::CreateRepo;
use crate::walredo::PostgresRedoManager;
use crate::CheckpointConfig;
use anyhow::{Context, Result};
use lazy_static::lazy_static;
use log::*;
use serde::{Deserialize, Serialize};
use serde_with::{serde_as, DisplayFromStr};
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, Mutex, MutexGuard};
use zenith_utils::zid::{ZTenantId, ZTimelineId};

lazy_static! {
    static ref TENANTS: Mutex<HashMap<ZTenantId, Tenant>> = Mutex::new(HashMap::new());
}

struct Tenant {
    state: TenantState,
    repo: Arc<dyn Repository>,
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

fn access_tenants() -> MutexGuard<'static, HashMap<ZTenantId, Tenant>> {
    TENANTS.lock().unwrap()
}

// Sets up wal redo manager and repository for tenant. Reduces code duplocation.
// Used during pageserver startup, or when new tenant is attached to pageserver.
pub fn load_local_repo(
    conf: &'static PageServerConf,
    tenant_id: ZTenantId,
    remote_index: &Arc<tokio::sync::RwLock<RemoteTimelineIndex>>,
) -> Arc<dyn Repository> {
    let mut m = access_tenants();
    let tenant = m.entry(tenant_id).or_insert_with(|| {
        // Set up a WAL redo manager, for applying WAL records.
        let walredo_mgr = PostgresRedoManager::new(conf, tenant_id);

        // Set up an object repository, for actual data storage.
        let repo: Arc<dyn Repository> = Arc::new(LayeredRepository::new(
            conf,
            Arc::new(walredo_mgr),
            tenant_id,
            Arc::clone(remote_index),
            conf.remote_storage_config.is_some(),
        ));
        Tenant {
            state: TenantState::Idle,
            repo,
        }
    });
    Arc::clone(&tenant.repo)
}

/// Updates tenants' repositories, changing their timelines state in memory.
pub fn apply_timeline_sync_status_updates(
    conf: &'static PageServerConf,
    remote_index: Arc<tokio::sync::RwLock<RemoteTimelineIndex>>,
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
    trace!("Sync status updates: {:?}", sync_status_updates);

    for (tenant_id, tenant_timelines_sync_status_updates) in sync_status_updates {
        let repo = load_local_repo(conf, tenant_id, &remote_index);

        for (timeline_id, timeline_sync_status_update) in tenant_timelines_sync_status_updates {
            match repo.apply_timeline_remote_sync_status_update(timeline_id, timeline_sync_status_update)
            {
                Ok(_) => debug!(
                    "successfully applied timeline sync status update: {} -> {}",
                    timeline_id, timeline_sync_status_update
                ),
                Err(e) => error!(
                    "Failed to apply timeline sync status update for tenant {}. timeline {} update {} Error: {:#}",
                    tenant_id, timeline_id, timeline_sync_status_update, e
                ),
            }
        }
    }
}

///
/// Shut down all tenants. This runs as part of pageserver shutdown.
///
pub fn shutdown_all_tenants() {
    let mut m = access_tenants();
    let mut tenantids = Vec::new();
    for (tenantid, tenant) in m.iter_mut() {
        tenant.state = TenantState::Stopping;
        tenantids.push(*tenantid)
    }
    drop(m);

    thread_mgr::shutdown_threads(Some(ThreadKind::WalReceiver), None, None);
    thread_mgr::shutdown_threads(Some(ThreadKind::GarbageCollector), None, None);
    thread_mgr::shutdown_threads(Some(ThreadKind::Checkpointer), None, None);

    // Ok, no background threads running anymore. Flush any remaining data in
    // memory to disk.
    //
    // We assume that any incoming connections that might request pages from
    // the repository have already been terminated by the caller, so there
    // should be no more activity in any of the repositories.
    //
    // On error, log it but continue with the shutdown for other tenants.
    for tenantid in tenantids {
        debug!("shutdown tenant {}", tenantid);
        match get_repository_for_tenant(tenantid) {
            Ok(repo) => {
                if let Err(err) = repo.checkpoint_iteration(CheckpointConfig::Flush) {
                    error!(
                        "Could not checkpoint tenant {} during shutdown: {:?}",
                        tenantid, err
                    );
                }
            }
            Err(err) => {
                error!(
                    "Could not get repository for tenant {} during shutdown: {:?}",
                    tenantid, err
                );
            }
        }
    }
}

pub fn create_tenant_repository(
    conf: &'static PageServerConf,
    tenantid: ZTenantId,
    remote_index: Arc<tokio::sync::RwLock<RemoteTimelineIndex>>,
) -> Result<Option<ZTenantId>> {
    match access_tenants().entry(tenantid) {
        Entry::Occupied(_) => {
            debug!("tenant {} already exists", tenantid);
            Ok(None)
        }
        Entry::Vacant(v) => {
            let wal_redo_manager = Arc::new(PostgresRedoManager::new(conf, tenantid));
            let repo = timelines::create_repo(
                conf,
                tenantid,
                CreateRepo::Real {
                    wal_redo_manager,
                    remote_index,
                },
            )?;
            v.insert(Tenant {
                state: TenantState::Idle,
                repo,
            });
            Ok(Some(tenantid))
        }
    }
}

pub fn get_tenant_state(tenantid: ZTenantId) -> Option<TenantState> {
    Some(access_tenants().get(&tenantid)?.state)
}

///
/// Change the state of a tenant to Active and launch its checkpointer and GC
/// threads. If the tenant was already in Active state or Stopping, does nothing.
///
pub fn activate_tenant(conf: &'static PageServerConf, tenant_id: ZTenantId) -> Result<()> {
    let mut m = access_tenants();
    let tenant = m
        .get_mut(&tenant_id)
        .with_context(|| format!("Tenant not found for id {}", tenant_id))?;

    info!("activating tenant {}", tenant_id);

    match tenant.state {
        // If the tenant is already active, nothing to do.
        TenantState::Active => {}

        // If it's Idle, launch the checkpointer and GC threads
        TenantState::Idle => {
            thread_mgr::spawn(
                ThreadKind::Checkpointer,
                Some(tenant_id),
                None,
                "Checkpointer thread",
                true,
                move || crate::tenant_threads::checkpoint_loop(tenant_id, conf),
            )?;

            let gc_spawn_result = thread_mgr::spawn(
                ThreadKind::GarbageCollector,
                Some(tenant_id),
                None,
                "GC thread",
                true,
                move || crate::tenant_threads::gc_loop(tenant_id, conf),
            )
            .with_context(|| format!("Failed to launch GC thread for tenant {}", tenant_id));

            if let Err(e) = &gc_spawn_result {
                error!(
                    "Failed to start GC thread for tenant {}, stopping its checkpointer thread: {:?}",
                    tenant_id, e
                );
                thread_mgr::shutdown_threads(Some(ThreadKind::Checkpointer), Some(tenant_id), None);
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

pub fn get_repository_for_tenant(tenantid: ZTenantId) -> Result<Arc<dyn Repository>> {
    let m = access_tenants();
    let tenant = m
        .get(&tenantid)
        .with_context(|| format!("Tenant {} not found", tenantid))?;

    Ok(Arc::clone(&tenant.repo))
}

// Retrieve timeline for tenant. Load it into memory if it is not already loaded
pub fn get_timeline_for_tenant_load(
    tenantid: ZTenantId,
    timelineid: ZTimelineId,
) -> Result<Arc<dyn Timeline>> {
    get_repository_for_tenant(tenantid)?
        .get_timeline_load(timelineid)
        .with_context(|| format!("Timeline {} not found for tenant {}", timelineid, tenantid))
}

#[serde_as]
#[derive(Serialize, Deserialize, Clone)]
pub struct TenantInfo {
    #[serde_as(as = "DisplayFromStr")]
    pub id: ZTenantId,
    pub state: TenantState,
}

pub fn list_tenants() -> Result<Vec<TenantInfo>> {
    access_tenants()
        .iter()
        .map(|v| {
            let (id, tenant) = v;
            Ok(TenantInfo {
                id: *id,
                state: tenant.state,
            })
        })
        .collect()
}

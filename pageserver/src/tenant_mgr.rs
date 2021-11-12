//! This module acts as a switchboard to access different repositories managed by this
//! page server.

use crate::branches;
use crate::config::PageServerConf;
use crate::layered_repository::LayeredRepository;
use crate::repository::{Repository, Timeline, TimelineSyncState};
use crate::tenant_threads;
use crate::walredo::PostgresRedoManager;
use anyhow::{anyhow, bail, Context, Result};
use lazy_static::lazy_static;
use log::*;
use serde::{Deserialize, Serialize};
use std::collections::{hash_map, HashMap};
use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use zenith_utils::zid::{ZTenantId, ZTimelineId};

lazy_static! {
    static ref TENANTS: Mutex<HashMap<ZTenantId, Tenant>> = Mutex::new(HashMap::new());
}

struct Tenant {
    state: TenantState,
    repo: Option<Arc<dyn Repository>>,
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

static SHUTDOWN_REQUESTED: AtomicBool = AtomicBool::new(false);

/// Updates tenants' repositories, changing their timelines state in memory.
pub fn set_timeline_states(
    conf: &'static PageServerConf,
    timeline_states: HashMap<ZTenantId, HashMap<ZTimelineId, TimelineSyncState>>,
) {
    if timeline_states.is_empty() {
        debug!("no timeline state updates to perform");
        return;
    }

    info!("Updating states for {} timelines", timeline_states.len());
    trace!("States: {:?}", timeline_states);

    let mut m = access_tenants();
    for (tenant_id, timeline_states) in timeline_states {
        let tenant = m.entry(tenant_id).or_insert_with(|| Tenant {
            state: TenantState::Idle,
            repo: None,
        });
        if let Err(e) = put_timelines_into_tenant(conf, tenant, tenant_id, timeline_states) {
            error!(
                "Failed to update timeline states for tenant {}: {:#}",
                tenant_id, e
            );
        }
    }
}

fn put_timelines_into_tenant(
    conf: &'static PageServerConf,
    tenant: &mut Tenant,
    tenant_id: ZTenantId,
    timeline_states: HashMap<ZTimelineId, TimelineSyncState>,
) -> anyhow::Result<()> {
    let repo = match tenant.repo.as_ref() {
        Some(repo) => Arc::clone(repo),
        None => {
            // Set up a WAL redo manager, for applying WAL records.
            let walredo_mgr = PostgresRedoManager::new(conf, tenant_id);

            // Set up an object repository, for actual data storage.
            let repo: Arc<dyn Repository> = Arc::new(LayeredRepository::new(
                conf,
                Arc::new(walredo_mgr),
                tenant_id,
                conf.remote_storage_config.is_some(),
            ));
            tenant.repo = Some(Arc::clone(&repo));
            repo
        }
    };

    for (timeline_id, timeline_state) in timeline_states {
        repo.set_timeline_state(timeline_id, timeline_state)
            .with_context(|| {
                format!(
                    "Failed to update timeline {} state to {:?}",
                    timeline_id, timeline_state
                )
            })?;
    }

    Ok(())
}

// Check this flag in the thread loops to know when to exit
pub fn shutdown_requested() -> bool {
    SHUTDOWN_REQUESTED.load(Ordering::Relaxed)
}

pub fn shutdown_all_tenants() -> Result<()> {
    SHUTDOWN_REQUESTED.swap(true, Ordering::Relaxed);

    let tenantids = list_tenantids()?;

    for tenantid in &tenantids {
        set_tenant_state(*tenantid, TenantState::Stopping)?;
    }

    for tenantid in tenantids {
        // Wait for checkpointer and GC to finish their job
        tenant_threads::wait_for_tenant_threads_to_stop(tenantid);

        let repo = get_repository_for_tenant(tenantid)?;
        debug!("shutdown tenant {}", tenantid);
        repo.shutdown()?;
    }
    Ok(())
}

pub fn create_repository_for_tenant(
    conf: &'static PageServerConf,
    tenantid: ZTenantId,
) -> Result<()> {
    let wal_redo_manager = Arc::new(PostgresRedoManager::new(conf, tenantid));
    let repo = Some(branches::create_repo(conf, tenantid, wal_redo_manager)?);

    match access_tenants().entry(tenantid) {
        hash_map::Entry::Occupied(_) => bail!("tenant {} already exists", tenantid),
        hash_map::Entry::Vacant(v) => {
            v.insert(Tenant {
                state: TenantState::Idle,
                repo,
            });
        }
    }

    Ok(())
}

pub fn get_tenant_state(tenantid: ZTenantId) -> Option<TenantState> {
    Some(access_tenants().get(&tenantid)?.state)
}

pub fn set_tenant_state(tenantid: ZTenantId, newstate: TenantState) -> Result<TenantState> {
    let mut m = access_tenants();
    let tenant = m.get_mut(&tenantid);

    match tenant {
        Some(tenant) => {
            if newstate == TenantState::Idle && tenant.state != TenantState::Active {
                // Only Active tenant can become Idle
                return Ok(tenant.state);
            }
            info!("set_tenant_state: {} -> {}", tenant.state, newstate);
            tenant.state = newstate;
            Ok(tenant.state)
        }
        None => bail!("Tenant not found for id {}", tenantid),
    }
}

pub fn get_repository_for_tenant(tenantid: ZTenantId) -> Result<Arc<dyn Repository>> {
    let m = access_tenants();
    let tenant = m
        .get(&tenantid)
        .ok_or_else(|| anyhow!("Tenant not found for tenant {}", tenantid))?;

    match &tenant.repo {
        Some(repo) => Ok(Arc::clone(repo)),
        None => anyhow::bail!("Repository for tenant {} is not yet valid", tenantid),
    }
}

pub fn get_timeline_for_tenant(
    tenantid: ZTenantId,
    timelineid: ZTimelineId,
) -> Result<Arc<dyn Timeline>> {
    get_repository_for_tenant(tenantid)?
        .get_timeline(timelineid)?
        .local_timeline()
        .ok_or_else(|| anyhow!("cannot fetch timeline {}", timelineid))
}

fn list_tenantids() -> Result<Vec<ZTenantId>> {
    access_tenants()
        .iter()
        .map(|v| {
            let (tenantid, _) = v;
            Ok(*tenantid)
        })
        .collect()
}

#[derive(Serialize, Deserialize, Clone)]
pub struct TenantInfo {
    #[serde(with = "hex")]
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

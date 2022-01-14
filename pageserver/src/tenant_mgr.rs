//! This module acts as a switchboard to access different repositories managed by this
//! page server.

use crate::branches;
use crate::config::PageServerConf;
use crate::layered_repository::LayeredRepository;
use crate::repository::{Repository, Timeline, TimelineSyncState};
use crate::thread_mgr;
use crate::thread_mgr::ThreadKind;
use crate::walredo::PostgresRedoManager;
use crate::CheckpointConfig;
use anyhow::{anyhow, bail, Context, Result};
use lazy_static::lazy_static;
use log::*;
use serde::{Deserialize, Serialize};
use std::collections::{hash_map, HashMap};
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
        let tenant = m.entry(tenant_id).or_insert_with(|| {
            // Set up a WAL redo manager, for applying WAL records.
            let walredo_mgr = PostgresRedoManager::new(conf, tenant_id);

            // Set up an object repository, for actual data storage.
            let repo: Arc<dyn Repository> = Arc::new(LayeredRepository::new(
                conf,
                Arc::new(walredo_mgr),
                tenant_id,
                conf.remote_storage_config.is_some(),
            ));
            Tenant {
                state: TenantState::Idle,
                repo,
            }
        });
        if let Err(e) = put_timelines_into_tenant(tenant, tenant_id, timeline_states) {
            error!(
                "Failed to update timeline states for tenant {}: {:?}",
                tenant_id, e
            );
        }
    }
}

fn put_timelines_into_tenant(
    tenant: &mut Tenant,
    tenant_id: ZTenantId,
    timeline_states: HashMap<ZTimelineId, TimelineSyncState>,
) -> anyhow::Result<()> {
    for (timeline_id, timeline_state) in timeline_states {
        // If the timeline is being put into any other state than Ready,
        // stop any threads operating on it.
        //
        // FIXME: This is racy. A page service thread could just get
        // handle on the Timeline, before we call set_timeline_state()
        if !matches!(timeline_state, TimelineSyncState::Ready(_)) {
            thread_mgr::shutdown_threads(None, Some(tenant_id), Some(timeline_id));

            // Should we run a final checkpoint to flush all the data to
            // disk? Doesn't seem necessary; all of the states other than
            // Ready imply that the data on local disk is corrupt or incomplete,
            // and we don't want to flush that to disk.
        }

        tenant
            .repo
            .set_timeline_state(timeline_id, timeline_state)
            .with_context(|| {
                format!(
                    "Failed to update timeline {} state to {:?}",
                    timeline_id, timeline_state
                )
            })?;
    }

    Ok(())
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

pub fn create_repository_for_tenant(
    conf: &'static PageServerConf,
    tenantid: ZTenantId,
) -> Result<()> {
    let wal_redo_manager = Arc::new(PostgresRedoManager::new(conf, tenantid));
    let repo = branches::create_repo(conf, tenantid, wal_redo_manager)?;

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

///
/// Change the state of a tenant to Active and launch its checkpointer and GC
/// threads. If the tenant was already in Active state or Stopping, does nothing.
///
pub fn activate_tenant(conf: &'static PageServerConf, tenantid: ZTenantId) -> Result<()> {
    let mut m = access_tenants();
    let tenant = m
        .get_mut(&tenantid)
        .ok_or_else(|| anyhow!("Tenant not found for id {}", tenantid))?;

    info!("activating tenant {}", tenantid);

    match tenant.state {
        // If the tenant is already active, nothing to do.
        TenantState::Active => {}

        // If it's Idle, launch the checkpointer and GC threads
        TenantState::Idle => {
            thread_mgr::spawn(
                ThreadKind::Checkpointer,
                Some(tenantid),
                None,
                "Checkpointer thread",
                move || crate::tenant_threads::checkpoint_loop(tenantid, conf),
            )?;

            // FIXME: if we fail to launch the GC thread, but already launched the
            // checkpointer, we're in a strange state.

            thread_mgr::spawn(
                ThreadKind::GarbageCollector,
                Some(tenantid),
                None,
                "GC thread",
                move || crate::tenant_threads::gc_loop(tenantid, conf),
            )?;

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
        .ok_or_else(|| anyhow!("Tenant not found for tenant {}", tenantid))?;

    Ok(Arc::clone(&tenant.repo))
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

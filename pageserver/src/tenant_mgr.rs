//! This module acts as a switchboard to access different repositories managed by this
//! page server.

use crate::branches;
use crate::config::PageServerConf;
use crate::layered_repository::LayeredRepository;
use crate::repository::{Repository, Timeline};
use crate::tenant_threads;
use crate::walredo::PostgresRedoManager;
use anyhow::{anyhow, bail, Context, Result};
use lazy_static::lazy_static;
use log::{debug, info};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;
use std::fs;
use std::str::FromStr;
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
    // This tenant only exists in cloud storage. It cannot be accessed.
    CloudOnly,
    // This tenant exists in cloud storage, and we are currently downloading it to local disk.
    // It cannot be accessed yet, not until it's been fully downloaded to local disk.
    Downloading,
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
            TenantState::CloudOnly => f.write_str("CloudOnly"),
            TenantState::Downloading => f.write_str("Downloading"),
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

pub fn init(conf: &'static PageServerConf) {
    for dir_entry in fs::read_dir(conf.tenants_path()).unwrap() {
        let tenantid =
            ZTenantId::from_str(dir_entry.unwrap().file_name().to_str().unwrap()).unwrap();

        {
            let mut m = access_tenants();
            let tenant = Tenant {
                state: TenantState::CloudOnly,
                repo: None,
            };
            m.insert(tenantid, tenant);
        }

        init_repo(conf, tenantid);
        info!("initialized storage for tenant: {}", &tenantid);
    }
}

fn init_repo(conf: &'static PageServerConf, tenant_id: ZTenantId) {
    // Set up a WAL redo manager, for applying WAL records.
    let walredo_mgr = PostgresRedoManager::new(conf, tenant_id);

    // Set up an object repository, for actual data storage.
    let repo = Arc::new(LayeredRepository::new(
        conf,
        Arc::new(walredo_mgr),
        tenant_id,
        true,
    ));

    let mut m = access_tenants();
    let tenant = m.get_mut(&tenant_id).unwrap();
    tenant.repo = Some(repo);
    tenant.state = TenantState::Idle;
}

pub fn register_timeline_download(
    conf: &'static PageServerConf,
    tenant_id: ZTenantId,
    timeline_id: ZTimelineId,
) {
    log::info!(
        "Registering new download, tenant id {}, timeline id: {}",
        tenant_id,
        timeline_id
    );

    {
        let mut m = access_tenants();
        let tenant = m.entry(tenant_id).or_insert_with(|| Tenant {
            state: TenantState::Downloading,
            repo: None,
        });
        tenant.state = TenantState::Downloading;
        match &tenant.repo {
            Some(repo) => {
                init_timeline(repo.as_ref(), timeline_id);
                tenant.state = TenantState::Idle;
                return;
            }
            None => log::warn!("Initialize new repo"),
        }
        tenant.state = TenantState::Idle;
    }

    // init repo updates Tenant state
    init_repo(conf, tenant_id);
    let new_repo = get_repository_for_tenant(tenant_id).unwrap();
    init_timeline(new_repo.as_ref(), timeline_id);
}

fn init_timeline(repo: &dyn Repository, timeline_id: ZTimelineId) {
    match repo.get_timeline(timeline_id) {
        Ok(_timeline) => log::info!("Successfully initialized timeline {}", timeline_id),
        Err(e) => log::error!("Failed to init timeline {}, reason: {:#}", timeline_id, e),
    }
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
    {
        let mut m = access_tenants();
        // First check that the tenant doesn't exist already
        if m.get(&tenantid).is_some() {
            bail!("tenant {} already exists", tenantid);
        }
        let tenant = Tenant {
            state: TenantState::CloudOnly,
            repo: None,
        };
        m.insert(tenantid, tenant);
    }

    let wal_redo_manager = Arc::new(PostgresRedoManager::new(conf, tenantid));
    let repo = branches::create_repo(conf, tenantid, wal_redo_manager)?;

    let mut m = access_tenants();
    let tenant = m.get_mut(&tenantid).unwrap();
    tenant.repo = Some(repo);
    tenant.state = TenantState::Idle;

    Ok(())
}

// If tenant is not found in the repository, return CloudOnly state
pub fn get_tenant_state(tenantid: ZTenantId) -> TenantState {
    let m = access_tenants();
    match m.get(&tenantid) {
        Some(tenant) => tenant.state,
        None => TenantState::CloudOnly,
    }
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
        None => bail!("Tenant not found for tenant {}", tenantid),
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
        .get_timeline(timelineid)
        .with_context(|| format!("cannot fetch timeline {}", timelineid))
}

fn list_tenantids() -> Result<Vec<ZTenantId>> {
    let m = access_tenants();
    m.iter()
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
    let m = access_tenants();
    m.iter()
        .map(|v| {
            let (id, tenant) = v;
            Ok(TenantInfo {
                id: *id,
                state: tenant.state,
            })
        })
        .collect()
}

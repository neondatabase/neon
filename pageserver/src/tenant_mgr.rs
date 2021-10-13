//! This module acts as a switchboard to access different repositories managed by this
//! page server.

use crate::branches;
use crate::layered_repository::LayeredRepository;
use crate::repository::{Repository, Timeline};
use crate::walreceiver;
use crate::walredo::PostgresRedoManager;
use crate::PageServerConf;
use anyhow::{anyhow, bail, Context, Result};
use lazy_static::lazy_static;
use log::info;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;
use std::fs;
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::thread;
use std::thread::JoinHandle;
use zenith_utils::zid::{ZTenantId, ZTimelineId};

lazy_static! {
    static ref TENANTS: Mutex<HashMap<ZTenantId, Tenant>> = Mutex::new(HashMap::new());
}

struct Tenant {
    state: TenantState,
    repo: Option<Arc<dyn Repository>>,
}

impl Drop for Tenant {
    fn drop(&mut self) {
        info!("drop Tenant for tenant {}", self.state);
    }
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone, Copy)]
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
            TenantState::Stopping => f.write_str("Stopping"),
        }
    }
}

fn access_tenants() -> MutexGuard<'static, HashMap<ZTenantId, Tenant>> {
    TENANTS.lock().unwrap()
}

struct TenantHandleEntry {
    checkpointer_handle: Option<JoinHandle<()>>,
    gc_handle: Option<JoinHandle<()>>,
}

// Logically these handles belong to Repository,
// but it's just simpler to store them separately
lazy_static! {
    static ref TENANT_HANDLES: Mutex<HashMap<ZTenantId, TenantHandleEntry>> =
        Mutex::new(HashMap::new());
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

    let checkpointer_handle = LayeredRepository::launch_checkpointer_thread(conf, repo.clone());
    let gc_handle = LayeredRepository::launch_gc_thread(conf, repo.clone());

    let mut handles = TENANT_HANDLES.lock().unwrap();
    let h = TenantHandleEntry {
        checkpointer_handle: Some(checkpointer_handle),
        gc_handle: Some(gc_handle),
    };

    handles.insert(tenant_id, h);

    let mut m = access_tenants();
    let tenant = m.get_mut(&tenant_id).unwrap();
    tenant.repo = Some(repo);
    tenant.state = TenantState::Active;
}

// TODO kb Currently unused function, will later be used when the relish storage downloads a new layer.
// Relevant PR: https://github.com/zenithdb/zenith/pull/686
pub fn register_relish_download(
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
        let tenant = m.get_mut(&tenant_id).unwrap();
        tenant.state = TenantState::Downloading;
        match &tenant.repo {
            Some(repo) => init_timeline(repo.as_ref(), timeline_id),
            None => {
                log::info!("Initialize new repo");
            }
        }
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
pub fn pageserver_shutdown_requested() -> bool {
    SHUTDOWN_REQUESTED.load(Ordering::Relaxed)
}

pub fn wait_for_tenant_threads_to_stop(tenantid: ZTenantId) {
    let mut handles = TENANT_HANDLES.lock().unwrap();
    if let Some(h) = handles.get_mut(&tenantid) {
        h.checkpointer_handle.take().map(JoinHandle::join);
        info!("checkpointer for tenant {} has stopped", tenantid);
        h.gc_handle.take().map(JoinHandle::join);
        info!("gc for tenant {} has stopped", tenantid);
    }
}

pub fn shutdown_all_tenants(conf: &'static PageServerConf) -> Result<()> {
    SHUTDOWN_REQUESTED.swap(true, Ordering::Relaxed);

    let tenantids = list_tenantids()?;
    let mut join_handles = Vec::new();

    for tenantid in tenantids {
        set_tenant_state(tenantid, TenantState::Stopping)?;

        let h = thread::Builder::new()
            .name("tenant_drop waiter thread".into())
            .spawn(move || wait_tenant_drop(conf, tenantid, false))?;
        join_handles.push(h);
    }

    for handle in join_handles.into_iter() {
        handle
            .join()
            .expect("thread panicked")
            .expect("thread exited with an error");
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
    tenant.state = TenantState::Active;

    Ok(())
}

pub fn get_tenant_state(tenantid: ZTenantId) -> Option<TenantState> {
    let m = access_tenants();
    m.get(&tenantid).map(|t| t.state)
}

pub fn set_tenant_state(tenantid: ZTenantId, state: TenantState) -> Result<()> {
    let mut m = access_tenants();
    let tenant = m.get_mut(&tenantid);

    match tenant {
        Some(tenant) => tenant.state = state,
        None => bail!("Tenant not found for tenant {}", tenantid),
    }

    Ok(())
}

pub fn tenant_shutdown_requested(tenantid: ZTenantId) -> bool {
    match get_tenant_state(tenantid) {
        Some(state) => state == TenantState::Stopping || state == TenantState::CloudOnly,
        None => true,
    }
}

pub fn get_repository_for_tenant(tenantid: ZTenantId) -> Result<Arc<dyn Repository>> {
    let m = access_tenants();
    let tenant = m
        .get(&tenantid)
        .ok_or_else(|| anyhow!("Tenant not found for tenant {}", tenantid));

    match &tenant?.repo {
        Some(repo) => Ok(Arc::clone(repo)),
        None => anyhow::bail!("Repository is not valid for tenant {}", tenantid),
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

pub fn list_timelineids(tenantid: ZTenantId) -> Result<Vec<ZTimelineId>> {
    let repo = get_repository_for_tenant(tenantid)?;
    Ok(repo.list_timelineids())
}

// Perform graceful tenant shutdown.
// Wait for all tenant workers to complete their job, free resources
// and update the state to "CloudOnly", which represents complete tenant shutdown.
pub fn wait_tenant_drop(
    conf: &'static PageServerConf,
    tenantid: ZTenantId,
    remove_ondisk_files: bool,
) -> Result<()> {
    // Shutdown walreceiver threads for each tenant's timeline.
    let timelines = list_timelineids(tenantid)?;
    for timelineid in timelines {
        walreceiver::stop_walreceiver(timelineid);
    }

    // Wait for checkpointer and GC to finish their job
    wait_for_tenant_threads_to_stop(tenantid);

    // Remove tenant's on-disk data
    // TODO: We can add an option to skip this step if we only want to stop tenant,
    // not remove it completely.
    if remove_ondisk_files {
        let repo_dir = conf.tenant_path(&tenantid);
        if let Err(e) = fs::remove_dir_all(&repo_dir) {
            log::error!("could not remove tenant repo-dir: {:#}", e);
        }
    }

    // Now stop the wal-redo process.
    // Note that it is important to stop it after checkpointer.
    let repo = get_repository_for_tenant(tenantid)?;
    repo.shutdown()?;

    // And finally remove tenant repository from memory
    // and update it's state
    // TODO: Should drop command remove tenant from
    // pageserver TENANTS or we need a separate command/option for that?
    // Something like 'tenant detach'/ 'tenant drop --detach'.
    let mut m = access_tenants();
    let tenant = m.get_mut(&tenantid);
    match tenant {
        Some(tenant) => {
            info!("reset tenant state for {}", tenantid);
            tenant.repo.take();
            tenant.state = TenantState::CloudOnly;
        }
        None => bail!("Tenant not found for tenant {}", tenantid),
    }

    info!("drop_tenant({}) is DONE", tenantid);
    Ok(())
}

// Serve tenant drop request.
// This function only updates the tenant state to "Stopping" and returns.
// Separate thread waits for all tenant processes to complete, frees resources
// and updates the state to "CloudOnly", which represents complete tenant shutdown.
pub fn drop_tenant(conf: &'static PageServerConf, tenantid: ZTenantId) -> Result<()> {
    set_tenant_state(tenantid, TenantState::Stopping)?;

    thread::Builder::new()
        .name("tenant_drop waiter thread".into())
        .spawn(move || wait_tenant_drop(conf, tenantid, true))?;

    Ok(())
}

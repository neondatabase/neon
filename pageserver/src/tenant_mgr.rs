//! This module acts as a switchboard to access different repositories managed by this
//! page server.

use crate::branches;
use crate::layered_repository::LayeredRepository;
use crate::repository::{Repository, Timeline};
use crate::walredo::PostgresRedoManager;
use crate::PageServerConf;
use anyhow::{anyhow, bail, Context, Result};
use lazy_static::lazy_static;
use log::{debug, info};
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::fs;
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::thread::JoinHandle;
use zenith_utils::zid::{ZTenantId, ZTimelineId};

lazy_static! {
    static ref REPOSITORY: Mutex<HashMap<ZTenantId, Arc<dyn Repository>>> =
        Mutex::new(HashMap::new());
}

fn access_repository() -> MutexGuard<'static, HashMap<ZTenantId, Arc<dyn Repository>>> {
    REPOSITORY.lock().unwrap()
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
    let mut m = access_repository();
    for dir_entry in fs::read_dir(conf.tenants_path()).unwrap() {
        let tenantid =
            ZTenantId::from_str(dir_entry.unwrap().file_name().to_str().unwrap()).unwrap();
        let repo = init_repo(conf, tenantid);
        info!("initialized storage for tenant: {}", &tenantid);
        m.insert(tenantid, repo);
    }
}

fn init_repo(conf: &'static PageServerConf, tenant_id: ZTenantId) -> Arc<LayeredRepository> {
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

    repo
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
    match access_repository().entry(tenant_id) {
        Entry::Occupied(o) => init_timeline(o.get().as_ref(), timeline_id),
        Entry::Vacant(v) => {
            log::info!("New repo initialized");
            let new_repo = init_repo(conf, tenant_id);
            init_timeline(new_repo.as_ref(), timeline_id);
            v.insert(new_repo);
        }
    }
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

pub fn stop_tenant_threads(tenantid: ZTenantId) {
    let mut handles = TENANT_HANDLES.lock().unwrap();
    if let Some(h) = handles.get_mut(&tenantid) {
        h.checkpointer_handle.take().map(JoinHandle::join);
        debug!("checkpointer for tenant {} has stopped", tenantid);
        h.gc_handle.take().map(JoinHandle::join);
        debug!("gc for tenant {} has stopped", tenantid);
    }
}

pub fn shutdown_all_tenants() -> Result<()> {
    SHUTDOWN_REQUESTED.swap(true, Ordering::Relaxed);

    let tenants = list_tenants()?;
    for tenantid in tenants {
        stop_tenant_threads(tenantid);
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
    let mut m = access_repository();

    // First check that the tenant doesn't exist already
    if m.get(&tenantid).is_some() {
        bail!("tenant {} already exists", tenantid);
    }
    let wal_redo_manager = Arc::new(PostgresRedoManager::new(conf, tenantid));
    let repo = branches::create_repo(conf, tenantid, wal_redo_manager)?;

    m.insert(tenantid, repo);

    Ok(())
}

pub fn get_repository_for_tenant(tenantid: ZTenantId) -> Result<Arc<dyn Repository>> {
    access_repository()
        .get(&tenantid)
        .map(Arc::clone)
        .ok_or_else(|| anyhow!("repository not found for tenant name {}", tenantid))
}

pub fn get_timeline_for_tenant(
    tenantid: ZTenantId,
    timelineid: ZTimelineId,
) -> Result<Arc<dyn Timeline>> {
    get_repository_for_tenant(tenantid)?
        .get_timeline(timelineid)
        .with_context(|| format!("cannot fetch timeline {}", timelineid))
}

fn list_tenants() -> Result<Vec<ZTenantId>> {
    let o = &mut REPOSITORY.lock().unwrap();

    o.iter()
        .map(|tenant| {
            let (tenantid, _) = tenant;
            Ok(*tenantid)
        })
        .collect()
}

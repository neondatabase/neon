//! This module acts as a switchboard to access different repositories managed by this
//! page server.

use crate::branches;
use crate::layered_repository::LayeredRepository;
use crate::repository::{Repository, Timeline};
use crate::walredo::PostgresRedoManager;
use crate::PageServerConf;
use anyhow::{anyhow, bail, Context, Result};
use lazy_static::lazy_static;
use log::info;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::fs;
use std::str::FromStr;
use std::sync::{Arc, Mutex, MutexGuard};
use zenith_utils::zid::{ZTenantId, ZTimelineId};

lazy_static! {
    static ref REPOSITORY: Mutex<HashMap<ZTenantId, Arc<dyn Repository>>> =
        Mutex::new(HashMap::new());
}

fn access_repository() -> MutexGuard<'static, HashMap<ZTenantId, Arc<dyn Repository>>> {
    REPOSITORY.lock().unwrap()
}

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
    LayeredRepository::launch_checkpointer_thread(conf, repo.clone());
    LayeredRepository::launch_gc_thread(conf, repo.clone());
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

pub fn insert_repository_for_tenant(tenantid: ZTenantId, repo: Arc<dyn Repository>) {
    access_repository().insert(tenantid, repo);
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

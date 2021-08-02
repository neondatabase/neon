//! This module acts as a switchboard to access different repositories managed by this
//! page server.

use crate::branches;
use crate::layered_repository::LayeredRepository;
use crate::object_repository::ObjectRepository;
use crate::repository::Repository;
use crate::rocksdb_storage::RocksObjectStore;
use crate::walredo::PostgresRedoManager;
use crate::{PageServerConf, RepositoryFormat, ZTenantId};
use anyhow::{anyhow, bail, Result};
use lazy_static::lazy_static;
use log::info;
use std::collections::HashMap;
use std::fs;
use std::str::FromStr;
use std::sync::{Arc, Mutex};

lazy_static! {
    pub static ref REPOSITORY: Mutex<HashMap<ZTenantId, Arc<dyn Repository>>> =
        Mutex::new(HashMap::new());
}

pub fn init(conf: &'static PageServerConf) {
    let mut m = REPOSITORY.lock().unwrap();

    for dir_entry in fs::read_dir(conf.tenants_path()).unwrap() {
        let tenantid =
            ZTenantId::from_str(dir_entry.unwrap().file_name().to_str().unwrap()).unwrap();

        // Set up a WAL redo manager, for applying WAL records.
        let walredo_mgr = PostgresRedoManager::new(conf, tenantid);

        // Set up an object repository, for actual data storage.
        let repo: Arc<dyn Repository + Sync + Send> = match conf.repository_format {
            RepositoryFormat::Layered => {
				let repo = Arc::new(LayeredRepository::new(
					conf,
					Arc::new(walredo_mgr),
					tenantid,
				));
				if conf.gc_horizon != 0 {
					crate::layered_repository::LayeredRepository::launch_gc_thread(conf, repo.clone());
				} else {
					info!("Garbage collection is disabled");
				}
				repo
			}
            RepositoryFormat::RocksDb => {
                let obj_store = RocksObjectStore::open(conf, &tenantid).unwrap();

                Arc::new(ObjectRepository::new(
                    conf,
                    Arc::new(obj_store),
                    Arc::new(walredo_mgr),
                    tenantid,
                ))
            }
        };

        info!("initialized storage for tenant: {}", &tenantid);
        m.insert(tenantid, repo);
    }
}

pub fn create_repository_for_tenant(
    conf: &'static PageServerConf,
    tenantid: ZTenantId,
) -> Result<()> {
    let mut m = REPOSITORY.lock().unwrap();

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
    let o = &mut REPOSITORY.lock().unwrap();
    o.insert(tenantid, repo);
}

pub fn get_repository_for_tenant(tenantid: &ZTenantId) -> Result<Arc<dyn Repository>> {
    let o = &REPOSITORY.lock().unwrap();
    o.get(tenantid)
        .map(|repo| Arc::clone(repo))
        .ok_or_else(|| anyhow!("repository not found for tenant name {}", tenantid))
}

//! This module acts as a switchboard to access different repositories managed by this
//! page server. Currently, a Page Server can only manage one repository, so there
//! isn't much here. If we implement multi-tenancy, this will probably be changed into
//! a hash map, keyed by the tenant ID.

use crate::branches;
use crate::object_repository::ObjectRepository;
use crate::repository::Repository;
use crate::rocksdb_storage::RocksObjectStore;
use crate::walredo::PostgresRedoManager;
use crate::PageServerConf;
use lazy_static::lazy_static;
use std::sync::{Arc, Mutex};

lazy_static! {
    pub static ref REPOSITORY: Mutex<Option<Arc<dyn Repository>>> = Mutex::new(None);
}

pub fn init(conf: &'static PageServerConf) {
    {
        let mut m = REPOSITORY.lock().unwrap();

        let obj_store = RocksObjectStore::open(conf).unwrap();

        // Set up a WAL redo manager, for applying WAL records.
        let walredo_mgr = PostgresRedoManager::new(conf);

        // we have already changed current dir to the repository.
        let repo = ObjectRepository::new(conf, Arc::new(obj_store), Arc::new(walredo_mgr));

        *m = Some(Arc::new(repo));
    }
    // Force loading timelines
    let _ = branches::get_branches(conf);
}

pub fn get_repository() -> Arc<dyn Repository> {
    let o = &REPOSITORY.lock().unwrap();
    Arc::clone(o.as_ref().unwrap())
}

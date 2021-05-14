//! This module acts as a switchboard to access different repositories managed by this
//! page server. Currently, a Page Server can only manage one repository, so there
//! isn't much here. If we implement multi-tenancy, this will probably be changed into
//! a hash map, keyed by the tenant ID.

use crate::PageServerConf;
//use crate::repository::Repository;
use crate::repository::rocksdb::RocksRepository;
use lazy_static::lazy_static;
use std::path::Path;
use std::sync::{Arc, Mutex};

lazy_static! {
    pub static ref REPOSITORY: Mutex<Option<Arc<RocksRepository>>> = Mutex::new(None);
}

pub fn init(conf: &PageServerConf) {
    let mut m = REPOSITORY.lock().unwrap();

    // we have already changed current dir to the repository.
    let repo = RocksRepository::new(conf, Path::new("."));

    *m = Some(Arc::new(repo));
}

pub fn get_repository() -> Arc<RocksRepository> {
    let o = &REPOSITORY.lock().unwrap();
    Arc::clone(o.as_ref().unwrap())
}

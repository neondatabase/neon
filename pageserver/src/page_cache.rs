//! This module acts as a switchboard to access different repositories managed by this
//! page server. Currently, a Page Server can only manage one repository, so there
//! isn't much here. If we implement multi-tenancy, this will probably be changed into
//! a hash map, keyed by the tenant ID.

use crate::repository::inmemory::InMemoryRepository;
use crate::PageServerConf;
use lazy_static::lazy_static;
use std::sync::{Arc, Mutex};

lazy_static! {
    pub static ref REPOSITORY: Mutex<Option<Arc<InMemoryRepository>>> = Mutex::new(None);
}

pub fn init(conf: &PageServerConf) {
    let mut m = REPOSITORY.lock().unwrap();

    *m = Some(Arc::new(InMemoryRepository::new(conf)));
}

pub fn get_repository() -> Arc<InMemoryRepository> {
    let o = &REPOSITORY.lock().unwrap();
    Arc::clone(o.as_ref().unwrap())
}

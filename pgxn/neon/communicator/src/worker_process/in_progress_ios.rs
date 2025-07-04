//! Lock table to ensure that only one IO request is in flight for a given
//! block (or relation or database metadata) at a time

use std::cmp::Eq;
use std::hash::Hash;
use std::sync::Arc;

use tokio::sync::{Mutex, OwnedMutexGuard};

use clashmap::ClashMap;
use clashmap::Entry;

use pageserver_page_api::RelTag;

#[derive(Clone, Eq, Hash, PartialEq)]
pub enum RequestInProgressKey {
    Db(u32),
    Rel(RelTag),
    Block(RelTag, u32),
}

type RequestId = u64;

pub type RequestInProgressTable = MutexHashMap<RequestInProgressKey, RequestId>;

// more primitive locking thingie:

pub struct MutexHashMap<K, V>
where
    K: Clone + Eq + Hash,
{
    lock_table: ClashMap<K, (V, Arc<Mutex<()>>)>,
}

pub struct MutexHashMapGuard<'a, K, V>
where
    K: Clone + Eq + Hash,
{
    pub key: K,
    map: &'a MutexHashMap<K, V>,
    mutex: Arc<Mutex<()>>,
    _guard: OwnedMutexGuard<()>,
}

impl<'a, K, V> Drop for MutexHashMapGuard<'a, K, V>
where
    K: Clone + Eq + Hash,
{
    fn drop(&mut self) {
        let (_old_key, old_val) = self.map.lock_table.remove(&self.key).unwrap();
        assert!(Arc::ptr_eq(&old_val.1, &self.mutex));

        // the guard will be dropped as we return
    }
}

impl<K, V> MutexHashMap<K, V>
where
    K: Clone + Eq + Hash,
    V: std::fmt::Display + Copy,
{
    pub fn new() -> MutexHashMap<K, V> {
        MutexHashMap {
            lock_table: ClashMap::new(),
        }
    }

    pub async fn lock<'a>(&'a self, key: K, val: V) -> MutexHashMapGuard<'a, K, V> {
        let my_mutex = Arc::new(Mutex::new(()));
        let my_guard = Arc::clone(&my_mutex).lock_owned().await;

        loop {
            let (request_id, lock) = match self.lock_table.entry(key.clone()) {
                Entry::Occupied(e) => {
                    let e = e.get();
                    (e.0, Arc::clone(&e.1))
                }
                Entry::Vacant(e) => {
                    e.insert((val, Arc::clone(&my_mutex)));
                    break;
                }
            };
            tracing::info!("waiting for conflicting IO {request_id} to complete");
            let _ = lock.lock().await;
            tracing::info!("conflicting IO {request_id} completed");
        }

        MutexHashMapGuard {
            key,
            map: self,
            mutex: my_mutex,
            _guard: my_guard,
        }
    }
}

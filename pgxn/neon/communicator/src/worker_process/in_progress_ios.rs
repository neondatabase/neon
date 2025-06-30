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

pub type RequestInProgressTable = MutexHashSet<RequestInProgressKey>;

// more primitive locking thingie:

pub struct MutexHashSet<K>
where
    K: Clone + Eq + Hash,
{
    lock_table: ClashMap<K, Arc<Mutex<()>>>,
}

pub struct MutexHashSetGuard<'a, K>
where
    K: Clone + Eq + Hash,
{
    pub key: K,
    set: &'a MutexHashSet<K>,
    mutex: Arc<Mutex<()>>,
    _guard: OwnedMutexGuard<()>,
}

impl<'a, K> Drop for MutexHashSetGuard<'a, K>
where
    K: Clone + Eq + Hash,
{
    fn drop(&mut self) {
        let (_old_key, old_val) = self.set.lock_table.remove(&self.key).unwrap();
        assert!(Arc::ptr_eq(&old_val, &self.mutex));

        // the guard will be dropped as we return
    }
}

impl<K> MutexHashSet<K>
where
    K: Clone + Eq + Hash,
{
    pub fn new() -> MutexHashSet<K> {
        MutexHashSet {
            lock_table: ClashMap::new(),
        }
    }

    pub async fn lock<'a>(&'a self, key: K) -> MutexHashSetGuard<'a, K> {
        let my_mutex = Arc::new(Mutex::new(()));
        let my_guard = Arc::clone(&my_mutex).lock_owned().await;

        loop {
            let lock = match self.lock_table.entry(key.clone()) {
                Entry::Occupied(e) => Arc::clone(e.get()),
                Entry::Vacant(e) => {
                    e.insert(Arc::clone(&my_mutex));
                    break;
                }
            };
            let _ = lock.lock().await;
        }

        MutexHashSetGuard {
            key,
            set: self,
            mutex: my_mutex,
            _guard: my_guard,
        }
    }
}

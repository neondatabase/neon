//! A set for cancelling random http connections

use std::hash::{BuildHasher, BuildHasherDefault};
use std::num::NonZeroUsize;
use std::time::Duration;

use indexmap::IndexMap;
use parking_lot::Mutex;
use rand::{thread_rng, Rng};
use rustc_hash::FxHasher;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

type Hasher = BuildHasherDefault<FxHasher>;

pub struct CancelSet {
    shards: Box<[Mutex<CancelShard>]>,
    // keyed by random uuid, fxhasher is fine
    hasher: Hasher,
}

pub(crate) struct CancelShard {
    tokens: IndexMap<uuid::Uuid, (Instant, CancellationToken), Hasher>,
}

impl CancelSet {
    pub fn new(shards: usize) -> Self {
        CancelSet {
            shards: (0..shards)
                .map(|_| {
                    Mutex::new(CancelShard {
                        tokens: IndexMap::with_hasher(Hasher::default()),
                    })
                })
                .collect(),
            hasher: Hasher::default(),
        }
    }

    pub(crate) fn take(&self) -> Option<CancellationToken> {
        for _ in 0..4 {
            if let Some(token) = self.take_raw(thread_rng().gen()) {
                return Some(token);
            }
            tracing::trace!("failed to get cancel token");
        }
        None
    }

    pub(crate) fn take_raw(&self, rng: usize) -> Option<CancellationToken> {
        NonZeroUsize::new(self.shards.len())
            .and_then(|len| self.shards[rng % len].lock().take(rng / len))
    }

    pub(crate) fn insert(&self, id: uuid::Uuid, token: CancellationToken) -> CancelGuard<'_> {
        let shard = NonZeroUsize::new(self.shards.len()).map(|len| {
            let hash = self.hasher.hash_one(id) as usize;
            let shard = &self.shards[hash % len];
            shard.lock().insert(id, token);
            shard
        });
        CancelGuard { shard, id }
    }
}

impl CancelShard {
    fn take(&mut self, rng: usize) -> Option<CancellationToken> {
        NonZeroUsize::new(self.tokens.len()).and_then(|len| {
            // 10 second grace period so we don't cancel new connections
            if self.tokens.get_index(rng % len)?.1 .0.elapsed() < Duration::from_secs(10) {
                return None;
            }

            let (_key, (_insert, token)) = self.tokens.swap_remove_index(rng % len)?;
            Some(token)
        })
    }

    fn remove(&mut self, id: uuid::Uuid) {
        self.tokens.swap_remove(&id);
    }

    fn insert(&mut self, id: uuid::Uuid, token: CancellationToken) {
        self.tokens.insert(id, (Instant::now(), token));
    }
}

pub(crate) struct CancelGuard<'a> {
    shard: Option<&'a Mutex<CancelShard>>,
    id: Uuid,
}

impl Drop for CancelGuard<'_> {
    fn drop(&mut self) {
        if let Some(shard) = self.shard {
            shard.lock().remove(self.id);
        }
    }
}

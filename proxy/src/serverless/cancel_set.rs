//! A set for cancelling random http connections

use std::{
    hash::{BuildHasher, BuildHasherDefault},
    num::NonZeroUsize,
};

use indexmap::IndexMap;
use parking_lot::Mutex;
use rand::{thread_rng, Rng};
use rustc_hash::FxHasher;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

type Hasher = BuildHasherDefault<FxHasher>;

pub struct CancelSet {
    shards: Box<[Mutex<CancelShard>]>,
    // keyed by random uuid, fxhasher is fine
    hasher: Hasher,
}

pub struct CancelShard {
    tokens: IndexMap<uuid::Uuid, CancellationToken, Hasher>,
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

    pub fn take(&self) -> Option<CancellationToken> {
        self.take_raw(thread_rng().gen())
    }

    pub fn take_raw(&self, rng: usize) -> Option<CancellationToken> {
        NonZeroUsize::new(self.shards.len())
            .and_then(|len| self.shards[(rng >> 16) % len].lock().take(rng))
    }

    pub fn insert(&self, id: uuid::Uuid, token: CancellationToken) -> CancelGuard<'_> {
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
        NonZeroUsize::new(self.tokens.len())
            .and_then(|len| self.tokens.swap_remove_index(rng % len))
            .map(|(_, v)| v)
    }

    fn remove(&mut self, id: uuid::Uuid) {
        self.tokens.swap_remove(&id);
    }

    fn insert(&mut self, id: uuid::Uuid, token: CancellationToken) {
        self.tokens.insert(id, token);
    }
}

pub struct CancelGuard<'a> {
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

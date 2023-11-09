use crate::key::Key;
use serde::{Deserialize, Serialize};
use utils::id::NodeId;

#[derive(Ord, PartialOrd, Eq, PartialEq, Clone, Copy, Serialize, Deserialize, Debug)]
pub struct ShardNumber(pub u8);

#[derive(Ord, PartialOrd, Eq, PartialEq, Clone, Copy, Serialize, Deserialize, Debug)]
pub struct ShardCount(pub u8);

impl ShardNumber {
    fn within_count(&self, rhs: ShardCount) -> bool {
        self.0 < rhs.0
    }
}

/// Stripe size in number of pages
#[derive(Clone, Copy, Serialize, Deserialize, Eq, PartialEq, Debug)]
pub struct ShardStripeSize(pub u32);

/// Layout version: for future upgrades where we might change how the key->shard mapping works
#[derive(Clone, Copy, Serialize, Deserialize, Eq, PartialEq, Debug)]
pub struct ShardLayout(u8);

const LAYOUT_V1: ShardLayout = ShardLayout(1);

/// Default stripe size in pages: 256MiB divided by 8kiB page size.
const DEFAULT_STRIPE_SIZE: ShardStripeSize = ShardStripeSize(256 * 1024 / 8);

/// The ShardIdentity contains the information needed for one member of map
/// to resolve a key to a shard, and then check whether that shard is ==self.
#[derive(Clone, Copy, Serialize, Deserialize, Eq, PartialEq, Debug)]
pub struct ShardIdentity {
    pub layout: ShardLayout,
    pub number: ShardNumber,
    pub count: ShardCount,
    pub stripe_size: ShardStripeSize,
}

/// The location of a shard contains both the logical identity of the pageserver
/// holding it (control plane's perspective), and the physical page service port
/// that postgres should use (endpoint's perspective).
#[derive(Clone)]
pub struct ShardLocation {
    pub id: NodeId,
    pub page_service: (url::Host, u16),
}

/// The ShardMap is sufficient information to map any Key to the page service
/// which should store it.
#[derive(Clone)]
struct ShardMap {
    layout: ShardLayout,
    count: ShardCount,
    stripe_size: ShardStripeSize,
    pageservers: Vec<Option<ShardLocation>>,
}

impl ShardMap {
    pub fn get_location(&self, shard_number: ShardNumber) -> &Option<ShardLocation> {
        assert!(shard_number.within_count(self.count));
        self.pageservers.get(shard_number.0 as usize).unwrap()
    }

    pub fn get_identity(&self, shard_number: ShardNumber) -> ShardIdentity {
        assert!(shard_number.within_count(self.count));
        ShardIdentity {
            layout: self.layout,
            number: shard_number,
            count: self.count,
            stripe_size: self.stripe_size,
        }
    }

    /// Return Some if the key is assigned to a particular shard.  Else the key
    /// should be ingested by all shards (e.g. dbdir metadata).
    pub fn get_shard_number(&self, key: &Key) -> Option<ShardNumber> {
        if self.count < ShardCount(2) || key_is_broadcast(key) {
            None
        } else {
            Some(key_to_shard_number(self.count, self.stripe_size, key))
        }
    }

    pub fn default_with_shards(shard_count: ShardCount) -> Self {
        ShardMap {
            layout: LAYOUT_V1,
            count: shard_count,
            stripe_size: DEFAULT_STRIPE_SIZE,
            pageservers: (0..shard_count.0 as usize).map(|_| None).collect(),
        }
    }
}

impl ShardIdentity {
    /// An identity with number=0 count=0 is a "none" identity, which represents legacy
    /// tenants.  Modern single-shard tenants should not use this: they should
    /// have number=0 count=1.
    pub fn none() -> Self {
        Self {
            number: ShardNumber(0),
            count: ShardCount(0),
            layout: LAYOUT_V1,
            stripe_size: DEFAULT_STRIPE_SIZE,
        }
    }

    pub fn new(number: ShardNumber, count: ShardCount, stripe_size: ShardStripeSize) -> Self {
        Self {
            number,
            count,
            layout: LAYOUT_V1,
            stripe_size,
        }
    }

    pub fn get_shard_number(&self, key: &Key) -> ShardNumber {
        key_to_shard_number(self.count, self.stripe_size, key)
    }

    /// Return true if the key should be ingested by this shard
    pub fn is_key_local(&self, key: &Key) -> bool {
        if self.count < ShardCount(2) || key_is_broadcast(key) {
            true
        } else {
            key_to_shard_number(self.count, self.stripe_size, key) == self.number
        }
    }

    pub fn slug(&self) -> String {
        if self.count > ShardCount(0) {
            format!("-{:02x}{:02x}", self.number.0, self.count.0)
        } else {
            String::new()
        }
    }
}

impl Default for ShardIdentity {
    /// The default identity is to be the only shard for a tenant, i.e. the legacy
    /// pre-sharding case.
    fn default() -> Self {
        ShardIdentity {
            layout: LAYOUT_V1,
            number: ShardNumber(0),
            count: ShardCount(1),
            stripe_size: DEFAULT_STRIPE_SIZE,
        }
    }
}

/// Whether this key should be ingested by all shards
fn key_is_broadcast(key: &Key) -> bool {
    // TODO: deduplicate wrt pgdatadir_mapping.rs
    fn is_rel_block_key(key: &Key) -> bool {
        key.field1 == 0x00 && key.field4 != 0
    }

    // TODO: can we be less conservative?  Starting point is to broadcast everything
    // except for rel block keys
    !is_rel_block_key(key)
}

/// Provide the same result as the function in postgres `hashfn.h` with the same name
fn murmurhash32(data: u32) -> u32 {
    let mut h = data;

    h ^= h >> 16;
    h *= 0x85ebca6b;
    h ^= h >> 13;
    h *= 0xc2b2ae35;
    h ^= h >> 16;
    h
}

/// Provide the same result as the function in postgres `hashfn.h` with the same name
fn hash_combine(mut a: u32, b: u32) -> u32 {
    a ^= b + 0x9e3779b9 + (a << 6) + (a >> 2);
    a
}

/// Where a Key is to be distributed across shards, select the shard.  This function
/// does not account for keys that should be broadcast across shards.
///
/// The hashing in this function must exactly match what we do in postgres smgr
/// code.  The resulting distribution of pages is intended to preserve locality within
/// `stripe_size` ranges of contiguous block numbers in the same relation, while otherwise
/// distributing data pseudo-randomly.
///
/// The mapping of key to shard is not stable across changes to ShardCount: this is intentional
/// and will be handled at higher levels when shards are split.
fn key_to_shard_number(count: ShardCount, stripe_size: ShardStripeSize, key: &Key) -> ShardNumber {
    // Fast path for un-sharded tenants or broadcast keys
    if count < ShardCount(2) || key_is_broadcast(key) {
        return ShardNumber(0);
    }

    // spcNode
    let mut hash = murmurhash32(key.field2);
    // dbNode
    hash = hash_combine(hash, murmurhash32(key.field3));
    // relNode
    hash = hash_combine(hash, murmurhash32(key.field4));
    // blockNum/stripe size
    hash = hash_combine(hash, murmurhash32(key.field6 / stripe_size.0));

    let shard = (hash % count.0 as u32) as u8;

    ShardNumber(shard)
}

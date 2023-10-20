use std::hash::Hasher;

use crate::key::Key;
use mur3;
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
            return true;
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

/// Where a Key is to be distributed across shards, select the shard.  This function
/// does not account for keys that should be broadcast across shards.
fn key_to_shard_number(count: ShardCount, stripe_size: ShardStripeSize, key: &Key) -> ShardNumber {
    // Fast path for un-sharded tenants or broadcast keys
    if count < ShardCount(2) || key_is_broadcast(key) {
        return ShardNumber(0);
    }

    let mut hasher = mur3::Hasher32::with_seed(0);
    hasher.write_u8(key.field1);
    hasher.write_u32(key.field2);
    hasher.write_u32(key.field3);
    hasher.write_u32(key.field4);
    let hash = hasher.finish32();

    let blkno = key.field6;

    let stripe = hash + (blkno / stripe_size.0);

    let shard = stripe as u8 % (count.0 as u8);

    ShardNumber(shard)
}

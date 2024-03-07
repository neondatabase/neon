# Sharding Phase 1: Static Key-space Sharding

## Summary

To enable databases with sizes approaching the capacity of a pageserver's disk,
it is necessary to break up the storage for the database, or _shard_ it.

Sharding in general is a complex area. This RFC aims to define an initial
capability that will permit creating large-capacity databases using a static configuration
defined at time of Tenant creation.

## Motivation

Currently, all data for a Tenant, including all its timelines, is stored on a single
pageserver. The local storage required may be several times larger than the actual
database size, due to LSM write inflation.

If a database is larger than what one pageserver can hold, then it becomes impossible
for the pageserver to hold it in local storage, as it must do to provide service to
clients.

### Prior art

In Neon:

- Layer File Spreading: https://www.notion.so/neondatabase/One-Pager-Layer-File-Spreading-Konstantin-21fd9b11b618475da5f39c61dd8ab7a4
- Layer File SPreading: https://www.notion.so/neondatabase/One-Pager-Layer-File-Spreading-Christian-eb6b64182a214e11b3fceceee688d843
- Key Space partitioning: https://www.notion.so/neondatabase/One-Pager-Key-Space-Partitioning-Stas-8e3a28a600a04a25a68523f42a170677

Prior art in other distributed systems is too broad to capture here: pretty much
any scale out storage system does something like this.

## Requirements

- Enable creating a large (for example, 16TiB) database without requiring dedicated
  pageserver nodes.
- Share read/write bandwidth costs for large databases across pageservers, as well
  as storage capacity, in order to avoid large capacity databases acting as I/O hotspots
  that disrupt service to other tenants.
- Our data distribution scheme should handle sparse/nonuniform keys well, since postgres
  does not write out a single contiguous ranges of page numbers.

_Note: the definition of 'large database' is arbitrary, but the lower bound is to ensure that a database
that a user might create on a current-gen enterprise SSD should also work well on
Neon. The upper bound is whatever postgres can handle: i.e. we must make sure that the
pageserver backend is not the limiting factor in the database size_.

## Non Goals

- Independently distributing timelines within the same tenant. If a tenant has many
  timelines, then sharding may be a less efficient mechanism for distributing load than
  sharing out timelines between pageservers.
- Distributing work in the LSN dimension: this RFC focuses on the Key dimension only,
  based on the idea that separate mechanisms will make sense for each dimension.

## Impacted Components

pageserver, control plane, postgres/smgr

## Terminology

**Key**: a postgres page number, qualified by relation. In the sense that the pageserver is a versioned key-value store,
the page number is the key in that store. `Key` is a literal data type in existing code.

**LSN dimension**: this just means the range of LSNs (history), when talking about the range
of keys and LSNs as a two dimensional space.

## Implementation

### Key sharding vs. LSN sharding

When we think of sharding across the two dimensional key/lsn space, this is an
opportunity to think about how the two dimensions differ:

- Sharding the key space distributes the _write_ workload of ingesting data
  and compacting. This work must be carefully managed so that exactly one
  node owns a given key.
- Sharding the LSN space distributes the _historical read_ workload. This work
  can be done by anyone without any special coordination, as long as they can
  see the remote index and layers.

The key sharding is the harder part, and also the more urgent one, to support larger
capacity databases. Because distributing historical LSN read work is a relatively
simpler problem that most users don't have, we defer it to future work. It is anticipated
that some quite simple P2P offload model will enable distributing work for historical
reads: a node which is low on space can call out to peer to ask it to download and
serve reads from a historical layer.

### Key mapping scheme

Having decided to focus on key sharding, we must next decide how we will map
keys to shards. It is proposed to use a "wide striping" approach, to obtain a good compromise
between data locality and avoiding entire large relations mapping to the same shard.

We will define two spaces:

- Key space: unsigned integer
- Shard space: integer from 0 to N-1, where we have N shards.

### Key -> Shard mapping

Keys are currently defined in the pageserver's getpage@lsn interface as follows:

```
pub struct Key {
    pub field1: u8,
    pub field2: u32,
    pub field3: u32,
    pub field4: u32,
    pub field5: u8,
    pub field6: u32,
}


fn rel_block_to_key(rel: RelTag, blknum: BlockNumber) -> Key {
    Key {
        field1: 0x00,
        field2: rel.spcnode,
        field3: rel.dbnode,
        field4: rel.relnode,
        field5: rel.forknum,
        field6: blknum,
    }
}
```

_Note: keys for relation metadata are ignored here, as this data will be mirrored to all
shards. For distribution purposes, we only care about user data keys_

The properties we want from our Key->Shard mapping are:

- Locality in `blknum`, such that adjacent `blknum` will usually map to
  the same stripe and consequently land on the same shard, even though the overall
  collection of blocks in a relation will be spread over many stripes and therefore
  many shards.
- Avoid the same blknum on different relations landing on the same stripe, so that
  with many small relations we do not end up aliasing data to the same stripe/shard.
- Avoid vulnerability to aliasing in the values of relation identity fields, such that
  if there are patterns in the value of `relnode`, these do not manifest as patterns
  in data placement.

To accomplish this, the blknum is used to select a stripe, and stripes are
assigned to shards in a pseudorandom order via a hash. The motivation for
pseudo-random distribution (rather than sequential mapping of stripe to shard)
is to avoid I/O hotspots when sequentially reading multiple relations: we don't want
all relations' stripes to touch pageservers in the same order.

To map a `Key` to a shard:

- Hash the `Key` field 4 (relNode).
- Divide field 6 (`blknum`) field by the stripe size in pages, and combine the
  hash of this with the hash from the previous step.
- The total hash modulo the shard count gives the shard holding this key.

Why don't we use the other fields in the Key?

- We ignore `forknum` for key mapping, because it distinguishes different classes of data
  in the same relation, and we would like to keep the data in a relation together.
- We would like to use spcNode and dbNode, but cannot. Postgres database creation operations can refer to an existing database as a template, such that the created
  database's blocks differ only by spcNode and dbNode from the original. To enable running
  this type of creation without cross-pageserver communication, we must ensure that these
  blocks map to the same shard -- we do this by excluding spcNode and dbNode from the hash.

### Data placement examples

For example, consider the extreme large databases cases of postgres data layout in a system with 8 shards
and a stripe size of 32k pages:

- A single large relation: `blknum` division will break the data up into 4096
  stripes, which will be scattered across the shards.
- 4096 relations of of 32k pages each: each relation will map to exactly one stripe,
  and that stripe will be placed according to the hash of the key fields 4. The
  data placement will be statistically uniform across shards.

Data placement will be more uneven on smaller databases:

- A tenant with 2 shards and 2 relations of one stripe size each: there is a 50% chance
  that both relations land on the same shard and no data lands on the other shard.
- A tenant with 8 shards and one relation of size 12 stripes: 4 shards will have double
  the data of the other four shards.

These uneven cases for small amounts of data do not matter, as long as the stripe size
is an order of magnitude smaller than the amount of data we are comfortable holding
in a single shard: if our system handles shard sizes up to 10-100GB, then it is not an issue if
a tenant has some shards with 256MB size and some shards with 512MB size, even though
the standard deviation of shard size within the tenant is very high. Our key mapping
scheme provides a statistical guarantee that as the tenant's overall data size increases,
uniformity of placement will improve.

### Important Types

#### `ShardIdentity`

Provides the information needed to know whether a particular key belongs
to a particular shard:

- Layout version
- Stripe size
- Shard count
- Shard index

This structure's size is constant. Note that if we had used a differnet key
mapping scheme such as consistent hashing with explicit hash ranges assigned
to each shard, then the ShardIdentity's size would grow with the shard count: the simpler
key mapping scheme used here enables a small fixed size ShardIdentity.

### Pageserver changes

#### Structural

Everywhere the Pageserver currently deals with Tenants, it will move to dealing with
`TenantShard`s, which are just a `Tenant` plus a `ShardIdentity` telling it which part
of the keyspace it owns. An un-sharded tenant is just a `TenantShard` whose `ShardIdentity`
covers the whole keyspace.

When the pageserver writes layers and index_part.json to remote storage, it must
include the shard index & count in the name, to avoid collisions (the count is
necessary for future-proofing: the count will vary in time). These keys
will also include a generation number: the [generation numbers](025-generation-numbers.md) system will work
exactly the same for TenantShards as it does for Tenants today: each shard will have
its own generation number.

#### Storage Format: Keys

For tenants with >1 shard, layer files implicitly become sparse: within the key
range described in the layer name, the layer file for a shard will only hold the
content relevant to stripes assigned to the shard.

For this reason, the LayerFileName within a tenant is no longer unique: different shards
may use the same LayerFileName to refer to different data. We may solve this simply
by including the shard number in the keys used for layers.

The shard number will be included as a prefix (as part of tenant ID), like this:

`pageserver/v1/tenants/<tenant_id>-<shard_number><shard_count>/timelines/<timeline id>/<layer file name>-<generation>`

`pageserver/v1/tenants/<tenant_id>-<shard_number><shard_count>/timelines/<timeline id>/index_part.json-<generation>`

Reasons for this particular format:

- Use of a prefix is convenient for implementation (no need to carry the shard ID everywhere
  we construct a layer file name), and enables efficient listing of index_parts within
  a particular shard-timeline prefix.
- Including the shard _count_ as well as shard number means that in future when we implement
  shard splitting, it will be possible for a parent shard and one of its children to write
  the same layer file without a name collision. For example, a parent shard 0_1 might split
  into two (0_2, 1_2), and in the process of splitting shard 0_2 could write a layer or index_part
  that is distinct from what shard 0_1 would have written at the same place.

In practice, we expect shard counts to be relatively small, so a `u8` will be sufficient,
and therefore the shard part of the path can be a fixed-length hex string like `{:02X}{:02X}`,
for example a single-shard tenant's prefix will be `0001`.

For backward compatibility, we may define a special `ShardIdentity` that has shard_count==0,
and use this as a cue to construct paths with no prefix at all.

#### Storage Format: Indices

In the phase 1 described in this RFC, shards only reference layers they write themselves. However,
when we implement shard splitting in future, it will be useful to enable shards to reference layers
written by other shards (specifically the parent shard during a split), so that shards don't
have to exhaustively copy all data into their own shard-prefixed keys.

To enable this, the `IndexPart` structure will be extended to store the (shard number, shard count)
tuple on each layer, such that it can construct paths for layers written by other shards. This
naturally raises the question of who "owns" such layers written by ancestral shards: this problem
will be addressed in phase 2.

For backward compatibility, any index entry without shard information will be assumed to be
in the legacy shardidentity.

#### WAL Ingest

In Phase 1, all shards will subscribe to the safekeeper to download WAL content. They will filter
it down to the pages relevant to their shard:

- For ordinary user data writes, only retain a write if it matches the ShardIdentity
- For metadata describing relations etc, all shards retain these writes.

The pageservers must somehow give the safekeeper correct feedback on remote_consistent_lsn:
one solution here is for the 0th shard to periodically peek at the IndexParts for all the other shards,
and have only the 0th shard populate remote_consistent_lsn. However, this is relatively
expensive: if the safekeeper can be made shard-aware then it could be taught to use
the max() of all shards' remote_consistent_lsns to decide when to trim the WAL.

#### Compaction/GC

No changes needed.

The pageserver doesn't have to do anything special during compaction
or GC. It is implicitly operating on the subset of keys that map to its ShardIdentity.
This will result in sparse layer files, containing keys only in the stripes that this
shard owns. Where optimizations currently exist in compaction for spotting "gaps" in
the key range, these should be updated to ignore gaps that are due to sharding, to
avoid spuriously splitting up layers ito stripe-sized pieces.

### Compute Endpoints

Compute endpoints will need to:

- Accept a vector of connection strings as part of their configuration from the control plane
- Route pageserver requests according to mapping the hash of key to the correct
  entry in the vector of connection strings.

Doing this in compute rather than routing requests via a single pageserver is
necessary to enable sharding tenants without adding latency from extra hops.

### Control Plane

Tenants, or _Projects_ in the control plane, will each own a set of TenantShards (this will
be 1 for small tenants). Logic for placement of tenant shards is just the same as the current logic for placing
tenants.

Tenant lifecycle operations like deletion will require fanning-out to all the shards
in the tenant. The same goes for timeline creation and deletion: a timeline should
not be considered created until it has been created in all shards.

#### Selectively enabling sharding for large tenants

Initially, we will explicitly enable sharding for large tenants only.

In future, this hint mechanism will become optional when we implement automatic
re-sharding of tenants.

## Future Phases

This section exists to indicate what will likely come next after this phase.

Phases 2a and 2b are amenable to execution in parallel.

### Phase 2a: WAL fan-out

**Problem**: when all shards consume the whole WAL, the network bandwidth used
for transmitting the WAL from safekeeper to pageservers is multiplied by a factor
of the shard count.

Network bandwidth is not our most pressing bottleneck, but it is likely to become
a problem if we set a modest shard count (~8) on a significant number of tenants,
especially as those larger tenants which we shard are also likely to have higher
write bandwidth than average.

### Phase 2b: Shard Splitting

**Problem**: the number of shards in a tenant is defined at creation time and cannot
be changed. This causes excessive sharding for most small tenants, and an upper
bound on scale for very large tenants.

To address this, a _splitting_ feature will later be added. One shard can split its
data into a number of children by doing a special compaction operation to generate
image layers broken up child-shard-wise, and then writing out an `index_part.json` for
each child. This will then require external coordination (by the control plane) to
safely attach these new child shards and then move them around to distribute work.
The opposite _merging_ operation can also be imagined, but is unlikely to be implemented:
once a Tenant has been sharded, the marginal efficiency benefit of merging is unlikely to justify
the risk/complexity of implementing such a rarely-encountered scenario.

### Phase N (future): distributed historical reads

**Problem**: while sharding based on key is good for handling changes in overall
database size, it is less suitable for spiky/unpredictable changes in the read
workload to historical layers. Sudden increases in historical reads could result
in sudden increases in local disk capacity required for a TenantShard.

Example: the extreme case of this would be to run a tenant for a year, then create branches
with ancestors at monthly intervals. This could lead to a sudden 12x inflation in
the on-disk capacity footprint of a TenantShard, since it would be serving reads
from all those disparate historical layers.

If we can respond fast enough, then key-sharding a tenant more finely can help with
this, but splitting may be a relatively expensive operation and the increased historical
read load may be transient.

A separate mechanism for handling heavy historical reads could be something like
a gossip mechanism for pageservers to communicate
about their workload, and then a getpageatlsn offload mechanism where one pageserver can
ask another to go read the necessary layers from remote storage to serve the read. This
requires relativly little coordination because it is read-only: any node can service any
read. All reads to a particular shard would still flow through one node, but the
disk capactity & I/O impact of servicing the read would be distributed.

## FAQ/Alternatives

### Why stripe the data, rather than using contiguous ranges of keyspace for each shard?

When a database is growing under a write workload, writes may predominantly hit the
end of the keyspace, creating a bandwidth hotspot on that shard. Similarly, if the user
is intensively re-writing a particular relation, if that relation lived in a particular
shard then it would not achieve our goal of distributing the write work across shards.

### Why not proxy read requests through one pageserver, so that endpoints don't have to change?

1. This would not achieve scale-out of network bandwidth: a busy tenant with a large
   database would still cause a load hotspot on the pageserver routing its read requests.
2. The additional hop through the "proxy" pageserver would add latency and overall
   resource cost (CPU, network bandwidth)

### Layer File Spreading: use one pageserver as the owner of a tenant, and have it spread out work on a per-layer basis to peers

In this model, there would be no explicit sharding of work, but the pageserver to which
a tenant is attached would not hold all layers on its disk: instead, it would call out
to peers to have them store some layers, and call out to those peers to request reads
in those layers.

This mechanism will work well for distributing work in the LSN dimension, but in the key
space dimension it has the major limitation of requiring one node to handle all
incoming writes, and compactions. Even if the write workload for a large database
fits in one pageserver, it will still be a hotspot and such tenants may still
de-facto require their own pageserver.

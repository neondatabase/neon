# Sharding Phase 1: Static Key-space Sharding

## Summary

To enable databases with sizes approaching the capacity of a pageserver's disk,
it is necessary to break up the storage for the database, or _shard_ it.

Sharding in general is a complex area.  This RFC aims to define a modest initial
capability that will permit creating large-capacity databases using a static configuration
defined at time of Tenant creation.

## Motivation

Currently, all data for a Tenant, including all its timelines, is stored on a single
pageserver.  The local storage required may be several times larger than the actual
database size, due to LSM write inflation.

If a database is larger than what one pageserver can hold, then it becomes impossible
for the pageserver to hold it in local storage, as it must do to provide service to
clients.

### Prior art

Numerous: sharding is a long-discussed feature for the pageserver.

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

*Note: the definition of 'large database' is arbitrary, but the lower bound is to ensure that a database
that a user might create on a current-gen enterprise SSD should also work well on
Neon.  The upper bound is whatever postgres can handle: i.e. we must make sure that the
pageserver backend is not the limiting factor in the database size*.

## Non Goals

- Independently distributing timelines within the same tenant.  If a tenant has many
  timelines, then sharding may be a less efficient mechanism for distributing load than
  sharing out timelines between pageservers.
- Distributing work in the LSN dimension: this RFC focuses on the Key dimension only,
  based on the idea that separate mechanisms will make sense for each dimension.

## Impacted Components

pageserver, control plane, safekeeper (optional)

## Terminology

**Key**: a postgres page number.  In the sense that the pageserver is a versioned key-value store,
the page number is the key in that store.

**LSN dimension**: this just means the range of LSNs (history), when talking about the range
of keys and LSNs as a two dimensional space.

## Implementation

### Key sharding vs. LSN sharding

When we think of sharding across the two dimensional key/lsn space, this is an
opportunity to think about how the two dimensions differ:
- Sharding the key space distributes the _write_ workload of ingesting data
  and compacting.  This work must be carefully managed so that exactly one
  node owns a given key.
- Sharding the LSN space distributes the _historical read_ workload.  This work
  can be done by anyone without any special coordination, as long as they can
  see the remote index and layers.

The key sharding is the harder part, and also the more urgent one, to support larger
capacity databases.  Because distributing historical LSN read work is a relatively
simpler problem that most users don't have, we defer it to future work.  It is anticipated
that some quite simple P2P offload model will enable distributing work for historical
reads: a node which is low on space can call out to peer to ask it to download and
serve reads from a historical layer.

### Key mapping scheme

Having decided to focus on key sharding, we must next decide how we will map
keys to shards.

It is proposed to use a "wide striping" approach, to obtain a good compromise
between data locality and avoiding entire large relations mapping to the same shard.

The mapping is quite simple:
- Define a stripe size, such as 256MiB.  Map this to a key count, such that a contiguous
  range of 256MiB keys would all fall into this stripe, i.e. divide by 8kiB to get 32k.
- Map a key to a stripe by integer division.
- Map a stripe to a shard by taking the shard index modulo the shard count.

This scheme will achieve a good balance as long as there is no aliasing of the keys
to the stripe width.  In the example above, if someone had 4 shards and wrote
keys that were all 4*32k apart, they would all map to the same shard.  However, we do
not have to worry about this, since end users do not control page numbers: as long as
we do not pick stripe sizes that map to any problematic postgres behaviors, we'll be fine.

### Important Types

#### `ShardMap`

Provides all the information needed to route a request for a particular
key to the correct pageserver:
- Stripe size
- Shard count
- Address of the pageserver hosting each shard

This structure's size is linear with the number of shards.

#### `ShardIdentity`

Provides the information needed to know whether a particular key belongs
to a particular shard:
- Stripe size
- Shard count
- Shard index

This structure's size is constant.

### Pageserver changes

Everywhere the Pageserver currently deals with Tenants, it will move to dealing with
TenantShards, which are just a `Tenant` plus a `ShardIdentity` telling it which part
of the keyspace it owns.

When the pageserver subscribes to a safekeeper for WAL updates, it must provide
its `ShardIdentity` to receive the relevant subset of the WAL.

When the pageserver writes layers and index_part.json to remote storage, it must
include the shard index & count in the name, to avoid collisions (the count is
necessary for future-proofing: the count will vary in time).  These keys
will also include a generation number: the [generation numbers](025-generation-numbers.md) system will work
exactly the same for TenantShards as it does for Tenants today: each shard will have
its own generation number.

The pageserver doesn't have to do anything special during ingestion, compaction
or GC.  It is implicitly operating on the subset of keys that map to its ShardIdentity.
This will result in sparse layer files, containing keys only in the stripes that this
shard owns.  Where optimizations currently exist in compaction for spotting "gaps" in
the key range, these should be updated to ignore gaps that are due to sharding, to
avoid spuriously splitting up layers ito stripe-sized pieces.

### Pageserver Controller changes

The pageserver controller is a new component, which is responsible for abstracting
away the business of managing individual tenant placement on pagservers.  It will
also act as the abstraction on top of sharding, so that the control plane continue
to see a Tenant as a single object, even though the reality is that it is many
TenantShards.

For the rest of this RFC, think of the Pageserver Controller as a component of
the control plane.  The actual implementation is beyond the scope of this RFC
and will be described in more detail elsewhere.

### Safekeeper changes

The safekeeper's API for subscribing to a WAL will be extended to enable callers
to provide a `ShardIdentity`.  In this mode it will only send WAL entries that
fall within the keyspace belonging to the shard, and WAL entries that are to
be mirrored to all shards.

Metadata updates describing databases+relations are mirrored to
all shards, and other WAL messages are only provided to the shard
that owns the key being updated.  For any operation that updates multiple
keys, it will be provided to all the shards whose key ranges intersect with
one or more of the keys referenced in the WAL message.

### Pageserver Controller

### Endpoints

Compute endpoints will need to:
- Accept a ShardMap as part of their configuration from the control plane
- Route pageserver requests according to that ShardMap

### Control Plane

#### Publishing ShardMap updates

The control plane will provide an API for the pageserver controller to publish updates
to the ShardMap for a tenant.  When such an update is provided, it will be used to
update the configuration of any endpoints currently active for the tenant.

The ShardMap will be opaque to the Control Plane: it doesn't need to do anything with it
other than storing and passing on to endpoints.

#### Attaching via the Pageserver Controller

The Control Plane will issue attach/create API calls to the pageserver controller
instead of directly to pageservers.  This will relieve the control plane of the need
to know about sharding.

#### Enabling sharding for large tenants

When a Tenant is created, it is up to the control plane to provide a hint to
the pageserver about how large it will be.  This may be implemented as a service tier,
where users creating very large databases would be onboarded to the tier, and then
the Tenants they create would be created with a larger number of shards.  For the
general population of users we should continue to use 1 shard by default.

## Next Steps

Clearly, the mechanism described in this RFC has substantial limitations:
- A) the number of shards in a tenant is defined at creation time.
- B) data is not distributed across the LSN dimension

To address `A`, a _splitting_ feature will later be added.  One shard can split its
data into a number of children by doing a special compaction operation to generate
image layers broken up child-shard-wise, and then writing out an index_part.json for
each child.  This will then require coordination with the pageserver controller to
safely attach these new child shards and then move them around to distribute work.
The opposite _merging_ operation can also be imagined, but is unlikely to be implemented:
once a Tenant has been sharded, there is little value in merging it again.

To address `B`, it is envisaged to have some gossip mechanism for pageservers to communicate
about their workload, and then a getpageatlsn offload mechanism where one pageserver can
ask another to go read the necessary layers from remote storage to serve the read.  This
requires relativly little coordination because it is read-only: any node can service any
read.  All reads to a particular shard would still flow through one node, but the
disk capactity & I/O impact of servicing the read would be distributed.

## FAQ/Alternatives

### Why stripe the data, rather than using contiguous ranges of keyspace for each shard?

When a database is growing under a write workload, writes may predominantly hit the
end of the keyspace, creating a bandwidth hotspot on that shard.  Similarly, if the user
is intensively re-writing a particular relation, if that relation lived in a particular
shard then it would not achieve our goal of distributing the write work across shards.

### Why not proxy read requests through one pageserver, so that endpoints don't have to change?

Two reasons:
1. This would not achieve scale-out of network bandwidth: a busy tenant with a large
   database would still cause a load hotspot on the pageserver routing its read requests. 
2. Implementing a proxy model as a stop-gap would not be a cheap option, because
   it requires making pageservers aware of their peers, and adding synchronisation to
   keep pageservers aware of their peers as they come and go.
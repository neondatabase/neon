# Shard splitting

## Summary

This RFC describes a new pageserver API for splitting an existing tenant shard into
multiple shards, and describes how to use this API to safely increase the total
shard count of a tenant.

## Motivation

In the [sharding RFC](031-sharding-static.md), a mechanism was introduced to scale
tenants beyond the capacity of a single pageserver by breaking up the key space
into stripes, and distributing these stripes across many pageservers. However,
the shard count was defined once at tenant creation time and not varied thereafter.

In practice, the expected size of a database is rarely known at creation time, and
it is inefficient to enable sharding for very small tenants: we need to be
able to create a tenant with a small number of shards (such as 1), and later expand
when it becomes clear that the tenant has grown in size to a point where sharding
is beneficial.

### Prior art

Many distributed systems have the problem of choosing how many shards to create for
tenants that do not specify an expected size up-front. There are a couple of general
approaches:

- Write to a key space in order, and start a new shard when the highest key advances
  past some point. This doesn't work well for Neon, because we write to our key space
  in many different contiguous ranges (per relation), rather than in one contiguous
  range. To adapt to this kind of model, we would need a sharding scheme where each
  relation had its own range of shards, which would be inefficient for the common
  case of databases with many small relations.
- Monitor the system, and automatically re-shard at some size threshold. For
  example in Ceph, the [pg_autoscaler](https://github.com/ceph/ceph/blob/49c27499af4ee9a90f69fcc6bf3597999d6efc7b/src/pybind/mgr/pg_autoscaler/module.py)
  component monitors the size of each RADOS Pool, and adjusts the number of Placement
  Groups (Ceph's shard equivalent).

## Requirements

- A configurable capacity limit per-shard is enforced.
- Changes in shard count do not interrupt service beyond requiring postgres
  to reconnect (i.e. milliseconds).
- Human being does not have to choose shard count

## Non Goals

- Shard splitting is always a tenant-global operation: we will not enable splitting
  one shard while leaving others intact.
- The inverse operation (shard merging) is not described in this RFC. This is a lower
  priority than splitting, because databases grow more often than they shrink, and
  a database with many shards will still work properly if the stored data shrinks, just
  with slightly more overhead (e.g. redundant WAL replication)
- Shard splitting is only initiated based on capacity bounds, not load. Splitting
  a tenant based on load will make sense for some medium-capacity, high-load workloads,
  but is more complex to reason about and likely is not desirable until we have
  shard merging to reduce the shard count again if the database becomes less busy.

## Impacted Components

pageserver, storage controller

(the _storage controller_ is the evolution of what was called `attachment_service` in our test environment)

## Terminology

**Parent** shards are the shards that exist before a split. **Child** shards are
the new shards created during a split.

**Shard** is synonymous with _tenant shard_.

**Shard Index** is the 2-tuple of shard number and shard count, written in
paths as {:02x}{:02x}, e.g. `0001`.

## Background

In the implementation section, a couple of existing aspects of sharding are important
to remember:

- Shard identifiers contain the shard number and count, so that "shard 0 of 1" (`0001`) is
  a distinct shard from "shard 0 of 2" (`0002`). This is the case in key paths, local
  storage paths, and remote index metadata.
- Remote layer file paths contain the shard index of the shard that created them, and
  remote indices contain the same index to enable building the layer file path. A shard's
  index may reference layers that were created by another shard.
- Local tenant shard directories include the shard index. All layers downloaded by
  a tenant shard are stored in this shard-prefixed path, even if those layers were
  initially created by another shard: tenant shards do not read and write one anothers'
  paths.
- The `Tenant` pageserver type represents one tenant _shard_, not the whole tenant.
  This is for historical reasons and will be cleaned up in future, but the existing
  name is used here to help comprehension when reading code.

## Implementation

Note: this section focuses on the correctness of the core split process. This will
be fairly inefficient in a naive implementation, and several important optimizations
are described in a later section.

There are broadly two parts to the implementation:

1. The pageserver split API, which splits one shard on one pageserver
2. The overall tenant split proccess which is coordinated by the storage controller,
   and calls into the pageserver split API as needed.

### Pageserver Split API

The pageserver will expose a new API endpoint at `/v1/tenant/:tenant_shard_id/shard_split`
that takes the new total shard count in the body.

The pageserver split API operates on one tenant shard, on one pageserver. External
coordination is required to use it safely, this is described in the later
'Split procedure' section.

#### Preparation

First identify the shard indices for the new child shards. These are deterministic,
calculated from the parent shard's index, and the number of children being created (this
is an input to the API, and validated to be a power of two). In a trivial example, splitting
0001 in two always results in 0002 and 0102.

Child shard indices are chosen such that the childrens' parts of the keyspace will
be subsets of the parent's parts of the keyspace.

#### Step 1: write new remote indices

In remote storage, splitting is very simple: we may just write new index_part.json
objects for each child shard, containing exactly the same layers as the parent shard.

The children will have more data than they need, but this avoids any exhausive
re-writing or copying of layer files.

The index key path includes a generation number: the parent shard's current
attached generation number will also be used for the child shards' indices. This
makes the operation safely retryable: if everything crashes and restarts, we may
call the split API again on the parent shard, and the result will be some new remote
indices for the child shards, under a higher generation number.

#### Step 2: start new `Tenant` objects

A new `Tenant` object may be instantiated for each child shard, while the parent
shard still exists. When calling the tenant_spawn function for this object,
the remote index from step 1 will be read, and the child shard will start
to ingest WAL to catch up from whatever was in the remote storage at step 1.

We now wait for child shards' WAL ingestion to catch up with the parent shard,
so that we can safely tear down the parent shard without risking an availability
gap to clients reading recent LSNs.

#### Step 3: tear down parent `Tenant` object

Once child shards are running and have caught up with WAL ingest, we no longer
need the parent shard. Note that clients may still be using it -- when we
shut it down, any page_service handlers will also shut down, causing clients
to disconnect. When the client reconnects, it will re-lookup the tenant,
and hit the child shard instead of the parent (shard lookup from page_service
should bias toward higher ShardCount shards).

Note that at this stage the page service client has not yet been notified of
any split. In the trivial single split example:

- Shard 0001 is gone: Tenant object torn down
- Shards 0002 and 0102 are running on the same pageserver where Shard 0001 used to live.
- Clients will continue to connect to that server thinking that shard 0001 is there,
  and all requests will work, because any key that was in shard 0001 is definitely
  available in either shard 0002 or shard 0102.
- Eventually, the storage controller (not the pageserver) will decide to migrate
  some child shards away: at that point it will do a live migration, ensuring
  that the client has an updated configuration before it detaches anything
  from the original server.

#### Complete

When we send a 200 response to the split request, we are promising the caller:

- That the child shards are persistent in remote storage
- That the parent shard has been shut down

This enables the caller to proceed with the overall shard split operation, which
may involve other shards on other pageservers.

### Storage Controller Split procedure

Splitting a tenant requires calling the pageserver split API, and tracking
enough state to ensure recovery + completion in the event of any component (pageserver
or storage controller) crashing (or request timing out) during the split.

1. call the split API on all existing shards. Ensure that the resulting
   child shards are pinned to their pageservers until _all_ the split calls are done.
   This pinning may be implemented as a "split bit" on the tenant shards, that
   blocks any migrations, and also acts as a sign that if we restart, we must go
   through some recovery steps to resume the split.
2. Once all the split calls are done, we may unpin the child shards (clear
   the split bit). The split is now complete: subsequent steps are just migrations,
   not strictly part of the split.
3. Try to schedule new pageserver locations for the child shards, using
   a soft anti-affinity constraint to place shards from the same tenant onto different
   pageservers.

Updating computes about the new shard count is not necessary until we migrate
any of the child shards away from the parent's location.

### Recovering from failures

#### Rolling back an incomplete split

An incomplete shard split may be rolled back quite simply, by attaching the parent shards to pageservers,
and detaching child shards. This will lose any WAL ingested into the children after the parents
were detached earlier, but the parents will catch up.

No special pageserver API is needed for this. From the storage controllers point of view, the
procedure is:

1. For all parent shards in the tenant, ensure they are attached
2. For all child shards, ensure they are not attached
3. Drop child shards from the storage controller's database, and clear the split bit on the parent shards.

Any remote storage content for child shards is left behind. This is similar to other cases where
we may leave garbage objects in S3 (e.g. when we upload a layer but crash before uploading an
index that references it). Future online scrub/cleanup functionality can remove these objects, or
they will be removed when the tenant is deleted, as tenant deletion lists all objects in the prefix,
which would include any child shards that were rolled back.

If any timelines had been created on child shards, they will be lost when rolling back. To mitigate
this, we will **block timeline creation during splitting**, so that we can safely roll back until
the split is complete, without risking losing timelines.

Rolling back an incomplete split will happen automatically if a split fails due to some fatal
reason, and will not be accessible via an API:

- A pageserver fails to complete its split API request after too many retries
- A pageserver returns a fatal unexpected error such as 400 or 500
- The storage controller database returns a non-retryable error
- Some internal invariant is violated in the storage controller split code

#### Rolling back a complete split

A complete shard split may be rolled back similarly to an incomplete split, with the following
modifications:

- The parent shards will no longer exist in the storage controller database, so these must
  be re-synthesized somehow: the hard part of this is figuring the parent shards' generations. This
  may be accomplished either by probing in S3, or by retaining some tombstone state for deleted
  shards in the storage controller database.
- Any timelines that were created after the split complete will disappear when rolling back
  to the tenant shards. For this reason, rolling back after a complete split should only
  be done due to serious issues where loss of recently created timelines is acceptable, or
  in cases where we have confirmed that no timelines were created in the intervening period.
- Parent shards' layers must not have been deleted: this property will come "for free" when
  we first roll out sharding, by simply not implementing deletion of parent layers after
  a split. When we do implement such deletion (see "Cleaning up parent-shard layers" in the
  Optimizations section), it should apply a TTL to layers such that we have a
  defined walltime window in which rollback will be possible.

The storage controller will expose an API for rolling back a complete split, for use
in the field if we encounter some critical bug with a post-split tenant.

#### Retrying API calls during Pageserver Restart

When a pageserver restarts during a split API call, it may witness on-disk content for both parent and
child shards from an ongoing split. This does not intrinsically break anything, and the
pageserver may include all these shards in its `/re-attach` request to the storage controller.

In order to support such restarts, it is important that the storage controller stores
persistent records of each child shard before it calls into a pageserver, as these child shards
may require generation increments via a `/re-attach` request.

The pageserver restart will also result in a failed API call from the storage controller's point
of view. Recall that if _any_ pageserver fails to split, the overall split operation may not
complete, and all shards must remain pinned to their current pageserver locations until the
split is done.

The pageserver API calls during splitting will retry on transient errors, so that
short availability gaps do not result in a failure of the overall operation. The
split in progress will be automatically rolled back if the threshold for API
retries is reached (e.g. if a pageserver stays offline for longer than a typical
restart).

#### Rollback on Storage Controller Restart

On startup, the storage controller will inspect the split bit for tenant shards that
it loads from the database. If any splits are in progress:

- Database content will be reverted to the parent shards
- Child shards will be dropped from memory
- The parent and child shards will be included in the general startup reconciliation that
  the storage controller does: any child shards will be detached from pageservers because
  they don't exist in the storage controller's expected set of shards, and parent shards
  will be attached if they aren't already.

#### Storage controller API request failures/retries

The split request handler will implement idempotency: if the [`Tenant`] requested to split
doesn't exist, we will check for the would-be child shards, and if they already exist,
we consider the request complete.

If a request is retried while the original request is still underway, then the split
request handler will notice an InProgress marker in TenantManager, and return 503
to encourage the client to backoff/retry. This is the same as the general pageserver
API handling for calls that try to act on an InProgress shard.

#### Compute start/restart during a split

If a compute starts up during split, it will be configured with the old sharding
configuration. This will work for reads irrespective of the progress of the split
as long as no child hards have been migrated away from their original location, and
this is guaranteed in the split procedure (see earlier section).

#### Pageserver fails permanently during a split

If a pageserver permanently fails (i.e. the storage controller availability state for it
goes to Offline) while a split is in progress, the splitting operation will roll back, and
during the roll back it will skip any API calls to the offline pageserver. If the offline
pageserver becomes available again, any stale locations will be cleaned up via the normal reconciliation process (the `/re-attach` API).

### Handling secondary locations

For correctness, it is not necessary to split secondary locations. We can simply detach
the secondary locations for parent shards, and then attach new secondary locations
for child shards.

Clearly this is not optimal, as it will result in re-downloads of layer files that
were already present on disk. See "Splitting secondary locations"

### Conditions to trigger a split

The pageserver will expose a new API for reporting on shards that are candidates
for split: this will return a top-N report of the largest tenant shards by
physical size (remote size). This should exclude any tenants that are already
at the maximum configured shard count.

The API would look something like:
`/v1/top_n_tenant?shard_count_lt=8&sort_by=resident_size`

The storage controller will poll that API across all pageservers it manages at some appropriate interval (e.g. 60 seconds).

A split operation will be started when the tenant exceeds some threshold. This threshold
should be _less than_ how large we actually want shards to be, perhaps much less. That's to
minimize the amount of work involved in splitting -- if we want 100GiB shards, we shouldn't
wait for a tenant to exceed 100GiB before we split anything. Some data analysis of existing
tenant size distribution may be useful here: if we can make a statement like "usually, if
a tenant has exceeded 20GiB they're probably going to exceed 100GiB later", then we might
make our policy to split a tenant at 20GiB.

The finest split we can do is by factors of two, but we can do higher-cardinality splits
too, and this will help to reduce the overhead of repeatedly re-splitting a tenant
as it grows. An example of a very simple heuristic for early deployment of the splitting
feature would be: "Split tenants into 8 shards when their physical size exceeds 64GiB": that
would give us two kinds of tenant (1 shard and 8 shards), and the confidence that once we had
split a tenant, it will not need re-splitting soon after.

## Optimizations

### Flush parent shard to remote storage during split

Any data that is in WAL but not remote storage at time of split will need
to be replayed by child shards when they start for the first time. To minimize
this work, we may flush the parent shard to remote storage before writing the
remote indices for child shards.

It is important that this flush is subject to some time bounds: we may be splitting
in response to a surge of write ingest, so it may be time-critical to split. A
few seconds to flush latest data should be sufficient to optimize common cases without
running the risk of holding up a split for a harmful length of time when a parent
shard is being written heavily. If the flush doesn't complete in time, we may proceed
to shut down the parent shard and carry on with the split.

### Hard linking parent layers into child shard directories

Before we start the Tenant objects for child shards, we may pre-populate their
local storage directories with hard links to the layer files already present
in the parent shard's local directory. When the child shard starts and downloads
its remote index, it will find all those layer files already present on local disk.

This avoids wasting download capacity and makes splitting faster, but more importantly
it avoids taking up a factor of N more disk space when splitting 1 shard into N.

This mechanism will work well in typical flows where shards are migrated away
promptly after a split, but for the general case including what happens when
layers are evicted and re-downloaded after a split, see the 'Proactive compaction'
section below.

### Filtering during compaction

Compaction, especially image layer generation, should skip any keys that are
present in a shard's layer files, but do not match the shard's ShardIdentity's
is_key_local() check. This avoids carrying around data for longer than necessary
in post-split compactions.

This was already implemented in https://github.com/neondatabase/neon/pull/6246

### Proactive compaction

In remote storage, there is little reason to rewrite any data on a shard split:
all the children can reference parent layers via the very cheap write of the child
index_part.json.

In local storage, things are more nuanced. During the initial split there is no
capacity cost to duplicating parent layers, if we implement the hard linking
optimization described above. However, as soon as any layers are evicted from
local disk and re-downloaded, the downloaded layers will not be hard-links any more:
they'll have real capacity footprint. That isn't a problem if we migrate child shards
away from the parent node swiftly, but it risks a significant over-use of local disk
space if we do not.

For example, if we did an 8-way split of a shard, and then _didn't_ migrate 7 of
the shards elsewhere, then churned all the layers in all the shards via eviction,
then we would blow up the storage capacity used on the node by 8x. If we're splitting
a 100GB shard, that could take the pageserver to the point of exhausting disk space.

To avoid this scenario, we could implement a special compaction mode where we just
read historic layers, drop unwanted keys, and write back the layer file. This
is pretty expensive, but useful if we have split a large shard and are not going to
migrate the child shards away.

The heuristic conditions for triggering such a compaction are:

- A) eviction plus time: if a child shard
  has existed for more than a time threshold, and has been requested to perform at least one eviction, then it becomes urgent for this child shard to execute a proactive compaction to reduce its storage footprint, at the cost of I/O load.
- B) resident size plus time: we may inspect the resident layers and calculate how
  many of them include the overhead of storing pre-split keys. After some time
  threshold (different to the one in case A) we still have such layers occupying
  local disk space, then we should proactively compact them.

### Cleaning up parent-shard layers

It is functionally harmless to leave parent shard layers in remote storage indefinitely.
They would be cleaned up in the event of the tenant's deletion.

As an optimization to avoid leaking remote storage capacity (which costs money), we may
lazily clean up parent shard layers once no child shards reference them.

This may be done _very_ lazily: e.g. check every PITR interval. The cleanup procedure is:

- list all the key prefixes beginning with the tenant ID, and select those shard prefixes
  which do not belong to the most-recently-split set of shards (_ancestral shards_, i.e. `shard*count < max(shard_count) over all shards)`, and those shard prefixes which do have the latest shard count (_current shards_)
- If there are no _ancestral shard_ prefixes found, we have nothing to clean up and
  may drop out now.
- find the latest-generation index for each _current shard_, read all and accumulate the set of layers belonging to ancestral shards referenced by these indices.
- for all ancestral shards, list objects in the prefix and delete any layer which was not
  referenced by a current shard.

If this cleanup is scheduled for 1-2 PITR periods after the split, there is a good chance that child shards will have written their own image layers covering the whole keyspace, such that all parent shard layers will be deletable.

The cleanup may be done by the scrubber (external process), or we may choose to have
the zeroth shard in the latest generation do the work -- there is no obstacle to one shard
reading the other shard's indices at runtime, and we do not require visibility of the
latest index writes.

Cleanup should be artificially delayed by some period (for example 24 hours) to ensure
that we retain the option to roll back a split in case of bugs.

### Splitting secondary locations

We may implement a pageserver API similar to the main splitting API, which does a simpler
operation for secondary locations: it would not write anything to S3, instead it would simply
create the child shard directory on local disk, hard link in directories from the parent,
and set up the in memory (TenantSlot) state for the children.

Similar to attached locations, a subset of secondary locations will probably need re-locating
after the split is complete, to avoid leaving multiple child shards on the same pageservers,
where they may use excessive space for the tenant.

## FAQ/Alternatives

### What should the thresholds be set to?

Shard size limit: the pre-sharding default capacity quota for databases was 200GiB, so this could be a starting point for the per-shard size limit.

Max shard count:

- The safekeeper overhead to sharding is currently O(N) network bandwidth because
  the un-filtered WAL is sent to all shards. To avoid this growing out of control,
  a limit of 8 shards should be temporarily imposed until WAL filtering is implemented
  on the safekeeper.
- there is also little benefit to increasing the shard count beyond the number
  of pageservers in a region.

### Is it worth just rewriting all the data during a split to simplify reasoning about space?

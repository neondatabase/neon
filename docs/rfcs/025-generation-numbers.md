# Pageserver generation numbers for safe failover & tenant migration

## Summary

A scheme of logical "generation numbers" for pageservers and their attachments is proposed, along with
changes to the remote storage format to include these generation numbers in S3 keys.

Using the control plane as the issuer of these generation numbers enables strong anti-split-brain
properties in the pageserver cluster without implementing a consensus mechanism directly
in the pageservers.

## Motivation

Currently, the pageserver's remote storage format does not provide a mechanism for addressing
split brain conditions that may happen when replacing a node during failover or when migrating
a tenant from one pageserver to another. From a remote storage perspective, a split brain condition
occurs whenever two nodes both think they have the same tenant attached, and both can write to S3. This
can happen in the case of a network partition, pathologically long delays (e.g. suspended VM), or software
bugs.

This blocks robust implementation of failover from unresponsive pageservers, due to the risk that
the unresponsive pageserver is still writing to S3.

### Prior art

- 020-pageserver-s3-coordination.md
- 023-the-state-of-pageserver-tenant-relocation.md
- https://www.notion.so/neondatabase/Proposal-Pageserver-MVCC-S3-Storage-8a424c0c7ec5459e89d3e3f00e87657c

This RFC has broad similarities to the proposal to implement a MVCC scheme in
S3 object names, but this RFC avoids a general purpose transaction scheme in
favour of more specialized "generations" that work like a transaction ID that
always has the same lifetime as a pageserver process and/or tenant attachment.

## Requirements

- Accommodate storage backends with no atomic or fencing capability (i.e. work within
  S3's limitation that there are no atomics and clients can't be fenced)
- Don't depend on any STONITH or node fencing in the compute layer (i.e. we will not
  assume that we can reliably kill and EC2 instance and have it die)
- Enable per-tenant granularity migration/failover, so that the workload after a failure
  is spread across a number of peers, rather than monolithically moving all load from
  a particular server to some other server (we do not rule out the latter case, but should
  not constrain ourselves to it).

## Design Tenets

These are not requirements, but are ideas that guide the following design:

- Avoid implementing another consensus system: we already have a strongly consistent
  database in the control plane that can do atomic operations where needed, and we also
  have a Paxos implementation in the safekeeper.
- Avoiding locking in to specific models of how failover will work (e.g. do not assume that
  all the tenants on a pageserver will fail over as a unit).
- Avoid doing synchronization that scales with the number of tenants, unless absolutely
  necessary.
- Be strictly correct when it comes to data integrity. Occasional failures of availability
  are tolerable, occasional data loss is not.

## Non Goals

The changes in this RFC intentionally isolate the design decision of how to define
logical generations IDs and object storage format in a way that is somewhat flexible with
respect to how actual orchestration of failover works.

This RFC intentionally does not cover:

- Failure detection
- Orchestration of failover
- Standby modes to keep data ready for fast migration
- Intentional multi-writer operation on tenants (multi-writer scenarios are assumed to be transient split-brain situations).
- Sharding.

## Impacted Components

pageserver, control plane, safekeeper (optional)

## Proposed implementation

### Summary

- **Generation numbers** are introduced for pageserver node lifetimes tenant attachments
- **Object keys are suffixed** with the generation numbers
- **Safety in split brain for multiple nodes running with
  the same node ID** is provided by the pageserver node generation in the object key: the concurrent nodes
  will not write to the same key.
- **Safety in split brain situations for multiply-attached tenants** is provided by the
  tenant attach generation in the object key: the competing pageservers will not
  try to write to the same keys.
- **The control plane is used to issue generation numbers** to avoid the need for
  a built-in consensus system in the pageserver, although this could in principle
  be changed without changing the storage format.
- **The safekeeper may refuse RPCs from zombie pageservers** via the node generation number.
- **Deletions are deferred via deletion lists** with occasional synchronization
  to the control plane to ensure safety.

### Generation numbers

Two logical entities will get new "generation numbers", which are monotonically increasing
integers with a global guarantee that they will not be re-used:

- Pageservers: a generation ID is acquired at startup and lasts until the process ends or another
  process with the same node ID starts.
- Tenant attachments: for a given tenant, each time its attachment is changed, a per-tenant generation
  number increases.

This provides some important invariants:

- If there are multiple pageservers running with the same node ID, they are guaranteed to have
  a different generation ID.
- If there are multiple pageservers running with the same node ID, we may unambiguously know which
  of them "wins" by picking the higher generation number.
- If two pageservers have the same tenant attached, they are guaranteed to have different attachment
  generation numbers.

The node generation number defines a global "in" definition for pageserver nodes: a node whose
generation matches the generation the control plane most recently issued is a member of the cluster
in good standing. Any node with any older generation ID is not.

Distinction between generation numbers and Node ID:

- The purpose of a generation ID is to provide a stronger guarantee of uniqueness than a Node ID.
- Generation ID is guaranteed to be globally unique across space and time. Node ID is not unique
  in the event of starting a replacement node after an original node is network partitioned, or
  in the event of a human error starting a replacement node that re-uses a node ID.
- Node ID is written to disk in a pageserver's configuration, whereas generation number is
  received at runtime just after startup.
- The two concepts could be collapsed into one if we used ephemeral node IDs that were only ever used one time,
  but this makes it harder to efficiently handle the case of the same physical node restarting, where we may want
  to retain the same attachments as it had before restart, without having to map them to a different node ID.
- We may in future run the pageserver in a different cluster runtime environment (e.g. k8s) where
  guaranteeing that the cluster manager (e.g. k8s) doesn't run two concurrent instances with the same
  node ID is much harder.

#### Why use per-tenant _and_ per-node generation numbers?

The essential generation number is the tenant attachment number: this alone would be sufficient
to implement safe migration and failover. However, it would also require that all such events
carry an O(tenant_count) cost, or to have a global sequence number for all changes to attachments (which
would imply synchronization between all attachment changes): the pageserver generation number
is included to avoid writing this kind of system design constraint into the storage format.

The pageserver generation number acts as an optimization and also provides flexibility in future designs:

- if we wanted to have pageservers do their own HA in future without control plane in the loop on a per-tenant basis,
  then node generation numbers make that possible.
- the pageserver generation number also enables future storage of per-pageserver data in S3 (e.g.
  housekeeping type content) that may not be within the scope of a tenant attachment.
- pageserver generation numbers enable building fencing into any network protocol (for example
  communication with safekeepers) to refuse to communicate with stale/partitioned nodes.

The tenant attachment generation and node generation are mostly independent, but have one key
coupling:

- Control plane must not "steal" a tenant from a node (by attaching to another node without detaching
  from the first node) without also incrementing the original node's generation number, which
  effectively marks the original node as dead. (doing a "clean" transfer by detaching
  from one node before attaching to another does not require a change in node generation number).
- This enables nodes to trust that if their generation ID is still current, then all their
  attachments are as well.

### Object Key Changes

All object keys (layer objects and index objects) will have a suffix as follows:

```
   <node id>-<attachment generation>-<node generation>
```

To avoid unreasonably long keys, we may make some reasonable assumptions about max lengths
of these integers:

- 32 bit attachment generation (this easily accommodates reasonable worst case scenarios like
  a node restarting every minute for 5 years)
- 16 bit node ID (this doesn't have to accommodate all pageservers ever, just all the pageservers
  that act together in a logical cluster)
- 32 bit node generation (as with attachment generation, this can handle even very frequent restarts)

So an example suffix for generation 1 of node 0 might look like:

```
   00000001-0000-00000001
```

This suffix provides a lot of flexibility in how we may handle migration/restarts/failover
at runtime:

- We can replace a pageserver with another physical node having the same pageserver ID, and
  the pageserver ID in the name will protect from conflicting writes. This applies even if we
  have no robust way to terminate a partitioned pageserver: the older-generation node may
  continue writing objects under their own generation number, but those will not conflict
  with objects written by the newer generation pageserver.
- Two pageservers may have the same tenant attached without risking stepping on each others
  writes.
- Ultimately, pageservers might self-coordinate such that a single "attachment" refers to
  multiple pageservers, and they may internally do HA without troubling the control
  plane to do new attachments, as the node generation makes this safe.

### Deletion Changes

While writes are de-conflicted by writers always using their own generation number in the key,
deletions are slightly more challenging: if a pageserver A is isolated, and the true active node is
pageserver B, then it is dangerous for A to do any object deletions, even of objects that it wrote
itself, because pageserver's B metadata might reference those objects.

To solve this without doing excessive synchronization, deletions are accumulated in deletion lists
(also known as deadlists, or garbage lists), to be executed later. The actual execution of one of
these lists has two preconditions:

- That the executing node has written out the metadata index file since it decided to do
  the deletion (i.e. no to-be-deleted files are referenced by the index)
- That after writing that index file, the deleting node has confirmed that it is still the latest
  generation (i.e. none of its attached tenants have been re-attached to another node).

The order of operations for correctness is:

1. Decide to delete one or more objects
2. Write updated metadata (this means that if we are still the rightful leader, any subsequent leader will
   read metadata that does not reference the about-to-be-deleted object).
3. Confirm we hold a non-stale generation (this validates the assumption in step 2, and taken
   together, these provide a guarantee that no future leader will attempt to read the objects
   we are about to delete).
4. Actually delete the objects

Because step 3 puts load on the control plane, deletion lists should be executed very lazily: write
them out to S3 and then execute them as a low priority batch operation in the background.

Deletion lists have an a couple of additional benefits:

- efficiency: accumulate enough objects
  to populate plural DeleteObjects requests, rather than issuing many smaller `DeleteObject` requests.
- read replicas: if some other node is reading data in S3 via some older metadata, the longer we delay
  deletions in a deletion list, the less up-to-date we require that read replica to be when serving
  reads for a historical LSN (i.e. for historical reads, leaving some stale objects around is
  a good thing). In practice this race is relatively short, just the duration of an in-flight read operation
  that might be traversing some slightly stale metadata.

There is some flexibility in exactly how deletion should work, but a simple implementation would look
like this:

- All Tenant objects hold a buffer of object keys to delete, which is flushed to the next step whenever
  they write their index file (i.e. step 2 in the steps above)
- On flush of the per-tenant buffer, keys to delete are gathered into a shared DeletionAccumulator,
  which writes out deletion lists to S3 in batches of some size chosen for efficiency.
- A background task wakes up and processes deletion lists. Before executing the actual deletion, it must call
  into the control plane (step 3 in the steps above).

Deletion lists in S3 may _not_ be executed as-is by future writers for the tenant: they
are only valid within a particular node generation. Future writers may either drop the deletion
list and let orphan object cleanup handle the objects (see below), or they may reconcile the
deletion objects with latest metadata to ensure all victim objects are really not referenced by
latest metadata.

### WAL trim changes

The WAL may be trimmed by the safekeepers in response to feedback from the pageserver -- data
that has been ingested into S3 can be safely dropped by the safekeepers. At present, safekeepers retain
a perpetual copy of the write log in S3, so we don't risk data loss from a bad trim, but this RFC accounts for
the future state where safekeepers only retain WAL content up til the point that it has been ingested
by pageservers.

We may solve for safety issues here in the same way as for deletions: before indicating to safekeepers
that they may trim up to a particular LSN, pageservers should communicate with the control plane
to confirm that their node generation is current. This guarantees that any future generation will
be starting from the last metadata that this generation wrote out, i.e. the S3 state which contains
all the data from the WAL up to the proposed trim point.

The pageservers indicate what to trim by the `remote_consistent_lsn` field in messages to
the safekeeper. The pageservers will change to only advertise the true remote_consistent_lsn
after they have periodically checked the validity of their generation: this will slightly
delay WAL trimming by whatever period we decide to make that control plane check-in.

### Housekeeping: Cleaning up orphan objects

An orphan object is any object which is no longer referenced by a running node or by metadata.

Examples of how orphan objects arise:

- A node is doing compaction and writes a layer object, then crashes before it writes the
  index_part.json that references that layer.
- A partition node carries on running for some time, and writes out an unbounded number of
  objects while it believes itself to be the rightful writer for a tenant.

Orphan objects are functionally harmless, but have a small cost due to S3 capacity consumed. We
may clean them up at some time in the future, but doing a ListObjectsv2 operation and cross
referencing with the

### Index changes

IndexPart currently stores keys and LSNs sufficient to reconstruct key names: this would be
extended to store the generation numbers with which a particular object was written, to enable
reconstructing the suffix as well.

This will increase the size of the file, but only modestly: layers are already encoded as
their string-ized form, so the overhead is about 20 bytes per layer. This will be less if/when
the index storage format is migrated to a binary format from JSON.

### Pageserver startup changes

- The pageserver must obtain a generation number by some means (see Control Plane Changes below) before
  doing any remote writes.
- For efficiency, the pageserver _should_ check that its attachments are still valid before proceeding
  to do any writes. Writes to stale attachments are safe, but wasteful. If all the attachments are
  the same as before restart, then it is not necessary to re-attach anything: the pageserver may proceed
  with the same attachment generation as before (safe because the node generation has incremented)

### `pageserver::Tenant` startup changes

Within the pageserver, a tenant's startup must account for the possibility that this node was
not the last node to write to the tenant's remote storage, and therefore execute a recovery to
get an up to date LayerMap.

Unless a pageserver is sure (via control plane) that all its attachments are the same as its last
startup, it must synchronize metadata from S3 before doing anything. This will mean finding the
index_part.json written by the last node to do writes on this tenant.

We can find the latest index_part.json as follows:

- In a clean migration of a tenant, the control plane may remember the tenant's most recent
  generation numbers, and provide them to the new node in the attach request. This is sufficient
  for the new node to directly GET the latest index.
- Alternatively, we may use the storage broker for this: if we are continuously publishing the generation
  information to the broker, then a newly attached tenant may query this information back and if
  the attachment generation is exactly 1 less than the current attachment, or if the attachment is
  the same but the node generation is exactly 1 less and the node ID is the same, then we may use that
  information to calculate the most recent index key. See "Optional Safekeeper changes" below for where
  we publish generation info to the safekeeper & in turn to the storage broker.
- As a fallback, newly attached tenants may issue a ListObjectsv2 request using index_part as a prefix
  to enumerate the indices and pick the most recent one by attachment generation. The listing would
  typically only return 1-2 indices.

Once the new attachment has written out its index_part.json, it may clean up historic index_part.json
files that were found (this would usually be exactly one, from the previous attachment).

### Control Plane Changes

#### Managing generation numbers

The control plane is responsible for issuing generation numbers: this work is done in the control plane
because without adding a consensus system to the pageservers, they do not have a way to reliably
allocate unique generation numbers while tolerating network partitions.

Assigning generation numbers to attachments is very simple, because designating a particular pageserver
as the attachment point for a tenant is already a database transaction: this transaction just needs
to increment the generation number as well as setting the pageserver.

Assigning generation numbers to nodes at startup is a larger change: pageserver startup must be
gated on communication from the control plane. We may either implement this via

- pull: the pageserver calling up to the control plane to announce itself and request a generation number
- push: in the opposite direction by having the control plane call down to the pageserver to give it
  a generation number.

The choice of pull or push does not affect the proposed storage format changes.

#### Synchronizing attachments on pageserver startup

In "pageserver startup changes" above, it is noted that the pageserver should synchronize its list of
attachments on startup, to avoid starting redundant writes to S3 for tenants that are already being
written from some other more up-to-date node.

However, it is important to avoid O(tenant_count) communications where possible, to avoid excessive
load on the control plane. Especially, there are two common special cases that can be made very
fast:

- A pageserver that restarts without any attachments being changed: this would happen if a pageserver
  restarts quickly, before the control plane has even noticed an outage and moved any attachments.
- A pageserver who has had _all_ their attachments removed between generations: this would happen
  if the control plane has responded to the node's absence by re-attaching all tenants to
  other nodes, before the original node returns to service.

### Optional Safekeeper changes

As an optimization, we may extend the safekeeper to be asynchronously updated about pageserver node
generation numbers. We may do this by including the generation in messages from pageserver to safekeeper,
and having the safekeeper write the highest generation number it has seen to the storage broker in
SafekeeperTimelineInfo (or more efficiently, by introducing a per-pageserver structure in storage broker).

Once the safekeeper has visibility of the most recent generation, it may reject requests from pageservers
with stale generation numbers: this would reduce any possible extra load on the safekeeper from stale pageservers,
and provide feedback to stale pageservers that they should shut down.

This is not required for safety: reads from a stale pageserver are functionally harmless and only
waste system resources. Logical writes (i.e. updates to remote_persistent_lsn that can cause trimming)
are handled on the pageserver side (see "WAL trim changes" above).

#### Optimization: fencing reads from stale pageservers

As an optimization, the safekeeper may track the most recent known generation for each pageserver node ID,
and respond with an error to any communications from older generations: this would help isolated
pageserver nodes to shut down more quickly once they realize that they are stale, rather than continuing
to put load on the safekeeper and write objects to S3 that are already superseded by later generations.

## Operational impact

### Availability

Coordination of generation numbers via the control plane introduce a dependency for certain
operations:

1. Starting new pageservers (or activating pageservers after a restart)
2. Executing enqueued deletions
3. Advertising updated `remote_consistent_lsn` to enable WAL trimming

Item 1. would mean that some in-place restarts that previously would have resumed service even if the control plane were
unavailable, will now not resume service ot users until the control plane is available. We could
avoid this by having a timeout on communication with the control plane, and after some timeout,
resume service with the node's previous generation (assuming this was persisted to disk). However,
this is unlikely to be needed as the control plane is already an essential & highly available component.

Item 2. is a non-issue operationally: it's harmless to delay deletions, the only impact of objects pending deletion is
the S3 capacity cost.

Item 3. is an issue if safekeepers are low on disk space and the control plane is unavailable for a long time.

### Rollout

Although there is coupling between components, we may deploy most of the new data plane components
independently of the control plane: initially they can just use a static generation number.

#### Phase 1

The pageserver is deployed with some special config to:

- Always act like everything is generation 1 and do not wait for a control plane issued generation on startup.
- Skip the places in deletion and remote_consistent_lsn updates where we would call into control plane

The storage broker will tolerate the timeline state omitting generation numbers.

The safekeeper will be aware of both new and old versions of `PageserverFeedback` message, and tolerate
the old version.

#### Phase 2

The control plane changes are deployed: control plane will now track and increment generation numbers.

#### Phase 3

The pageserver is deployed with its control-plane-dependent changes enabled: it will now require
the control plane to issue a generation number, and require to communicate with the control plane
prior to processing deletions.

### On-disk backward compatibility

Backward compatibility with existing data is straightforward:

- When reading the index, we may assume that any layer whose metadata doesn't include
  generations will have a generation-less key path.
- When locating the index file on attachment, we may use the "fallback" listing path
  and if there is only a generation-less index, that is the one we load.

It is not necessary to re-write an existing layers: even new index files will be able
to represent generation-less layers.

### On-disk forward compatibility

We will do a two phase rollout, probably over multiple releases because we will naturally
have some of the read-side code ready before the overall functionality is ready:

1. Deploy pageservers which understand the new index format and generation numbers
   in keys, but do not write objects with generation numbers in the keys.
2. Deploy pageservers that write objects with generation numbers in the keys.

Old pageservers will be oblivious to generation numbers. That means that they can't
read objects with generation numbers in the name.

# Appendix A: Examples of use in high availability/failover

The generation numbers proposed in this RFC are adaptable to a variety of different
failover scenarios and models. The sections below sketch how they would work in practice.

### Fast restart of a pageserver

"fast" here means that the restart is done before any other element in the system
has taken action in response to the node being down.

- After restart, the node does no writes until it can obtain a fresh generation
  number from the control plane.
- Once issued a new generation number, the node synchronizes with control plane to learn of any
  changes to its attachments, and receives a fast O(1) response informing it that it may continue
  to operate with all its existing attachments.
- Node proceeds to operate as normal, trusting that all its local content
  for attached tenants is up to date.

The only difference between this and how the system behaves today is that
the node generation number increases, and the pageserver's startup depends
on availability of the control plane. The improvement is an increase in _safety_: we
are no longer assuming that we may continue to write to all our attached
tenants' data, we are confirming it before writing.

### Unexpected "death" (possible network partition) of a pageserver

Consider a network partition where somewhere there is a functioning node
that believes it is the rightful holder of a particular node ID, and it
can write to S3 even though it can't communicate with our control plane.

This is the general case of a node dying: we must always assume that the
unresponsive node might be network partitioned and still writing to S3.

#### Case A: re-attachment to other nodes

1. Let's say node 0 fails in a cluster of three nodes 0, 1, 2.
2. Some external mechanism notices that the node is unavailable and initiates
   movement of all tenants attached to that node to a different node. In
   this example it would mean incrementing the attachment generation
   of all tenants that were attached to node 0, and attaching them
   to node 1 or 2 based on some distribution rule (this might be round
   robin, or capacity based).
3. A tenant which is now attached to node 1 will _also_ still be attached to node
   0, from the perspective of node 0. Let's say it was originally attached with
   generation suffix 00000001-0000-00000001 -- its new suffix on node 1
   might be 00000002-0001-00000001 (attachment gen 2, on node 1).
4. S3 writes will continue from nodes 0 and 1: there will be an index*part.json-00000001-0000-00000001
   \_and* an index_part.json-00000002-0001-00000001. Objects written under the old suffix
   after the new attachment was created do not matter from the rest of the system's
   perspective: the endpoints are reading from the new attachment location. Objects
   written by node 0 are just garbage that can be cleaned up at leisure.

Eventually, node 0 will somehow realize it should stop running by some means:

- A hard-stop of the VM it is running on
- It tries to communicate with another peer that sends it an error response
  indicating its generation is out of date
- It tries to do some deletions, and discovers when synchronizing with the
  control plane that its generation number is stale.

#### Case B: direct node replacement with blank node (cold standby)

1. Let's say node 0 fails, and there may be some other peers but they aren't relevant.
2. Some external mechanism notices that the node is unavailable, and creates
   a "new node 0" (Node 0b) which is a physically separate server. The original node 0
   (Node 0a) may still be running.
3. On startup, node 0b acquires a new generation number. It doesn't have any local state,
   so the control plane must send attach requests to node 0b for all the tenants. However,
   these do not have to increment the attachment generation as they're still attached to
   logical node 0, it's just a different physical node. So we have an O(tenant_count) communication
   to the pageserver, but not an O(tenant_count) write to the database.
4. S3 writes continue from nodes 0a and 0b, but the writes do not collide due to different
   node generation in the suffix, and the writes from node 0a are not visible to the rest
   of the system because endpoints are reading only from node 0b.

# Appendix B: interoperability with other features

## Sharded Keyspace

The design in this RFC maps neatly to a sharded keyspace design where subsets of the key space
for a tenant are assigned to different pageservers:

- the "unit of work" for attachments becomes something like a TenantShard rather than a Tenant
- TenantShards get generation numbers just as Tenants do.
- Write workload (ingest, compaction) for a tenant is spread out across pageservers via
  TenantShards, but each TenantShard still has exactly one valid writer at a time.

## Read replicas

For historical reads to LSNs below the remote persistent LSN, any node may act as a reader at any
time: remote data is logically immutable data, and the use of deferred deletion in this RFC helps
mitigate the fact that remote data is not _physically_ immutable (i.e. the actual data for a given
page moves around as compaction happens).

A read replica needs to be aware of generations in remote data in order to read the latest
metadata (find the index_part.json with the latest suffix). It may either query this
from the control plane, or more efficiently read it from the storage broker -- publishing
generation information to the storage broker.

## Seamless migration

To make tenant migration totally seamless, we will probably want to intentionally double-attach
a tenant briefly, serving reads from the old node while waiting for the new node to be ready.

This doesn't conflict with this RFC. A node may go through a transfer routine where it flushes
data to S3 and then goes into a readonly state pending the new node's active state. The attachment
generation number would increment when the control plane decides to "commit" the transfer by making
the new location the leader.

This might be implemented as a specialization of "warm secondary locations" (below), where doing
a clean migration is just creating a temporary warm secondary location.

## Warm secondary locations

To enable faster tenant movement after a pageserver is lost, we will probably want to spend some
disk capacity on keeping standby locations populated with local disk data.

There's no conflict between this RFC and that: implementing warm secondary locations on a per-tenant basis
would be a separate change to the control plane to store standby location(s) for a tenant. Because
the standbys do not write to S3, they do not need to be assigned generation IDs. When a tenant is
re-attached to a standby location, that would increment the tenant attachment generation and this
would work the same as any other attachment change, but with a warm cache.

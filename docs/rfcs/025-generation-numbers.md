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

## Implementation Part 1: Correctness

### Summary

- **Generation numbers** are introduced for pageserver node lifetimes and tenant attachments.

  - node generation increments each time a pageserver starts, before it can write to S3
  - attachment generation increments each time the control plane modifies a tenant (`Project`)'s assigned pageserver (an _attachment_'s lifetime is the association between a tenant and a pageserver)
  - the two generations are independent: a pageserver may restart while keeping the same
    attachment generation numbers, and attachments may be changed while a pageserver's
    generation number stays the same.

- **Object keys are suffixed** with the generation numbers
- **Safety in split brain for multiple nodes running with
  the same node ID** is provided by the pageserver node generation in the object key: the concurrent nodes
  will not write to the same key.
- **Safety for multiply-attached tenants** is provided by the
  tenant attach generation in the object key: the competing pageservers will not
  try to write to the same keys.
- **Safety for deletions** is provided by pageservers batching up deletions, then calling out to the control plane
  to validate that their generation is still current before executing these batches.
- **The control plane is used to issue generation numbers** to avoid the need for
  a built-in consensus system in the pageserver, although this could in principle
  be changed without changing the storage format.
- **The safekeeper may refuse RPCs from zombie pageservers** via the node generation number.

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

The most important generation number is the tenant attachment number: this alone would be sufficient
to implement safe migration and failover, if we assume that our control plane will never concurrently
run two nodes with the same node ID.

Running two nodes with the same ID might sound far-fetched, but it would happen very easily if we
ran the pageserver in a k8s environment with a StatefulSet, as k8s provides no guarantee that
an old pod is dead before starting a new one.

The node generation is useful other ways, beyond it's core correctness purpose:

- Including both generations (and the node ID) in object keys also provides some flexibility in
  how HA should work in future: we could move to a model where failover can happen within one
  attachment generation number (i.e. without control plane coordination) without changing the
  storage format, because node IDs/generations would de-conflict writes from peers.
- The node generation is also useful for managing remote objects which are not per-tenant,
  such as the persistent per-node deletion queue which is proposed in this RFC.
- node generation numbers enable building fencing into any network protocol (for example
  communication with safekeepers) to refuse to communicate with stale/partitioned nodes.

The two generation numbers have a different behavior for stale generations:

- A pageserver with a stale generation number should immediately terminate: it is never intended
  to have two pageservers with the same node ID.
- An attachment with a stale generation number is permitted to continue operating (ingesting WAL
  and serving reads), but not to do any deletions. This enables a multi-attached state for tenants,
  facilitating live migration (this will be articulated in the next RFC that describes HA and
  migrations).

### Object Key Changes

All object keys (layer objects and index objects) will have a suffix as follows:

```
   <attachment generation>-<node id>-<node generation>
```

The ordering of elements in the suffix is not important for correctness. As a speculative
optimization, the attachment generation is first in case we want to use this when searching
using a prefixed ListObjects2 for the most recent index from the current or previous attachment generation.

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

This suffix is the primary mechanism for protecting against split-brain situations, and
enabling safe multi-attachment of tenants:

- Two pageservers running with the same node ID (e.g. after a failure, where there is
  some rogue pageserver still running) will not try to write to the same objects.
- Multiple attachments of the same tenant will not try to write to the same objects, as
  each attachment would have a distinct attachment generation.

### Deletion Part 1: Remote object deletions

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
  generation (i.e. it is the legitimate holder of its node ID), _and_ that its attachments for the tenants
  whose data is being deleted are current.

The order of operations for correctness is:

1. Decide to delete one or more objects
2. Write updated metadata (this means that if we are still the rightful leader, any subsequent leader will
   read metadata that does not reference the about-to-be-deleted object).
3. Confirm we hold a non-stale generation for the node and the attachment(s) doing deletion.
4. Actually delete the objects

Note that at stage 3 we are only confirming that deletions of objects no longer referenced
by the metadata written in step 2 are safe. While Step 3 is in flight, new deletions may
be enqueued, but these are not safe to execute until a subsequent iteration of the
generation check.

Because step 3 puts load on the control plane, deletion lists should be executed lazily,
accumulating many deletions before executing them behind a batched generation check message
to the control plane, which would contain a list of tenant generation numbers to validate.

The result of the request to the control plane may indicate that only some of the attachment
generations are fresh: if this is the case, then the pageserver must selectively drop the
deletions from a stale generation, but still execute the deletions for attachments
that had a fresh generation.

The only correctness requirement for deletions is that they are _not_ executed prior
to the barrier in step 3. It is safe to delay a deletion, or indeed to never execute
it at all, if a node restarts while an in-memory queue of deletions is pending. Minimizing
leaking objects is an optimization, accomplished by making this queue persistent: see
[Persistent Deletion Queue](#persistent-deletion-queue) in the optimizations section.

### Deletion Part 2: WAL trim changes

Remote objects are not the only kind of deletion the pageserver does: it also indirectly deletes
WAL data, by feeding back remote_consistent_lsn to safekeepers, as a hint to the safekeepers that
they may drop data below this LSN.

We may solve for safety issues here in the same way as for deletions: before indicating to safekeepers
that they may trim up to a particular LSN, pageservers should communicate with the control plane
to confirm that their node generation is current. This guarantees that any future generation will
be starting from the last metadata that this generation wrote out, i.e. the S3 state which contains
all the data from the WAL up to the proposed trim point.

The pageservers indicate what to trim by the `remote_consistent_lsn` field in messages to
the safekeeper. The pageservers will change to only advertise the true remote_consistent_lsn
after they have periodically checked the validity of their generation: this will slightly
delay WAL trimming by whatever period we decide to make that control plane check-in.

When discussing correctness, read "deletion" as meaning both the deletion of remote objects,
and publishing updates to remote_consistent_lsn.

### Index changes

Since object keys now include a generation suffix, the index of these keys must also be updated.

IndexPart currently stores keys and LSNs sufficient to reconstruct key names: this would be
extended to store the generation numbers with which a particular object was written, to enable
reconstructing the suffix as well.

This will increase the size of the file, but only modestly: layers are already encoded as
their string-ized form, so the overhead is about 20 bytes per layer. This will be less if/when
the index storage format is migrated to a binary format from JSON.

### Pageserver startup changes

- The pageserver must obtain a generation number by some means (see Control Plane Changes below) before
  doing any remote writes.
- The pageserver _may_ also do some synchronization of its attachments to see which are still
  valid, see "Synchronizing attachments on pageserver startup" in the optimizations section.

### `pageserver::Tenant` startup changes

#### Attachment

We already reconcile with remote metadata during attachment. It is necessary to synchronize
with remote metadata even if there is some local metadata that could be brought up to
date by ingesting the WAL, because if another node updated
the remote storage, it might have advanced the remote_consistent_lsn such that the safekeeper drops WAL
content that would be needed for this node to recover. Reconcilation with remote metadata also acts as an optimization, to avoid redundantly
re-ingesting WAL data that is already in S3.

Because index files are now suffixed with generation numbers, some changes are needed
to load them:

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
files that were found, unless the control plane has indicated to us that the tenant is multiply attached
(see the subsequent HA RFC for this concept).

#### Node Startup with existing Attachment

During attachment, we must also write out the attachment generation number to local storage,
so that on subsequent node startup we can tell if our attachment is still current. On node startup, when we see an already-attached tenant, it is possible that the tenant was attached to a different node while this node was offline.
We may still safely start ingesting data and serving reads, but we may not process deletions: deletions
will be blocked when the deletion queue checks the attachment's generation and finds that it is
not the latest. We may continue operating the attachment as normal until the control plane
asks us to detach: while we are not the latest generation, it is possible that some endpoint
is still configured to use us for reads, so we should continue operating until the control plane
asks us to detach.

On a new attachment (i.e. we do not have local disk state that was written within the attachment's
generation number), we must recover metadata from remote storage. We can find the latest index_part.json as follows:

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

#### Pageserver-facing API

The pageservers are currently only called into by control plane: the control plane does not
expose an API for pageservers to call into.

It would be possible to avoid this in a couple of ways:

- with a push-polling mechanism where the control plane
  sends an "any requests?" POST to the pageserver and the pageserver makes its requests
  in the response body
- with a broadcast mechanism where the control plane sends generation information for all attachments
  to the pageserver periodically, without knowledge of which ones the pageserver really
  requires.

There is not a compelling reason for either of the above options though: we may simply expose an
extra API endpoint on the control plane, and deploy pageservers with proper authentication
tokens to access it.

This "downward-facing" or "south-facing" API should be kept distinct from the
general control plane API: the pageserver should only have access to the endpoints
it needs for synchronization, and not the broader control plane functionality.

## Implementation Part 2: Optimizations

### Persistent deletion queue

Between writing our a new index_part.json that doesn't reference an object,
and executing the deletion, an object passes through a window where it is
only referenced in memory, and could be leaked if the pageserver is stopped
uncleanly. That introduces conflicting incentives: we would like to delay
deletions to minimize control plane load and allow aggregation of full-sized
DeleteObjects requests, but we would also like to minimize leakage by executing
deletions promptly.

To resolve this, we may make the deletion queue persistent, writing out
deletion lists as S3 objects for later execution. This shrinks the window
of possible object loss to the gap between writing index_part.json, and
writing the next deletion list ot S3. The actual deletion of objects
(with its requirement to sync with the control plane) may happen much
later.

The flow of deletion becomes:

1. Enqueue in memory
2. Enqueue persistently by writing out a deletion list, storing the
   attachment generations and node generation.
3. Validate the deletion list by calling to the control plane
4. Execute the valid parts of the deletion list (i.e. call DeleteObjects)

There is existing work (https://github.com/neondatabase/neon/pull/4960) to
create a deletion queue: this would be extended by adding the "step 3" validation
step.

Since this whole section is an optimization, there is a lot of flexibility
in exactly how the deletion queue should work, especially in the timing
of the validation step:

- A (simplest):validate the
  deletion list before persisting it. This has the downside of delaying
  persistence of deletes until the control plane is available.
- B: validate a list at the point of executing it. This has the downside
  that because we're doing the validation much later, there is a good chance
  some attachments might have changed, and we will end up leaking objects.
- C (preferred): validate lazily in the background after persistence of the list, maintaining
  an "executable" pointer into the list of deletion lists to reflect
  which ones are elegible for execution, and re-writing deletion lists if
  they are found to contain some operations not elegible for execution. This
  is the most efficient approach in terms of I/O, at the cost of some complexity
  in implementation.

Persistence is to S3 rather than local disk so that if a pageserver
is stopped uncleanly and then the same node_id is started on a fresh
physical machine, the deletion queue can still continue without leaking
objects.

As well as reducing leakage, the ability of a persistent queue to accumulate
deletions over long timescale has side benefits:

- Over enough time we will always accumulate enough keys to issue full-sized
  (1000 object) DeleteObjects requests, minimizing overall request count.
- If in future we implement a read-only mode for pageservers to read the
  data of tenants, then delaying deletions avoids the need for that remote
  pageserver to be fully up to date with the latest index when serving reads
  from old LSNs. The read-only node might be using stale metadata that refers
  to objects eliminated in a recent compaction, but can still service reads
  without refreshing metadata as long as the old objects deletion is delayed.

### Synchronizing attachments on pageserver startup

For correctness, it is not necessary for a node to synchronize its attachments on startup: it
may continue to ingest+serve tenants on stale attachment generation numbers harmlessly.

As an optimization, we may avoid doing spurious S3 writes within a stale generation,
by using the same generation-checking API that is used for deletions. On startup,
concurrently with loading state from disk, the pageserver may issue RPCs to
the control plane to discover if any of its attachments are stale.

If an attachment is stale, then the pageserver will not do any S3 writes. However,
the attachment will still ingest the WAL and serve reads: this is necessary
for high availability, as some endpoint might still be using this
node for reads. To avoid overwhelming local disk with data that cannot be
offloaded to remote storage, we may impose some time/space threshold on
the attachment when operating in this mode: when exceeded, the attachment
would go into Broken state. It is the responsibility of the control plane
to ensure that endpoints are using the latest attachment location before this
happens.

In principle we could avoid the need to exchange O(attachment_count) information
at startup by having the control plane keep track of whether any changes
happened that would affect the pageserver's attachments while it was unavailable,
but this would impose complexity on the control plane code to track such
dirty/clean state. It is simpler to just check all the attachments, and
relatively inexpensive since validating generation numbers is a read-only
request to the control plane.

### Optional: Cleaning up orphan objects

An orphan object is any object which is no longer referenced by a running node or by metadata.

Examples of how orphan objects arise:

- A node is doing compaction and writes a layer object, then crashes before it writes the
  index_part.json that references that layer.
- A partition node carries on running for some time, and writes out an unbounded number of
  objects while it believes itself to be the rightful writer for a tenant.

Orphan objects are functionally harmless, but have a small cost due to S3 capacity consumed. We
may clean them up at some time in the future, but doing a ListObjectsv2 operation and cross
referencing with the latest metadata to identify objects which are not referenced. This kind
of deletion would go through the same deletion queue as other object deletions, and be subject
to the same checks for generation number freshness.

### Optional: Safekeeper optimization for stale pageservers

As an optimization, we may extend the safekeeper to be asynchronously updated about pageserver node
generation numbers. We may do this by including the generation in messages from pageserver to safekeeper,
and having the safekeeper write the highest generation number it has seen for each node to the storage broker.

Once the safekeeper has visibility of the most recent generation, it may reject requests from pageservers
with stale generation numbers: this would reduce any possible extra load on the safekeeper from stale pageservers,
and provide feedback to stale pageservers that they should shut down.

This is not required for safety: reads from a stale pageserver are functionally harmless and only
waste system resources. Logical writes (i.e. updates to remote_persistent_lsn that can cause trimming)
are handled on the pageserver side (see "WAL trim changes" above).

Note that the safekeeper should only reject reads for stale _pageserver_ generations. Stale _attachment_
generations are valid for reads, as a tenant may be multi-attached during a migration, where two different
pageservers are both replaying the WALs for the same tenant.

## Operational impact

### Availability

Coordination of generation numbers via the control plane introduce a dependency for certain
operations:

1. Starting new pageservers (or activating pageservers after a restart)
2. Executing enqueued deletions
3. Advertising updated `remote_consistent_lsn` to enable WAL trimming

Item 1. would mean that some in-place restarts that previously would have resumed service even if the control plane were
unavailable, will now not resume service to users until the control plane is available. We could
avoid this by having a timeout on communication with the control plane, and after some timeout,
resume service with the node's previous generation (assuming this was persisted to disk). However,
this is unlikely to be needed as the control plane is already an essential & highly available component.

Item 2. is a non-issue operationally: it's harmless to delay deletions, the only impact of objects pending deletion is
the S3 capacity cost.

Item 3. is an issue if safekeepers are low on disk space and the control plane is unavailable for a long time.

For a managed service, the general approach should be to make sure we are monitoring & respond fast enough
that control plane outages are bounded in time. The separate of console and control plane will also help
to keep the control plane itself simple and robust.

We should also implement an "escape hatch" config for node generation numbers, where in a major disaster outage,
we may manually run pageservers with a hand-selected generation number, so that we can bring them online
independently of a control plane.

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
  to operate with all its existing attachments (assuming the optimization from
  "Optimizations" section for pageserver startup)
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
4. S3 writes will continue from nodes 0 and 1: there will be an index_part.json-00000001-0000-00000001
   \_and\* an index_part.json-00000002-0001-00000001. Objects written under the old suffix
   after the new attachment was created do not matter from the rest of the system's
   perspective: the endpoints are reading from the new attachment location. Objects
   written by node 0 are just garbage that can be cleaned up at leisure. Node 0 will
   not do any deletions because it can't synchronize with control plane.

Eventually, node 0 will somehow realize it should stop running by some means, although
this is not necessary for correctness:

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

This RFC enables that double-attachment: two nodes may be attached at the same time, with the migration destination
having a higher generation number. The old node will be able to ingest and serve reads, but not
do any deletes. The new node's attachment must also avoid doing deletes, a new piece of state
will be needed for this in the control plane's definition of an attachment.

## Warm secondary locations

To enable faster tenant movement after a pageserver is lost, we will probably want to spend some
disk capacity on keeping standby locations populated with local disk data.

There's no conflict between this RFC and that: implementing warm secondary locations on a per-tenant basis
would be a separate change to the control plane to store standby location(s) for a tenant. Because
the standbys do not write to S3, they do not need to be assigned generation IDs. When a tenant is
re-attached to a standby location, that would increment the tenant attachment generation and this
would work the same as any other attachment change, but with a warm cache.

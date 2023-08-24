# Pageserver generation numbers for safe failover & tenant migration

## Summary

A scheme of logical "generation numbers" for tenant attachment to pageservers is proposed, along with
changes to the remote storage format to include these generation numbers in S3 keys.

Using the control plane as the issuer of these generation numbers enables strong anti-split-brain
properties in the pageserver cluster without implementing a consensus mechanism directly
in the pageservers.

## Motivation

Currently, the pageserver's remote storage format does not provide a mechanism for addressing
split brain conditions that may happen when replacing a node or when migrating
a tenant from one pageserver to another.

From a remote storage perspective, a split brain condition occurs whenever two nodes both think
they have the same tenant attached, and both can write to S3. This can happen in the case of a
network partition, pathologically long delays (e.g. suspended VM), or software bugs.

In the current deployment model, control plane guarantees that a tenant is attached to one
pageserver at a time, thereby ruling out split-brain conditions resulting from dual
attachment (however, there is always the risk of a control plane bug). This control
plane guarantee prevents robust response to failures, as if a pageserver is unresponsive we may not detach from it. The mechanism in this RFC

Futher, lack of safety during split-brain conditions blocks two important features where occasional
split-brain conditions are part of the design assumptions:

- seamless tenant migration ([RFC PR](https://github.com/neondatabase/neon/pull/5029))
- automatic pageserver instance failure handling (aka "failover") (RFC TBD)

### Prior art

- 020-pageserver-s3-coordination.md
- 023-the-state-of-pageserver-tenant-relocation.md
- 026-pageserver-s3-mvcc.md

This RFC has broad similarities to the proposal to implement a MVCC scheme in
S3 object names, but this RFC avoids a general purpose transaction scheme in
favour of more specialized "generations" that work like a transaction ID that
always has the same lifetime as a pageserver process or tenant attachment, whichever
is shorter.

## Requirements

- Accommodate storage backends with no atomic or fencing capability (i.e. work within
  S3's limitation that there are no atomics and clients can't be fenced)
- Don't depend on any STONITH or node fencing in the compute layer (i.e. we will not
  assume that we can reliably kill and EC2 instance and have it die)
- Scoped per-tenant, not per-pageserver; for _seamless tenant migration_, we need
  per-tenant granularity, and for _failover_, we likely want to spread the workload
  of the failed pageserver instance to a number of peers, rather than monolithically
  moving the entire workload to another machine.
  We do not rule out the latter case, but should not constrain ourselves to it.

## Design Tenets

These are not requirements, but are ideas that guide the following design:

- Avoid implementing another consensus system: we already have a strongly consistent
  database in the control plane that can do atomic operations where needed, and we also
  have a Paxos implementation in the safekeeper.
- Avoiding locking in to specific models of how failover will work (e.g. do not assume that
  all the tenants on a pageserver will fail over as a unit).
- Avoid doing synchronization that scales with the number of tenants, unless absolutely
  necessary. For example, a pageserver should be able to start up without having to
  have each tenant individually re-attached.
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

The interaction between this RFC and those features is discussed in [Appendix B](#appendix-b-interoperability-with-other-features)

## Impacted Components

pageserver, control plane, safekeeper (optional)

## Implementation Part 1: Correctness

### Summary

- **Generation numbers** are introduced for tenants when attached to a pageserver

  - attachment generation increments each time the control plane modifies a tenant (`Project`)'s assigned pageserver, or when the assigned pageserver restarts
  - the control plane is the authority for generation numbers: only it may
    increment a generation number.

- **Object keys are suffixed** with the generation number
- **Safety for multiply-attached tenants** is provided by the
  tenant attach generation in the object key: the competing pageservers will not
  try to write to the same keys.
- **Safety in split brain for multiple nodes running with
  the same node ID** is provided by the pageserver calling out to the control plane
  on startup, to increment the generations of any attached tenants
- **Safety for deletions** is provided by pageservers validating with control plane that neither pageserver process nor the attachment has been superseded since it last updated a timeline's index.
- **The control plane is used to issue generation numbers** to avoid the need for
  a built-in consensus system in the pageserver, although this could in principle
  be changed without changing the storage format.

### Generation numbers

A generation number is associated with each tenant in the control plane,
and each time the attachment status of the tenant changes, this is incremented.
Changes in attachment status include:

- Attaching the tenant to a different pageserver
- A page server restarting, and "re-attaching" its tenants on startup

These increments of attachment generation provide invariants we need to avoid
split-brain issues in storage:

- If two pageservers have the same tenant attached, they are guaranteed to have different attachment generation numbers, because the generation would increment
  while attaching the second one.
- If there are multiple pageservers running with the same node ID, they are guaranteed to have different generation numbers, because the generation would increment
  when the second node started and re-attached its tenants.
- If there are multiple pageservers running with the same node ID, we may unambiguously know which
  of them "wins" by picking the higher generation number.

**Note** If we ever run pageservers on infrastructure that might transparently restart
a pageserver while leaving an old process running (e.g. a VM gets rescheduled
without the old one being fenced), then there is a risk of corruption, when
the control plane attaches the tenant, as follows:

- if the control plane sends an /attach request
  to node A, then node A dies and is replaced, and the control plane times out
  the request and restarts, then it could end up with two physical nodes both using
  the same attachment generation.
- This is not an issue when using EC2 instances with ephemeral storage, as long
  as the control plane never re-uses a node ID, but it would need re-examining
  if running on different infrastructure.
- To robustly protect against this class of issue, we would either:
  - add a "node generation" to distinguish between different processes holding the
    same node_id.
  - or, dispense with static node_id entirely and issue an ephemeral ID to each
    pageserver process when it starts.

### Object Key Changes

#### Generation suffix

All object keys (layer objects and index objects) will contain the attachment
generation as a [suffix](#why-a-generation-suffix-rather-than-prefix).
This suffix is the primary mechanism for protecting against split-brain situations, and
enabling safe multi-attachment of tenants:

- Two pageservers running with the same node ID (e.g. after a failure, where there is
  some rogue pageserver still running) will not try to write to the same objects, because at startup they will have re-attached tenants and thereby incremented
  generation numbers.
- Multiple attachments (to different pageservers) of the same tenant will not try to write to the same objects, as each attachment would have a distinct generation.

The generation is appended in hex format (8 byte string representing
u32), to all our existing key names. A u32's range limit would permit
27 restarts _per second_ over a 5 year system lifetime: orders of magnitude more than
is realistic.

The exact meaning of the generation suffix can evolve over time if necessary, for
example if we chose to implement a failover mechanism internally to the pageservers
rather than going via the control plane. The storage format just sees it as a number,
with the only semantic property being that the highest numbered index is the latest.

#### Index changes

Since object keys now include a generation suffix, the index of these keys must also be updated. IndexPart currently stores keys and LSNs sufficient to reconstruct key names: this would be extended to store the generation as well.

This will increase the size of the file, but only modestly: layers are already encoded as
their string-ized form, so the overhead is about 10 bytes per layer. This will be less if/when
the index storage format is migrated to a binary format from JSON.

#### Visibility

_This section doesn't describe code changes, but extends on the consequences of the
object key changes given above_

##### Visibility of objects to pageservers

Pageservers can of course list objects in S3 at any time, but in practice their
visible set is based on the contents of their LayerMap, which is initialized
from the `index_part.json.???` that they load.

Starting with the `index_part` from the most recent previous generation
(see [loading index_part](#finding-the-remote-indices-for-timelines)), a pageserver
initially has visibility of all the objects that its predecessor generation referenced.
These objects are guaranteed to remain visible until the current generation is
superseded, via pageservers in older generations avoiding deletions (see [deletion](#deletion)).

The "most recent previous generation" is _not_ necessarily the most recent
in terms of walltime, it is the one that is readable at the time a new generation
starts. Consider the following sequence of a tenant being re-attached to different
pageserver nodes:

- Create + attach on PS1 in generation 1
- PS1 Do some work, write out index_part.json-0001
- Attach to PS2 in generation 2
- Read index_part.json-0001
- PS2 starts doing some work...
- Attach to PS3 in generation 3
- Read index_part.json-0001
- **...PS2 finishes its work: now it writes index_part.json-0002**
- PS3 writes out index_part.json-0003

In the above sequence, the ancestry of indices is:

```
0001 -> 0002
     |
     -> 0003
```

This is not an issue for safety: if the 0002 references some object that is
not in 0001, then 0003 simply does not see it, and will re-do whatever
work was required (e.g. ingesting WAL or doing compaction). Objects referenced
by only the 0002 index will never be read by future attachment generations, and
will eventually be cleaned up by a scrub (see [scrubbing](#cleaning-up-orphan-objects-scrubbing)).

##### Visibility of LSNs to clients

Because index_part.json is now written with a generation suffix, which data
is visible depends on which generation the reader is operating in:

- If one was passively reading from S3 from outside of a pageserver, the
  visibility of data would depend on which index_part.json-<generation> file
  one had chosen to read from.
- If two pageservers have the same tenant attached, they may have different
  data visible as they're independently replaying the WAL, and maintaining
  independent LayerMaps that are written to independent index_part.json files.
  Data does not have to be remotely committed to be visible.
- For a pageserver writing with a stale generation, historic LSNs
  remain readable until another pageserver (with a higher generation suffix)
  decides to execute GC deletions. At this point, we may think of the stale
  attachment's generation as having logically ended: during its existence
  the generation had a consistent view of the world.
- For a newly attached pageserver, its highest visible LSN may appears to
  go backwards with respect to an earlier attachment, if that earlier
  attachment had not uploaded all data to S3 before the new attachment.

### Deletion

#### Generation number validation

While writes are de-conflicted by writers always using their own generation number in the key,
deletions are slightly more challenging: if a pageserver A is isolated, and the true active node is
pageserver B, then it is dangerous for A to do any object deletions, even of objects that it wrote
itself, because pageserver's B metadata might reference those objects.

We solve this by inserting a "generation validation" step between the write of a remote index
that un-links a particular object from the index, and the actual deletion of the object, such
that deletions strictly obey the following ordering:

1. Write out index_part.json: this guarantees that any subsequent reader of the metadata will
   not try and read the object we unlinked.
2. Call out to control plane to validate that the generation which we use for our attachment is still the latest.
3. If step 2 passes, it is safe to delete the object. Why? The check-in with control plane
   together with our visibility rules guarantees that any later generation
   will use either the exact `index_part.json` that we uploaded in step 1, or a successor
   of it; not an earlier one. In both cases, the `index_part.json` doesn't reference the
   key we are deleting anymore, so, the key is invisible to any later attachment generation.
   Hence it's safe to delete it.

Note that at step 2 we are only confirming that deletions of objects _no longer referenced
by the specific `index_part.json` written in step 1_ are safe. If we were attempting other deletions concurrently,
these would need their own generation validation step.

If step 2 fails, we may leak the object. This is safe, but has a cost: see [scrubbing](#cleaning-up-orphan-objects-scrubbing). We may avoid this entirely outside of node
failures, if we do proper flushing of deletions on clean shutdown and clean migration.

To avoid doing a huge number of control plane requests to perform generation validation,
validation of many tenants will be done in a single request, and deletions will be queued up
prior to validation: see [Persistent deletion queue](#persistent-deletion-queue) for more.

#### `remote_consistent_lsn` updates

Remote objects are not the only kind of deletion the pageserver does: it also indirectly deletes
WAL data, by feeding back remote_consistent_lsn to safekeepers, as a signal to the safekeepers that
they may drop data below this LSN.

For the same reasons that deletion of objects must be guarded by an attachment generation number
validation step, updates to `remote_consistent_lsn` are subject to the same rules, using
an ordering as follows:

1. upload the index_part that covers data up to LSN `L0` to S3
2. call to control plane to validate their attachment generation
3. update the `remote_consistent_lsn` that they send to the safekeepers to `L0`

**Note:** at step 3 we are not advertising the _latest_ remote_consistent_lsn, we are
advertising the value in the index_part that we uploaded in step 1. This provides
a strong ordering guarantee.

Internally to the pageserver, each timeline will have two remote_consistent_lsn values: the one that
reflects its latest write to remote storage, and the one that reflects the most
recent validation of generation number. It is only the latter value that may
be advertised to the outside world (i.e. to the safekeeper).

The control plane remains unaware of `remote_consistent_lsn`: it only has to validate
the freshness of generation numbers, thereby granting the pageserver permission to
share the information with the safekeeper.

For convenience, in subsequent sections and RFCs we will use "deletion" to mean both deletion
of objects in S3, and updates to the `remote_consistent_lsn`, as updates to the remote consistent
LSN are de-facto deletions done via the safekeeper, and both kinds of deletion are subject to
the same generation validation requirement.

### Pageserver attach/startup changes

#### Attachment

Calls to `/v1/tenant/{tenant_id}/attach` are augmented with an additional
`generation` field in the body.

The pageserver must persist this attachment generation number before starting
the `Tenant` tasks. We will create a new per-tenant metadata file that records
the attachment generation number, and will also be used in future work for
implementing fast migration, where the tenant has more states than simple
attached & detached.

#### Re-attachment on startup

As it currently does, the pageserver may use local disk state to identify which tenants/timelines
were attached before its most recent restart. However, it is now necessary
to call out to the control plane to advance the generation number before
doing any remote I/O for the attachment: we call this process **re-attaching**
on startup.

Advancing the generation number is necessary to avoid the pageserver assuming
that it is running in an environment where a previous instance of "itself" must
have been fenced before this instance started: e.g. if running in an environment
with containers/VMs using shared block storage where some stale container/VM might
still think it has the same tenants attached that we do.

To avoid sending O(tenants) requests to the control plane, in practice
we would send a batched request to re-attach many tenants at once.

Re-attaching has two possible outcomes for each tenant:

- Increment the generation number, and return the incremented number: this
  tenant may now go active with the new generation number.
- Do not increment the generation number: this tenant may start, but may
  not write to remote storage. This case is for nodes that the control
  plane is intentionally leaving on a stale generation, for example when
  doing a migration between pageservers, and the origin pageserver restarts
  during the migration.

#### Reconciliation

On attachment, the `Tenant` may have some local disk content from previous
activity involving this tenant (e.g. it used to be attached here), and this
needs reconciling with remote storage. Note that "reconcile" in this context is
used generically, and does not refer specifically to the logic currently
in the `reconcile_with_remote` function.

Because pageservers have access to the WAL, they don't strictly have to
use the latest remote storage contents at all: if the WAL history still contains
the data required to bring the pageserver's local state up to date with
the tip of the WAL, the pageserver could recover from the WAL and then
overwrite whatever is in remote storage.

However, it is preferable to recover from remote storage rather than
to replay WAL, for the following reasons:

- Simplicity: recovering from remote storage gives identical behavior
  whether attaching a tenant for the first time or re-attaching later.
- Efficiency: ingesting the WAL has significant cost beyond the I/O
  of reading it (compaction, image layer generation). We should avoid
  doing this work twice if there is already remote content available.
- Reduced load on safekeepers, which are a less scalable resource than S3.

#### Finding the remote indices for timelines

Because index files are now suffixed with generation numbers, the pageserver
cannot always GET the remote index in one request, because it can't always
know a-priori what the latest remote index is.

Typically, the most recent generation to write an index would be our own
generation minus 1. However, this might not be the case: the previous
node might have started and acquired a generation number, and then crashed
before writing out a remote index.

In the general case and as a fallback, the pageserver may list all the `index_part.json`
files for a timeline, sort them by generation, and pick the highest that is `<=`
its current generation for this attachment. The tenant should never load an index
with an attachment generation _newer_ than its own: tenants
are allowed to be attached with stale attachment generations during a multiply-attached
phase in a migration, and in this instance if the old location's pageserver restarts,
it should not try and load the newer generation's index.

To summarize, on starting a timeline, the pageserver will:

1. Issue a GET for index_part.json-<my generation - 1>
2. If 1 failed, issue a ListObjectsv2 request for index_part.json\* and
   pick the newest.

One could optimize this further by using the control plane to record specifically
which generation most recently wrote an index_part.json, if necessary, to increase
the probability of finding the index_part.json in one GET. One could also improve
the chances by having pageservers proactively write out index_part.json after they
get a new generation ID.

#### Cleaning up previous generations' remote indices

Deletion of old indices is not necessary for correctness, although it is necessary
to avoid the ListObjects fallback in the previous section becoming ever more expensive.

Once the new attachment has written out its index_part.json, it may asynchronously clean up historic index_part.json
objects that were found.

We may choose to implement this deletion either as an explicit step after we
write out index_part for the first time in a pageserver's lifetime, or for
simplicity just do it periodically as part of the background scrub (see [scrubbing](#cleaning-up-orphan-objects-scrubbing));

### Control Plane Changes

#### Store generations for attaching tenants

- The `Project` table must store an attachment generation number for use when
  attaching the tenant to a new pageserver.
- The `/v1/tenant/:tenant_id/attach` pageserver API will require an attachment generation number,
  which the control plane can supply by simply incrementing the `Project`'s attachment
  generation number each time the tenant is attached to a different server: the same database
  transaction that changes the assigned pageserver should also change the attachment generation.

#### Generation API

This section describes an API that could be provided directly by the control plane,
or built as a separate microservice. In earlier parts of the RFC, when we
discuss the control plane providing generation numbers, we are referring to this API.

The API endpoints used by the pageserver to acquire and validate generation
numbers are quite simple, and only require access to some persistent and
linerizable storage (such as a database).

Building this into the control plane is proposed as a least-effort option to exploit existing infrastructure and enable
updating attachment generations in the same transaction as updating
the `Project` itself, but it is not mandatory: this "Generation API" could
be built as a microservice. In practice, we will write such a miniature service
anyway, to enable E2E pageserver/compute testing without control plane.

The endpoints required by pageservers are:

##### `/re-attach`

- Request: `{node_id: <u32>}`
- Response:
  - 200 `{tenants: [{id: <TenantId>, gen: <u32>}]}`
  - 404: unknown node_id
  - (Future: 429: flapping detected, perhaps nodes are fighting for the same node ID,
    or perhaps this node was in a retry loop)
  - (On unknown tenants, omit tenant from `tenants` array)
- Server behavior: query database for which tenants should be attached to this pageserver.
  - for each tenant that should be attached, increment the attachment generation and
    include the new generation in the response
- Client behavior:
  - for all tenants in the response, activate with the new generation number
  - for any local disk content _not_ referenced in the response, act as if we
    had been asked to detach it (i.e. delete local files)

**Note** this process currently deletes local state and avoids dual-attaching, but
this will change when the control plane is updated in subsequent work to have
the concept of multiple locations.

**Note** the `node_id` in this request will change in future if we move to ephemeral
node IDs, to be replaced with some correlation ID that helps the control plane realize
if a process is running with the same storage as a previous pageserver process (e.g.
we might use EC instance ID, or we might just write some UUID to the disk the first
time we use it)

##### `/validate`

- Request: `{'tenants': [{tenant: <tenant id>, attach_gen: <gen>}, ...]}'`
- Response:
  - 200 `{'tenants': [{tenant: <tenant id>, status: <bool>}...]}`
  - (On unknown tenants, omit tenant from `tenants` array)
- Purpose: enable the pageserver to discover whether the generations it holds are still current
- Server behavior: this is a read-only operation: simply compare the generations in the request with
  the generations known to the server, and set status to `true` if they match.
- Client behavior: clients must not do deletions within a tenant's remote data until they have
  received a response indicating the generation they hold for the tenant is current.

### Timeline/Branch creation

All of the previous arguments for safety have described operations within
a timeline, where we may describe a sequence that includes updates to
index_part.json, and where reads and writes are coming from a postgres
endpoint (writes via the safekeeper).

Creating or destroying timeline is a bit different, because writes
are coming from the control plane.

We must be safe against scenarios such as:

- A tenant is attached to pageserver B while pageserver A is
  in the middle of servicing an RPC from the control plane to
  create or delete a tenant.
- A pageserver A has been sent a timeline creation request
  but becomes unresponsive. The tenant is attached to a
  different pageserver B, and the timeline creation request
  is sent there too.

To properly complete a timeline create/delete request, we must
be sure _after_ the pageserver writes to remote storage, that its
generation number is still up to date:

- The pageserver can do this by calling into the control plane
  before responding to a request.
- Alternatively, the pageserver can include its generation in
  the response, and let the control plane validate that this
  generation is still current after receiving the response.

If some very slow node tries to do a timeline creation _after_
a more recent generation node has already created the timeline
and written some data into it, that must not cause harm. This
is provided in timeline creations by the way all the objects
within the timeline's remote path include a generation suffix:
a slow node in an old generation that attempts to "create" a timeline
that already exists will just emit an index_part.json with
an old generation suffix.

Timeline IDs are never reused, so we don't have
to worry about the case of create/delete/create cycles. If they
were re-used during a disaster recovery "un-delete" of a timeline,
that special case can be handled by calling out to all available pageservers
to check that they return 404 for the timeline, and to flush their
deletion queues in case they had any deletions pending from the
timeline.

**During timeline/tenant deletion, the control plane must not regard an operation
as complete when it receives a `202 Accepted` response**, because the node
that sent that response might become permanently unavailable. The control
plane must wait for the deletion to be truly complete (e.g. by polling
for 404 while the tenant is still attached to the same pageserver), and
handle the case where the pageserver becomes unavailable, either by waiting
for a replacement with the same node_id, or by re-attaching the tenant elsewhere.

**Sending a tenant/timeline deletion to a stale pageserver will still result
in deletion** -- the control plane must persist its intent to delete
a timeline/tenant before issuing any RPCs, and then once it starts, it must
keep retrying until the tenant/timeline is gone. This is already handled
by using a persistent `Operation` record that is retried indefinitely.

Tenant/timeline deletion operations are exempt from generation validation
on deletes, and therefore don't have to go through the same deletion
queue as GC/compaction layer deletions. This is because once a
delete is issued by the control plane, it is a promise that the
control plane will keep trying until the deletion is done, so even stale
pageservers are permitted to go ahead and delete the objects.

Timeline deletion may result in a special kind of object leak, where
the latest generation attachment completes a deletion (including erasing
all objects in the timeline path), but some slow/partitioned node is
writing into the timeline path with a stale generation number. This would
not be caught by any per-timeline scrubbing (see [scrubbing](#cleaning-up-orphan-objects-scrubbing)), since scrubbing happens on the
attached pageserver, and once the timeline is deleted it isn't attached anywhere.
This scenario should be pretty rare, and the control plane can make it even
rarer by ensuring that if a tenant is in a multi-attached state (e.g. during
migration), we wait for that to complete before processing the deletion. Beyond
that, we may implement some other top-level scrub of timelines in
an external tool, to identify any tenant/timeline paths that are not found
in the control plane database.

Examples:

- Deletion, node restarts partway through:
  - By the time we returned 204, we have written a remote delete marker
  - Any subsequent incarnation of the same node_id will see the remote
    delete marker and continue to process the deletion
  - We only require that the control plane does not change the tenant's
    attachment while waiting for the deletion.
  - If the original pageserver is lost permanently and no replacement
    with the same node_id is available, then the control plane must recover
    by re-attaching the tenant to a different node.
- Creation, node becomes unresponsive partway through.
  - Control plane will see HTTP request timeout, keep re-issuing
    request to whoever is the latest attachment point for the tenant
    until it succeeds.
  - Stale nodes may be trying to execute timeline creation: they will
    write out index_part.json files with
    stale attachment generation: these will be eventually cleaned up
    by the same mechanism as other old indices.

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
_deletion lists_ as soon as a Timeline decides to commit to a deletion,
and then executing these in the background at a later time.

_Note: The deletion queue's reason for existence is optimization rather than correctness,
so there is a lot of flexibility in exactly how the it should work,
as long as it obeys the rule to validate generations before executing deletions,
so the following details are not essential to the overall RFC._

#### Scope

The deletion queue will be global per pageserver, not per-tenant. There
are several reasons for this choice:

- Use the queue as a central point to coalesce validation requests to the
  control plane: this avoids individual `Timeline` objects ever touching
  the control plane API, and avoids them having to know the rules about
  validating deletions. This separation of concerns will avoid burdening
  the already many-LoC `Timeline` type with even more responsibility.
- Decouple the deletion queue from Tenant attachment lifetime: we may
  "hibernate" an inactive tenant by tearing down its `Tenant`/`Timeline`
  objects in the pageserver, without having to wait for deletions to be done.
- Amortize the cost of I/O for the persistent queue, instead of having many
  tiny queues.
- Coalesce deletions into a smaller number of larger DeleteObjects calls

Because of the cost of doing I/O for persistence, and the desire to coalesce
generation validation requests across tenants, and coalesce deletions into
larger DeleteObjects requests, there will be one deletion queue per pageserver
rather than one per tenant. This has the added benefit that when deactivating
a tenant, we do not have to drain their deletion queue: deletions can proceed
for a tenant whose main `Tenant` object has been torn down.

#### Flow of deletion

The flow of deletion becomes:

1. Enqueue in memory: build up some deletions to write out a deletion list
2. Enqueue persistently by writing out a deletion list, storing the
   attachment generations for each timeline with layers referenced in the list.
3. Validate the deletion list by calling to the control plane, and persist
   some record that the list is valid (or rewrite it to remove invalid parts).
4. Delete the keys that were enqueued with a generation that passed the validation
   in the previous step

#### Ordering

Deletions may only be persisted to the queue once the remote index_part.json
reflecting the deletion has been written.

If a deletion list is read from local disk after a restart, it is guaranteed
that whatever generation wrote that deletion list has therefore already
uploaded its index_part.json file, and therefore when we started up, we would
have seen that remote metadata if it was in the generation immediately before
our own.

This enables the following reasoning:

- a) If I validate my current generation and the deletion list is in that
  generation, I may execute it.
- b) If I validate my current generation and the deletion list is from
  the immediately preceding generation _and_ that preceding generation
  ran on the same server I am running on, then I may execute it.

The `b` case is the nuanced one, but also the important one: it is what
enables replaying a persistent deletion queue after restart without
having to drop any un-validated entries. After a restart, the main
pageserver startup code may tip off the deletion queue about which timelines
have incremented their attachment generation by exactly one, and are therefore
elegible to validate deletions from the previous generation.

#### Validation

Deletion execution is gated on validation of the generations associated with
each deletion.

We may validate lists as a whole, sequentially number the lists, and track
validation with a "validated sequence number" pointer into the list. To advance it,
some background task would do the following procedure for each deletion list
in order:

1. Scan the DeletionList and aggregate a map of tenant to attachment generation
2. Send a request to the control plane to validate these generations
3. Update the queue as follows:

- If all generations are valid (the usual case) then
  the whole list may be executed. We may efficiently record the result of
  this validation by advancing a "valid sequence" to point to the sequence
  number of the deletion list we just validated.
- If only some contents of a list are valid, then rather than storing
  some structured validation result, we will just re-write the list
  to omit the parts we can't validate, log a warning about the leaked
  objects, and then advance our valid sequence number to point to
  the re-written list.

4. At some later point, actually execute the list that we validated
   in step 3.

#### Deletion queue replay on startup

At startup, the pageserver will replay the lists on disk. Validation
will pick up where it left off: if there are unvalidated lists present,
then it may still be possible to validate them if the timeline's attachment
generation is only 1 greater than that in the list, and it was attached
to this node in its previous generation.

The number of objects leaked in this process depends on how frequently we
do validation during normal operations, and whether the previous pageserver
instance was terminated cleanly and validated its lists in the process. Usually,
we would have shut down cleanly and not leak any objects.

#### Proactive validation-flush

There are some circumstances where it is helpful to intentionally
flush the validation process (i.e. validate all prior deletions):

- On graceful pagegserver shutdown, as we anticipate that some or
  all of our attachments may be re-assigned while we are offline.
- On tenant detach.

This flushing is entirely optional, and may be time-bound: e.g.
if it takes more than 5 seconds to flush during shutdown, just give
up.

#### Deletion queue persistence format

Persistence is to local disk, rather than S3, to avoid any possible
split-brain issues and to avoid having to clean up objects after a pageserver
is decommissioned. However, when decommissioned cleanly, a pageserver
should be requested to drain its deletion queue before we dispose
of it, to avoid leaking some objects. If we unexpectedly lose a pageserver
node and its disk, we will leak some objects: not harmful for correctness,
and to be cleaned up eventually by the [scrubber](#cleaning-up-orphan-objects-scrubbing)

The persisted queue is broken up into a series of lists, written out
once some threshold number of deletions are accumulated. This should
be tuned to target a file size of ~1MB to avoid the expense of
writing and reading many tiny files. The lists are written and read
atomically, to avoid coupling the code too much to use of a local filesystem,
in case we wanted to switch to using an object store in future.

Each deletion list has a sequence number: this records the logical
ordering of the lists, so that we may use sequence numbers to succinctly
store knowledge about up to which point the deletions have been validated.

In addition to the lists themselves, a header file is used
to store state about how far validation has proceeded: if the header's
"valid sequence" has passed a particular list, then that list may
be executed.

Deletion queue files will be stored in a sibling of the `tenants/` directory, and
with the node generation number in the name. The paths would look something like:

```bash
  # Deletion List
  deletion/<node id>/<sequence>-<generation>.list

  # Header object, stores the highest sequence & validated sequence
  deletion/<node id>/header-<generation>
```

Each entry in a deletion list is structured to contain the tenant & timeline,
and the attachment generation.

```rust
/// One of these per deletion list object
struct DeletionList {
  deletions: Vec<DeletionEntry>
}

/// N of these per deletion list
struct DeletionEntry {
  tenant_id: TenantId,
  timeline_id: TimelineId,
  generation: AttachmentGeneration
  keys: Vec<String>,
}
```

#### Operations that may skip the queue

Deletions of an entire timeline are exempt from generation number validation. Once the
control plane sends the deletion request, there is no requirement to retain the readability
of any data within the timeline, and all objects within the timeline path may be deleted
at any time from the control plane's deletion request onwards.

Since deletions of smaller timelines won't have enough objects to compose a full sized
DeleteObjects request, it is still useful to send these through the last part of the
deletion pipeline to coalesce with other executing deletions: to enable this, the
deletion queue should expose two input channels: one for deletions that must be
processed in a generation-aware way, and a fast path for timeline deletions, where
that fast path may skip validation and the persistent queue.

### Cleaning up orphan objects (scrubbing)

An orphan object is any object which is no longer referenced by a running node or by metadata.

Examples of how orphan objects arise:

- A node is doing compaction and writes a layer object, then crashes before it writes the
  index_part.json that references that layer.
- A partition node carries on running for some time, and writes out an unbounded number of
  objects while it believes itself to be the rightful writer for a tenant.
- A pageserver crashes between un-linking an object from the index, and persisting
  the object to its deletion queue.

Orphan objects are functionally harmless, but have a small cost due to S3 capacity consumed. We
may clean them up at some time in the future, but doing a ListObjectsv2 operation and cross
referencing with the latest metadata to identify objects which are not referenced.

Scrubbing will be done only by an attached pageserver (not some third party process), and deletions requested during scrub will go through the same
validation as all other deletions: the attachment generation must be
fresh. This avoids the possibility of a stale pageserver incorrectly
thinking than an object written by a newer generation is stale, and deleting
it.

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
this is unlikely to be needed as the control plane is already an essential & highly available component. Also, having a node re-use an old generation number would complicate
reasoning about the system, as it would break the invariant that one (node_id, generation)
tuple uniquely identifies one process lifetime -- it is not recommended to implement this.

Item 2. is a non-issue operationally: it's harmless to delay deletions, the only impact of objects pending deletion is
the S3 capacity cost.

Item 3. is an issue if safekeepers are low on disk space and the control plane is unavailable for a long time.

For a managed service, the general approach should be to make sure we are monitoring & respond fast enough
that control plane outages are bounded in time. The separation of console and control plane will also help
to keep the control plane itself simple and robust.

We will also implement an "escape hatch" config generation numbers, where in a major disaster outage,
we may manually run pageservers with a hand-selected generation number, so that we can bring them online
independently of a control plane.

### Rollout

Although there is coupling between components, we may deploy most of the new data plane components
independently of the control plane: initially they can just use a static generation number.

#### Phase 1

The pageserver is deployed with some special config to:

- Always act like everything is generation 1 and do not wait for a control plane issued generation on attach
- Skip the places in deletion and remote_consistent_lsn updates where we would call into control plane

#### Phase 2

The control plane changes are deployed: control plane will now track and increment generation numbers.

#### Phase 3

The pageserver is deployed with its control-plane-dependent changes enabled: it will now require
the control plane to service re-attach requests on startup, and handle generation
validation requests.

### On-disk backward compatibility

Backward compatibility with existing data is straightforward:

- When reading the index, we may assume that any layer whose metadata doesn't include
  generations will have a path without generation suffix.
- When locating the index file on attachment, we may use the "fallback" listing path
  and if there is only an index without generation suffix, that is the one we load.

It is not necessary to re-write existing layers: even new index files will be able
to represent generation-less layers.

### On-disk forward compatibility

We will do a two phase rollout, probably over multiple releases because we will naturally
have some of the read-side code ready before the overall functionality is ready:

1. Deploy pageservers which understand the new index format and generation suffixes
   in keys, but do not write objects with generation numbers in the keys.
2. Deploy pageservers that write objects with generation numbers in the keys.

Old pageservers will be oblivious to generation numbers. That means that they can't
read objects with generation numbers in the name. This is why we must
first step must deploy the ability to read, before the second step
starts writing them.

# Frequently Asked Questions

## Why a generation _suffix_ rather than _prefix_?

The choice is motivated by object listing, since one can list by prefix but not
suffix.

In [finding remote indices](#finding-the-remote-indices-for-timelines), we rely
on being able to do a prefix listing for `<tenant>/<timeline>/index_part.json*`:
we don't care about listing layers within a particular generation, and if
we already know the index generation we would GET the index directly.

The converse case of using a generation prefix and listing by generation is
not needed: one could imagine listing by generation while scrubbing (so that
a particular generation's layers could be scrubbed), but this is not part
of normal operations, and the [scrubber](#cleaning-up-orphan-objects-scrubbing) probably won't work that way anyway.

## Wouldn't it be simpler to have a separate deletion queue per timeline?

Functionally speaking, we could. That's how RemoteTimelineClient currently works,
but this approach does not map well to a long-lived persistent queue with
generation validation.

Anything we do per-timeline generates tiny random I/O, on a pageserver with
tens of thousands of timelines operating: to be ready for high scale, we should:

- A) Amortize costs where we can (e.g. a shared deletion queue)
- B) Expect to put tenants into a quiescent state while they're not
  busy: i.e. we shouldn't keep a tenant alive to service its deletion queue.

This was discussed in the [scope](#scope) part of the deletion queue section.

# Appendix A: Examples of use in high availability/failover

The generation numbers proposed in this RFC are adaptable to a variety of different
failover scenarios and models. The sections below sketch how they would work in practice.

### Fast restart of a pageserver

"fast" here means that the restart is done before any other element in the system
has taken action in response to the node being down.

- After restart, the node issues a re-attach request to the control plane, and
  receives new generation numbers for all its attached tenants.
- Tenants may be activated with the generation number in the re-attach response.
- If any of its attachments were in fact stale (i.e. had be reassigned to another
  node while this node was offline), then
  - the re-attach response will inform
    the tenant of this by _not_ incrementing the generation for that attachment.
  - This will implicitly block deletions in the tenant, but as an optimization
    the pageserver should also proactively stop doing S3 uploads when it notices this stale-generation state.
  - The control plane is expected to eventually detach this tenant from the
    pageserver.

### Failure of a pageserver

In this context, read "failure" as the most ambiguous possible case, where
a pageserver is unavailable to clients and control plane, but may still be executing and talking
to S3.

#### Case A: re-attachment to other nodes

1. Let's say node 0 becomes unresponsive in a cluster of three nodes 0, 1, 2.
2. Some external mechanism notices that the node is unavailable and initiates
   movement of all tenants attached to that node to a different node. In
   this example it would mean incrementing the attachment generation
   of all tenants that were attached to node 0, and attaching them
   to node 1 or 2 based on some distribution rule (this might be round
   robin, or capacity based).
3. A tenant which is now attached to node 1 will _also_ still be attached to node
   0, from the perspective of node 0. Node 0 will still be using its old generation,
   node 1 will be using a newer generation.
4. S3 writes will continue from nodes 0 and 1: there will be an index_part.json-00000001
   \_and\* an index_part.json-00000002. Objects written under the old suffix
   after the new attachment was created do not matter from the rest of the system's
   perspective: the endpoints are reading from the new attachment location. Objects
   written by node 0 are just garbage that can be cleaned up at leisure. Node 0 will
   not do any deletions because it can't synchronize with control plane, or if it could, it would be informed that is no longer the most recent generation and hence not do the garbage collection.

Node 0 is not "failed" per-se in this situation: if it becomes responsive again,
then the control plane may detach the tenants that have been re-attached
elsewhere from node 0.

#### Case B: direct node replacement with same node_id and drive

This is the scenario we would experience if running pageservers in some dynamic
VM/container environment that would auto-replace a given node_id when it became
unresponsive, with the node's storage supplied by some network block device
that is attached to the replacement VM/container.

1. Let's say node 0 fails, and there may be some other peers but they aren't relevant.
2. Some external mechanism notices that the node is unavailable, and creates
   a "new node 0" (Node 0b) which is a physically separate server. The original node 0
   (Node 0a) may still be running, because we do not assume the environment fences nodes.
3. On startup, node 0b re-attaches and gets higher attachment generations for
   all tenants.
4. S3 writes continue from nodes 0a and 0b, but the writes do not collide due to different
   generation in the suffix, and the writes from node 0a are not visible to the rest
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

_This section is about a passive reader of S3 pageserver state, not a postgres
read replica_

For historical reads to LSNs below the remote persistent LSN, any node may act as a reader at any
time: remote data is logically immutable data, and the use of deferred deletion in this RFC helps
mitigate the fact that remote data is not _physically_ immutable (i.e. the actual data for a given
page moves around as compaction happens).

A read replica needs to be aware of generations in remote data in order to read the latest
metadata (find the index_part.json with the latest suffix). It may either query this
from the control plane, or find it with ListObjectsv2 request

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
the standbys do not write to S3, they do not need to be assigned generation numbers. When a tenant is
re-attached to a standby location, that would increment the tenant attachment generation and this
would work the same as any other attachment change, but with a warm cache.

## Ephemeral node IDs

This RFC intentionally avoids changing anything fundamental about how pageservers are identified
and registered with the control plane, to avoid coupling the implementation of pageserver split
brain protection with more fundamental changes in the management of the pageservers.

However, we also accommodate the possibility of a future change to fully ephemeral pageserver IDs,
where an attachment would implicitly have a lifetime bounded to one pageserver process lifetime,
as the pageserver ID would change on restart. In this model, separate node and attachment
generations are unnecessary, but the storage format doesn't change: the [generation suffix](#generation-suffix)
may simply be the attachment generation, without any per-node component, as the attachment
generation would change any time an attached node restarted.

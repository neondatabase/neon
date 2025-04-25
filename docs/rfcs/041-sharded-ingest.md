# 
Created on Aug 2024
Implemented on Jan 2025

## Summary

Data in large tenants is split up between multiple pageservers according to key hashes, as
introduced in the [sharding RFC](031-sharding-static.md) and [shard splitting RFC](032-shard-splitting.md).

Whereas currently we send all WAL to all pageserver shards, and each shard filters out the data that it needs,
in this RFC we add a mechanism to filter the WAL on the safekeeper, so that each shard receives
only the data it needs.

This will place some extra CPU load on the safekeepers, in exchange for reducing the network bandwidth
for ingesting WAL back to scaling as O(1) with shard count, rather than O(N_shards).

## Motivation

1. Large databases require higher shard counts.  Whereas currently we run with up to 8 shards for tenants
with a few TB of storage, the next order of magnitude capacity increase will require tens of shards, such
that sending all WAL to all shards is impractical in terms of bandwidth.
2. For contemporary database sizes (~2TB), the pageserver is the bottleneck for ingest: since each
   shard has to decode and process the whole WAL, sharding doesn't fully relieve this bottleneck.  To achieve significantly higher ingest speeds, we need to filter the WAL earlier so that each pageserver
   only has to process relevant parts.

## Non Goals (if relevant)

We do not seek to introduce multiple WALs per timeline, or to share the work of handling a timeline's
WAL across safekeepers (beyond simple 3x replication).  This RFC may be thought of as an incremental
move of the ingestion bottleneck up the stack: instead of high write rates bottlenecking on the
pageserver, they will bottleneck on the safekeeper.

## Impacted components (e.g. pageserver, safekeeper, console, etc)

Safekeeper, pageserver.

There will be no control plane or storage controller coordination needed, as pageservers will directly
indicate their sharding parameters to the safekeeper when subscribing for WAL.

## Proposed implementation

Terminology:
- "Data pages" refers to postgres relation blocks, and SLRU blocks.
- "Metadata pages" refers to everything else the pageserver stores, such as relation sizes and
  directories of relations.

### Phase 1: Refactor ingest

Currently, pageserver ingest code is structured approximately as follows:
1. `handle_walreceiver_connection` reads a stream of binary WAL records off a network
   socket
2. `WalIngest::ingest_record` to translate the record into a series of page-level modifications
3. `DatadirModification` accumulates page updates from several `ingest_record` calls, and when
   its `commit()` method is called, flushes these into a Timeline's open `InMemoryLayer`.

This process currently assumes access to a pageserver `Timeline` throughout `ingest_record` and
from `DatadirModification`, which is used to do read-modify-write cycles on metadata pages
such as relation sizes and the master DBDIR page.  It also assumes that records are ingested
strictly one after the other: they cannot be ingested in parallel because each record assumes
that earlier records' changes have already been applied to `Timeline`.

This code will be refactored to disentangle the simple, fast decode of relation page writes
from the more complex logic for updating internal metadata.  An intermediate representation
called `InterpretedWalRecords` will be introduced.  This is similar to the internal state of
a `DatadirModification`, but does not require access to a Timeline.  Instead of storing
metadata updates as materialized writes to pages, it will accumulate these as abstract operations,
for example rather than including a write to a relation size key, this structure will include
an operation that indicates "Update relation _foo_'s size to the max of its current value and
_bar_", such that these may be applied later to a real Timeline.

The `DatadirModification` will be aware of the `EphemeralFile` format, so that as it accumulates
simple page writes of relation blocks, it can write them directly into a buffer in the serialized
format.  This will avoid the need to later deserialize/reserialize this data when passing the
structure between safekeeper and pageserver.

The new pipeline will be:
1. `handle_walreceiver_connection` reads a stream of binary WAL records off a network
2. A `InterpretedWalRecords` is generated from the incoming WAL records.  This does not
   require a reference to a Timeline.
3. The logic that is current spread between `WalIngest` and `DatadirModification` for updating
   metadata will be refactored to consume the metadata operations from the `InterpretedWalRecords`
   and turn them into literal writes to metadata pages.  This part must be done sequentially.
4. The resulting buffer of metadata page writes is combined with the buffer of relation block
   writes, and written into the `InMemoryLayer`.

Implemented in:
1. https://github.com/neondatabase/neon/pull/9472
2. https://github.com/neondatabase/neon/pull/9504
3. https://github.com/neondatabase/neon/pull/9524

### Phase 2: Decode & filter on safekeeper

In the previous phase, the ingest code was modified to be able to do most of its work without access to
a Timeline: this first stage of ingest simply converts a series of binary wal records into
a buffer of relation/SLRU page writes, and a buffer of abstract metadata writes.

The modified ingest code may be transplanted from pageserver to safekeeper (probably via a
shared crate).  The safekeeper->pageserver network protocol is modified to:
 - in subscription requests, send the `ShardIdentity` from the pageserver to the safekeeper
 - in responses, transmit a `InterpretedWalRecords` instead of a raw `WalRecord`.
 - use the `ShardIdentity` to filter the `ProcessedWalIngest` to relevant content for
   the subscribing shard before transmitting it.

The overall behavior of the pageserver->safekeeper interaction remains the same, in terms of
consistent LSN feedback, and connection management.  Only the payload of the subscriptions
changes, to express an LSN range of WAL as a filtered `ProcessedWalIngest` instead of the
raw data.

The ingest code on the pageserver can now skip the part where it does the first phase of
processing, as it will receive pre-processed, compressed data off the wire.

Note that `InterpretedWalRecord` batches multiple `InterpretedWalRecord(s)` in the same network
message. Safekeeper reads WAL in chunks of 16 blocks and then decodes as many Postgres WAL records
as possible. Each Postgres WAL record maps to one `InterpretedWalRecord` for potentially multiple shards.
Hence, the size of the batch is given by the number of Postgres WAL records that fit in 16 blocks.

The protocol needs to support evolution. Protobuf was chosen here with the view that, in the future,
we may migrate it to GRPC altogether

Implemented in:
1. https://github.com/neondatabase/neon/pull/9746
2. https://github.com/neondatabase/neon/pull/9821

### Phase 3: Fan out interpreted WAL

In the previous phase, the initial processing of WAL was moved to the safekeeper, but it is still
done once for each shard: this will generate O(N_shards) CPU work on the safekeeper (especially
when considering converting to Protobuf format and compression).

To avoid this, we fan-out WAL from one (tenant, timeline, shard) to all other shards subscribed on
the same safekeeper. Under normal operation, the WAL will be read from disk, decoded and interpreted
_only_ once per (safekeeper, timeline).

When the first shard of a sharded timeline subscribes to a given safekeeper a task is spawned
for the WAL reader (`InterpretedWalReader`). This task reads WAL, decodes, interprets it and sends
it to the sender (`InterpretedWalSender`). The sender is a future that is polled from the connection
task. When further shards subscribe on the safekeeper they will attach themselves to the existing WAL reader.
There's two cases to consider:
1. The shard's requested `start_lsn` is ahead of the current position of the WAL reader. In this case, the shard
will start receiving data when the reader reaches that LSN. The intuition here is that there's little to gain
by letting shards "front-run" since compute backpressure is based on the laggard LSN.
2. The shard's requested `start_lsn` is below the current position of the WAL reader. In this case, the WAL reader
gets reset to this requested position (same intuition). Special care is taken such that advanced shards do not receive
interpreted WAL records below their current position.

The approach above implies that there is at most one WAL reader per (tenant, timeline) on a given safekeeper at any point in time.
If this turns out to be operationally problematic, there's a trick we can deploy: `--max-delta-for-fanout` is an optional safekeeper
argument that controls the max absolute delta between a new shard and the current WAL position of the WAL reader. If the absolute
delta is above that value, a new reader is spawned. Note that there's currently no concurrency control on the number of WAL readers,
so it's recommended to use large values to avoid pushing CPU utilisation too high.

Unsharded tenants do not spawn a separate task for the interpreted WAL reader since there's no benefit to it. Instead they poll
the reader and sender concurrently from the connection task.

Shard splits are interesting here because it is the only case when the same shard might have two subscriptions at the same time.
This is handled by giving readers a unique identifier. Both shards will receive the same data while respecting their requested start
position.

Implemented in:
1. https://github.com/neondatabase/neon/pull/10190

## Deployment

Each phase shall be deployed independently. Special care should be taken around protocol changes.

## Observability Tips

* The safekeeper logs the protocol requested by the pageserver
along with the pageserver ID, tenant, timeline and shard: `starting streaming from`.
* There's metrics for the number of wal readers:
  * `safekeeper_wal_readers{kind="task", target=~"pageserver.*"}` gives the number of wal reader tasks for each SK
  * `safekeeper_wal_readers{kind="future", target=~"pageserver.*"}` gives the numer of wal readers polled inline by each SK
  * `safekeeper_interpreted_wal_reader_tasks` gives the number of wal reader tasks per tenant, timeline
* Interesting log lines for the fan-out reader:
  * `Spawning interpreted`: first shard creates the interpreted wal reader
  * `Fanning out`: a subsequent shard attaches itself to an interpreted wal reader
  * `Aborting interpreted`: all senders have finished and the reader task is being aborted

## Future Optimizations

This sections describes some improvement areas which may be revisited in the future.

### Buffering of Interpreted WAL

The interpreted WAL reader may buffer interpreted WAL records in user space to help with serving
subscriptions that are lagging behind the current position of the reader.

Counterpoints:
* Safekeepers serve many thousands of timelines and allocating a buffer for each might be wasteful,
especially given that it would go unused on the happy path.
* WAL is buffered in the kernel page cache. Usually we'd only pay the CPU cost of decoding and interpreting.

### Tweaking the Pagserver Safekeeper Selection Algorithm

We could make the pageserver aware of which safekeeper's already host shards for the timeline along
with their current WAL positions. The pageserver should then prefer safkeepers that are in the same
AZ _and_ already have a shard with a position close to the desired start position.

We currently run one safekeeper per AZ, so the point is mute until that changes.

### Pipelining first ingest phase

The first ingest phase is a stateless transformation of a binary WAL record into a pre-processed
output per shard.  To put multiple CPUs to work, we may pipeline this processing up to some defined buffer
depth.

## Alternatives considered

### Give safekeepers enough state to fully decode WAL

In this RFC, we only do the first phase of ingest on the safekeeper, because this is
the phase that is stateless.  Subsequent changes then happen on the pageserver, with
access to the `Timeline` state.

We could do more work on the safekeeper if we transmitted metadata state to the safekeeper
when subscribing to the WAL: for example, by telling the safekeeper all the relation sizes,
so that it could then generate all the metadata writes for relation sizes.

We avoid doing this for several reasons:
1. Complexity: it's a more invasive protocol change
2. Decoupling: having the safekeeper understand the `ProcessedWalIngest` already somewhat
   infects it with knowledge of the pageserver, but this is mainly an abstract structure
   that describes postgres writes.  However, if we taught the safekeeper about the exact
   way that pageserver deals with metadata keys, this would be a much tighter coupling.
3. Load: once the WAL has been processed to the point that it can be split between shards,
   it is preferable to share out work on the remaining shards rather than adding extra CPU
   load to the safekeeper.

### Do pre-processing on the compute instead of the safekeeper

Since our first stage of ingest is stateless, it could be done at any stage in the pipeline,
all the way up to the compute.

We choose not to do this, because it is useful for the safekeeper to store the raw WAL rather
than just the preprocessed WAL:
- The safekeeper still needs to be able to serve raw WAL back to postgres for e.g. physical replication
- It simplifies our paxos implementation to have the offset in the write log be literally
  the same as the LSN
- Raw WAL must have a stable protocol since we might have to re-ingest it at arbitrary points in the future.
  Storing raw WAL give us more flexibility to evolve the pageserver, safekeeper protocol.

### Do wal pre-processing on shard 0 or a separate service, send it to other shards from there

If we wanted to keep the safekeepers as entirely pure stores of raw WAL bytes, then
we could do the initial decode and shard-splitting in some other location:
- Shard 0 could subscribe to the full WAL and then send writes to other shards
- A new intermediate service between the safekeeper and pageserver could do the splitting.

So why not?
- Extra network hop from shard 0 to the final destination shard
- Clearly there is more infrastructure involved here compared with doing it inline on the safekeeper.
- Safekeepers already have very light CPU load: typical cloud instances shapes with appropriate
  disks for the safekeepers effectively have "free" CPU resources.
- Doing extra work on shard 0 would complicate scheduling of shards on pageservers, because
  shard 0 would have significantly higher CPU load under write workloads than other shards.

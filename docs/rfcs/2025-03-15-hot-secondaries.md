
# Pageserver Hot Secondaries

## Summary

It is proposed to add a new mode for pageserver tenant shard locations,
called "hot secondary", which is able to serve page_service requests but
does not do all the same housekeeping as an attached location, and does
not store any additional data in S3.

There is a stark tradeoff between resource cost and complexity: a very simple solution would be to have multiple full attached locations doing independent I/O, but this RFC proposes some additional complexity to
reduce cost.

## Background

In the [pageserver migration RFC](028-pageserver-migration.md), we introduced the concept of "warm secondaries".  These are pageserver locations that poll remote storage for a _heatmap_ describing which layers they should hold, and then download those layers from S3.  This enables them to rapidly transition into a usable attached location with a warm cache.

Combined with the storage controller's detect of pageserver failures, warm
secondaries enabled high availability of pageservers with a recovery time
objective (RTO) measured in seconds (depends on configured heartbeat frequency) -- occasional cloud instance failures are typically recovered
in well under a minute, without human intervention.

## Purpose

We aim to provide a sub-second RTO for pageserver failures, for mission
critical workloads.  To do this, we should enable the postgres client
to make its own decision about cutting over to a secondary, rather than
waiting for the controller to detect a failure and instruct it to
use a different pageserver.  These secondaries should be maintained
in a continuously readable state rather than requiring explicit activation.

Because low-RTO failover is intrinsically vulnerable to "flapping"/false
positives, reads from such a hot secondary will not "promote" the secondary: we don't want to flap back and forth at millisecond timescales.  Rather, reads will be served by hot secondaries at any time, 
but their transition to an attached (primary) location will still be
managed by the storage controller.

## Design of Hot Location Mode

At a high level, hot locations are basically the same `Tenant` and `Timeline` types as an attached location, but with some behavioral tweaks.  This RFC won't get into code structure details: these changes
may be expressed as different types (more robust) or as different modes
for existing types (less code churn, more complexity).

### Load and ingest

Initially, we may start in the same way as a normal attached location:
by discovering the latest metadata in remote storage and constructing
a LayerMap.

We should also do ingest as normal: subscribing to safekeeper and streaming
writes into ephemeral layer files that are then frozen into L0s.  However,
we do not want to wastefully upload these to S3 (they duplicate what the
attached location is already writing).

### "Virtual" compaction

Clearly ingesting but never uploading or compacting will generate an unbounded stack of L0 layers, unless we do something about it.

To solve this, we may add a special type of compaction that re-reads
from remote storage, updates the layer map to contain all L1
and image layers from the remote metadata, and triggers download of these.

We do not download remote L0s during virtual compaction, because the hot secondary has also been ingesting and generating these, so it would be wasteful.  We just trim any local L0s which are now covered by the L1 high watermark of the remote metadata, and retain any that are still needed to serve reads.

Note that this process is expected to generate some overlaps in LSN space: we might have an L0 that we generated locally which overlaps with an L1 from remote storage.  getpage@lsn logic must handle this, and avoid assuming non-overlapping layers (i.e. having read some deltas from L0, we must not read the same deltas again in an L1, we must remember what LSN we already passed).

The average total network download bandwidth of the hot secondary is equal to the rate at which the attached location generates L1 and image layers, plus the rate at which WAL is generated.

The average total disk write bandwidth is the sum of WAL generation rate plus L1/image generation rate: this is about the same as a normal attached location.  The average disk _read_ bandwidth of a hot secondary is far lower than an attached location because it is not reading back layers to compact them -- layers are only read in periods where the attached location was unavailable, so computes started reading from a hot secondary.

The trigger for virtual compaction can be similar to the existing trigger
for L1 compaction on attached locations: once we build up a deep stack of L0s, then we do virtual compaction to trim it.  This assumes that the attached location has kept up with compaction.  The hot secondary can be
more tolerant of a deeper L0 stack because it is less often serving
reads: for example it might make sense to trigger normal L1 compaction at 10 L0 layers, and trigger shallow compaction at 15 L0 layers, giving a good chance that by the time the hot secondary does compaction, the attached location has already written out some layer files for it to read.

### Handling missing layers/timelines

If an incoming request references a timeline that the hot secondary is
unaware of, it must go read from S3 to determine if the timeline exists, and if so then load it.

The hot secondary should also be tipped off by the storage controller when
timelines are created, so that in normal operation it is aware of timelines
immediately rather than having to load on demand (loading on demand could
have much higher latency for reads).

Hot secondaries may also experience 404s reading layers from remote storage, because the layer might have been deleted by the attached location
during compaction or GC.  If the hot secondary finds such a 404, it should
trigger a re-download of the timeline index.


## Optimisations/details

- We should add a read-only mode to RemoteTimelineClient

## Alternatives considered

### Full mirror

We could make hot secondary locations do all compaction, gc, etc operations
independently, and maintain their own set of layer files in S3.  These would essentially be separate tenants in pageserver terms, but consuming the same safekeeper timelines.

These locations would on longer be anything special in pageserver terms, they'd simply be attached locations that use some modified path like `<tenant_id>.secondary` to avoid colliding with the primary data.

The storage controller could have some `AttachedHotSecondary` placement
policy that configures the hot secondary location with some flag to indicate that the alternative storage path should be used.

Clearly the advantage of this approach is code simplicity.  However, the
downsides are substantial:
- Double object storage costs
- Compaction costs are doubled (CPU & disk read I/O), whereas the proposed
  implementation of hot secondaries only pays twice for the compaction _write_ IO as it writes compacted layers to local disk.

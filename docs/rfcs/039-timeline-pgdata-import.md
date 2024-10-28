# Timelime import from `PGDATA`

Created at: 2024-10-28
Author: Christian Schwarz (@problame)

## Summary

This document is a retroactive RFC for a feature to creating new root timelines
from a vanilla Postgres PGDATA directory dump.

## Motivation & Context

perf hedge

companion RFCs:
* cloud.git RFC that fits everyting together
  * READ THIS FIRST
* importer that produces the PGDATA

## Non-Goals

## Components

this doc constrained to storage pieces; companion RFC in cloud.git fits it into a full system

## Prior Art

hackathon code

## Proposed Implementation

### High-Level

refactor timeline creation (modes) & idempotency checking

add new timeline creation mode for import from pgdata
idempotency checking based on caller-provided idempotency key
creating task does the following
acquire creation guard
create anonymous empty timeline
upload index part to S3, with field indicating import is in progress
spawn tokio task that performs the import (bound to tenant cancellation & guard)
the import task produces layers, adds them to the layer map, uploads them using existing infrastructure
after import task is done
transition index part into `done` state
shut down the timeline object
load the timeline from remote storage, using the code that we use during tenant attach, and add it to Tenant::timelines

if pageserver restarts or the tenant is migrated during import, tenant attach resumes the import job, based on the
Index part field.

SEQUENCE DIAGRAM


### No Storage Controller Awareness

The storage controller is unaware of the import job, since it simply proxies the timeline creation request to each shard.

### PGDATA => Image Layer Conversion

The import task converts a PGDATA directory dump located in object storage into image layers.

shard awareness, including the new test for 0 disposable keys

range requests

### The Mechanics Of Executing The Conversion Code

Tokio Task Lifecycles / New Concept Of Long-Running Timeline Creation

This is the first time we make a timeline creation run long, and need to make it survive PS restarts and tenant migrations.

### Resumability & Forward Progress

Index Part field, document here.
Evoultion toward checkpointing in the index part is possible.

For v1, no checkpointing is implemented because the MTBF of pageserver nodes is much higher than
the highest expected import duration; and tenant migrations are rare enough.

### Resource Usage

Parallelized over multiple tokio tasks

### Interaction with Migrations / Generations

After migrating the tenant / reloading it with a newer generation, what happens?

### Security

The current implementation assumes that the PGDATA dump contents are trusted input,
and further that it does not change while the import is ongoing.

There is no hard technical need for trusting the PGDATA dump contents, since
the bulk of layer conversion is copying opaque 8k data blobs.

However, the conversion code uses serde-generated deserialization routines in some
places which haven't been audited for resource exhaustion attacks (e.g. OOM due to large allcations).

Further, the conversion code is littered with `.unwrap()` and `assert!()` statements, which
is a remnant from the fact that it was hackathon code. This risks panicking the pageserver
that executes the import, and our panic blast radius is currently not reliably constrained
to the tenant in which it originates.

### Progress Notification & Integration Into The Larger System

Timeline create API: no changes about durability semantics of create operation; cplane create
operation is done as soon as API returns 201 (earlier design 202 code, remove that from cplane draft)

Readiness for import: due to limitations in the control plane regarding long-running jobs, cplane
schedules the timeline creation before the PGDATA dunp has actually finished uploading.
As a workaround, the import task polls for a special status key in the PGDATA to appear, which
declares the PGDATA dump as finished uploading and not changing from thereon.
TODO: review linearizability of S3, i.e., can we rely on ListObjectsV2 to return the
full listing immediately after we observe the special status key? Or does the status key
need to contain the list of prefixes? That would be better anyway, could be signed, cf security).

New concept: completion notification; didn't need this before, now we do;
cplane must not use timeline before receiving completion notification.
mechanism:
* source of truth of completion is a special status object that import job uploads into the PGDATA location in S3
* how to make cplane aware of update: invocation of a notification upcall API
* import job retries delivery of the notification upcall until success response
* in response to upcall, cplane collects statuses of alls hards using ListObjectV2 prefix (it understands ShardId)

SEQUENCE DIAGRAM FROM GIST

### Shard-Split during Import is UB

The design does not handle shard-splits during import, so it's declared UB right now.

### Storage-Controller, Sharding

As before this RFC, the storage controller forwards timeline create requests to each
shard and is unaware that the timeline is importing, because so far storcon did not need
to be aware of timelines.

However, to make sharding transparent to cplane, it would make sense for storcon to
act as an intermediate for the import progress progress upcalls, i.e., batch up all
shards' completion upcalls and when that's done, deliver a tenant-scoped (instead of shard-scoped)
upcall to cplane.
That way, cplane need not know about tenants and storcon can internalize shard-split.

### Alternative Designs

#### External Task Performs Conversion

External task performs conversion and uploads layers & index part into S3.

=> copy gist arguments against that here.


## Future Work

### Work Required For Productization

TODOs at the top of import code, esp resource exhaustion attack vectors

Review linearizability of listobjectsv2 after observing pgdata upload done key.

Resource usage: play nice with other tenants

Regression test ensuring resumability (failpoints)

Storcon needs to be aware of import and
- inhibit shard split while ongoing (can relax this later)
- and redesign progress notification system to
  - avoid need to mutate pgdata, so it can be readonly

DESIGN WORK: does storage controller scheduler should be aware of ongoing migration?
=> it should control resource usage, maybe different policy for migrations, etc

Safety: fail shard split while import is in progress. Pageserver should enforce this.
Storcon needs to know and respsect this.

Azure support

### v2

Teach pageserver to natively read PGDATA relation files. Then it could be as simple
as reading a few control files, and otherwise using object copy operations for
moving the relation files.
Problems:
- how to deal with sharding? If we don't slice up the files, then each shard
download a full copy of the database. Tricky. Maybe can handle this with 1GiB stripe size,
but, pretty obscure.


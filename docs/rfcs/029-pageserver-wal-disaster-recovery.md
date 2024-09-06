# Name

Created on: 2023-09-08
Author: Arpad Müller

## Summary

Enable the pageserver to recover from data corruption events by implementing
a feature to re-apply historic WAL records in parallel to the already occurring
WAL replay.

The feature is outside of the user-visible backup and history story, and only
serves as a second-level backup for the case that there is a bug in the
pageservers that corrupted the served pages.

The RFC proposes the addition of two new features:
* recover a broken branch from WAL (downtime is allowed)
* a test recovery system to recover random branches to make sure recovery works

## Motivation

The historic WAL is currently stored in S3 even after it has been replayed by
the pageserver and thus been integrated into the pageserver's storage system.
This is done to defend from data corruption failures inside the pageservers.

However, application of this WAL in the disaster recovery setting is currently
very manual and we want to automate this to make it easier.

### Use cases

There are various use cases for this feature, like:

* The main motivation is replaying in the instance of pageservers corrupting
  data.
* We might want to, beyond the user-visible history features, through our
  support channels and upon customer request, in select instances, recover
  historic versions beyond the range of history that we officially support.
* Running the recovery process in the background for random tenant timelines
  to figure out if there was a corruption of data (we would compare with what
  the pageserver stores for the "official" timeline).
* Using the WAL to arrive at historic pages we can then back up to S3 so that
  WAL itself can be discarded, or at least not used for future replays.
  Again, this sounds a lot like what the pageserver is already doing, but the
  point is to provide a fallback to the service provided by the pageserver.

## Design

### Design constraints

The main design constraint is that the feature needs to be *simple* enough that
the number of bugs are as low, and reliability as high as possible: the main
goal of this endeavour is to achieve higher correctness than the pageserver.

For the background process, we cannot afford a downtime of the timeline that is
being cloned, as we don't want to restrict ourselves to offline tenants only.
In the scenario where we want to recover from disasters or roll back to a
historic lsn through support staff, downtimes are more affordable, and
inevitable if the original had been subject to the corruption. Ideally, the
two code paths would share code, so the solution would be designed for not
requiring downtimes.

### API endpoint changes

This RFC proposes two API endpoint changes in the safekeeper and the
pageserver.

Remember, the pageserver timeline API creation endpoint is to this URL:

```
/v1/tenant/{tenant_id}/timeline/
```

Where `{tenant_id}` is the ID of the tenant the timeline is created for,
and specified as part of the URL. The timeline ID is passed via the POST
request body as the only required parameter `new_timeline_id`.

This proposal adds one optional parameter called
`existing_initdb_timeline_id` to the request's json body. If the parameter
is not specified, behaviour should be as existing, so the pageserver runs
initdb.
If the parameter is specified, it is expected to point to a timeline ID.
In fact that ID might match `new_timeline_id`, what's important is that
S3 storage contains a matching initdb under the URL matching the given
tenant and timeline.

Having both `ancestor_timeline_id` and `existing_initdb_timeline_id`
specified is illegal and will yield in an HTTP error. This feature is
only meant for the "main" branch that doesn't have any ancestors
of its own, as only here initdb is relevant.

For the safekeeper, we propose the addition of the following copy endpoint:

```
/v1/tenant/{tenant_id}/timeline/{source_timeline_id}/copy
```
it is meant for POST requests with json, and the two URL parameters
`tenant_id` and `source_timeline_id`. The json request body contains
the two required parameters `target_timeline_id` and `until_lsn`.

After invoking, the copy endpoint starts a copy process of the WAL from
the source ID to the target ID. The lsn is updated according to the
progress of the API call.

### Higher level features

We want the API changes to support the following higher level features:

* recovery-after-corruption DR of the main timeline of a tenant. This
  feature allows for downtime.
* test DR of the main timeline into a special copy timeline. this feature
  is meant to run against selected production tenants in the background,
  without the user noticing, so it does not allow for downtime.

The recovery-after-corruption DR only needs the pageserver changes.
It works as follows:

* delete the timeline from the pageservers via timeline deletion API
* re-create it via timeline creation API (same ID as before) and set
  `existing_initdb_timeline_id` to the same timeline ID

The test DR requires also the copy primitive and works as follows:

* copy the WAL of the timeline to a new place
* create a new timeline for the tenant

## Non Goals

At the danger of being repetitive, the main goal of this feature is to be a
backup method, so reliability is very important. This implies that other
aspects like performance or space reduction are less important.

### Corrupt WAL

The process suggested by this RFC assumes that the WAL is free of corruption.
In some instances, corruption can make it into WAL, like for example when
higher level components like postgres or the application first read corrupt
data, and then execute a write with data derived from that earlier read. That
written data might then contain the corruption.

Common use cases can hit this quite easily. For example, an application reads
some counter, increments it, and then writes the new counter value to the
database.
On a lower level, the compute might put FPIs (Full Page Images) into the WAL,
which have corrupt data for rows unrelated to the write operation at hand.

Separating corrupt writes from non-corrupt ones is a hard problem in general,
and if the application was involved in making the corrupt write, a recovery
would also involve the application. Therefore, corruption that has made it into
the WAL is outside of the scope of this feature. However, the WAL replay can be
issued to right before the point in time where the corruption occurred. Then the
data loss is isolated to post-corruption writes only.

## Impacted components (e.g. pageserver, safekeeper, console, etc)

Most changes would happen to the pageservers.
For the higher level features, maybe other components like the console would
be involved.

We need to make sure that the shadow timelines are not subject to the usual
limits and billing we apply to existing timelines.

## Proposed implementation

The first problem to keep in mind is the reproducibility of `initdb`.
So an initial step would be to upload `initdb` snapshots to S3.

After that, we'd have the endpoint spawn a background process which
performs the replay of the WAL to that new timeline. This process should
follow the existing workflows as closely as possible, just using the
WAL records of a different timeline.

The timeline created will be in a special state that solely looks for WAL
entries of the timeline it is trying to copy. Once the target LSN is reached,
it turns into a normal timeline that also accepts writes to its own
timeline ID.

### Scalability

For now we want to run this entire process on a single node, and as
it is by nature linear, it's hard to parallelize. However, for the
verification workloads, we can easily start the WAL replay in parallel
for different points in time. This is valuable especially for tenants
with large WAL records.

Compare this with the tricks to make addition circuits execute with
lower latency by making them perform the addition for both possible
values of the carry bit, and then, in a second step, taking the
result for the carry bit that was actually obtained.

The other scalability dimension to consider is the WAL length, which
is a growing question as tenants accumulate changes. There are
possible approaches to this, including creating snapshots of the
page files and uploading them to S3, but if we do this for every single
branch, we lose the cheap branching property.

### Implementation by component

The proposed changes for the various components of the neon architecture
are written up in this notion page:

https://www.notion.so/neondatabase/Pageserver-disaster-recovery-one-pager-4ecfb5df16ce4f6bbfc3817ed1a6cbb2

### Unresolved questions

none known (outside of the mentioned ones).

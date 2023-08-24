# Name

Created on: 2023-08-24
Author: Arpad Müller

## Summary

Enable the pageserver to recover from data corruption disasters by implementing
a feature to re-apply historic WAL records in parallel to the already occurring
WAL replay.

The feature is outside of the user-visible backup and history story, and only
serves as a second-level backup for the case that there is a bug in the
pageservers that corrupted the served pages.

The pageserver can be triggered via an API endpoint to create a new copy of a
timeline, applying the WAL of a different timeline, either from the start,
right after initdb, or from a historic point.

## Motivation

The historic WAL is currently stored in S3 even after it has been replayed by
the pageserver and thus been integrated into the pageserver's storage system.
This is done to defend from data corruption failures inside the pageservers.

However, application of this WAL is currently very manual and we could automate
this to make it easier.

### Use cases

There are various use cases for this feature, like:

* The main motivation is replaying in the instance of pageservers corrupting
  data, which we already try to prevent, but it can occur.
* We might want to, beyond the user-visible history features, through our
  support channels and upon customer request, recover historic versions beyond
  the range of history that we officially support.
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
historic lsn through support staff, downtimes are more affordable. Ideally, the
two code paths would share code, so the solution would be designed for not
requiring downtimes.

### API endpoints

The proposed design of the new pageserver API endpoint is:

```
/v1/tenant/:tenant_id/timeline/:timeline_id/recover?from_lsn=<from_lsn>&to_lsn=<lsn>&target=<target_timeline_id>
```

The `recover` endpoint answers to `POST` requests and issues a WAL recovery
process. It takes the WAL of the specified timeline, and writes its results
into a *different* timeline, the target timeline. This two timeline scheme
ensures that we can stil serve requests and preserves immutability guarantees
assumed by the [generation numbers RFC], as in corruption situations we might
indeed end up with different content than the original timeline we are storing
data from.

Where the parameters are:

* `:tenant_id`: the id of the tenant to recover
* `:timeline_id`: the id of the timeline to recover
* `from_lsn`: The lsn at the start of our recovery process.
  Optional, if not specified the recovery process is fully WAL based.
  If specified, the recovery process starts from data at the *main* timeline
  at the specific lsn (maybe we still trust that lsn). In any case, the target
  timeline must be less advanced than `from_lsn`.
* `to_lsn`: the end of the lsn range we want to recover. As usual, it's an
  exclusive limit. This allows playing back the WAL to a specific moment in the
  past, if this is wanted/needed. E.g. there might be a data-corrupting
  pageserver crash at lsn 1234 and we don't want that so we play back until
  1233.
* `target`: the timeline id we are writing into. The parameter is optional;
  if no target timeline is provided, a new timeline is created.
  One specifies either both `from_lsn` and `target`, or neither.
  Writing into the same timeline is forbidden.

[generation numbers RFC]: https://github.com/neondatabase/neon/pull/4919

### Higher level features

The API endpoint described above allows for a large set of higher level
features to be implemented.

* recovery not just one timeline, but of multiple timelines. Oncluding, if
  wanted, all timelines of the tenant.
* recovery just until a specific moment in time, even if pageserver history is
  completely turned off.
* a repeated job to *detect* errors/corruption, both to verify that the backup
  works and to verify that the backed up original is not corrupted.

## Non Goals

At the danger of being repetitive, the main goal of this feature is to be a
backup method, so reliability is very important. This implies that other
aspects like performance or space reduction are less important.

## Impacted components (e.g. pageserver, safekeeper, console, etc)

Most changes would happen to the pageservers.
We can implement many features on top of this one, in the console.

There is some small billing impact: Due to the nature of many use cases, we
might want to refrain from billing for the created shadow timelines.

## Proposed implementation

The first problem to keep in mind is the reproducability of `initdb`.

### Reliability, failure modes and corner cases (if relevant)

### Interaction/Sequence diagram (if relevant)

### Scalability (if relevant)

### Security implications (if relevant)

### Unresolved questions

## Alternative implementation (if relevant)

## Pros/cons of proposed approaches (if relevant)

## Definition of Done (if relevant)

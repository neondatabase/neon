# Ancestor Timeline Deletion

Created on: 2024-02-23

Author: John Spray

# Summary

When a tenant creates a new timeline that they will treat as their 'main' history,
it is awkward to permanently retain an 'old main' timeline as its ancestor. Currently
this is necessary because it is forbidden to delete a timeline which has descendents.

A new pageserver API is proposed to 'adopt' data from a parent timeline into
one of its children, such that the link between ancestor and child can be severed,
leaving the parent in a state where it may then be deleted.

# Motivation

Retaining parent timelines currently has two costs:

- Cognitive load on users, who have to remember which is the "real" main timeline.
- Storage capacity cost, as the parent timeline will retain layers up to the
  child's timeline point, even if the child fully covers its keyspace with image
  layers and will never actually read from the parent.

# Solution

A new pageserver API `PUT /v1/tenant/:tenant_id/timeline/:timeline_id/detach_ancestor`
will be added. The `timeline_id` in this URL is that of the _child_ timeline that we
wish to detach from its parent.

On success, this API will leave the following state:

- The detached child timeline will no longer have an ancestor, and will contain all
  the data needed to service reads without recursing into an ancestor.
- Any other children of the parent whose timeline points were at a lower LSN than
  the detached child timeline will be modified to have the child timeline as their
  new parent.
- The parent timeline will still exist, but the child will no longer have it as an
  ancestor. If this was the last timeline that depended on the parent, then the
  parent will become deletable.

This API's implementation will consist of a series of retryable steps, such that
on failures/timeout it can safely be called again to reach the target state.

## Example

### Before

The user has "rolled back" their project to LSN X, resulting in a "new main"
timeline. The parent "old main" timeline still exists, and they would like
to clean it up.

They have two other timelines A and B. A is from before the rollback point,
and B is from after the rollback point.

```
----"old main" timeline-------X-------------------------------------------->
                |             |                         |
                |-> child A   |                         |
                              |-> "new main" timeline   |
                                                        -> child B

```

### After calling detach ancestor API

The "new main" timeline is no longer dependent on old main, and neither
is child A, because it had a branch point before X.

The user may now choose to delete child B and "old main" to get to
a pristine state. Child B is likely to be unwanted since the user
chose to roll back to X, and it branches from after X. However, we
don't assume this in the API; it is up to the user to delete it.

```
|----"old main" timeline---------------------------------------------------->
                                                         |
                                                         |
                                                         |
                                                         -> child B

|----"new main" timeline--------->
                 |
                 |-> child A


```

### After removing timelines

We end up with a totally clean state that leaves no trace that a rollback
ever happened: there is only one root timeline.

```
| ----"new main" timeline----------->
                |
                |-> child A


```

## Caveats

Important things for API users to bear in mind:

- this API does not delete the parent timeline: you must still do that explicitly.
- if there are other child timelines ahead of the branch point of the detached
  child, the parent won't be deletable: you must either delete or detach those
  children.
- do _not_ simply loop over all children and detach them all: this can have an
  extremely high storage cost. The detach ancestor API is intended for use on a single
  timeline to make it the new "main".
- The detach ancestor API should also not be
  exposed directly to the user as button/API, because they might decide
  to click it for all the children and thereby generate many copies of the
  parent's data -- the detach ancestor API should be used as part
  of a high level "clean up after rollback" feature.

## `detach_ancestor` API implementation

Terms used in the following sections:

- "the child": the timeline whose ID is specified in the detach ancestor API URL, also
  called "new main" in the example.
- "the parent": the parent of "the child". Also called "old main" in the example.
- "the branch point" the ancestor_lsn of "the child"

### Phase 1: write out adopted layers to S3

The child will "adopt" layers from the parent, such that its end state contains
all the parent's history as well as its own.

For all layers in the parent's layer map whose high LSN is below the branch
point, issue S3 CopyObject requests to duplicate them into the child timeline's
prefix. Do not add them to the child's layer map yet.

For delta layers in the parent's layer map which straddle the branch point, read them
and write out only content up to the branch point into new layer objects.

This is a long running operation if the parent has many layers: it should be
implemented in a way that resumes rather than restarting from scratch, if the API
times out and is called again.

As an optimization, if there are no other timelines that will be adopted into
the child, _and_ the child's image layers already full cover the branch LSN,
then we may skip adopting layers.

### Phase 2: update the child's index

Having written out all needed layers in phase 1, atomically link them all
into the child's IndexPart and upload to S3. This may be done while the
child Timeline is still running.

### Phase 3: modify timelines ancestry

Modify the child's ancestor to None, and upload its IndexPart to persist the change.

For all timelines which have the same parent as the child, and have a branch
point lower than our branch point, switch their ancestor_timeline to the child,
and upload their IndexPart to persist the change.

## Alternatives considered

### Generate full image layer on child, rather than adopting parent deltas

This would work for the case of a single child, but would prevent re-targeting
other timelines that depended on the parent. If we detached many children this
way, the storage cost would become prohibitive (consider a 1TB database with
100 child timelines: it would cost 100TiB if they all generated their own image layers).

### Don't rewrite anything: just fake it in the API

We could add a layer of indirection that let a child "pretend" that it had no
ancestor, when in reality it still had the parent. The pageserver API could
accept deletion of ancestor timelines, and just update child metadata to make
them look like they have no ancestor.

This would not achieve the desired reduction in storage cost, and may well be more
complex to maintain than simply implementing the API described in this RFC.

### Avoid copying objects: enable child index to use parent layers directly

We could teach IndexPart to store a TimelineId for each layer, such that a child
timeline could reference a parent's layers directly, rather than copying them
into the child's prefix.

This would impose a cost for the normal case of indices that only target the
timeline's own layers, add complexity, and break the useful simplifying
invariant that timelines "own" their own path. If child timelines were
referencing layers from the parent, we would have to ensure that the parent
never runs GC/compaction again, which would make the API less flexible (the
proposal in this RFC enables deletion of the parent but doesn't require it.)

## Performance

### Adopting layers

- CopyObject is a relatively cheap operation, but we may need to issue tens of thousands
  of such requests: this can take up to tens of seconds and will compete for RemoteStorage
  semaphore units with other activity on the pageserver.
- If we are running on storage backend that doesn't implement CopyObject, then
  this part will be much more expensive as we would stream all layer content
  through the pageserver. This is no different to issuing a lot
  of reads to a timeline that does not have a warm local cache: it will move
  a lot of gigabytes, but that shouldn't break anything.
- Generating truncated layers for delta that straddle the branch point will
  require streaming read/write of all the layers in question.

### Updating timeline ancestry

The simplest way to update timeline ancestry will probably be to stop and start
all the Timeline objects: this is preferable to the complexity of making their
ancestry mutable at runtime.

There will be a corresponding "stutter" in the availability of the timelines,
of the order 10-100ms, which is the time taken to upload their IndexPart, and
restart the Timeline.

# Interaction with other features

## Concurrent timeline creation

If new historic timelines are created using the parent as an ancestor while the
detach ancestor API is running, they will not be re-parented to the child. This
doesn't break anything, but it leaves the parent in a state where it might not
be possible to delete it.

Since timeline creations are an explicit user action, this is not something we need to
worry about as the storage layer: a user who wants to delete their parent timeline will not create
new children, and if they do, they can choose to delete those children to
enable deleting the parent.

For the least surprise to the user, before starting the detach ancestor branch
operation, the control plane should wait until all branches are created and not
allow any branches to be created before the branch point on the ancestor branch
while the operation is ongoing.

## WAL based disaster recovery

WAL based disaster recovery currently supports only restoring of the main
branch. Enabling WAL based disaster recovery in the future requires that we
keep a record which timeline generated the WAL and at which LSN was a parent
detached. Keep a list of timeline ids and the LSN in which they were detached in
the `index_part.json`. Limit the size of the list to 100 first entries, after
which the WAL disaster recovery will not be possible.

## Sharded tenants

For sharded tenants, calls to the detach ancestor API will pass through the storage
controller, which will handle them the same as timeline creations: invoke first
on shard zero, and then on all the other shards.

# Node deletion API improvement

Created on 2025-07-07
Implemented on _TBD_

## Summary

This RFC describes improvements to the storage controller API for gracefully deleting pageserver
nodes.

## Motivation

The basic node deletion API introduced in [#8226](https://github.com/neondatabase/neon/issues/8333)
has several limitations:

- Deleted nodes can re-add themselves if they restart (e.g., a flaky node that keeps restarting and
we cannot reach via SSH to stop the pageserver). This issue has been resolved by tombstone
mechanism in [#12036](https://github.com/neondatabase/neon/issues/12036)
- Process of node deletion is not graceful, i.e. it just imitates a node failure, which can cause
issues for user processes

## Impacted components (e.g. pageserver, safekeeper, console, etc)

- storage controller
- pageserver (indirectly)

## Proposed implementation

### Tombstones

To resolve the problem of deleted nodes re-adding themselves, a tombstone mechanism was introduced
as part of the node stored information. Each node has a separate `NodeLifecycle` field with two
possible states: `Active` and `Deleted`. When node deletion completes, the database row is not
deleted but instead has its `NodeLifecycle` column switched to `Deleted`. Nodes with `Deleted`
lifecycle are treated as if the row is absent for most handlers, with several exceptions: reattach
and register functionality must be aware of tombstones. Additionally, new debug handlers are
available for listing and deleting tombstones via the `/debug/v1/tombstone` path.

### Gracefulness

The problem of making node deletion graceful is complex and involves several challenges:

- **Cancellable**: Since graceful deletion can be time-consuming, the operation must be cancellable
to allow administrators to abort the process if needed, e.g. if run by mistake.
- **Non-blocking**: Since graceful deletion can be time-consuming, we don't want to block
deployment operations like draining/filling on the node deletion process. We need clear policies
for handling concurrent operations: what happens when a drain/fill request arrives while deletion
is in progress, and what happens when a delete request arrives while drain/fill is in progress.
- **Persistent**: If the storage controller restarts during this long-running operation, we must
preserve progress and automatically resume the deletion process after the storage controller
restarts.
- **Migrated correctly**: We cannot simply use the existing drain mechanism for nodes scheduled for
deletion, as this would move shards to irrelevant locations. This could result in unnecessary load
on the storage controller and inefficient resource utilization.
- **Context-aware**: We need to improve our scheduling context awareness mechanism beyond the basic
API to correctly calculate preferred node when using non-locked state operations.
- **Force option**: Administrators need the ability to force immediate, non-graceful deletion when
time constraints or emergency situations require it, bypassing the normal graceful migration
process.

The proposed solution addresses these challenges through the following components:

- **New NodeLifecycle**: Introduce `NodeLifecycle::ScheduledForDeletion` to track nodes pending
deletion. This state persists across storage controller restarts and prevents re-registration.
- **New NodeSchedulingPolicy**: Introduce `NodeSchedulingPolicy::Deleting` to track the current
deletion process if any.
- **OperationTracker**: A service state object that manages all long-running operations with the
following capabilities:
  - Execute operations (drain/delete/fill) with proper concurrency control
  - Cancel ongoing operation
  - Queue deletion intents when there are another running operations
  - Enforce operation rules (e.g., only one drain/fill operation at a time)
  - Maintain persistent intent tracking for queued deletions
  - Provide better thread safety than the existing `Option<OperationHandler>` approach
- **Fast cancellation**: Implement fast cancellation for delete operations to prevent blocking
deployment operations.

### Reliability, failure modes and corner cases

In case of a storage controller failure and following restart, the system behavior depends on the
`NodeLifecycle` state:

- If `NodeLifecycle` is `Active`: No action is taken for this node.
- If `NodeLifecycle` is `Deleted`: The node will not be re-added.
- If `NodeLifecycle` is `ScheduledForDeletion`: A deletion background task will be launched for
this node. Since only one ongoing deletion task per storage controller is allowed, if there are
multiple nodes with `ScheduledForDeletion` state, additional deletion tasks will be queued.

In case of a pageserver node failure during deletion, the behavior depends on the `force` flag:
- If `force` is set: The node deletion will proceed regardless of the node's availability.
- If `force` is not set: The deletion will be cancelled after some number of retry attempts.

### Operations concurrency

The following sections describe the behavior when different types of requests arrive at the storage
controller and how they interact with ongoing operations.

#### Delete request

1. If node lifecycle is `NodeLifecycle::ScheduledForDeletion`:
    - Return `409 Conflict`: there is already an ongoing deletion request for this node
2. Update & persist lifecycle to `NodeLifecycle::ScheduledForDeletion`
3. If there is no active operation (drain/fill/delete):
    - Run deletion process for this node

#### Cancel delete request

1. If node lifecycle is not `NodeLifecycle::ScheduledForDeletion`:
    - Return `404 Not Found`: there is no current deletion request for this node
2. If the active operation is deleting this node, cancel it
3. Update & persist lifecycle to `NodeLifecycle::Active`
4. If there is no active operation (drain/fill/delete):
    - Try to find another candidate to delete and run the deletion process for that node

#### Drain/fill request

1. If there are already ongoing drain/fill processes:
    - Return `409 Conflict`: queueing of drain/fill processes is not supported
2. If there is an ongoing delete process:
    - Cancel it and wait until it is cancelled
3. Run the drain/fill process
4. After the drain/fill process is cancelled or finished:
    - Try to find another candidate to delete and run the deletion process for that node

#### Drain/fill cancel request

1. If the active operation is not the related process:
    - Return `400 Bad Request`: cancellation request is incorrect, operations are not the same
2. Cancel the active operation
3. Try to find another candidate to delete and run the deletion process for that node

### Unresolved questions

- Should fill operations be treated differently than drain operations when processing deletion? For
example, should a fill operation be cancelled immediately when a deletion is requested, while drain
operations might be allowed to complete first?

## Definition of Done

- [x] Fix flaky node scenario and introduce related debug handlers
- [ ] Node deletion intent is persistent - a node will be eventually deleted after a deletion
request regardless of draining/filling requests and restarts
- [ ] Node deletion can be graceful - deletion completes only after moving all tenant shards to
recommended locations
- [ ] Deploying does not break due to long deletions - drain/fill operations override deletion
process and deletion resumes after drain/fill completes
- [ ] `force` flag is implemented and provides fast, failure-tolerant node removal (e.g., when a
pageserver node does not respond)
- [ ] Legacy delete handler code is removed from storage_controller, test_runner, and storcon_cli

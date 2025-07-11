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
- Process of node deletion is not graceful, i.e. it just imitates a node failure

In this context, "graceful" node deletion means that users do not experience any disruption or
negative effects, provided the system remains in a healthy state (i.e., the remaining pageservers
can handle the workload and all requirements are met). To achieve this, the system must perform
live migration of all tenant shards from the node being deleted while the node is still running
and continue processing all incoming requests. The node is removed only after all tenant shards
have been safely migrated.

If we delete a node before its tenant shards are fully moved, the new node won't have all the
needed data (e.g. heatmaps) ready. This means user requests to the new node will be much slower at
first. If there are many tenant shards, this slowdown affects a huge amount of users.

Graceful node deletion is more complicated and can introduce new issues. It takes longer because
live migration of each tenant shard can last several minutes. Using non-blocking accessors may
also cause deletion to wait if other processes are holding inner state lock. It also gets trickier
because we need to handle other requests, like drain and fill, at the same time.

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

- **Cancellable**: The operation must be cancellable to allow administrators to abort the process
if needed, e.g. if run by mistake.
- **Non-blocking**: We don't want to block deployment operations like draining/filling on the node
deletion process. We need clear policies for handling concurrent operations: what happens when a
drain/fill request arrives while deletion is in progress, and what happens when a delete request
arrives while drain/fill is in progress.
- **Persistent**: If the storage controller restarts during this long-running operation, we must
preserve progress and automatically resume the deletion process after the storage controller
restarts.
- **Migrated correctly**: We cannot simply use the existing drain mechanism for nodes scheduled
for deletion, as this would move shards to irrelevant locations. The drain process expects the
node to return, so it only moves shards to backup locations, not to their preferred AZs. It also
leaves secondary locations unmoved. This could result in unnecessary load on the storage
controller and inefficient resource utilization.
- **Force option**: Administrators need the ability to force immediate, non-graceful deletion when
time constraints or emergency situations require it, bypassing the normal graceful migration
process.

See below for a detailed breakdown of the proposed changes and mechanisms.

#### Node lifecycle

New `NodeLifecycle` enum and a matching database field with these values:
- `Active`: The normal state. All operations are allowed.
- `ScheduledForDeletion`: The node is marked to be deleted soon. Deletion may be in progress or
will happen later, but the node will eventually be removed. All operations are allowed.
- `Deleted`: The node is fully deleted. No operations are allowed, and the node cannot be brought
back. The only action left is to remove its record from the database. Any attempt to register a
node in this state will fail.

This state persists across pageserver restarts.

**State transition**
```
        +--------------------+
    +---|       Active       |<---------------------+
    |   +--------------------+                      |
    |                     ^                         |
    | start_node_delete   | cancel_node_delete      |
    v                     |                         |
  +----------------------------------+              |
  |       ScheduledForDeletion       |              |
  +----------------------------------+              |
       |                                            |
       |                              node_register |
       |                                            |
       | delete_node (at the finish)                |
       |                                            |
       v                                            |
  +---------+         tombstone_delete        +----------+
  | Deleted |-------------------------------->|  no row  |
  +---------+                                 +----------+
```

#### NodeSchedulingPolicy::Deleting

A `Deleting` variant to the `NodeSchedulingPolicy` enum. This means the deletion function is
running for the node right now. Only one node can have the `Deleting` policy at a time.

The `NodeSchedulingPolicy::Deleting` state is persisted in the database. However, after a storage
controller restart, any node previously marked as `Deleting` will have its scheduling policy reset
to `Pause`. The policy will only transition back to `Deleting` when the deletion operation is
actively started again, as triggered by the node's `NodeLifecycle::ScheduledForDeletion` state.

`NodeSchedulingPolicy` transition details:
1. When `node_delete` begins, set the policy to `NodeSchedulingPolicy::Deleting`.
2. If `node_delete` is cancelled (for example, due to a concurrent drain operation), revert the
policy to its previous value.
3. After `node_delete` completes, the final value of the scheduling policy is irrelevant, since
`NodeLifecycle::Deleted` prevents any further access to this field.

The deletion process cannot be initiated for nodes currently undergoing deployment-related
operations (`Draining`, `Filling`, or `PauseForRestart` policies). Deletion will only be triggered
once the node transitions to either the `Active` or `Pause` state.

#### OperationTracker

A replacement for `Option<OperationHandler> ongoing_operation`, the `OperationTracker` is a
dedicated service state object responsible for managing all long-running node operations (drain,
fill, delete) with robust concurrency control.

Key responsibilities:
- Orchestrates the execution of operations
- Supports cancellation of currently running operations
- Queues delete operations if another operation is already in progress
- Enforces operation constraints, e.g. allowing only single drain/fill operation at a time
- Persists deletion state, enabling recovery of pending deletions across restarts
- Ensures thread safety across concurrent requests

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
- If `force` is not set: The deletion will be retried a limited number of times. If the node
remains unavailable, the deletion process will pause and automatically resume when the node
becomes healthy again.

### Operations concurrency

The following sections describe the behavior when different types of requests arrive at the storage controller and how they interact with ongoing operations.

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

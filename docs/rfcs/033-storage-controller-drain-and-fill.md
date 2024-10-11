# Graceful Restarts of Storage Controller Managed Clusters

## Summary
This RFC describes new storage controller APIs for draining and filling tenant shards from/on pageserver nodes.
It also covers how these new APIs should be used by an orchestrator (e.g. Ansible) in order to implement
graceful cluster restarts.

## Motivation

Pageserver restarts cause read availablity downtime for tenants.

For example pageserver-3 @ us-east-1 was unavailable for a randomly
picked tenant (which requested on-demand activation) for around 30 seconds
during the restart at 2024-04-03 16:37 UTC.

Note that lots of shutdowns on loaded pageservers do not finish within the
[10 second systemd enforced timeout](https://github.com/neondatabase/infra/blob/0a5280b383e43c063d43cbf87fa026543f6d6ad4/.github/ansible/systemd/pageserver.service#L16). This means we are shutting down without flushing ephemeral layers
and have to reingest data in order to serve requests after restarting, potentially making first request latencies worse.

This problem is not yet very acutely felt in storage controller managed pageservers since
tenant density is much lower there. However, we are planning on eventually migrating all
pageservers to storage controller management, so it makes sense to solve the issue proactively.

## Requirements

- Pageserver re-deployments cause minimal downtime for tenants
- The storage controller exposes HTTP API hooks for draining and filling tenant shards
from a given pageserver. Said hooks can be used by an orchestrator proces or a human operator.
- The storage controller exposes some HTTP API to cancel draining and filling background operations.
- Failures to drain or fill the node should not be fatal. In such cases, cluster restarts should proceed
as usual (with downtime).
- Progress of draining/filling is visible through metrics

## Non Goals

- Integration with the control plane
- Graceful restarts for large non-HA tenants.

## Impacted Components

- storage controller
- deployment orchestrator (i.e. Ansible)
- pageserver (indirectly)

## Terminology

** Draining ** is the process through which all tenant shards that can be migrated from a given pageserver
are distributed across the rest of the cluster.

** Filling ** is the symmetric opposite of draining. In this process tenant shards are migrated onto a given
pageserver until the cluster reaches a resonable, quiescent distribution of tenant shards across pageservers.

** Node scheduling policies ** act as constraints to the scheduler. For instance, when a
node is set in the `Paused` policy, no further shards will be scheduled on it.

** Node ** is a pageserver. Term is used interchangeably in this RFC.

** Deployment orchestrator ** is a generic term for whatever drives our deployments.
Currently, it's an Ansible playbook.

## Background

### Storage Controller Basics (skip if already familiar)

Fundamentally, the storage controller is a reconciler which aims to move from the observed mapping between pageservers and tenant shards to an intended mapping. Pageserver nodes and tenant shards metadata is durably persisted in a database, but note that the mapping between the two entities is not durably persisted. Instead, this mapping (*observed state*) is constructed at startup by sending `GET location_config` requests to registered pageservers.

An internal scheduler maps tenant shards to pageservers while respecting certain constraints. The result of scheduling is the *intent state*. When the intent state changes, a *reconciliation* will inform pageservers about the new assigment via `PUT location_config` requests and will notify the compute via the configured hook.

### Background Optimizations

The storage controller performs scheduling optimizations in the background. It will
migrate attachments to warm secondaries and replace secondaries in order to balance
the cluster out.

### Reconciliations Concurrency Limiting

There's a hard limit on the number of reconciles that the storage controller
can have in flight at any given time. To get an idea of scales, the limit is
128 at the time of writing.

## Implementation

Note: this section focuses on the core functionality of the graceful restart process.
It doesn't neccesarily describe the most efficient approach. Optimizations are described
separately in a later section.

### Overall Flow

This section describes how to implement graceful restarts from the perspective
of Ansible, the deployment orchestrator. Pageservers are already restarted sequentially.
The orchestrator shall implement the following epilogue and prologue steps for each
pageserver restart:

#### Prologue

The orchestrator shall first fetch the pageserver node id from the control plane or
the pageserver it aims to restart directly. Next, it issues an HTTP request
to the storage controller in order to start the drain of said pageserver node.
All error responses are retried with a short back-off. When a 202 (Accepted)
HTTP code is returned, the drain has started. Now the orchestrator polls the
node status endpoint exposed by the storage controller in order to await the
end of the drain process. When the `policy` field of the node status response
becomes `PauseForRestart`, the drain has completed and the orchestrator can
proceed with restarting the pageserver.

The prologue is subject to an overall timeout. It will have a value in the ballpark
of minutes. As storage controller managed pageservers become more loaded this timeout
will likely have to increase.

#### Epilogue

After restarting the pageserver, the orchestrator issues an HTTP request
to the storage controller to kick off the filling process. This API call
may be retried for all error codes with a short backoff. This also serves
as a synchronization primitive as the fill will be refused if the pageserver
has not yet re-attached to the storage controller. When a 202(Accepted) HTTP
code is returned, the fill has started. Now the orchestrator polls the node
status endpoint exposed by the storage controller in order to await the end of
the filling process. When the `policy` field of the node status response becomes
`Active`, the fill has completed and the orchestrator may proceed to the next pageserver.

Again, the epilogue is subject to an overall timeout. We can start off with
using the same timeout as for the prologue, but can also consider relying on
the storage controller's background optimizations with a shorter timeout.

In the case that the deployment orchestrator times out, it attempts to cancel
the fill. This operation shall be retried with a short back-off. If it ultimately
fails it will require manual intervention to set the nodes scheduling policy to
`NodeSchedulingPolicy::Active`. Not doing that is not immediately problematic,
but it constrains the scheduler as mentioned previously.

### Node Scheduling Policy State Machine

The state machine below encodes the behaviours discussed above and
the various failover situations described in a later section.

Assuming no failures and/or timeouts the flow should be:
`Active -> Draining -> PauseForRestart -> Active -> Filling -> Active`

```
                          Operator requested drain
               +-----------------------------------------+
               |                                         |
       +-------+-------+                         +-------v-------+
       |               |                         |               |
       |     Pause     |             +----------->    Draining   +----------+
       |               |             |           |               |          |
       +---------------+             |           +-------+-------+          |
                                     |                   |                  |
                                     |                   |                  |
                      Drain requested|                   |                  |
                                     |                   |Drain complete    | Drain failed
                                     |                   |                  | Cancelled/PS reattach/Storcon restart
                                     |                   |                  |
                             +-------+-------+           |                  |
                             |               |           |                  |
               +-------------+    Active     <-----------+------------------+
               |             |               |           |
Fill requested |             +---^---^-------+           |
               |                 |   |                   |
               |                 |   |                   |
               |                 |   |                   |
               |   Fill completed|   |                   |
               |                 |   |PS reattach        |
               |                 |   |after restart      |
       +-------v-------+         |   |           +-------v-------+
       |               |         |   |           |               |
       |    Filling    +---------+   +-----------+PauseForRestart|
       |               |                         |               |
       +---------------+                         +---------------+
```

### Draining/Filling APIs

The storage controller API to trigger the draining of a given node is:
`PUT /v1/control/node/:node_id/{drain,fill}`.

The following HTTP non-success return codes are used.
All of them are safely retriable from the perspective of the storage controller.
- 404: Requested node was not found
- 503: Requested node is known to the storage controller, but unavailable
- 412: Drain precondition failed: there is no other node to drain to or the node's schedulling policy forbids draining
- 409: A {drain, fill} is already in progress. Only one such background operation
is allowed per node.

When the drain is accepted and commenced a 202 HTTP code is returned.

Drains and fills shall be cancellable by the deployment orchestrator or a
human operator via: `DELETE /v1/control/node/:node_id/{drain,fill}`. A 200
response is returned when the cancelation is successful. Errors are retriable.

### Drain Process

Before accpeting a drain request the following validations is applied:
* Ensure that the node is known the storage controller
* Ensure that the schedulling policy is `NodeSchedulingPolicy::Active` or `NodeSchedulingPolicy::Pause`
* Ensure that another drain or fill is not already running on the node
* Ensure that a drain is possible (i.e. check that there is at least one
schedulable node to drain to)

After accepting the drain, the scheduling policy of the node is set to
`NodeSchedulingPolicy::Draining` and persisted in both memory and the database.
This disallows the optimizer from adding or removing shards from the node which
is desirable to avoid them racing.

Next, a separate Tokio task is spawned to manage the draining. For each tenant
shard attached to the node being drained, demote the node to a secondary and
attempt to schedule the node away. Scheduling might fail due to unsatisfiable
constraints, but that is fine. Draining is a best effort process since it might
not always be possible to cut over all shards.

Importantly, this task manages the concurrency of issued reconciles in order to
avoid drowning out the target pageservers and to allow other important reconciles
to proceed.

Once the triggered reconciles have finished or timed out, set the node's scheduling
policy to `NodeSchedulingPolicy::PauseForRestart` to signal the end of the drain.

A note on non HA tenants: These tenants do not have secondaries, so by the description
above, they would not be migrated. It makes sense to skip them (especially the large ones)
since, depending on tenant size, this might be more disruptive than the restart since the
pageserver we've moved to do will need to on-demand download the entire working set for the tenant.
We can consider expanding to small non-HA tenants in the future.

### Fill Process

Before accpeting a fill request the following validations is applied:
* Ensure that the node is known the storage controller
* Ensure that the schedulling policy is `NodeSchedulingPolicy::Active`.
This is the only acceptable policy for the fill starting state. When a node re-attaches,
it set the scheduling policy to `NodeSchedulingPolicy::Active` if it was equal to
`NodeSchedulingPolicy::PauseForRestart` or `NodeSchedulingPolicy::Draining` (possible end states for a node drain).
* Ensure that another drain or fill is not already running on the node

After accepting the drain, the scheduling policy of the node is set to
`NodeSchedulingPolicy::Filling` and persisted in both memory and the database.
This disallows the optimizer from adding or removing shards from the node which
is desirable to avoid them racing.

Next, a separate Tokio task is spawned to manage the draining. For each tenant
shard where the filled node is a secondary, promote the secondary. This is done
until we run out of shards or the counts of attached shards become balanced across
the cluster.

Like for draining, the concurrency of spawned reconciles is limited.

### Failure Modes & Handling

Failures are generally handled by transition back into the `Active`
(neutral) state. This simplifies the implementation greatly at the
cost of adding transitions to the state machine. For example, we
could detect the `Draining` state upon restart and proceed with a drain,
but how should the storage controller know that's what the orchestrator
needs still?

#### Storage Controller Crash

When the storage controller starts up reset the node scheduling policy
of all nodes in states `Draining`, `Filling` or `PauseForRestart` to
`Active`. The rationale is that when the storage controller restarts,
we have lost context of what the deployment orchestrator wants. It also
has the benefit of making things easier to reason about.

#### Pageserver Crash During Drain

The pageserver will attempt to re-attach during restart at which
point the node scheduling policy will be set back to `Active`, thus
reenabling the scheduler to use the node.

#### Non-drained Pageserver Crash During Drain

What should happen when a pageserver we are draining to crashes during the
process. Two reasonable options are: cancel the drain and focus on the failover
*or* do both, but prioritise failover. Since the number of concurrent reconciles
produced by drains/fills are limited, we get the later behaviour for free.
My suggestion is we take this approach, but the cancellation option is trivial
to implement as well.

#### Pageserver Crash During Fill

The pageserver will attempt to re-attach during restart at which
point the node scheduling policy will be set back to `Active`, thus
reenabling the scheduler to use the node.

#### Pageserver Goes unavailable During Drain/Fill

The drain and fill jobs handle this by stopping early. When the pageserver
is detected as online by storage controller heartbeats, reset its scheduling
policy to `Active`. If a restart happens instead, see the pageserver crash
failure mode.

#### Orchestrator Drain Times Out

Orchestrator will still proceed with the restart.
When the pageserver re-attaches, the scheduling policy is set back to
`Active`.

#### Orchestrator Fill Times Out

Orchestrator will attempt to cancel the fill operation. If that fails,
the fill will continue until it quiesces and the node will be left
in the `Filling` scheduling policy. This hinders the scheduler, but is
otherwise harmless. A human operator can handle this by setting the scheduling
policy to `Active`, or we can bake in a fill timeout into the storage controller.

## Optimizations

### Location Warmth

When cutting over to a secondary, the storage controller will wait for it to
become "warm" (i.e. download enough of the tenants data). This means that some
reconciliations can take significantly longer than others and hold up precious
reconciliations units. As an optimization, the drain stage can only cut over
tenants that are already "warm". Similarly, the fill stage can prioritise the
"warmest" tenants in the fill.

Given that the number of tenants by the storage controller will be fairly low
for the foreseable future, the first implementation could simply query the tenants
for secondary status. This doesn't scale well with increasing tenant counts, so
eventually we will need new pageserver API endpoints to report the sets of
"warm" and "cold" nodes.

## Alternatives Considered

### Draining and Filling Purely as Scheduling Constraints

At its core, the storage controller is a big background loop that detects changes
in the environment and reacts on them. One could express draining and filling
of nodes purely in terms of constraining the scheduler (as opposed to having
such background tasks).

While theoretically nice, I think that's harder to implement and more importantly operate and reason about.
Consider cancellation of a drain/fill operation. We would have to update the scheduler state, create
an entirely new schedule (intent state) and start work on applying that. It gets trickier if we wish
to cancel the reconciliation tasks spawned by drain/fill nodes. How would we know which ones belong
to the conceptual drain/fill? One could add labels to reconciliations, but it gets messy in my opinion.

It would also mean that reconciliations themselves have side effects that persist in the database
(persist something to the databse when the drain is done), which I'm not conceptually fond of.

## Proof of Concept

This RFC is accompanied by a POC which implements nearly everything mentioned here
apart from the optimizations and some of the failure handling:
https://github.com/neondatabase/neon/pull/7682

# Rolling Storage Controller Restarts

## Summary

This RFC describes the issues around the current storage controller restart procedure
and describes an implementation which reduces downtime to a few milliseconds on the happy path.

## Motivation

Storage controller upgrades (restarts, more generally) can cause multi-second availability gaps.
While the storage controller does not sit on the main data path, it's generally not acceptable
to block management requests for extended periods of time (e.g. https://github.com/neondatabase/neon/issues/8034).

### Current Implementation

The storage controller runs in a Kubernetes Deployment configured for one replica and strategy set to [Recreate](https://kubernetes.io/docs/concepts/workloads/controllers/deployment/#recreate-deployment).
In non Kubernetes terms, during an upgrade, the currently running storage controller is stopped and, only after,
a new instance is created.

At start-up, the storage controller calls into all the pageservers it manages (retrieved from DB) to learn the
latest locations of all tenant shards present on them. This is usually fast, but can push into tens of seconds
under unfavourable circumstances: pageservers are heavily loaded or unavailable.

## Prior Art

There's probably as many ways of handling restarts gracefully as there are distributed systems. Some examples include:
* Active/Standby architectures: Two or more instance of the same service run, but traffic is only routed to one of them.
For fail-over, traffic is routed to one of the standbys (which becomes active).
* Consensus Algorithms (Raft, Paxos and friends): The part of consensus we care about here is leader election: peers communicate to each other
and use a voting scheme that ensures the existence of a single leader (e.g. Raft epochs).

## Requirements

* Reduce storage controller unavailability during upgrades to milliseconds
* Minimize the interval in which it's possible for more than one storage controller
to issue reconciles.
* Have one uniform implementation for restarts and upgrades
* Fit in with the current Kubernetes deployment scheme

## Non Goals

* Implement our own consensus algorithm from scratch
* Completely eliminate downtime storage controller downtime. Instead we aim to reduce it to the point where it looks
like a transient error to the control plane

## Impacted Components

* storage controller
* deployment orchestration (i.e. Ansible)
* helm charts

## Terminology

* Observed State: in-memory mapping between tenant shards and their current pageserver locations - currently built up
at start-up by quering pageservers
* Deployment: Kubernetes [primitive](https://kubernetes.io/docs/concepts/workloads/controllers/deployment/) that models
a set of replicas

## Implementation

### High Level Flow

At a very high level the proposed idea is to start a new storage controller instance while
the previous one is still running and cut-over to it when it becomes ready. The new instance,
should coordinate with the existing one and transition responsibility gracefully. While the controller
has built in safety against split-brain situations (via generation numbers), we'd like to avoid such
scenarios since they can lead to availability issues for tenants that underwent changes while two controllers
were operating at the same time and require operator intervention to remedy.

### Kubernetes Deployment Configuration

On the Kubernetes configuration side, the proposal is to update the storage controller `Deployment`
to use `spec.strategy.type = RollingUpdate`, `spec.strategy.rollingUpdate.maxSurge=1` and `spec.strategy.maxUnavailable=0`.
Under the hood, Kubernetes creates a new replica set and adds one pod to it (`maxSurge=1`). The old replica set does not
scale down until the new replica set has one replica in the ready state (`maxUnavailable=0`).

The various possible failure scenarios are investigated in the [Handling Failures](#handling-failures) section.

### Storage Controller Start-Up

This section describes the primitives required on the storage controller side and the flow of the happy path.

#### Database Table For Leader Synchronization

A new table should be added to the storage controller database for leader synchronization during startup.
This table will always contain at most one row. The proposed name for the table is `leader` and the schema
contains two elements:
* `hostname`: represents the hostname for the current storage controller leader - should be addressible
from other pods in the deployment
* `start_timestamp`: holds the start timestamp for the current storage controller leader (UTC timezone) - only required
for failure case handling: see [Previous Leader Crashes Before New Leader Readiness](#previous-leader-crashes-before-new-leader-readiness)

Storage controllers will read the leader row at start-up and then update it to mark themselves as the leader
at the end of the start-up sequence. We want compare-and-exchange semantics for the update: avoid the
situation where two concurrent updates succeed and overwrite each other. The default Postgres isolation
level is `READ COMMITTED`, which isn't strict enough here. This update transaction should use at least `REPEATABLE
READ` isolation level in order to [prevent lost updates](https://www.interdb.jp/pg/pgsql05/08.html). Currently,
the storage controller uses the stricter `SERIALIZABLE` isolation level for all transactions. This more than suits
our needs here.

```
START TRANSACTION ISOLATION LEVEL REPEATABLE READ
UPDATE leader SET hostname=<new_hostname>, start_timestamp=<new_start_ts>
WHERE hostname=<old_hostname>, start_timestampt=<old_start_ts>;
```

If the transaction fails or if no rows have been updated, then the compare-and-exchange is regarded as a failure.

#### Step Down API

A new HTTP endpoint should be added to the storage controller: `POST /control/v1/step_down`. Upon receiving this
request the leader cancels any pending reconciles and goes into a mode where it replies with 503 to all other APIs
and does not issue any location configurations to its pageservers. The successful HTTP response will return a serialized
snapshot of the observed state.

If other step down requests come in after the initial one, the request is handled and the observed state is returned (required
for failure scenario handling - see [Handling Failures](#handling-failures)).

#### Graceful Restart Happy Path

At start-up, the first thing the storage controller does is retrieve the sole row from the new
`leader` table. If such an entry exists, send a `/step_down` PUT API call to the current leader.
This should be retried a few times with a short backoff (see [1]). The aspiring leader loads the
observed state into memory and the start-up sequence proceeds as usual, but *without* querying the
pageservers in order to build up the observed state.

Before doing any reconciliations or persistence change, update the `leader` database table as described in the [Database Table For Leader Synchronization](database-table-for-leader-synchronization)
section. If this step fails, the storage controller process exits.

Note that no row will exist in the `leaders` table for the first graceful restart. In that case, force update the `leader` table
(without the WHERE clause) and perform with the pre-existing start-up procedure (i.e. build observed state by querying pageservers).

Summary of proposed new start-up sequence:
1. Call `/step_down`
2. Perform any pending database migrations
3. Load state from database
4. Load observed state returned in step (1) into memory
5. Do initial heartbeat round (may be moved after 5)
7. Mark self as leader by updating the database
8. Reschedule and reconcile everything

Some things to note from the steps above:
* The storage controller makes no changes to the cluster state before step (5) (i.e. no location config
calls to the pageserver and no compute notifications)
* Ask the current leader to step down before loading state from database so we don't get a lost update
if the transactions overlap.
* Before loading the observed state at step (3), cross-validate against the database. If validation fails,
fall back to asking the pageservers about their current locations.
* Database migrations should only run **after** the previous instance steps down (or the step down times out).


[1] The API call might fail because there's no storage controller running (i.e. [restart](#storage-controller-crash-or-restart)),
so we don't want to extend the unavailability period by much. We still want to retry since that's not the common case.

### Handling Failures

#### Storage Controller Crash Or Restart

The storage controller may crash or be restarted outside of roll-outs. When a new pod is created, its call to
`/step_down` will fail since the previous leader is no longer reachable. In this case perform the pre-existing
start-up procedure and update the leader table (with the WHERE clause). If the update fails, the storage controller
exists and consistency is maintained.

#### Previous Leader Crashes Before New Leader Readiness

When the previous leader (P1) crashes before the new leader (P2) passses the readiness check, Kubernetes will
reconcile the old replica set and create a new pod for it (P1'). The `/step_down` API call will fail for P1'
(see [2]).

Now we have two cases to consider:
* P2 updates the `leader` table first: The database update from P1' will fail and P1' will exit, or be terminated
by Kubernetes depending on timings.
* P1' updates the `leader` table first: The `hostname` field of the `leader` row stays the same, but the `start_timestamp` field changes.
The database update from P2 will fail (since `start_timestamp` does not match). P2 will exit and Kubernetes will
create a new replacement pod for it (P2'). Now the entire dance starts again, but with P1' as the leader and P2' as the incumbent.

[2] P1 and P1' may (more likely than not) be the same pod and have the same hostname. The implementation
should avoid this self reference and fail the API call at the client if the persisted hostname matches
the current one.

#### Previous Leader Crashes After New Leader Readiness

The deployment's replica sets already satisfy the deployment's replica count requirements and the
Kubernetes deployment rollout will just clean up the dead pod.

#### New Leader Crashes Before Pasing Readiness Check

The deployment controller scales up the new replica sets by creating a new pod. The entire procedure is repeated
with the new pod.

#### Network Partition Between New Pod and Previous Leader

This feels very unlikely, but should be considered in any case. P2 (the new aspiring leader) fails the `/step_down`
API call into P1 (the current leader). P2 proceeds with the pre-existing startup procedure and updates the `leader` table.
Kubernetes will terminate P1, but there may be a brief period where both storage controller can drive reconciles.

### Dealing With Split Brain Scenarios

As we've seen in the previous section, we can end up with two storage controller running at the same time. The split brain
duration is not bounded since the Kubernetes controller might become partitioned from the pods (unlikely though). While these
scenarios are not fatal, they can cause tenant unavailability, so we'd like to reduce the chances of this happening.
The rest of this section sketches some safety measure. It's likely overkill to implement all of them however.

### Ensure Leadership Before Producing Side Effects

The storage controller has two types of side effects: location config requests into pageservers and compute notifications into the control plane.
Before issuing either, the storage controller could check that it is indeed still the leader by querying the database. Side effects might still be
applied if they race with the database updatem, but the situation will eventually be detected. The storage controller process should terminate in these cases.

### Leadership Lease

Up until now, the leadership defined by this RFC is static. In order to bound the length of the split brain scenario, we could require the leadership
to be renewed periodically. Two new columns would be added to the leaders table:
1. `last_renewed` - timestamp indicating when the lease was last renewed
2. `lease_duration` - duration indicating the amount of time after which the lease expires

The leader periodically attempts to renew the lease by checking that it is in fact still the legitimate leader and updating `last_renewed` in the
same transaction. If the update fails, the process exits. New storage controller instances wishing to become leaders must wait for the current lease
to expire before acquiring leadership if they have not succesfully received a response to the `/step_down` request.

### Notify Pageserver Of Storage Controller Term

Each time that leadership changes, we can bump a `term` integer column in the `leader` table. This term uniquely identifies a leader.
Location config requests and re-attach responses can include this term. On the pageserver side, keep the latest term in memory and refuse
anything which contains a stale term (i.e. smaller than the current one).

### Observability

* The storage controller should expose a metric which describes it's state (`Active | WarmingUp | SteppedDown`).
Per region alerts should be added on this metric which triggers when:
  + no storage controller has been in the `Active` state for an extended period of time
  + more than one storage controllers are in the `Active` state

* An alert that periodically verifies that the `leader` table is in sync with the metric above would be very useful.
We'd have to expose the storage controller read only database to Grafana (perhaps it is already done).

## Alternatives

### Kubernetes Leases

Kubernetes has a [lease primitive](https://kubernetes.io/docs/concepts/architecture/leases/) which can be used to implement leader election.
Only one instance may hold a lease at any given time. This lease needs to be periodically renewed and has an expiration period.

In our case, it would work something like this:
* `/step_down` deletes the lease or stops it from renewing
* lease acquisition becomes part of the start-up procedure

The kubert crate implements a [lightweight lease API](https://docs.rs/kubert/latest/kubert/lease/struct.LeaseManager.html), but it's still
not exactly trivial to implement.

This approach has the benefit of baked in observability (`kubectl describe lease`), but:
* We offload the responsibility to Kubernetes which makes it harder to debug when things go wrong.
* More code surface than the simple "row in database" approach. Also, most of this code would be in
a dependency not subject to code review, etc.
* Hard to test. Our testing infra does not run the storage controller in Kubernetes and changing it do
so is not simple and complictes and the test set-up.

To my mind, the "row in database" approach is straightforward enough that we don't have to offload this
to something external.

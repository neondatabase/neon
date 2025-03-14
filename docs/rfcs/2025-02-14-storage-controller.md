
## Summary

This is a retrospective RFC to document the design of the `storage-controller` service.

This service manages the physical mapping of Tenants and Timelines to Pageservers and Safekeepers.  It
acts as the API for "storage" as an abstract concept: enabling other parts of the system to reason
about things like creating/deleting tenants and timelines without having to understand exactly which
pageserver and safekeeper to communicate, or any subtle rules about how to orchestrate these things.

The storage controller was implemented in the first half of 2024 as an essential part
of storage sharding, especially [shard splitting](032-shard-splitting.md).

It initially managed only pageservers, but has extended in 2025 to also manage safekeepers.  In
some places you may seen unqualified references to 'nodes' -- those are pageservers.

## Design Choices

### Durability

We rely on an external postgres for all durable state.  No local storage is used.

We avoid any unnecessary I/O to durable storage.  For example:
- most tracking of in-flight changes to the system is done in-memory rather than recording progress/steps in a database
- When migrating tenant shards between pageservers we only touch the database to increment generation numbers,
  we do not persist the total state of a tenant shard.

Being frugal with database I/O has two benefits:
- It avoids the database becoming a practical scaling bottleneck (we expect in-memory scale issues to be hit
  before we hit e.g. transactions-per-second issues)
- It reduces cost when using a cloud database service to run the controller's postgres database.

The trade-off is that there is a "bootstrapping" problem: a controller can't be deployed in isolation, one
must first have some existing database system.  In practice, we expect that Neon is deployed in one of the
following ways:
- into a cloud which has a postgres service that can be used to run the controller
- into a mature on-prem environment that has existing facilities for running databases
- into a test/dev environment where a simple one-node vanilla postgres installation is sufficient

### Consensus

The controller does _not_ implement any strong consensus mechanism of its own.  Instead:
- Where strong consistency is required (for example, for pageserver generation numbers), this
  responsibility is delegated to a transaction in our postgres database.
- Highly available deploys are done using a simple in-database record of what controller instances
  are available, distinguished by timestamps, rather than having controllers directly negotiate a leader.

Avoiding strong consensus among controller processes is a cost saving (we avoid running three controllers
all the time), and simplifies implementation (we do not have to phrase all configuration changes as e.g raft
transactions).

The trade-off is that under some circumstances a controller with partial network isolation can cause availability
issues in the cluster, by making changes to pageserver state that might disagree with what the "true" active
controller is trying to do.  The impact of this is bounded by our `controllers` database table, that enables
a rogue node to eventually realise that it is not the leader and step down.  If a rogue node can't reach
the database, then it implicitly stops making progress.  A rogue controller cannot durably damage the system
because pageserver data and safekeeper configs are protected by generation numbers that are only updated
via postgres transactions (i.e. no controller "trusts itself" to independently make decisions about generations).

### Scale

We design for high but not unlimited scale.  The memory footprint of each tenant shard is small (~8kB), so
it is realistic to scale up to a million attached shards on a server with modest resources.  Tenants in
a detached state (i.e. not active on pageservers) do not need to be managed by storage controller, and can
be relegated from memory to the database.

Typically, a tenant shard is updated about once a week, when we do a deploy.  During deploys, we relocate
a few thousand tenants from each pageserver while it is restarted, so it is extremely rare for the controller
to have to do O(N) work (on all shards at once).

There are places where we do O(N) work:
- On normal startup, when loading from the database into memory
- On unclean startup (with no handover of observed state from a previous controller), where we will
  scan all shards on all pageservers.

It is important that these locations are written efficiently.  At high scale we should still expect runtimes
of the order tens of seconds to complete a storage controller start.

When the practical scale limit of a single storage controller is reached, just deploy another one with its
own pageservers & safekeepers: each controller+its storage servers should be thought of as a logical cluster
or "cell" of storage.

# High Level Design

The storage controller is an in-memory system (i.e. state for all attached
tenants is held in memory _as well as_ being represented in durable postgres storage).

## Infrastructure

The storage controller is an async rust binary using tokio.

The storage controller is built around the `Service` type.  This implements
all the entry points for the outside world's interaction with the controller (HTTP handlers are mostly thin wrappers of service functions),
and holds most in-memory state (e.g. the list of tenant shards).

The state is held in a `ServiceInner` wrapped in a RwLock.  This monolithic
lock is used to simplify reasoning about code that mutates state: each function that takes a write lock may be thought of as a serializable transaction on the in-memory state.  This lock is clearly a bottleneck, but
nevertheless is scalable to managing millions of tenants.

Persistent state is held in a postgres database, and we use the `diesel` crate to provide database client functionality.  All database access is wrapped in the `Persistence` type -- this makes it easy to understand which
code is touching the database.  The database is only used when necessary, i.e. for state that cannot be recovered another way.  For example, we do not store the secondary pageserver locations of tenant shards in the database, rather we learn these at startup from running pageservers, and/or make scheduling decisions to fill in the gaps.  This adds some complexity, but massively reduces the load on the database, and enables running the storage controller with a very cheap postgres instance.

## Pageserver tenant scheduling & reconciliation

### Intent & observed state

Each tenant shard is represented by type `TenantShard`, which has an 'intent' and 'observed' state.  Setting the
intent state is called _scheduling_, and doing remote I/O to make observed
state match intent state is called _reconciliation_.

The `Scheduler` type is responsible for making choices about the intent
state, such as choosing a pageserver for a new tenant shard, or assigning
a replacement pageserver when the original one fails.

The observed state is updated after tenant reconciliation (see below), and
has the concept of a `None` state for a pageserver, indicating unknown state.  This is used to ensure that we can safely clean up after we start
but do not finish a remote call to a pageserver, or if a pageserver restarts and we are uncertain of its state.

### Tenant Reconciliation

The `Reconciler` type is responsible for updating pageservers to achieve
the intent state.  It is instantiated when `Service` determines that a shard requires reconciliation, and owned by a background tokio task that
runs it to completion.  Reconciler does not have access to the `Service` state: it is populated with a snapshot of relevant information when constructed, and submits is results to a channel that `Service` consumes
to update the tenant shard's observed state.

The Reconciler does have access to the database, but only uses it for
a single purpose: updating shards' generation numbers immediately before
attaching them to a pageserver.

Operations that change a tenant's scheduling will spawn a reconciler if
necessary, and there is also a background loop which checks every shard
for the need to reconcile -- this background loop ensures eventual progress
if some earlier reconciliations failed for some reason.

The reconciler has a general purpose code path which will attach/detach from pageservers as necessary, and a special case path for live migrations.  The live migration case is more common in practice, and is taken whenever the current observed state indicates that we have a healthy attached location to migrate from.  This implements live migration as described in the earlier [live migration RFC](028-pageserver-migration.md).

### Scheduling optimisation

During the periodic background reconciliation loop, the controller also
performance _scheduling optimization_.  This is the process of looking for
shards that are in sub-optimal locations, and moving them.

Typically, this means:
- Shards attached outside their preferred AZ (e.g. after a node failure), to migrate them back to their preferred AZ
- Shards attached on the same pageserver as some other shards in the same
  tenant, to migrate them elsewhere (e.g. after a shard split)

Scheduling optimisation is a multi-step process to ensure graceful cutovers, e.g. by creating new secondary location, waiting for it to
warm up, then cutting over.  This is not done as an explicit queue
of operations, but rather by iteratively calling the optimisation
function, which will recognise each intervening state as something
that can generate the next optimisation.

### Pageserver heartbeats and failure

The `Heartbeater` type is responsible for detecting when a pageserver
becomes unavailable.  This is fed back into `Service` for action: when
a pageserver is marked unavailable, tenant shards on that pageserver are
rescheduled and Reconcilers are spawned to cut them over to their new location.

## Pageserver timeline CRUD operations

By CRUD operations, we mean creating and deleting timelines.  The authoritative storage for which timelines exist on the pageserver
is in S3, and is governed by the pageserver's system of generation
numbers.  Because a shard can be attached to multiple pageservers
concurrently, we need to handle this when doing timeline CRUD operations:
- A timeline operation is only persistent if _after_ the ack from a pageserver, that pageserver's generation is still the latest.
- For deletions in particular, they are only persistent if _all_ attached
  locations have acked the deletion operation, since if only the latest one
  has acked then the timeline could still return from the dead if some old-generation attachment writes an index for it.

## Zero-downtime controller deployments

When two storage controllers run at the same time, they coordinate via
the database to establish one leader, and the other controller may proxy
requests to this leader

See  [Storage controller restarts RFC](037-storage-controller-restarts.md).

Note that this is not a strong consensus mechanism: the controller must also survive split-brain situations.  This is respected by code that
e.g. increments version numbers, which uses database transactions that
check the expected value before modifying it.  A split-brain situation can
impact availability (e.g. if two controllers are fighting over where to
attach a shard), but it should never impact durability and data integrity.

## Graceful drain & fill of pageservers during deploys

The storage controller has functionality for draining + filling pageservers
while deploying new pageserver binaries, so that clients are not actively
using a pageserver while it restarts.

See [Graceful restarts RFC](033-storage-controller-drain-and-fill.md)

## Safekeeper timeline scheduling

This is currently under development, see  [Safekeeper dynamic membership change RFC](035-safekeeper-dynamic-membership-change.md).
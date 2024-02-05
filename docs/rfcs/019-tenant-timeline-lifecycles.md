# Managing Tenant and Timeline lifecycles

## Summary

The pageserver has a Tenant object in memory for each tenant it manages, and a
Timeline for each timeline. There are a lot of tasks that operate on the tenants
and timelines with references to those objects. We have some mechanisms to track
which tasks are operating on each Tenant and Timeline, and to request them to
shutdown when a tenant or timeline is deleted, but it does not cover all uses,
and as a result we have many race conditions around tenant/timeline shutdown.

## Motivation

We have a bunch of race conditions that can produce weird errors and can be hard
to track down.

## Non Goals

This RFC only covers the problem of ensuring that a task/thread isn't operating
on a Tenant or Timeline. It does not cover what states, aside from Active and
non-Active, each Tenant and Timeline should have, or when exactly the transitions
should happen.

## Impacted components (e.g. pageserver, safekeeper, console, etc)

Pageserver. Although I wonder if the safekeeper should have a similar mechanism.

## Current situation

Most pageserver tasks of are managed by task_mgr.rs:

- LibpqEndpointListener
- HttpEndPointListener
- WalReceiverManager and -Connection
- GarbageCollector and Compaction
- InitialLogicalSizeCalculation

In addition to those tasks, the walreceiver performs some direct tokio::spawn
calls to spawn tasks that are not registered with 'task_mgr'. And all of these
tasks can spawn extra operations with tokio spawn_blocking.

Whenever a tenant or timeline is removed from the system, by pageserver
shutdown, delete_timeline or tenant-detach operation, we rely on the task
registry in 'task_mgr.rs' to wait until there are no tasks operating on the
tenant or timeline, before its Tenant/Timeline object is removed. That relies on
each task to register itself with the tenant/timeline ID in
'task_mgr.rs'. However, there are many gaps in that. For example,
GarbageCollection and Compaction tasks are registered with the tenant, but when
they proceed to operate on a particular timeline of the tenant, they don't
register with timeline ID. Because of that, the timeline can be deleted while GC
or compaction is running on it, causing failures in the GC or compaction (see
https://github.com/neondatabase/neon/issues/2442).

Another problem is that the task registry only works for tokio Tasks. There is
no way to register a piece of code that runs inside spawn_blocking(), for
example.

## Proposed implementation

This "voluntary" registration of tasks is fragile. Let's use Rust language features
to enforce that a tenant/timeline cannot be removed from the system when there is
still some code operating on it.

Let's introduce new Guard objects for Tenant and Timeline, and do all actions through
the Guard object. Something like:

TenantActiveGuard: Guard object over Arc<Tenant>. When you acquire the guard,
the code checks that the tenant is in Active state. If it's not, you get an
error. You can change the state of the tenant to Stopping while there are
ActiveTenantGuard objects still on it, to prevent new ActiveTenantGuards from
being acquired, but the Tenant cannot be removed until all the guards are gone.

TenantMaintenanceGuard: Like ActiveTenantGuard, but can be held even when the
tenant is not in Active state. Used for operations like attach/detach. Perhaps
allow only one such guard on a Tenant at a time.

Similarly for Timelines. We don't currently have a "state" on Timeline, but I think
we need at least two states: Active and Stopping. The Stopping state is used at
deletion, to prevent new TimelineActiveGuards from appearing, while you wait for
existing TimelineActiveGuards to die out.

The shutdown-signaling, using shutdown_watcher() and is_shutdown_requested(),
probably also needs changes to deal with the new Guards. The rule is that if you
have a TenantActiveGuard, and the tenant's state changes from Active to
Stopping, the is_shutdown_requested() function should return true, and
shutdown_watcher() future should return.

This signaling doesn't necessarily need to cover all cases. For example, if you
have a block of code in spawn_blocking(), it might be acceptable if
is_shutdown_requested() doesn't return true even though the tenant is in
Stopping state, as long as the code finishes reasonably fast.

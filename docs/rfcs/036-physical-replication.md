# Physical Replication

This RFC is a bit special in that we have already implemented physical
replication a long time ago. However, we never properly wrote down all
the decisions and assumptions, and in the last months when more users
have started to use the feature, numerous issues have surfaced.

This RFC documents the design decisions that have been made.

## Summary

PostgreSQL has a feature called streaming replication, where a replica
streams WAL from the primary and continuously applies it. It is also
known as "physical replication", to distinguish it from logical
replication.  In PostgreSQL, a replica is initialized by taking a
physical backup of the primary. In Neon, the replica is initialized
from a slim "base backup" from the pageserver, just like a primary,
and the primary and the replicas connect to the same pageserver,
sharing the storage.

There are two kinds of read-only replicas in Neon:
- replicas that follow the primary, and
- "static" replicas that are pinned at a particular LSN.

A static replica is useful e.g. for performing time-travel queries and
running one-off slow queries without affecting the primary. A replica
that follows the primary can be used e.g. to scale out read-only
workloads.

## Motivation

Read-only replicas allow offloading read-only queries. It's useful for
isolation, if you want to make sure that read-only queries don't
affect the primary, and it's also an easy way to provide guaranteed
read-only access to an application, without having to mess with access
controls.

## Non Goals (if relevant)

This RFC is all about WAL-based *physical* replication. Logical
replication is a different feature.

Neon also has the capability to launch "static" read-only nodes which
do not follow the primary, but are pinned to a particular LSN. They
can be used for long-running one-off queries, or for Point-in-time
queries. They work similarly to read replicas that follow the primary,
but some things are simpler: there are no concerns about cache
invalidation when the data changes on the primary, or worrying about
transactions that are in-progress on the primary.

## Impacted components (e.g. pageserver, safekeeper, console, etc)

- Control plane launches the replica
- Replica Postgres instance connects to the safekeepers, to stream the WAL
- The primary does not know about the standby, except for the hot standby feedback
- The primary and replicas all connect to the same pageservers


## Decisions, Issues

### Cache invalidation in replica

When a read replica follows the primary in PostgreSQL, it needs to
stream all the WAL from the primary and apply all the records, to keep
the local copy of the data consistent with the primary. In Neon, the
replica can fetch the updated page versions from the pageserver, so
it's not necessary to apply all the WAL. However, it needs to ensure
that any pages that are currently in the Postgres buffer cache, or the
Local File Cache, are either updated, or thrown away so that the next
read of the page will fetch the latest version.

We choose to apply the WAL records for pages that are already in the
buffer cache, and skip records for other pages. Somewhat arbitrarily,
we also apply records affecting catalog relations, fetching the old
page version from the pageserver if necessary first. See
`neon_redo_read_buffer_filter()` function.

The replica wouldn't necessarily need to see all the WAL records, only
the records that apply to cached pages. For simplicity, we do stream
all the WAL to the replica, and the replica simply ignores WAL records
that require no action.

Like in PostgreSQL, the read replica maintains a "replay LSN", which
is the LSN up to which the replica has received and replayed the
WAL. The replica can lag behind the primary, if it cannot quite keep
up with the primary, or if a long-running query conflicts with changes
that are about to be applied, or even intentionally if the user wishes
to see delayed data (see recovery_min_apply_delay). It's important
that the replica sees a consistent view of the whole cluster at the
replay LSN, when it's lagging behind.

In Neon, the replica connects to a safekeeper to get the WAL
stream. That means that the safekeepers must be able to regurgitate
the original WAL as far back as the replay LSN of any running read
replica. (A static read-only node that does not follow the primary
does not require a WAL stream however). The primary does not need to
be running, and when it is, the replicas don't incur any extra
overhead to the primary (see hot standby feedback though).

### In-progress transactions

In PostgreSQL, when a hot standby server starts up, it cannot
immediately open up for queries. It first needs to establish a
complete list of in-progress transactions, including subtransactions,
that are running at the primary, at the current replay LSN. Normally
that happens quickly, when the replica sees a "running-xacts" or
shutdown checkpoint WAL record, because the primary writes a
running-xacts record at every checkpoint, and in PostgreSQL the
replica always starts the WAL replay from a checkpoint REDO point. If
there are a lot of subtransactions in progress, however, the standby
might need to wait for old transactions to complete before it can open
up for queries.

In Neon that problem is worse: a replica can start at any LSN, so
there's no guarantee that it will see a running-xacts record any time
soon. In particular, if the primary is not running when the replica is
started, it might never see a running-xacts record.

To make things worse, we initially missed this issue, and always
started accepting queries at replica startup, even if it didn't have
the transaction information. That could lead to incorrect query
results and data corruption later. However, as we fixed that, we
introduced a new problem compared to what we had before: previously
the replica would always start up, but after fixing that bug, it might
not. In a superficial way, the old behavior was better (but could lead
to serious issues later!). That made fixing that bug was very hard,
because as we fixed it, we made things (superficially) worse for
others.

See https://github.com/neondatabase/neon/pull/7288 which fixed the
bug, and follow-up PRs https://github.com/neondatabase/neon/pull/8323
and https://github.com/neondatabase/neon/pull/8484 to try to claw back
the cases that started to cause trouble as fixing it. As of this
writing, there are still cases where a replica might not immediately
start up, causing the control plane operation to fail.

One long-term fix for this is to switch to using so-called CSN
snapshots in read replica. That would make it unnecessary to have the
full in-progress transaction list in the replica at startup time. See
https://commitfest.postgresql.org/48/4912/ for a work-in-progress
patch to upstream to implement that.

### Recovery conflicts and Hot standby feedback

It's possible that a tuple version is vacuumed away in the primary,
even though it is still needed by a running transactions in the
replica. This is called a "recovery conflict", and PostgreSQL provides
various options for dealing with it. By default, the WAL replay will
wait up to 30 s for the conflicting query to finish. After that, it
will kill the running query, so that the WAL replay can proceed.

Another way to avoid the situation is to enable the
[`hot_standby_feedback`](https://www.postgresql.org/docs/current/runtime-config-replication.html#GUC-HOT-STANDBY-FEEDBACK)
option. When it is enabled, the primary will refrain from vacuuming
tuples that are still needed in the primary. That means potentially
bloating the primary, which violates the usual rule that read replicas
don't affect the operations on the primary, which is why it's off by
default.

Neon supports `hot_standby_feedback` by passing the feedback messages
from the replica to the safekeepers, and from safekeepers to the
primary.

### Interaction with Pageserver GC

The read replica can lag behind the primary. If there are recovery
conflicts or the replica cannot keep up for some reason, the lag can
in principle grow indefinitely. The replica will issue all GetPage
requests to the pageservers at the current replay LSN, and needs to
see the old page versions.

If the retention period in the pageserver is set to be small, it may
have already garbage collected away the old page versions. That will
cause read errors in the compute, and can mean that the replica cannot
make progress with the replication anymore.

This is an unsolved problem at the moment. A "lease" mechanism is in
the works, where the replica could hold a lease on the old LSN,
preventing the pageserver from advancing the GC horizon past that
point.


### Synchronous replication

We haven't put any effort into synchronous replication yet.

PostgreSQL provides multiple levels of synchronicity. In the weaker
levels, a transaction is not acknowledged as committed to the client
in the primary until the WAL has been streamed to a replica or flushed
to disk there. Those modes don't make senses in Neon, because the
safekeepers handle durability.

`synchronous_commit=remote_apply` mode would make sense. In that mode,
the commit is not acknowledged to the client until it has been
replayed in the replica. That ensures that after commit, you can see
the commit in the replica too (aka. read-your-write consistency).

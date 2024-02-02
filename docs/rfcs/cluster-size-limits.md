Cluster size limits
==================

## Summary

One of the resource consumption limits for free-tier users is a cluster size limit.

To enforce it, we need to calculate the timeline size and check if the limit is reached before relation create/extend operations.
If the limit is reached, the query must fail with some meaningful error/warning.
We may want to exempt some operations from the quota to allow users free space to fit back into the limit.

The stateless compute node that performs validation is separate from the storage that calculates the usage, so we need to exchange cluster size information between those components.

## Motivation

Limit the maximum size of a PostgreSQL instance to limit free tier users (and other tiers in the future).
First of all, this is needed to control our free tier production costs.
Another reason to limit resources is risk management — we haven't (fully) tested and optimized neon for big clusters,
so we don't want to give users access to the functionality that we don't think is ready.

## Components

* pageserver - calculate the size consumed by a timeline and add it to the feedback message.
* safekeeper - pass feedback message from pageserver to compute.
* compute - receive feedback message, enforce size limit based on GUC `neon.max_cluster_size`.
* console - set and update `neon.max_cluster_size` setting

## Proposed implementation

First of all, it's necessary to define timeline size.

The current approach is to count all data, including SLRUs. (not including WAL)
Here we think of it as a physical disk underneath the Postgres cluster.
This is how the `LOGICAL_TIMELINE_SIZE` metric is implemented in the pageserver.

Alternatively, we could count only relation data. As in pg_database_size().
This approach is somewhat more user-friendly because it is the data that is really affected by the user.
On the other hand, it puts us in a weaker position than other services, i.e., RDS.
We will need to refactor the timeline_size counter or add another counter to implement it.

Timeline size is updated during wal digestion. It is not versioned and is valid at the last_received_lsn moment.
Then this size should be reported to compute node.

`current_timeline_size` value is included in the walreceiver's custom feedback message: `ReplicationFeedback.`

(PR about protocol changes https://github.com/neondatabase/neon/pull/1037).

This message is received by the safekeeper and propagated to compute node as a part of `AppendResponse`.

Finally, when compute node receives the `current_timeline_size` from safekeeper (or from pageserver directly), it updates the global variable.

And then every neon_extend() operation checks if limit is reached `(current_timeline_size > neon.max_cluster_size)` and throws `ERRCODE_DISK_FULL` error if so.
(see Postgres error codes [https://www.postgresql.org/docs/devel/errcodes-appendix.html](https://www.postgresql.org/docs/devel/errcodes-appendix.html))

TODO:
We can allow autovacuum processes to bypass this check, simply checking `IsAutoVacuumWorkerProcess()`.
It would be nice to allow manual VACUUM and VACUUM FULL to bypass the check, but it's uneasy to distinguish these operations at the low level.
See issues https://github.com/neondatabase/neon/issues/1245
https://github.com/neondatabase/neon/issues/1445

TODO:
We should warn users if the limit is soon to be reached.

### **Reliability, failure modes and corner cases**

1. `current_timeline_size` is valid at the last received and digested by pageserver lsn.

    If pageserver lags behind compute node, `current_timeline_size` will lag too. This lag can be tuned using backpressure, but it is not expected to be 0 all the time.

    So transactions that happen in this lsn range may cause limit overflow. Especially operations that generate (i.e., CREATE DATABASE) or free (i.e., TRUNCATE) a lot of data pages while generating a small amount of WAL. Are there other operations like this?

    Currently, CREATE DATABASE operations are restricted in the console. So this is not an issue.


### **Security implications**

We treat compute as an untrusted component. That's why we try to isolate it with secure container runtime or a VM.
Malicious users may change the `neon.max_cluster_size`, so we need an extra size limit check.
To cover this case, we also monitor the compute node size in the console.

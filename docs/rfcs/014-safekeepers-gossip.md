# Safekeeper gossip

Extracted from this [PR](https://github.com/neondatabase/rfcs/pull/13)

## Motivation

In some situations, safekeeper (SK) needs coordination with other SK's that serve the same tenant:

1. WAL deletion. SK needs to know what WAL was already safely replicated to delete it. Now we keep WAL indefinitely.
2. Deciding on who is sending WAL to the pageserver. Now sending SK crash may lead to a livelock where nobody sends WAL to the pageserver.
3. To enable SK to SK direct recovery without involving the compute

## Summary

Compute node has connection strings to each safekeeper. During each compute->safekeeper connection establishment, the compute node should pass down all that connection strings to each safekeeper. With that info, safekeepers may establish Postgres connections to each other and periodically send ping messages with LSN payload.

## Components

safekeeper, compute, compute<->safekeeper protocol, possibly console (group SK addresses)

## Proposed implementation

Each safekeeper can periodically ping all its peers and share connectivity and liveness info. If the ping was not receiver for, let's say, four ping periods, we may consider sending safekeeper as dead. That would mean some of the alive safekeepers should connect to the pageserver. One way to decide which one exactly: `make_connection = my_node_id == min(alive_nodes)`

Since safekeepers are multi-tenant, we may establish either per-tenant physical connections or per-safekeeper ones. So it makes sense to group "logical" connections between corresponding tenants on different nodes into a single physical connection. That means that we should implement an interconnect thread that maintains physical connections and periodically broadcasts info about all tenants.

Right now console may assign any 3 SK addresses to a given compute node. That may lead to a high number of gossip connections between SK's. Instead, we can assign safekeeper triples to the compute node. But if we want to "break"/" change" group by an ad-hoc action, we can do it.

### Corner cases

- Current safekeeper may be alive but may not have connectivity to the pageserver

  To address that, we need to gossip visibility info. Based on that info, we may define SK as alive only when it can connect to the pageserver.

- Current safekeeper may be alive but may not have connectivity with the compute node.

  We may broadcast last_received_lsn and presence of compute connection and decide who is alive based on that.

- It is tricky to decide when to shut down gossip connections because we need to be sure that pageserver got all the committed (in the distributed sense, so local SK info is not enough) records, and it may never lose them. It is not a strict requirement since `--sync-safekeepers` that happen before the compute start will allow the pageserver to consume missing WAL, but it is better to do that in the background. So the condition may look like that: `majority_max(flush_lsn) == pageserver_s3_lsn` Here we rely on the two facts:
    - that `--sync-safekeepers` happened after the compute shutdown, and it advanced local commit_lsn's allowing pageserver to consume that WAL.

    - we wait for the `pageserver_s3_lsn` advancement to avoid pageserver's last_received_lsn/disk_consistent_lsn going backward due to the disk/hardware failure and subsequent S3 recovery

    If those conditions are not met, we will have some gossip activity (but that may be okay).

## Pros/cons

Pros:

- distributed, does not introduce new services (like etcd), does not add console as a storage dependency
- lays the foundation for gossip-based recovery

Cons:

- Only compute knows a set of safekeepers, but they should communicate even without compute node. In case of safekeepers restart, we will lose that info and can't gossip anymore. Hence we can't trim some WAL tail until the compute node start. Also, it is ugly.

- If the console assigns a random set of safekeepers to each Postgres, we may end up in a situation where each safekeeper needs to have a connection with all other safekeepers. We can group safekeepers into isolated triples in the console to avoid that. Then "mixing" would happen only if we do rebalancing.

## Alternative implementation

We can have a selected node (e.g., console) with everybody reporting to it.

## Security implications

We don't increase the attack surface here. Communication can happen in a private network that is not exposed to users.

## Scalability implications

The only thing that may grow as we grow the number of computes is the number of gossip connections. But if we group safekeepers and assign a compute node to the random SK triple, the number of connections would be constant.

# WAL service

The neon WAL service acts as a holding area and redistribution
center for recently generated WAL. The primary Postgres server streams
the WAL to the WAL safekeeper, and treats it like a (synchronous)
replica. A replication slot is used in the primary to prevent the
primary from discarding WAL that hasn't been streamed to the WAL
service yet.

```
+--------------+              +------------------+
|              |     WAL      |                  |
| Compute node |  ----------> |   WAL Service    |
|              |              |                  |
+--------------+              +------------------+
                                     |
                                     |
                                     | WAL
                                     |
                                     |
                                     V
                              +--------------+
                              |              |
                              | Pageservers  |
                              |              |
                              +--------------+
```


The WAL service consists of multiple WAL safekeepers that all store a
copy of the WAL. A WAL record is considered durable when the majority
of safekeepers have received and stored the WAL to local disk. A
consensus algorithm based on Paxos is used to manage the quorum.

```
  +-------------------------------------------+
  | WAL Service                               |
  |                                           |
  |                                           |
  |  +------------+                           |
  |  | safekeeper |                           |
  |  +------------+                           |
  |                                           |
  |  +------------+                           |
  |  | safekeeper |                           |
  |  +------------+                           |
  |                                           |
  |  +------------+                           |
  |  | safekeeper |                           |
  |  +------------+                           |
  |                                           |
  +-------------------------------------------+
```

The primary connects to the WAL safekeepers, so it works in a "push"
fashion.  That's different from how streaming replication usually
works, where the replica initiates the connection. To do that, there
is a component called the "WAL proposer". The WAL proposer is a
background worker that runs in the primary Postgres server. It
connects to the WAL safekeeper, and sends all the WAL. (PostgreSQL's
archive_commands works in the "push" style, but it operates on a WAL
segment granularity. If PostgreSQL had a push style API for streaming,
WAL propose could be implemented using it.)

The Page Server connects to the WAL safekeeper, using the same
streaming replication protocol that's used between Postgres primary
and standby. You can also connect the Page Server directly to a
primary PostgreSQL node for testing.

In a production installation, there are multiple WAL safekeepers
running on different nodes, and there is a quorum mechanism using the
Paxos algorithm to ensure that a piece of WAL is considered as durable
only after it has been flushed to disk on more than half of the WAL
safekeepers. The Paxos and crash recovery algorithm ensures that only
one primary node can be actively streaming WAL to the quorum of
safekeepers.

See [this section](safekeeper-protocol.md) for a more detailed description of
the consensus protocol. spec/ contains TLA+ specification of it.

# Q&A

Q: Why have a separate service instead of connecting Page Server directly to a
   primary PostgreSQL node?
A: Page Server is a single server which can be lost. As our primary
   fault-tolerant storage is S3, we do not want to wait for it before
   committing a transaction. The WAL service acts as a temporary fault-tolerant
   storage for recent data before it gets to the Page Server and then finally
   to S3. Whenever WALs and pages are committed to S3, WAL's storage can be
   trimmed.

Q: What if the compute node evicts a page, needs it back, but the page is yet
   to reach the Page Server?
A: If the compute node has evicted a page, changes to it have been WAL-logged
   (that's why it is called Write Ahead logging; there are some exceptions like
   index builds, but these are exceptions). These WAL records will eventually
   reach the Page Server. The Page Server notes that the compute node requests
   pages with a very recent LSN and will not respond to the compute node until a
   corresponding WAL is received from WAL safekeepers.

Q: How long may Page Server wait for?
A: Not too long, hopefully. If a page is evicted, it probably was not used for
   a while, so the WAL service have had enough time to push changes to the Page
   Server. To limit the lag, tune backpressure using `max_replication_*_lag` settings.

Q: How do WAL safekeepers communicate with each other?
A: They may only send each other messages via the compute node, they never
   communicate directly with each other.

Q: Why have a consensus algorithm if there is only a single compute node?
A: Actually there may be moments with multiple PostgreSQL nodes running at the
   same time. E.g. we are bringing one up and one down. We would like to avoid
   simultaneous writes from different nodes, so there should be a consensus on
   who is the primary node.

# Terminology

WAL service - The service as whole that ensures that WAL is stored durably.

WAL safekeeper - One node that participates in the quorum. All the safekeepers
together form the WAL service.

WAL acceptor, WAL proposer - In the context of the consensus algorithm, the Postgres
compute node is also known as the WAL proposer, and the safekeeper is also known
as the acceptor. Those are the standard terms in the Paxos algorithm.

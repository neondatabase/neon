# Glossary

### Authentication

### Backpressure

Backpressure is used to limit the lag between pageserver and compute node or WAL service.

If compute node or WAL service run far ahead of Page Server,
the time of serving page requests increases. This may lead to timeout errors.

To tune backpressure limits use `max_replication_write_lag`, `max_replication_flush_lag` and `max_replication_apply_lag` settings.
When lag between current LSN (pg_current_wal_flush_lsn() at compute node) and minimal write/flush/apply position of replica exceeds the limit
backends performing writes are blocked until the replica is caught up.
### Base image (page image)

### Basebackup

A tarball with files needed to bootstrap a compute node[] and a corresponding command to create it.
NOTE:It has nothing to do with PostgreSQL pg_basebackup.

### Branch

We can create branch at certain LSN using `neon_local timeline branch` command.
Each Branch lives in a corresponding timeline[] and has an ancestor[].


### Checkpoint (PostgreSQL)

NOTE: This is an overloaded term.

A checkpoint record in the WAL marks a point in the WAL sequence at which it is guaranteed that all data files have been updated with all information from shared memory modified before that checkpoint;

### Checkpoint (Layered repository)

NOTE: This is an overloaded term.

Whenever enough WAL has been accumulated in memory, the page server []
writes out the changes from the in-memory layer into a new delta layer file. This process
is called "checkpointing".

Configuration parameter `checkpoint_distance` defines the distance
from current LSN to perform checkpoint of in-memory layers.
Default is `DEFAULT_CHECKPOINT_DISTANCE`.

### Compaction

A background operation on layer files. Compaction takes a number of L0
layer files, each of which covers the whole key space and a range of
LSN, and reshuffles the data in them into L1 files so that each file
covers the whole LSN range, but only part of the key space.

Compaction should also opportunistically leave obsolete page versions
from the L1 files, and materialize other page versions for faster
access. That hasn't been implemented as of this writing, though.


### Compute node

Stateless Postgres node that stores data in pageserver.

### Garbage collection

The process of removing old on-disk layers that are not needed by any timeline anymore.

### Fork

Each of the separate segmented file sets in which a relation is stored. The main fork is where the actual data resides. There also exist two secondary forks for metadata: the free space map and the visibility map.

### Layer

A layer contains data needed to reconstruct any page versions within the
layer's Segment and range of LSNs.

There are two kinds of layers, in-memory and on-disk layers. In-memory
layers are used to ingest incoming WAL, and provide fast access
to the recent page versions. On-disk layers are stored as files on disk, and
are immutable. See [pageserver-storage.md](./pageserver-storage.md) for more.

### Layer file (on-disk layer)

Layered repository on-disk format is based on immutable files.  The
files are called "layer files". There are two kinds of layer files:
image files and delta files. An image file contains a "snapshot" of a
range of keys at a particular LSN, and a delta file contains WAL
records applicable to a range of keys, in a range of LSNs.

### Layer map

The layer map tracks what layers exist in a timeline.

### Layered repository

Neon repository implementation that keeps data in layers.

### LSN

The Log Sequence Number (LSN) is a unique identifier of the WAL record[] in the WAL log.
The insert position is a byte offset into the logs, increasing monotonically with each new record.
Internally, an LSN is a 64-bit integer, representing a byte position in the write-ahead log stream.
It is printed as two hexadecimal numbers of up to 8 digits each, separated by a slash.
Check also [PostgreSQL doc about pg_lsn type](https://www.postgresql.org/docs/devel/datatype-pg-lsn.html)
Values can be compared to calculate the volume of WAL data that separates them, so they are used to measure the progress of replication and recovery.

In Postgres and Neon LSNs are used to describe certain points in WAL handling.

PostgreSQL LSNs and functions to monitor them:
* `pg_current_wal_insert_lsn()` - Returns the current write-ahead log insert location.
* `pg_current_wal_lsn()` - Returns the current write-ahead log write location.
* `pg_current_wal_flush_lsn()` - Returns the current write-ahead log flush location.
* `pg_last_wal_receive_lsn()` - Returns the last write-ahead log location that has been received and synced to disk by streaming replication. While streaming replication is in progress this will increase monotonically.
* `pg_last_wal_replay_lsn ()` - Returns the last write-ahead log location that has been replayed during recovery. If recovery is still in progress this will increase monotonically.
[source PostgreSQL documentation](https://www.postgresql.org/docs/devel/functions-admin.html):

Neon safekeeper LSNs. See [safekeeper protocol section](safekeeper-protocol.md) for more information.
* `CommitLSN`: position in WAL confirmed by quorum safekeepers.
* `RestartLSN`: position in WAL confirmed by all safekeepers.
* `FlushLSN`: part of WAL persisted to the disk by safekeeper.
* `VCL`: the largest LSN for which we can guarantee availability of all prior records.

Neon pageserver LSNs:
* `last_record_lsn` - the end of last processed WAL record.
* `disk_consistent_lsn` - data is known to be fully flushed and fsync'd to local disk on pageserver up to this LSN.
* `remote_consistent_lsn` - The last LSN that is synced to remote storage and is guaranteed to survive pageserver crash.
TODO: use this name consistently in remote storage code. Now `disk_consistent_lsn` is used and meaning depends on the context.
* `ancestor_lsn` - LSN of the branch point (the LSN at which this branch was created)

TODO: add table that describes mapping between PostgreSQL (compute), safekeeper and pageserver LSNs.

### Logical size

The pageserver tracks the "logical size" of a timeline. It is the
total size of all relations in all Postgres databases on the
timeline. It includes all user and system tables, including their FSM
and VM forks. But it does not include SLRUs, twophase files or any
other such data or metadata that lives outside relations.

The logical size is calculated by the pageserver, and is sent to
PostgreSQL via feedback messages to the safekeepers. PostgreSQL uses
the logical size to enforce the size limit in the free tier. The
logical size is also shown to users in the web console.

The logical size is not affected by branches or the physical layout of
layer files in the pageserver. If you have a database with 1 GB
logical size and you create a branch of it, both branches will have 1
GB logical size, even though the branch is copy-on-write and won't
consume any extra physical disk space until you make changes to it.

### Page (block)

The basic structure used to store relation data. All pages are of the same size.
This is the unit of data exchange between compute node and pageserver.

### Pageserver

Neon storage engine: repositories + wal receiver + page service + wal redo.

### Page service

The Page Service listens for GetPage@LSN requests from the Compute Nodes,
and responds with pages from the repository.


### PITR (Point-in-time-recovery)

PostgreSQL's ability to restore up to a specified LSN.

### Primary node


### Proxy

Postgres protocol proxy/router.
This service listens psql port, can check auth via external service
and create new databases and accounts (control plane API in our case).

### Relation

The generic term in PostgreSQL for all objects in a database that have a name and a list of attributes defined in a specific order.

### Replication slot


### Replica node


### Repository

Repository stores multiple timelines, forked off from the same initial call to 'initdb'
and has associated WAL redo service.
One repository corresponds to one Tenant.

### Retention policy

How much history do we need to keep around for PITR and read-only nodes?

### Segment

A physical file that stores data for a given relation. File segments are
limited in size by a compile-time setting (1 gigabyte by default), so if a
relation exceeds that size, it is split into multiple segments.

### SLRU

SLRUs include pg_clog, pg_multixact/members, and
pg_multixact/offsets. There are other SLRUs in PostgreSQL, but
they don't need to be stored permanently (e.g. pg_subtrans),
or we do not support them in neon yet (pg_commit_ts).

### Tenant (Multitenancy)
Tenant represents a single customer, interacting with Neon.
Wal redo[] activity, timelines[], layers[] are managed for each tenant independently.
One pageserver[] can serve multiple tenants at once.
One safekeeper

See `docs/multitenancy.md` for more.

### Timeline

Timeline accepts page changes and serves get_page_at_lsn() and
get_rel_size() requests. The term "timeline" is used internally
in the system, but to users they are exposed as "branches", with
human-friendly names.

NOTE: this has nothing to do with PostgreSQL WAL timelines.

### XLOG

PostgreSQL alias for WAL[].

### WAL (Write-ahead log)

The journal that keeps track of the changes in the database cluster as user- and system-invoked operations take place. It comprises many individual WAL records[] written sequentially to WAL files[].

### WAL acceptor, WAL proposer

In the context of the consensus algorithm, the Postgres
compute node is also known as the WAL proposer, and the safekeeper is also known
as the acceptor. Those are the standard terms in the Paxos algorithm.

### WAL receiver (WAL decoder)

The WAL receiver connects to the external WAL safekeeping service (or
directly to the primary) using PostgreSQL physical streaming
replication, and continuously receives WAL. It decodes the WAL records,
and stores them to the repository.

We keep one WAL receiver active per timeline.

### WAL record

A low-level description of an individual data change.

### WAL redo

A service that runs PostgreSQL in a special wal_redo mode
to apply given WAL records over an old page image and return new page image.

### WAL safekeeper

One node that participates in the quorum. All the safekeepers
together form the WAL service.

### WAL segment (WAL file)

Also known as WAL segment or WAL segment file. Each of the sequentially-numbered files that provide storage space for WAL. The files are all of the same predefined size and are written in sequential order, interspersing changes as they occur in multiple simultaneous sessions.

### WAL service

The service as whole that ensures that WAL is stored durably.

### Web console


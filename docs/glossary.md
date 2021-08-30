# Glossary

### Authentication

### Base image (page image)

### Basebackup

A tarball with files needed to bootstrap a compute node[] and a corresponding command to create it.
NOTE:It has nothing to do with PostgreSQL pg_basebackup.

### Branch

We can create branch at certain LSN using `zenith branch` command.
Each Branch lives in a corresponding timeline[] and has an ancestor[].


### Checkpoint (PostgreSQL)

NOTE: This is an overloaded term.

A checkpoint record in the WAL marks a point in the WAL sequence at which it is guaranteed that all data files have been updated with all information from shared memory modified before that checkpoint; 

### Checkpoint (Layered repository)

NOTE: This is an overloaded term.

Whenever enough WAL has been accumulated in memory, the page server []
writes out the changes in memory into new layer files[]. This process
is called "checkpointing". The page server only creates layer files for
relations that have been modified since the last checkpoint. 

### Compute node

Stateless Postgres node that stores data in pageserver.

### Garbage collection

### Fork

Each of the separate segmented file sets in which a relation is stored. The main fork is where the actual data resides. There also exist two secondary forks for metadata: the free space map and the visibility map.
Each PostgreSQL fork is considered a separate relish.

### Layer file

Layered repository on-disk format is based on immutable files.  The
files are called "layer files". Each file corresponds to one 10 MB
segment of a PostgreSQL relation fork. There are two kinds of layer
files: image files and delta files. An image file contains a
"snapshot" of the segment at a particular LSN, and a delta file
contains WAL records applicable to the segment, in a range of LSNs.

### Layered repository

### LSN


### Page (block)

The basic structure used to store relation data. All pages are of the same size.
This is the unit of data exchange between compute node and pageserver.

### Pageserver

Zenith storage engine: page cache repositories + wal receiver + page service + wal redo.

### Page cache

This module acts as a switchboard to access different repositories managed by this pageserver.

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

### Relish

We call each relation and other file that is stored in the
repository a "relish". It comes from "rel"-ish, as in "kind of a
rel", because it covers relations as well as other things that are
not relations, but are treated similarly for the purposes of the
storage layer.

### Replication slot


### Replica node


### Repository

Repository stores multiple timelines, forked off from the same initial call to 'initdb'
and has associated WAL redo service.
One repository corresponds to one Tenant.

### Retention policy

How much history do we need to keep around for PITR and read-only nodes?

### SLRU

SLRUs include pg_clog, pg_multixact/members, and
pg_multixact/offsets. There are other SLRUs in PostgreSQL, but
they don't need to be stored permanently (e.g. pg_subtrans),
or we do not support them in zenith yet (pg_commit_ts).
Each SLRU segment is considered a separate relish[].

### Tenant (Multitenancy)
Tenant represents a single customer, interacting with Zenith.
Wal redo[] activity, timelines[], snapshots[] are managed for each tenant independently.
One pageserver[] can serve multiple tenants at once.
One safekeeper 

See `docs/multitenancy.md` for more.

### Timeline

Timeline is a page cache workhorse that accepts page changes
and serves get_page_at_lsn() and get_rel_size() requests.

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
and stores them to the page cache repository.

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


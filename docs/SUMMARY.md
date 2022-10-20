# Summary

[Introduction]()
- [Separation of Compute and Storage](./separation-compute-storage.md)

# Architecture

- [Compute]()
  - [WAL proposer]()
  - [WAL Backpressure]()
  - [Postgres changes](./core_changes.md)

- [Pageserver](./pageserver.md)
    - [Services](./pageserver-services.md)
    - [Thread management](./pageserver-thread-mgmt.md)
    - [WAL Redo](./pageserver-walredo.md)
    - [Page cache](./pageserver-pagecache.md)
    - [Storage](./pageserver-storage.md)
        - [Datadir mapping]()
        - [Layer files]()
        - [Branching]()
        - [Garbage collection]()
    - [Cloud Storage]()
    - [Processing a GetPage request](./pageserver-processing-getpage.md)
    - [Processing WAL](./pageserver-processing-wal.md)
	- [Management API]()
	- [Tenant Rebalancing]()

- [WAL Service](walservice.md)
  - [Consensus protocol](safekeeper-protocol.md)
  - [Management API]()
  - [Rebalancing]()

- [Control Plane]()

- [Proxy]()

- [Source view](./sourcetree.md)
  - [docker.md](./docker.md) — Docker images and building pipeline.
  - [Error handling and logging]()
  - [Testing]()
    - [Unit testing]()
    - [Integration testing]()
    - [Benchmarks]()


- [Glossary](./glossary.md)

# Uncategorized

- [authentication.md](./authentication.md)
- [multitenancy.md](./multitenancy.md) — how multitenancy is organized in the pageserver and Zenith CLI.
- [settings.md](./settings.md)
#FIXME: move these under sourcetree.md
#- [postgres_ffi/README.md](/libs/postgres_ffi/README.md)
#- [test_runner/README.md](/test_runner/README.md)


# RFCs

- [RFCs](./rfcs/README.md)

- [002-storage](rfcs/002-storage.md)
- [003-laptop-cli](rfcs/003-laptop-cli.md)
- [004-durability](rfcs/004-durability.md)
- [005-zenith_local](rfcs/005-zenith_local.md)
- [006-laptop-cli-v2-CLI](rfcs/006-laptop-cli-v2-CLI.md)
- [006-laptop-cli-v2-repository-structure](rfcs/006-laptop-cli-v2-repository-structure.md)
- [007-serverless-on-laptop](rfcs/007-serverless-on-laptop.md)
- [008-push-pull](rfcs/008-push-pull.md)
- [009-snapshot-first-storage-cli](rfcs/009-snapshot-first-storage-cli.md)
- [009-snapshot-first-storage](rfcs/009-snapshot-first-storage.md)
- [009-snapshot-first-storage-pitr](rfcs/009-snapshot-first-storage-pitr.md)
- [010-storage_details](rfcs/010-storage_details.md)
- [011-retention-policy](rfcs/011-retention-policy.md)
- [012-background-tasks](rfcs/012-background-tasks.md)
- [013-term-history](rfcs/013-term-history.md)
- [014-safekeepers-gossip](rfcs/014-safekeepers-gossip.md)
- [014-storage-lsm](rfcs/014-storage-lsm.md)
- [015-storage-messaging](rfcs/015-storage-messaging.md)
- [016-connection-routing](rfcs/016-connection-routing.md)
- [017-timeline-data-management](rfcs/017-timeline-data-management.md)
- [018-storage-messaging-2](rfcs/018-storage-messaging-2.md)
- [019-tenant-timeline-lifecycles](rfcs/019-tenant-timeline-lifecycles.md)
- [cluster-size-limits](rfcs/cluster-size-limits.md)

# Summary

# Looking for `neon.tech` docs?

This page linkes to a selection of technical content about the open source code in this repository.

Please visit https://neon.tech/docs for documentation about using the Neon service, which is based on the code
in this repository.

# Architecture

[Introduction]()
- [Separation of Compute and Storage](./separation-compute-storage.md)

- [Compute]()
  - [Postgres changes](./core_changes.md)

- [Pageserver](./pageserver.md)
    - [Services](./pageserver-services.md)
    - [Thread management](./pageserver-thread-mgmt.md)
    - [WAL Redo](./pageserver-walredo.md)
    - [Page cache](./pageserver-pagecache.md)
    - [Storage](./pageserver-storage.md)
    - [Processing a GetPage request](./pageserver-processing-getpage.md)
    - [Processing WAL](./pageserver-processing-wal.md)

- [WAL Service](walservice.md)
  - [Consensus protocol](safekeeper-protocol.md)

- [Source view](./sourcetree.md)
  - [docker.md](./docker.md) — Docker images and building pipeline.
  - [Error handling and logging](./error-handling.md)

- [Glossary](./glossary.md)

# Uncategorized

- [authentication.md](./authentication.md)
- [multitenancy.md](./multitenancy.md) — how multitenancy is organized in the pageserver and Zenith CLI.
- [settings.md](./settings.md)
#FIXME: move these under sourcetree.md
#- [postgres_ffi/README.md](/libs/postgres_ffi/README.md)
#- [test_runner/README.md](/test_runner/README.md)


# RFCs

Major changes are documented in RFCS:
- See [RFCs](./rfcs/README.md) for more information
- view the RFCs at https://github.com/neondatabase/neon/tree/main/docs/rfcs

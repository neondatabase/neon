# CHANGELOG

This is the Neon storage and compute changelog. It is a record of **notable changes** made to this project. The intended audience is developers, including those on the Neon team and external developers who may use or contribute to this project. The format of the changelog is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

The changes documented here may also appear in the [Neon Release Notes](https://neon.tech/docs/release-notes), but the information provided here is typically more technical in nature and aimed at developers. The _Neon Release Notes_ are more user-oriented and focused on benefits and usage of the new features or changes.

## 2023-05-23

### Added

- Compute: Implemented a `cargo neon` utility to facilitate setting up the Neon project locally. [Setup instructions](https://github.com/neondatabase/neon#running-neon-database) have been updated to reflect this change.

### Changed

- Compute: Updated PostgreSQL versions to 14.8 and 15.3, respectively.

## 2023-05-16

### Changed

- Proxy: Neon uses compute endpoint domain names to route incoming client connections. For example, to connect to the compute endpoint `ep-mute-recipe-239816`, we ask that you connect to `ep-mute-recipe-239816.us-east-2.aws.neon.tech`. However, the PostgreSQL wire protocol does not transfer the server domain name, so Neon relies on the Server Name Indication (SNI) extension of the TLS protocol to do this. Unfortunately, not all PostgreSQL clients support SNI. When these clients attempt to connect, they receive an error indicating that the "endpoint ID is not specified".

  As a workaround, Neon provides a special connection option that allows clients to specify the compute endpoint they are connecting to. The connection option was previously named `project`. This option name is now deprecated but remains supported for backward compatibility. The new name for the connection option is `endpoint`, which is used as shown in the following example:

   ```txt
  postgres://<user>:<password>@ep-mute-recipe-239816.us-east-2.aws.neon.tech/main?options=endpoint%3Dep-mute-recipe-239816
  ```

  For more information about this special connection option for PostgreSQL clients that do not support SNI, refer to our [connection workarounds](https://neon.tech/docs/connect/connectivity-issues#workarounds) documentation.

### Fixed

- Pageserver: Branch deletion status was not tracked in S3 storage, which could result in a deleted branch remaining accessible.
- Pageserver: Addressed intermittent `failed to flush page requests` errors by adjusting Pageserver timeout settings.

## 2023-5-05

### Added

- Pageserver: Added WAL receiver context information for `Timed out while waiting for WAL record` errors. The additional information is used for diagnostic purposes.
- Safekeeper, Pageserver: Added Safekeeper and Pageserver metrics that count the number of received queries, broker messages, removed WAL segments, and connection switch events.

### Fixed

- Safekeeper: When establishing a connection to a Safekeeper, an `Lsn::INVALID` value was sent from the Safekeeper to the Pageserver if there were no WAL records to send. This incorrectly indicated to the Pageserver that the Safekeeper was lagging behind, causing the Pageserver to connect to a different Safekeeper. Instead of `Lsn::INVALID`, the most recent `commit_lsn` value is now sent instead.

## 2023-05-09

### Changed

- Compute: Dockerfile updates for the [neondatabase/neon](https://github.com/neondatabase/neon) repository are now automatically pushed to our [public Docker Hub repository](https://hub.docker.com/u/neondatabase). This enhancement means that you no longer need to manually track and incorporate Dockerfile updates to build and test Neon locally. Instead, these changes will be available and ready to use directly from our Docker Hub repository.
- Safekeepers: Enabled parallel offload of WAL segments to remote storage. This change allows Safekeepers to upload up to five WAL segments concurrently.

## 2023-04-28

### Added

- Compute: Added support for the `US East (N. Virginia) — aws-us-east-1` region. For more information about Neon's region support, see [Regions](https://neon.tech/docs/introduction/regions).
- Compute: Added support for the `ip4r` and `pg_hint_plan` extensions. For more information about PostgreSQL extensions supported by Neon, see [PostgreSQL extensions](https://neon.tech/docs/extensions/pg-extensions).
- Compute: Added support for `lz4` and `zstd` WAL compression methods.
- Compute: Added support for `procps`, which is a set of utilities for process monitoring.
- Pageserver: Implemented `syscalls` changes in the WAL redo `seccomp` (secure computing mode) code to ensure AArch64 support.

## 2023-04-11

### Added

- Pageserver: Added `disk_size` and `instance_type` properties to the Pageserver API. This data is required to support assigning Neon projects to Pageservers based on Pageserver disk usage.
- Proxy: Added error reporting for unusually low `proxy_io_bytes_per_client metric` values.
- Proxy: Added support for additional domain names to enable partner integrations with Neon.
- Safekeeper: The `wal_backup_lsn` is now advanced after each WAL segment is offloaded to storage to avoid lags in WAL segment cleanup.
- Safekeeper: Added a timeout for reading from the socket in the Safekeeper WAL service to avoid an accumulation of waiting threads.

### Fixed

- Pageserver: Corrected an issue that caused data layer eviction to occur at a percentage above the configured disk-usage threshold.
- Proxy: The passwordless authentication proxy ignored non-wildcard common names, passing a `None` value instead. A non-wildcard common name is now set, and an error is reported if a `None` value is passed.

## 2023-03-28

### Changed

- Pageserver: Logical size and partitioning values are now computed before threshold-based eviction of data layers to avoid downloading previously evicted data layer files when restarting a Pageserver.
- Compute: Free space in the local file system that Neon uses for temporary files, unlogged tables, and the local file cache, is now monitored in order to maximize the space available for the local file cache.

### Fixed

- Pageserver: The delete timeline endpoint in the Pageserver management API did not return the proper HTTP code.
- Pageserver: Fixed an issue in a data storage size calculation that caused an incorrect value to be returned.
- Pageserver: Addressed unexpected data layer downloads that occurred after a Pageserver restart. The data layers most likely required for the data storage size calculation after a Pageserver restart are now retained.

## 2023-03-21

### Added

- Added metrics that enable detection of data layer eviction thrashing (repetition of eviction and on-demand download of data layers).
- Safekeeper: Added an internal metric to track bytes written or read in PostgreSQL connections to Safekeepers, which enables monitoring traffic between availability zones.

### Changed

- Pageserver: Improved the check for unexpected trailing data when importing a basebackup, which is tarball with files required to bootstrap a compute node.
- Pageserver: Separated the management and `libpq` configuration, making it possible to enable authentication for only the management HTTP API or the Compute API.
- Pageserver: Reduced the amount of metrics data collected for Pageservers.
- Pageserver: JWT (JSON Web Token) generation is now permitted to fail when running Pageservers with authentication disabled, which enables running without the 'openssl' binary. This change enables switching to the EdDSA algorithm for storing JWT authentication tokens.
- Pageserver: Switched to the EdDSA algorithm for the storage JWT authentication tokens. The Neon Control Plane only supports EdDSA.
- Pageserver, Safekeeper: Revised `$NEON_AUTH_TOKEN` variable handling when connecting from a compute to Pageservers and Safekeepers.
- Proxy: All compute node connection errors are now logged.

### Fixed

- Pageserver: Fixed an issue that resulted in old data layers not being garbage collected.
- Proxy: Fixed an issue that caused Websocket connections through the Proxy to become unresponsive.

### Removed

- Pageserver, Safekeeper: Removed unused Control Plane code.

## 2023-03-13

### Added

- Compute: Released a new `pg_tiktoken` PostgreSQL extension, created by the Neon engineering team. The  extension is a wrapper for [OpenAI’s tokenizer](https://github.com/openai/tiktoken). It provides fast and efficient tokenization of data stored in a PostgreSQL database.
  The extension supports two functions:

    - The `tiktoken_encode` function takes text input and returns tokenized output, making it easier to analyze and process text data.
    - The `tiktoken_count` function returns the number of tokens in a text, which is useful for checking text length limits, such as those imposed by OpenAI’s language models.
  
  For more information about the `pg_tiktoken` extension, refer to the blog post: [Announcing pg_tiktoken: A Postgres Extension for Fast BPE Tokenization](https://neon.tech/blog/announcing-pg_tiktoken-a-postgres-extension-for-fast-bpe-tokenization). The `pg_tiktoken` code is available on [GitHub](https://github.com/kelvich/pg_tiktoken).
- Compute: Added support for the PostgreSQL `prefix`, `hll` and `plpgsql_check` extensions. For more information about PostgreSQL extensions supported by Neon, see [PostgreSQL extensions](https://neon.tech/docs/extensions/pg-extensions).
- Compute, Pageserver, Safekeeper: Added support for RS384 and RS512 JWT tokens, used to securely transmit information as JSON objects.
- Autoscaling: Added support for scaling Neon's local file cache size when scaling a virtual machine.

### Removed

- Pageserver: Removed the block cursor cache, which provided little performance benefit and would hold page references that caused deadlocks.

## 2023-03-07

### Added

- Pageserver: Added logic to handle unexpected Write-Ahead Log (WAL) redo process failures, which could cause a `Broken pipe` error on the Pageserver. In case of a failure, the WAL redo process is now restarted, and requests to apply redo records are retried automatically.
- Pageserver: Added timeout logic for the copy operation that occurs when downloading a data layer. The timeout logic prevents a deadlock state if a data layer download is blocked.

### Fixed

- Safekeeper: Addressed `Failed to open WAL file` warnings that appeared in the Safekeeper log files. The warnings were due to an outdated `truncate_lsn` value on the Safekeeper, which caused the _walproposer_ (the Postgres compute node) to download WAL records starting from a Log Sequence Number (LSN) that was older than the `backup_lsn`. This resulted in unnecessary WAL record downloads from cold storage.

## 2023-03-03

### Added

- Compute: Added support for the PostgreSQL `rum` and `pgTAP` extensions. For more information about PostgreSQL extensions supported by Neon, see [PostgreSQL extensions](https://neon.tech/docs/extensions/pg-extensions).

### Fixed

- Pageserver: A system metric that monitors physical data size overflowed when a garbage collection operation was performed on an evicted data layer.
- Pageserver: An index upload was skipped when a compaction operation did not perform an on-demand download from storage. With no on-demand downloads, the compaction function would exit before scheduling the index upload.

## 2023-02-28

### Added

- Compute: Added support for the following PostgreSQL extensions:
  - `pg_graphql`
  - `pg_jsonschema`
  - `pg_hashids`
  - `pgrouting`
  - `hypopg`
  - Server Programming Interface (SPI) extensions:
    - `autoinc`
    - `insert_username`
    - `moddatetime`
    - `refint`
  
  For more information about PostgreSQL extensions supported by Neon, see [PostgreSQL extensions](https://neon.tech/docs/extensions/pg-extensions).

### Changed

- Compute: Updated supported PostgreSQL versions to [14.7](https://www.postgresql.org/docs/release/14.7/) and [15.2](https://www.postgresql.org/docs/release/15.2/), respectively.
- Pageserver: Optimized the log-structured merge-tree (LSM tree) implementation to reduce [write amplification](https://en.wikipedia.org/wiki/Write_amplification).

## 2023-02-21

### Added

- Compute: Added support for the PostgreSQL `xml2` and `pgjwt` extensions. For more information about PostgreSQL extensions supported by Neon, see [PostgreSQL extensions](https://neon.tech/docs/extensions/pg-extensions).

### Changed 

- Compute: Updated the versions for the following PostgreSQL extensions:
  - Updated the `address_standardizer`, `address_standardizer_data_us`, `postgis`, `postgis_raster`, `postgis_tiger_geocoder`, `postgis_topology` extensions to version `3.3.2`.
  - Updated the `plv8`, `plls`, `plcoffee` extensions to `3.1.5`.
  - Updated the `h3_pg` extension to `4.1.2`.

  Updating an extension version requires running an `ALTER EXTENSION <extension_name> UPDATE TO <new_version>` statement. For example, to update the `postgis_topology` extension to the newly supported version, run this statement:

  ```sql
  ALTER EXTENSION postgis_topology UPDATE TO '3.3.2';
  ```

- Proxy: Enabled `OpenTelemetry` tracing to capture all incoming requests. This change enables Neon to perform an end-to-end trace when a new connection is established.

### Fixed

- Pageserver: Corrected the storage size metrics calculation to ensure that only active branches are counted.

## 2023-02-14

### Added

- Compute: Added support for the PostgreSQL `pgvector`, `plls` and `plcoffee` extensions. For more information about PostgreSQL extensions supported by Neon, see [PostgreSQL extensions](https://neon.tech/docs/extensions/pg-extensions).
- Pageserver: Added an experimental feature that automatically evicts layer files from Pageservers to optimize local storage space usage. When enabled for a project, Pageservers periodically check the access timestamps of the project's layer files. If the most recent layer file access is further in the past than a configurable threshold, the Pageserver removes the layer file from local storage. The authoritative copy of the layer file remains in S3. A Pageserver can download a layer file from S3 on-demand if it is needed again, to reconstruct a page version for a client request, for example.

### Changed

- Proxy: Reduced network latencies for WebSocket and pooled connections by implementing caching mechanism for compute node connection information and enabling the `TCP_NODELAY` protocol option. The `TCP_NODELAY` option causes segments to be sent as soon as possible, even if there is only a small amount of data. For more information, refer to the [TCP protocol man page](https://linux.die.net/man/7/tcp).

## 2023-02-07

### Added

- Compute: Added support for the PostgreSQL `postgis-sfcgal` extension. For more information about PostgreSQL extensions supported by Neon, see [PostgreSQL extensions](https://neon.tech/docs/extensions/pg-extensions).
- Compute: Added support for [International Components for Unicode (ICU)](https://icu.unicode.org/), which permits defining collation objects that use ICU as the collation provider. For example:

    ```sql
    CREATE COLLATION german (provider = icu, locale = 'de');
    ```

## 2023-01-23

### Fixed

- Compute: Fixed a compute instance restart error. When a compute instance was restarted after a role was deleted in the console, the restart operation failed with a "role does not exist" error while attempting to reassign the objects owned by the deleted role.

## 2023-01-31

### Added

- Compute: Added support for the PostgreSQL `unit` extension. For more information about PostgreSQL extensions supported by Neon, see [PostgreSQL extensions](https://neon.tech/docs/extensions/pg-extensions).
- Pageserver: Implemented an asynchronous pipe for communication with the Write Ahead Log (WAL) redo process, which helps improves OLAP query performance.

### Changed

- Pageserver: Reimplemented the layer map used to track the data layers in a branch. The layer map now uses an immutable binary search tree (BST) data structure, which improves data layer lookup performance over the previous R-tree implementation. The data required to reconstruct page versions is stored as data layers in Neon Pageservers.
- Pageserver: Changed the garbage collection (`gc`) interval from 100 seconds to 60 minutes. This change reduces the frequency of layer map locks.

### Removed

- Compute: Removed logic that updated roles each time a Neon compute instance was restarted. Roles were updated on each restart to address a password-related backward compatibility issue that is no longer relevant.

## 2023-01-17

### Added

- Compute: Added support for several PostgreSQL extensions. Newly supported extensions include:
  - `bloom`
  - `pgrowlocks`
  - `intagg`
  - `pgstattuple`
  - `earthdistance`
  - `address_standardizer`
  - `address_standardizer_data_us`
  
  For more information about PostgreSQL extensions supported by Neon, see [PostgreSQL extensions](https://neon.tech/docs/extensions/pg-extensions).
- Compute: Added statistics to `EXPLAIN` that show prefetch hits and misses for sequential scans.

### Changed

- Compute: Updated the list of PostgreSQL client libraries and runtimes that Neon tests for connection support. The `pg8000` Python PostgreSQL driver, version 1.29.3 and higher, now supports connecting to Neon.
- Proxy: Updated the error message reported when attempting to connect from a client or driver that does not support Server Name Indication (SNI). For more information about the SNI requirement, see [Connect from old clients](https://neon.tech/docs/connect/connectivity-issues). Previously, the error message indicated that the "Project ID" is not specified. The error message now states that the "Endpoint ID" is not specified. Connecting to Neon with a Project ID remains supported for backward compatibility, but connecting with an Endpoint ID is now the recommended connection method. For general information about connecting to Neon, see [Connect from any application](https://neon.tech/docs/connect/connect-from-any-app).

## 2022-12-08

### Added

- Pageserver: Added support for on-demand download of layer files from cold storage. Layer files contain the data required reconstruct any version of a data page. On-demand download enables Neon to quickly distribute data across Pageservers and recover from local Pageserver failures. This feature augments Neon's storage capability by allowing data to be transferred efficiently from cold storage to Pageservers whenever the data is needed.

## 2022-11-16

### Added

- Pageserver: Added a tenant sizing model and an endpoint for retrieving the tenant size.

### Changed

- Pageserver: Moved the Write-Ahead Log (WAL) redo process code from Neon's `postgres` repository to the `neon` repository and created a separate `wal_redo` binary in order to reduce the amount of change in the `postgres` repository codebase.
- Compute: Updated prefetching support to store requests and responses in a ring buffer instead of a queue, which enables using prefetches from many relations concurrently.

### Removed

- Pageserver, Safekeeper, Compute, and Proxy: Reduced the size of Neon storage binaries by 50% by removing dependency debug symbols from the release build.
- Pageserver and Safekeeper: Removed support for the `--daemonize` option from the CLI process that starts the Pageserver and Safekeeper storage components. The required library is no longer being maintained and the option was only used in our test environment.

## 2022-10-25

### Added

- Compute: Added support for PostgreSQL 15.0 and its PostgreSQL extensions.
For information about supported extensions, see [Supported PostgreSQL extensions](https://neon.tech/docs/extensions/pg-extensions).
- Pageserver: Added a timeline `state` field to the `TimelineInfo` struct that is returned by the `timelines` internal management API endpoint. Timeline `state` information improves observability and communication between Pageserver modules.

### Changed

- Compute: Disabled the `wal_log_hints` parameter, which is the default PostgreSQL setting. The Pageserver-related issue that required enabling `wal_log_hints` has been addressed, and enabling `wal_log_hints` is no longer necessary.

## 2022-10-21

### Fixed

- Compute: Fixed an issue that prevented creating a database when the specified database name included trailing spaces.
- Pageserver: Fixed an `INSERT ... ON CONFLICT` handling issue for speculative Write-Ahead Log (WAL) record inserts. Under certain load conditions, records added with `INSERT ... ON CONFLICT` could be replayed incorrectly.
- Pageserver: Fixed a Free Space Map (FSM) and Visibility Map (VM) page reconstruction issue that caused compute nodes to start slowly under certain workloads.
- Pageserver: Fixed a garbage collection (GC) issue that could lead to a database failure under high load.
- Pageserver: Improved query performance for larger databases by improving R-tree layer map search. The envelope for each layer is now remembered so that it does not have to be reconstructed for each call.

## 2022-10-07

### Added

- Pageserver: Added initial support for online tenant relocation.
- Pageserver: Added support for multiple PostgreSQL versions.
- Compute: Added support for the `h3_pg` and `plv8` PostgreSQL extensions. For information about PostgreSQL extensions supported by Neon, see [PostgreSQL extensions](https://neon.tech/docs/extensions/pg-extensions).
- Compute: Added support for a future implementation of sequential scan prefetch, which improves I/O performance for operations such as table scans.

### Changed

- Pageserver: Increased the default `compaction_period` setting to 20 seconds to reduce the frequency of polling that is performed to determine if compaction is required. The frequency of polling with the previous setting of 1 could result in excessive CPU consumption when there are numerous tenants and projects.
- Compute: Moved the backpressure throttling algorithm to the Neon extension to minimize changes to the Neon PostgreSQL core code, and added a `backpressure_throttling_time` function that returns the total time spent throttling since the system was started.
- Proxy: Improved error messages and logging.

## 2022-09-01

### Added

- Compute: Added support for the `PostGIS` extension, version 3.3.0. For information about PostgreSQL extensions supported by Neon, see [PostgreSQL extensions](https://neon.tech/docs/extensions/pg-extensions).
- Proxy: Added support for forwarding the `options`, `application_name`, and `replication` connection parameters to compute nodes.

### Changed

- Compute: Updated the PostgreSQL version to 14.5.

## 2022-08-02

### Added

- Compute: Installed the `uuid-ossp` extension binaries, which provide functions for generating universally unique identifiers (UUIDs). `CREATE EXTENSION "uuid-ossp"` is now supported. For information about  extensions supported by Neon, see [Available PostgreSQL extensions]https://neon.tech/docs/extensions/pg-extensions).
- Compute: Added logging for compute node initialization failure during the 'basebackup' stage.
- Pageserver: Added reporting of the physical size with the tenant status, in the internal management API.

### Changed

- Pageserver: Merged the 'wal_receiver' endpoint with 'timeline_detail', in the internal management API.

### Fixed

- Pageserver: Avoided busy looping when deletion from cloud storage is skipped due to failed upload tasks.

## 2022-07-19

### Added

- Safekeeper: Added support for backing up Write-Ahead Logs (WAL) to S3 storage for disaster recovery.
- Safekeeper: Added support for downloading WAL from S3 storage on demand.
- Proxy: Added support for propagating SASL/SCRAM PostgreSQL authentication errors to clients.
- Pageserver: Implemented a page service `fullbackup` endpoint that works like basebackup but also sends relational files.
- Pageserver: Added support for importing a base backup taken from a standalone PostgreSQL instance or another Pageserver using `psql` copy.
- Compute: Enabled the use of the `CREATE EXTENSION` statement for users that are not database owners.

### Changed

- Safekeeper: Switched to [etcd](https://etcd.io/) subscriptions to keep Pageservers up to date with the Safekeeper status.
- Safekeeper: Implemented JSON Web Token (JWT) authentication in the Safekeeper HTTP API.
- Compute: Updated the PostgreSQL version to 14.4.
- Compute: Renamed the following custom configuration parameters:
  - `zenith.page_server_connstring` to `neon.pageserver_connstring`
  - `zenith.zenith_tenant` to `neon.tenant_id`
  - `zenith.zenith_timeline` to `neon.timeline_id`
  - `zenith.max_cluster_size` to `neon.max_cluster_size`
  - `wal_acceptors` to `safekeepers`
- Control Plane: Renamed `zenith_admin` role to `cloud_admin`.
- Pageserver: Updated the timeline size reported when `DROP DATABASE` is executed.
- Pageserver: Switched to per-tenant attach/detach. Download operations of all timelines for one tenant are now grouped together so that branches can be used safely with attach/detach.
- Pageserver: Decreased the number of threads by running gc and compaction in a blocking tokio thread pool.
- Compute: Enabled the use of the `CREATE EXTENSION` statement for users that are not database owners.

### Fixed

- Pageserver: Fixed the database size calculation to count Visibility Maps (VMs) and Free Space Maps (FSMs) in addition to the main fork of the relation.
- Safekeeper: Fixed the walreceiver connection selection mechanism:
  - Reconnecting to a Safekeeper immediately after it fails is now avoided by limiting candidates to those with the fewest connection attempts.
  - Increased the `max_lsn_wal_lag` default setting to avoid constant reconnections during normal work.
  - Fixed `wal_connection_attempts` maintenance, preventing busy reconnection loops.

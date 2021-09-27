## Current state

NOTE: I used current names of threads, which are quite chaotic.
It would be nice to use one uniform style.

#### pageserver
Main entry point for the pageserver executable.
At start pageserver sets up logging, auth, metrics collection and starts following threads:

- http_endpoint_thread
- Page Service thread

#### http_endpoint_thread
Thread that serves incoming service requests, such as create tenant/branch, list tenants/branches.

#### Page Service thread
Listens for connections, and launches a new `conn_handler` handler thread for each.

#### conn_handler() // these threads are not named
Per connection handler.
Communicates with one PostgresBackend using libpq protocol. Serves queries.

#### Threads that run per tenant (repository)
-------------

#### wal_redo_manager
Runs a child process with postgres binary in wal-redo mode.

#### WAL receiver thread
Connects to the WAL service, streams, parses and saves WAL in the repository.
Starts on callmemaybe request.

#### Checkpointer
Flushes in-memory layers to disk.
Currently it is launched at pageserver start only for pre-existing tenants.

#### GC
Removes on-disk layers that are not visible anymore.
Currently it is launched at pageserver start only for pre-existing tenants.

#### relish_uploader
Send layer files to S3 (or other object storage).

----------------

## Questions:

1. Should we always spawn tenant threads for all tenants given that we expect lots of inactive tenants?
Now we spawn Checkpointer and GC for all pre-existing tenants at pageserver start.
See `tenant_mgr::init()`.

2. How to stop threads and wal_redo process for inactive tenant? Should we introduce some timeout?

3. Should we use per tenant `page_service` threads?

4. What actions are needed for graceful pageserver shutdown?
    - Stop receiving new incoming connections.
    - Cancel existing connections.
    - Stop receiveing new WAL. Does WAL service need special actions on pageserver shutdown?
    - Shutdown checkpointer and GC to avoid interference with final checkpoint.
    - Checkpoint all timelines for each tenant.
    - Wait till everything is uploaded to s3.
    - Quit.

5. How checkpointer, GC and relish_uploader should work in general?
Now they loop forewer and awake once in `checkpoint_period`/`gc_period` to do the job.
Should we use some different way to control their activity?

Also we need to relauch these threads if they fail.

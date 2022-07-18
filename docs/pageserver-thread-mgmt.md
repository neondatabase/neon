## Thread management

Each thread in the system is tracked by the `thread_mgr` module. It
maintains a registry of threads, and which tenant or timeline they are
operating on. This is used for safe shutdown of a tenant, or the whole
system.

### Handling shutdown

When a tenant or timeline is deleted, we need to shut down all threads
operating on it, before deleting the data on disk. A thread registered
in the thread registry can check if it has been requested to shut down,
by calling `is_shutdown_requested()`. For async operations, there's also
a `shudown_watcher()` async task that can be used to wake up on shutdown.

### Sync vs async

The primary programming model in the page server is synchronous,
blocking code. However, there are some places where async code is
used. Be very careful when mixing sync and async code.

Async is primarily used to wait for incoming data on network
connections. For example, all WAL receivers have a shared thread pool,
with one async Task for each connection. Once a piece of WAL has been
received from the network, the thread calls the blocking functions in
the Repository to process the WAL.

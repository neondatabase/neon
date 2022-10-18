## Thread management

The pageserver uses Tokio for handling concurrency. Everything runs in
Tokio tasks, although some parts are written in blocking style and use
spawn_blocking().

Each Tokio task is tracked by the `task_mgr` module. It maintains a
registry of tasks, and which tenant or timeline they are operating
on.

### Handling shutdown

When a tenant or timeline is deleted, we need to shut down all tasks
operating on it, before deleting the data on disk. There's a function,
`shutdown_tasks`, to request all tasks of a particular tenant or
timeline to shutdown. It will also wait for them to finish.

A task registered in the task registry can check if it has been
requested to shut down, by calling `is_shutdown_requested()`. There's
also a `shudown_watcher()` Future that can be used with `tokio::select!`
or similar, to wake up on shutdown.


### Sync vs async

We use async to wait for incoming data on network connections, and to
perform other long-running operations. For example, each WAL receiver
connection is handled by a tokio Task. Once a piece of WAL has been
received from the network, the task calls the blocking functions in
the Repository to process the WAL.

The core storage code in `layered_repository/` is synchronous, with
blocking locks and I/O calls. The current model is that we consider
disk I/Os to be short enough that we perform them while running in a
Tokio task. If that becomes a problem, we should use `spawn_blocking`
before entering the synchronous parts of the code, or switch to using
tokio I/O functions.

Be very careful when mixing sync and async code!

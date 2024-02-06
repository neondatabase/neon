## Thread management

The pageserver uses Tokio for handling concurrency. Everything runs in
Tokio tasks, although some parts are written in blocking style and use
spawn_blocking().

We currently use std blocking functions for disk I/O, however.  The
current model is that we consider disk I/Os to be short enough that we
perform them while running in a Tokio task. Changing all the disk I/O
calls to async is a TODO.

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
also a `shutdown_watcher()` Future that can be used with `tokio::select!`
or similar, to wake up on shutdown.


### Async cancellation safety

In async Rust, futures can be "cancelled" at any await point, by
dropping the Future. For example, `tokio::select!` returns as soon as
one of the Futures returns, and drops the others. `tokio::time::timeout`
is another example. In the Rust ecosystem, some functions are
cancellation-safe, meaning they can be safely dropped without
side-effects, while others are not. See documentation of
`tokio::select!` for examples.

In the pageserver and safekeeper, async code is *not*
cancellation-safe by default. Unless otherwise marked, any async
function that you call cannot be assumed to be async
cancellation-safe, and must be polled to completion.

The downside of non-cancellation safe code is that you have to be very
careful when using `tokio::select!`, `tokio::time::timeout`, and other
such functions that can cause a Future to be dropped. They can only be
used with functions that are explicitly documented to be cancellation-safe,
or you need to spawn a separate task to shield from the cancellation.

At the entry points to the code, we also take care to poll futures to
completion, or shield the rest of the code from surprise cancellations
by spawning a separate task. The code that handles incoming HTTP
requests, for example, spawns a separate task for each request,
because Hyper will drop the request-handling Future if the HTTP
connection is lost.


#### How to cancel, then?

If our code is not cancellation-safe, how do you cancel long-running
tasks? Use CancellationTokens.

TODO: More details on that. And we have an ongoing discussion on what
to do if cancellations might come from multiple sources.

#### Exceptions
Some library functions are cancellation-safe, and are explicitly marked
as such. For example, `utils::seqwait`.

#### Rationale

The alternative would be to make all async code cancellation-safe,
unless otherwise marked. That way, you could use `tokio::select!` more
liberally. The reasons we didn't choose that are explained in this
section.

Writing code in a cancellation-safe manner is tedious, as you need to
scrutinize every `.await` and ensure that if the `.await` call never
returns, the system is in a safe, consistent state. In some ways, you
need to do that with `?` and early `returns`, too, but `.await`s are
easier to miss. It is also easier to perform cleanup tasks when a
function returns an `Err` than when an `.await` simply never
returns. You can use `scopeguard` and Drop guards to perform cleanup
tasks, but it is more tedious. An `.await` that never returns is more
similar to a panic.

Note that even if you only use building blocks that themselves are
cancellation-safe, it doesn't mean that the code as whole is
cancellation-safe. For example, consider the following code:

```
while let Some(i) = work_inbox.recv().await {
	if let Err(_) = results_outbox.send(i).await {
		println!("receiver dropped");
		return;
		}
	}
}
```

It reads messages from one channel, sends them to another channel. If
this code is cancelled at the `results_outbox.send(i).await`, the
message read from the receiver is lost. That may or may not be OK,
depending on the context.

Another reason to not require cancellation-safety is historical: we
already had a lot of async code that was not scrutinized for
cancellation-safety when this issue was raised. Scrutinizing all
existing code is no fun.

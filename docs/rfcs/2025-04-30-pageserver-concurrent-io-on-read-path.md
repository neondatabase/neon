# Concurrent IO for Pageserver Read Path

Date: May 6, 2025

## Summary

This document is a retroactive RFC on the Pageserver Concurrent IO work that happened in late 204 / early 2025.

The gist of it is that Pageserver's `Timeline::get_vectored` now _issues_ the data block read operations against layer files _as it traverses the layer map_ and only _wait_ once, for all of them, after traversal is complete.

Assuming 100% PS PageCache hits on the index blocks during traversal, this drives down the "wait-for-disk" time contribution down from `random_read_io_latency * O(number_of_values)` to `random_read_io_latency * O(1 + traversal)`.

The motivation for why this work had to happen when it happened was the switch of Pageserver to
- not cache user data blocks in PS PageCache and
- switch to use direct IO.
More context on this are given in complimentary RFC `./rfcs/2025-04-30-direct-io-for-pageserver.md`.

### Refs

- Epic: https://github.com/neondatabase/neon/issues/9378
- Prototyping happened during the Lisbon 2024 Offsite hackathon: https://github.com/neondatabase/neon/pull/9002
- Main implementation PR with good description: https://github.com/neondatabase/neon/issues/9378

Design and implementation by:
- Vlad Lazar <vlad@neon.tech>
- Christian Schwarz <christian@neon.tech>

## Background & Motivation

The Pageserver read path (`Timeline::get_vectored`) consists of two high-level steps:
- Retrieve the delta and image `Value`s required to reconstruct the requested Page@LSN (`Timeline::get_values_reconstruct_data`)
- Pass these values to walredo to reconstruct the page images.

The read path used to be single page but has been made multi-page some time ago (`030-vectored-timeline-get.md`).

The retrieval step above can be further broken down into the following jobs:
- **Traversal** of the layer map to figure out which `Value`s from which layer files are required for the page reconstruction.
  The algorithm for this is well-described in `030-vectored-timeline-get.md`.
- **Read IO Planning**: planning of the read IOs that need to be issued to the layer files / filesystem / disk.
  The main job here is to coalesce the small value reads into larger filesystem-level read operations.
  This layer also takes care of direct IO alignment and size-multiple requirements (cf the RFC for details.)
  Check `struct VectoredReadPlanner` and `mod vectored_dio_read` for how it's done.
- **Perform the read IO** using `tokio-epoll-uring`.

Before this project, above jobs were sequentially interleaved, meaning:
1. we would advance traversal, ...
2. discover, that we need to read a value, ...
3. read it from disk using `tokio-epoll-uring`, ...
4. goto 1 unless we're done.

This means that if N `Value`s need to be read to reconstruct a page, the time we spend waiting for disk will be we `random_read_io_latency * O(number_of_values)`.

## Design

The **traversal** and **read IO Planning** jobs happen sequentially, layer by layer, as before.
But instead of performing the read IOs inline, we submit the IOs to a concurrent tokio task for execution.
After the last read from the last layer is submitted, we wait for the IOs to complete.

Assuming the filesystem / disk is able to actually process the submitted IOs without queuing,
we arrive at _time spent waiting for disk_ ~ `random_read_io_latency * O(1 + traversal)`.

### Avoiding IOs For Traversal

The `traversal` component in above time-spent-waiting-for-disk estimation is dominant and needs to be minimized.

Before this work, traversal needed to perform IOs for the following:

1. The time we are waiting on PS PageCache for the disk btree index block reads.
2. When visiting a delta layer, reading the data block that contains a `Value` for a requested key,
   to determine whether the `Value::will_init` the page and therefore traversal can stop for this key.

The solution for (1) is to raise the PS PageCache size such that the hit rate is near 100%.
(Check out the `Background: History Of Caching In Pageserver` section in the RFC on Direct IO for more details.)

The solution for (2) is source `will_init` from the disk btree index keys, which fortunately
already encode this bit of information since the introduction of the current storage/layer format.

### Concurrent IOs, Submission & Completion

To separate IO submission from waiting for its completion, we introduce the notion of
an `IoConcurrency` struct through which IO futures are "spawned".

An IO is an opaque future, and waiting for completions is handled through
`tokio::sync::oneshot` channels.
The oneshot Receiver's take the place of the `img` and `records` fields
inside `VectoredValueReconstructState`.

When we're done visiting all the layers and submitting all the IOs along the way
we concurrently `collect_pending_ios` for each value, which means
for each value there is a future that awaits all the oneshot receivers
and then calls into walredo to reconstruct the page image.
Walredo is now invoked concurrently for each value instead of sequentially.
Walredo itself remains unchanged.

The spawned IO futures are driven to completion by a sidecar tokio task that
is separate from the task that performs all the layer visiting and spawning of IOs.
That tasks receives the IO futures via an unbounded mpsc channel and
drives them to completion inside a `FuturedUnordered`.

### Error handling

There are two error classes during reconstruct data retrieval:
* traversal errors: index lookup, move to next layer, and the like
* value read IO errors

A traversal error fails the entire get_vectored request, as before this PR.
A value read error only fails that value.

In any case, we preserve the existing behavior that once
`get_vectored` returns, all IOs are done. Panics and failing
to poll `get_vectored` to completion will leave the IOs dangling,
which is safe but shouldn't happen, and so, a rate-limited
log statement will be emitted at warning level.
There is a doc comment on `collect_pending_ios` giving more code-level
details and rationale.

### Sidecar Task Lifecycle

The sidecar tokio task is spawned as part of the `IoConcurrency::spawn_from_conf` struct.
The `IoConcurrency` object acts as a handle through which IO futures are submitted.

The spawned tokio task holds the `Timeline::gate` open.
It is _not_ sensitive to `Timeline::cancel`, but instead to the `IoConcurrency` object being dropped.

Once the `IoConcurrency` struct is dropped, no new IO futures can come in
but already submitted IO futures will be driven to completion regardless.
We _could_ safely stop polling these futures because `tokio-epoll-uring` op futures are cancel-safe.
But the underlying kernel and hardware resources are not magically freed up by that.
So, in the interest of closing out all outstanding resource usage before completing timeline shutdown, we wait for completion,
which under normal conditions should be in the low hundreds of microseconds.

It is advisable to make the `IoConcurrency` as long-lived as possible to minimize the amount of
tokio task churn (=> lower pressure on tokio). Generally this means creating it "high up" in the call stack.
The pain with this is that the `IoConcurrency` reference needs to be propagated "down" to
the (short-lived) functions/scope where we issue the IOs.
We would like to use `RequestContext` for this propagation in the future (issue [here](https://github.com/neondatabase/neon/issues/10460)).
For now, we just add another argument to the relevant code paths.

### Feature Gating

The `IoConcurrency` is an `enum` with two variants: `Sequential` and `SidecarTask`.

The behavior from before this project is available through `IoConcurrency::Sequential`,
which awaits the IO futures in place, without "spawning" or "submitting" them
anywhere.
The only significant change in `Sequential` mode compared to before this project
is the buffering of results in the `oneshot`s, i.e., allocation and indirection overheads,
which didn't have measurable impact on latency in our testing.

The `get_vectored_concurrent_io` pageserver config variable determines the runtime value,
**except** for the places that use `IoConcurrency::sequential` to get an `IoConcurrency` object.

### Alternatives Explored & Caveats Encountered

A few words on the rationale behind having a sidecar *task* and what
alternatives were considered but abandoned.

#### Why We Need A Sidecar *Task* / Why Just `FuturesUnordered` Doesn't Work

We explored to not have a sidecar task, and instead have a `FuturesUnordered` per
`Timeline::get_vectored`. We would queue all IO futures in it and poll it for the
first time after traversal is complete (i.e., at `collect_pending_ios`).

The obvious disadvantage, but not showstopper, is that we wouldn't be submitting
IOs until traversal is complete.

The showstopper however, is that deadlocks can happen if we don't drive the
IO futures to completion independently of the traversal task.
The reason is that both the IO futures and the traversal task may hold some
_and_ try to acquire _more_ shared limited resources.
For example, both the travseral task and IO future may try to acquire
* a `VirtualFile` file descriptor cache slot async mutex (observed during impl)
* a `tokio-epoll-uring` submission slot (observed during impl)
* a `PageCache` slot (currently this is not the case but we may move more code into the IO futures in the future)

#### Why We Don't Do `tokio::task`-per-IO-future

Another option is to spawn a short-lived `tokio::task` for each IO future.
We implemented and benchmarked it during development, but found little
throughput improvement and moderate mean & tail latency degradation.
Concerns about pressure on the tokio scheduler made us discard this variant.

## Future Work

In addition to what is listed here, also check the "Punted" list in the epic:
https://github.com/neondatabase/neon/issues/9378

### Enable `Timeline::get`

The only major code path that still uses `IoConcurrency::sequential` is `Timeline::get`.
The impact is that roughly the following parts of pageserver do not benefit yet:
- parts of basebackup
- reads performed by the ingest path
- most internal operations that read metadata keys (e.g. `collect_keyspace`!)

The solution is to propagate `IoConcurrency` via `RequestContext`:https://github.com/neondatabase/neon/issues/10460

The tricky part is to figure out at which level of the code the `IoConcurrency` is spawned (and added to the RequestContext).

Also, propagation via `RequestContext` makes makes it harder to tell during development whether a given
piece of code uses concurrent vs sequential mode: one has to recurisvely walk up the call tree to find the
place that puts the `IoConcurrency` into the `RequestContext`.
We'd have to use `::Sequential` as the conservative default value in a fresh `RequestContext`, and add some
observability to weed out places that fail to enrich with a properly spanwed `IoConcurrency::spawn_from_conf`.

### New `tokio-epoll-uring` API That Separates Submission & Wait-For-Completion

Instead of `$op().await` style API, it would be useful to have a different `tokio-epoll-uring` API
that separates enqueuing (without necessarily `io_uring_enter`ing the kernel each time), submission,
and then wait for completion.

The `$op().await` API is too opaque, so we _have_ to stuff it into a `FuturesUnordered`.

A split API as sketched above would allow traversal to ensure an IO operation is enqueued to the kernel/disk (and get back-pressure iff the io_uring squeue is full).
While avoiding spending of CPU cycles on processing of completions while we're still traversing.

The idea gets muddied by the fact that we may self-deadlock if we submit too much without completing.
So, the submission part of the split API needs to process completions if squeue is full.

In any way, this split API is precondition for the bigger issue with the design presented here,
which we dicsuss in the next section.

### Opaque Futures Are Brittle

The use of opaque futures to represent submitted IOs is a clever hack to minimize changes & allow for near-perfect feature-gating.
However, we take on **brittleness** because callers must guarantee that the submitted futures are independent.
By our experience, it is non-trivial to identify or rule out the interdependencies.
See the lengthy doc comment on the `IoConcurrency::spawn_io` method for more details.

The better interface and proper subsystem boundary is a _descriptive_ struct of what needs to be done ("read this range from this VirtualFile into this buffer")
and get back a means to wait for completion.
The subsystem can thereby reason by its own how operations may be related;
unlike today, where the submitted opaque future can do just about anything.

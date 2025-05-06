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

The solution for (1) 

### `will_init` is now sourced from disk btree index keys

Another aspect that is change
move to the next  is that the `get_values_reconstruct_data`
now sources `will_init` from the disk btree index key (which is PS-page_cache'd), instead
of from the `Value`, which would only available after the IO completes.

This allows us to progress with

### Concurrent IOs, Submission & Completion

To separate IO submission from waiting for its completion, while simultaneously
feature-gating the change, we introduce the notion of an `IoConcurrency` struct
through which IO futures are "spawned".

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

(The behavior from before this PR is available through `IoConcurrency::Sequential`,
which awaits the IO futures in place, without "spawning" or "submitting" them
anywhere.)

#### Alternatives Explored

A few words on the rationale behind having a sidecar *task* and what
alternatives were considered.

One option is to queue up all IO futures in a FuturesUnordered that is polled
the first time when we `collect_pending_ios`.

Firstly, the IO futures are opaque, compiler-generated futures that need
to be polled at least once to submit their IO. "At least once" because
tokio-epoll-uring may not be able to submit the IO to the kernel on first
poll right away.

Second, there are deadlocks if we don't drive the IO futures to completion
independently of the spawning task.
The reason is that both the IO futures and the spawning task may hold some
_and_ try to acquire _more_ shared limited resources.
For example, both spawning task and IO future may try to acquire
* a VirtualFile file descriptor cache slot async mutex (observed during impl)
* a tokio-epoll-uring submission slot (observed during impl)
* a PageCache slot (currently this is not the case but we may move more code into the IO futures in the future)

Another option is to spawn a short-lived `tokio::task` for each IO future.
We implemented and benchmarked it during development, but found little
throughput improvement and moderate mean & tail latency degradation.
Concerns about pressure on the tokio scheduler made us discard this variant.

The sidecar task could be obsoleted if the IOs were not arbitrary code but a well-defined struct.
However,
1. the opaque futures approach taken in this PR allows leaving the existing
   code unchanged, which
2. allows us to implement the `IoConcurrency::Sequential` mode for feature-gating
   the change.

Once the new mode sidecar task implementation is rolled out everywhere,
and `::Sequential` removed, we can think about a descriptive submission & completion interface.
The problems around deadlocks pointed out earlier will need to be solved then.
For example, we could eliminate VirtualFile file descriptor cache and tokio-epoll-uring slots.
The latter has been drafted in https://github.com/neondatabase/tokio-epoll-uring/pull/63.

See the lengthy doc comment on `spawn_io()` for more details.

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

### Feature Gating

The new behavior is opt-in via pageserver config.
The `Sequential` mode is the default.
The only significant change in `Sequential` mode compared to before
this PR is the buffering of results in the `oneshot`s.

## Code-Level Changes

Prep work:
  * Make `GateGuard` clonable.

Core Feature:
* Traversal code: track  `will_init` in `BlobMeta` and source it from
  the Delta/Image/InMemory layer index, instead of determining `will_init`
  after we've read the value. This avoids having to read the value to
  determine whether traversal can stop.
* Introduce `IoConcurrency` & its sidecar task.
  * `IoConcurrency` is the clonable handle.
  * It connects to the sidecar task via an `mpsc`.
* Plumb through `IoConcurrency` from high level code to the
  individual layer implementations' `get_values_reconstruct_data`.
  We piggy-back on the `ValuesReconstructState` for this.
   * The sidecar task should be long-lived, so, `IoConcurrency` needs
     to be rooted up "high" in the call stack.
   * Roots as of this PR:
     * `page_service`: outside of pagestream loop
     * `create_image_layers`: when it is called
     * `basebackup`(only auxfiles + replorigin + SLRU segments)
   * Code with no roots that uses `IoConcurrency::sequential`
     * any `Timeline::get` call
       * `collect_keyspace` is a good example
       * follow-up: https://github.com/neondatabase/neon/issues/10460
     * `TimelineAdaptor` code used by the compaction simulator, unused in practive
     * `ingest_xlog_dbase_create`
* Transform Delta/Image/InMemoryLayer to
  * do their values IO in a distinct `async {}` block
  * extend the residence of the Delta/Image layer until the IO is done
  * buffer their results in a `oneshot` channel instead of straight
    in `ValuesReconstructState`
  * the `oneshot` channel is wrapped in `OnDiskValueIo` / `OnDiskValueIoWaiter`
    types that aid in expressiveness and are used to keep track of
    in-flight IOs so we can print warnings if we leave them dangling.
* Change `ValuesReconstructState` to hold the receiving end of the
 `oneshot` channel aka `OnDiskValueIoWaiter`.
* Change `get_vectored_impl` to `collect_pending_ios` and issue walredo concurrently, in a `FuturesUnordered`.

Testing / Benchmarking:
* Support queue-depth in pagebench for manual benchmarkinng.
* Add test suite support for setting concurrency mode ps config
   field via a) an env var and b) via NeonEnvBuilder.
* Hacky helper to have sidecar-based IoConcurrency in tests.
   This will be cleaned up later.

More benchmarking will happen post-merge in nightly benchmarks, plus in staging/pre-prod.
Some intermediate helpers for manual benchmarking have been preserved in https://github.com/neondatabase/neon/pull/10466 and will be landed in later PRs.
(L0 layer stack generator!)

Drive-By:
* test suite actually didn't enable batching by default because
  `config.compatibility_neon_binpath` is always Truthy in our CI environment
  => https://neondb.slack.com/archives/C059ZC138NR/p1737490501941309
* initial logical size calculation wasn't always polled to completion, which was
  surfaced through the added WARN logs emitted when dropping a
  `ValuesReconstructState` that still has inflight IOs.
* remove the timing histograms `pageserver_getpage_get_reconstruct_data_seconds`
  and `pageserver_getpage_reconstruct_seconds` because with planning, value read
  IO, and walredo happening concurrently, one can no longer attribute latency
  to any one of them; we'll revisit this when Vlad's work on tracing/sampling
  through RequestContext lands.
* remove code related to `get_cached_lsn()`.
  The logic around this has been dead at runtime for a long time,
  ever since the removal of the materialized page cache in #8105.

## Testing

Unit tests use the sidecar task by default and run both modes in CI.
Python regression tests and benchmarks also use the sidecar task by default.
We'll test more in staging and possibly preprod.

# Future Work

Please refer to the parent epic for the full plan.

The next step will be to fold the plumbing of IoConcurrency
into RequestContext so that the function signatures get cleaned up.

Once `Sequential` isn't used anymore, we can take the next
big leap which is replacing the opaque IOs with structs
that have well-defined semantics.

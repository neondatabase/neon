# Direct IO For Pageserver

Date: Apr 30, 2025

## Summary

This document is a retroactive RFC. It
- provides some background on what direct IO is,
- motivates why Pageserver should be using it for its IO, and
- describes how we changed Pageserver to use it.

The [initial proposal](https://github.com/neondatabase/neon/pull/8240) that kicked off the work can be found in this closed GitHub PR.

People primarily involved in this project were:
- Yuchen Liang <yuchen@neon.tech>
- Vlad Lazar <vlad@neon.tech>
- Christian Schwarz <christian@neon.tech>

## Timeline

For posterity, here is the rough timeline of the development work that got us to where we are today.

- Jan 2024: [integrate `tokio-epoll-uring`](https://github.com/neondatabase/neon/pull/5824) along with owned buffers API
- March 2024: `tokio-epoll-uring` enabled in all regions in buffered IO mode
- Feb 2024 to June 2024: PS PageCache Bypass For Data Blocks
  - Feb 2024: [Vectored Get Implementation](https://github.com/neondatabase/neon/pull/6576) bypasses delta & image layer blocks for page requests
  - Apr to June 2024: [Epic: bypass PageCache for use data blocks](https://github.com/neondatabase/neon/issues/7386) addresses remaining users
- Aug to Nov 2024: direct IO: first code; preliminaries; read path coding; BufferedWriter; benchmarks show perf regressions too high, no-go.
- Nov 2024 to Jan 2025: address perf regressions by developing page_service pipelining (aka batching) and concurrent IO ([Epic](https://github.com/neondatabase/neon/issues/9376))
- Feb to March 2024: rollout batching, then concurrent+direct IO => read path and InMemoryLayer is now direct IO
- Apr 2025: develop & roll out direct IO for the write path

## Background: Terminology & Glossary

**kernel page cache**: the Linux kernel's page cache is a write-back cache for filesystem contents.
The cached unit is memory-page-sized & aligned chunks of the files that are being cached (typically 4k).
The cache lives in kernel memory and is not directly accessible through userspace.

**Buffered IO**: an application's read/write system calls go through the kernel page cache.
For example, a 10 byte sized read or write to offset 5000 in a file will load the file contents
at offset `[4096,8192)` into a free page in the kernel page cache. If necessary, it will evict
a page to make room (cf eviction). Then, the kernel performs a memory-to-memory copy of 10 bytes
from/to the offset `4` (`5000 = 4096 + 4`) within the cached page. If it's a write, the kernel keeps
track of the fact that the page is now "dirty" in some ancillary structure.

**Writeback**: a buffered read/write syscall returns after the memory-to-memory copy. The modifications
made by e.g. write system calls are not even *issued* to disk, let alone durable. Instead, the kernel
asynchronously writes back dirtied pages based on a variety of conditions. For us, the most relevant
ones are a) explicit request by userspace (`fsync`) and b) memory pressure.

**Memory pressure**: the kernel page cache is a best effort service and a user of spare memory capacity.
If there is no free memory, the kernel page allocator will take pages used by page cache to satisfy allocations.
Before reusing a page like that, the page has to be written back (writeback, see above).
The far-reaching consequence of this is that **any allocation of anonymous memory can do IO** if the only
way to get that memory is by eviction & re-using a dirty page cache page.
Notably, this includes a simple `malloc` in userspace, because eventually that boils down to `mmap(..., MAP_ANON, ...)`.
I refer to this effect as the "malloc latency backscatter" caused by buffered IO.

**Direct IO** allows application's read/write system calls to bypass the kernel page cache. The filesystem
is still involved because it is ultimately in charge of mapping the concept of files & offsets within them
to sectors on block devices. Typically, the filesystem poses size and alignment requirements for memory buffers
and file offsets (statx `Dio_mem_align` / `Dio_offset_align`), see [this gist](https://gist.github.com/problame/1c35cac41b7cd617779f8aae50f97155).
The IO operations will fail at runtime with EINVAL if the alignment requirements are not met.

**"buffered" vs "direct"**: the central distinction between buffered and direct IO is about who allocates and
fills the IO buffers, and who controls when exactly the IOs are issued. In buffered IO, it's the syscall handlers,
kernel page cache, and memory management subsystems (cf "writeback"). In direct IO, all of it is done by
the application.
It takes more effort by the application to program with direct instead of buffered IO.
The return is precise control over and a clear distinction between consumption/modification of memory vs disk.

**Pageserver PageCache**: Pageserver has an additional `PageCache` (referred to as PS PageCache from here on, as opposed to "kernel page cache").
Its caching unit is 8KiB blocks of the layer files written by Pageserver.
A miss in PageCache is filled by reading from the filesystem, through the `VirtualFile` abstraction layer.
The default size is tiny (64MiB), very much like Postgres's `shared_buffers`.
We ran production at 128MiB for a long time but gradually moved it up to 2GiB over the past ~year.

**VirtualFile** is Pageserver's abstraction for file IO, very similar to the facility in Postgres that bears the same name.
Its historical purpose appears to be working around open file descriptor limitations, which is practically irrelevant on Linux.
However, the facility in Pageserver is useful as an intermediary layer for metrics and abstracts over the different kinds of
IO engines that Pageserver supports (`std-fs` vs `tokio-epoll-uring`).

## Background: History Of Caching In Pageserver

For multiple years, Pageserver's `PageCache` was on the path of all read _and write_ IO.
It performed write-back to the kernel using buffered IO.

We converted it into a read-only cache of immutable data in [PR 4994](https://github.com/neondatabase/neon/pull/4994).

The introduction of `tokio-epoll-uring` required converting the code base to used owned IO buffers.
The `PageCache` pages are usable as owned IO buffers.

We then started bypassing PageCache for user data blocks.
Data blocks are the 8k blocks of data in layer files that hold the multiple `Value`s, as opposed to the disk btree index blocks that tell us which values exist in a file at what offsets.
The disk btree embedded in delta & image layers remains `PageCache`'d.
Epics for that work were:
- Vectored `Timeline::get` (cf RFC 30) skipped delta and image layer data block `PageCache`ing outright.
- Epic https://github.com/neondatabase/neon/issues/7386 took care of the remaining users for data blocks:
  - Materialized page cache (cached materialized pages; shown to be ~0% hit rate in practice)
  - InMemoryLayer
  - Compaction

The outcome of the above:
1. All data blocks are always read through the `VirtualFile` APIs, hitting the kernel buffered read path (=> kernel page cache).
2. Indirect blocks (=disk btree blocks) would be cached in the PS `PageCache`.

In production we size the PS `PageCache` to be 2GiB.
Thus drives hit rate up to ~99.95% and the eviction rate / replacement rates down to less than 200/second on a 1-minute average, on the busiest machines.
High baseline replacement rates are treated as a signal of resource exhaustion (page cache insufficient to host working set of the PS).
The response to this is to migrate tenants away, or increase PS `PageCache` size.
It is currently manual but could be automated, e.g., in Storage Controller.

In the future, we may eliminate the `PageCache` even for indirect blocks.
For example with an LRU cache that has as unit the entire disk btree content
instead of individual blocks.

## High-Level Design

So, before work on this project started, all data block reads and the entire write path of Pageserver were using kernel-buffered IO, i.e., the kernel page cache.
We now want to get the kernel page cache out of the picture by using direct IO for all interaction with the filesystem.
This achieves the following system properties:

**Predictable VirtualFile latencies**
* With buffered IO, reads are sometimes fast, sometimes slow, depending on kernel page cache hit/miss.
* With buffered IO, appends when writing out new layer files during ingest or compaction are sometimes fast, sometimes slow because of write-back backpressure.
* With buffered IO, the "malloc backscatter" phenomenon pointed out in the Glossary section is not something we actively observe.
  But we do have occasional spikes in Dirty memory amount and Memory PSI graphs, so it may already be affecting to some degree.
* By switching to direct IO, above operations will have the (predictable) device latency -- always.
  Reads and appends always go to disk.
  And malloc will not have to write back dirty data.

**Explicitness & Tangibility of resource usage**
* In a multi-tenant system, it is generally desirable and valuable to be *explicit* about the main resources we use for each tenant.
* By using direct IO, we become explicit about the resources *disk IOPs*  and *memory capacity* in a way that was previously being conflated through the kernel page cache, outside our immediate control.
* We will be able to build per-tenant observability of resource usage ("what tenant is causing the actual IOs that are sent to the disk?").
* We will be able to build accounting & QoS by implementing an IO scheduler that is tenant aware. The kernel is not tenant-aware and can't do that.

**CPU Efficiency**
* The involvement of the kernel page cache means one additional memory-to-memory copy on read and write path.
* Direct IO will eliminate that memory-to-memory copy, if we can make the userspace buffers used for the IO calls satisfy direct IO alignment requirements.

The **trade-off** is that we no longer get the theoretical benefits of the kernel page cache. These are:
- read latency improvements for repeat reads of the same data ("locality of reference")
  - asterisk: only if that state is still cache-resident by time of next access
- write throughput by having kernel page cache batch small VFS writes into bigger disk writes
  - asterisk: only if memory pressure is low enough that the kernel can afford to delay writeback

We are **happy to make this trade-off**:
- Because of the advantages listed above.
- Because we empirically have enough DRAM on Pageservers to serve metadata (=index blocks) from PS PageCache.
  (At just 2GiB PS PageCache size, we average a 99.95% hit rate).
  So, the latency of going to disk is only for data block reads, not the index traversal.
- Because **the kernel page cache is ineffective** at high tenant density anyway (#tenants/pageserver instance).
  And because dense packing of tenants will always be desirable to drive COGS down, we should design the system for it.
  (See the appendix for a more detailed explanation why this is).
- So, we accept that some reads that used to be fast by circumstance will have higher but **predictable** latency than before.

### Desired End State

The desired end state of the project is as follows, and with some asterisks, we have achieved it.

All IOs of the Pageserver data path use direct IO, thereby bypassing the kernel page cache.

In particular, the "data path" includes
- the wal ingest path
- compaction
- anything on the `Timeline::get` / `Timeline::get_vectored` path.

The production Pageserver config is tuned such that virtually all non-data blocks are cached in the PS PageCache.
Hit rate target is 99.95%.

There are no regressions to ingest latency.

The total "wait-for-disk time" contribution to random getpage request latency is `O(1 read IOP latency)`.
We accomplish that by having a near 100% PS PageCache hit rate so that layer index traversal effectively never needs not wait for IO.
Thereby, it can issue all the data blocks as it traverses the index, and only wait at the end of it (concurrent IO).

The amortized "wait-for-disk time" contribution of this direct IO proposal to a series of sequential getpage requests is `1/32 * read IOP latency` for each getpage request.
We accomplish this by server-side batching of up to 32 reads into a single `Timeline::get_vectored` call.
(This is an ideal world where our batches are full - that's not the case in prod today because of lack of queue depth).

## Design & Implementation

### Prerequisites

A lot of prerequisite work had to happen to enable use of direct IO.

To meet the "wait-for-disk time" requirements from the DoD, we implement for the read path:
- page_service level server-side batching (config field `page_service_pipelining`)
- concurrent IO (config field `get_vectored_concurrent_io`)
The work for both of these these was tracked [in the epic](https://github.com/neondatabase/neon/issues/9376).
Server-side batching will likely be obsoleted by the [#proj-compute-communicator](https://github.com/neondatabase/neon/pull/10799).
The Concurrent IO work is described in retroactive RFC `2025-04-30-pageserver-concurrent-io-on-read-path.md`.
The implementation is relatively brittle and needs further investment, see the `Future Work` section in that RFC.

For the write path, and especially WAL ingest, we need to hide write latency.
We accomplish this by implementing a (`BufferedWriter`) type that does double-buffering: flushes of the filled
buffer happen in a sidecar tokio task while new writes fill a new buffer.
We refactor InMemoryLayer as well as BlobWriter (=> delta and image layer writers) to use this new `BufferedWriter`.
The most comprehensive write-up of this work is in [the PR description](https://github.com/neondatabase/neon/pull/11558).

### Ensuring Adherence to Alignment Requirements

Direct IO puts requirements on
- memory buffer alignment
- io size (=memory buffer size)
- file offset alignment

The requirements are specific to a combination of filesystem/block-device/architecture(hardware page size!).

In Neon production environments we currently use ext4 with Linux 6.1.X on AWS and Azure storage-optimized instances (locally attached NVMe).
Instead of dynamic discovery using `statx`, we statically hard-code 512 bytes as the buffer/offset alignment and size-multiple.
We made this decision because:
- a) it is compatible with all the environments we need to run in
- b) our primary workload can be small-random-read-heavy (we do merge adjacent reads if possible, but the worst case is that all `Value`s that needs to be read are far apart)
- c) 512-byte tail latency on the production instance types is much better than 4k (p99.9: 3x lower, p99.99 5x lower).
- d) hard-coding at compile-time allows us to use the Rust type system to enforce the use of only aligned IO buffers, eliminating a source of runtime errors typically associated with direct IO.

This was [discussed here](https://neondb.slack.com/archives/C07BZ38E6SD/p1725036790965549?thread_ts=1725026845.455259&cid=C07BZ38E6SD).

The new `IoBufAligned` / `IoBufAlignedMut` marker traits indicate that a given buffer meets memory alignment requirements.
All `VirtualFile` APIs and several software layers built on top of them only accept buffers that implement those traits.
Implementors of the marker traits are:
- `IoBuffer` / `IoBufferMut`: used for most reads and writes
- `PageWriteGuardBuf`: for filling PS PageCache pages (index blocks!)

The alignment requirement is infectious; it permeates bottom-up throughout the code base.
We stop the infection at roughly the same layers in the code base where we stopped permeating the
use of owned-buffers-style API for tokio-epoll-uring. The way the stopping works is by introducing
a memory-to-memory copy from/to some unaligned memory location on the stack/current/heap.
The places where we currently stop permeating are sort of arbitrary. For example, it would probably
make sense to replace more usage of `Bytes` that we know holds 8k pages with 8k-sized `IoBuffer`s.

The `IoBufAligned` / `IoBufAlignedMut` types do not protect us from the following types of runtime errors:
- non-adherence to file offset alignment requirements
- non-adherence to io size requirements

The following higher-level constructs ensure we meet the requirements:
- read path: the `ChunkedVectoredReadBuilder` and `mod vectored_dio_read` ensure reads happen at aligned offsets and in appropriate size multiples.
- write path: `BufferedWriter` only writes in multiples of the capacity, at offsets that are `start_offset+N*capacity`; see its doc comment.

Note that these types are used always, regardless of whether direct IO is enabled or not.
There are some cases where this adds unnecessary overhead to buffered IO (e.g. all memcpy's inflated to multiples of 512).
But we could not identify meaningful impact in practice when we shipped these changes while we were still using buffered IO.

### Configuration / Feature Flagging

In the previous section we described how all users of VirtualFile were changed to always adhere to direct IO alignment and size-multiple requirements.
To actually enable direct IO, all we need to do is set the `O_DIRECT` flag in `open` syscalls / io_uring operations.

We set `O_DIRECT` based on:
- the VirtualFile API used to create/open the VirtualFile instance
- the `virtual_file_io_mode` configuration flag
- the OpenOptions `read` and/or `write` flags.

The VirtualFile APIs suffixed with `_v2` are the only ones that _may_ open with `O_DIRECT` depending on the other two factors in above list.
Other APIs never use `O_DIRECT`.
(The name is bad and should really be `_maybe_direct_io`.)

The reason for having new APIs is because all code used VirtualFile but implementation and rollout happened in consecutive phases (read path, InMemoryLayer, write path).
At the VirtualFile level, context on whether an instance of VirtualFile is on read path, InMemoryLayer, or write path is not available.

The `_v2` APIs then check make the decision to set `O_DIRECT` based on the `virtual_file_io_mode` flag and the OpenOptions `read`/`write` flags.
The result is the following runtime behavior:

|what|OpenOptions|`v_f_io_mode`<br/>=`buffered`|`v_f_io_mode`<br/>=`direct`|`v_f_io_mode`<br/>=`direct-rw`|
|-|-|-|-|-|
|`DeltaLayerInner`|read|()|O_DIRECT|O_DIRECT|
|`ImageLayerInner`|read|()|O_DIRECT|O_DIRECT|
|`InMemoryLayer`|read + write|()|()*|O_DIRECT|
|`DeltaLayerWriter`| write | () | () |  O_DIRECT |
|`ImageLayerWriter`| write | () | () |  O_DIRECT |
|`download_layer_file`|write |()|()|O_DIRECT|

The `InMemoryLayer` is marked with `*` because there was a period when it *did* use O_DIRECT under `=direct`.
That period was when we implemented and shipped the first version of `BufferedWriter`.
We used it in `InMemoryLayer` and `download_layer_file` but it was only sensitive to `v_f_io_mode` in `InMemoryLayer`.
The introduction of `=direct-rw`, and the switch of the remaining write path to `BufferedWriter`, happened later,
in https://github.com/neondatabase/neon/pull/11558.

Note that this way of feature flagging inside VirtualFile makes it less and less a general purpose POSIX file access abstraction.
For example, with `=direct-rw` enabled, it is no longer possible to open a `VirtualFile` without `O_DIRECT`. It'll always be set.

## Correctness Validation

The correctness risks with this project were:
- Memory safety issues in the `IoBuffer` / `IoBufferMut` implementation.
  These types expose an API that is largely identical to that of the `bytes` crate and/or Vec.
- Runtime errors (=> downtime / unavailability) because of non-adherence to alignment/size-multiple requirements, resulting in EINVAL on the read path.

We sadly do not have infrastructure to run pageserver under `cargo miri`.
So for memory safety issues, we relied on careful peer review.

We do assert the production-like alignment requirements in testing builds.
However, these asserts were added retroactively.
The actual validation before rollout happened in staging and pre-prod.
We eventually enabled  `=direct`/`=direct-rw` for Rust unit tests and the regression test suite.
I cannot recall a single instance of staging/pre-prod/production errors caused by non-adherence to alignment/size-multiple requirements.
Evidently developer testing was good enough.

## Performance Validation

The read path went through a lot of iterations of benchmarking in staging and pre-prod.
The benchmarks in those environments demonstrated performance regressions early in the implementation.
It was actually this performance testing that made us implement batching and concurrent IO to avoid unacceptable regressions.

The write path was much quicker to validate because `bench_ingest` covered all of the (less numerous) access patterns.

## Future Work

There is minor and major follow-up work that can be considered in the future.
Check the (soon-to-be-closed) Epic https://github.com/neondatabase/neon/issues/8130's "Follow-Ups" section for a current list.

Read Path:
- PS PageCache hit rate is crucial to unlock concurrent IO and reasonable latency for random reads generally.
  Instead of reactively sizing PS PageCache, we should estimate the required PS PageCache size
  and potentially also use that to drive placement decisions of shards from StorageController
  https://github.com/neondatabase/neon/issues/9288
- ... unless we get rid of PS PageCache entirely and cache the index block in a more specialized cache.
  But even then, an estimation of the working set would be helpful to figure out caching strategy.

Write Path:
- BlobWriter and its users could switch back to a borrowed API  https://github.com/neondatabase/neon/issues/10129
- ... unless we want to implement bypass mode for large writes https://github.com/neondatabase/neon/issues/10101
- The `TempVirtualFile` introduced as part of this project could internalize more of the common usage pattern: https://github.com/neondatabase/neon/issues/11692
- Reduce conditional compilation around `virtual_file_io_mode`: https://github.com/neondatabase/neon/issues/11676

Both:
- A performance simulation mode that pads VirtualFile op latencies to typical NVMe latencies, even if the underlying storage is faster.
  This would avoid misleadingly good performance on developer systems and in benchmarks on systems that are less busy than production hosts.
  However, padding latencies at microsecond scale is non-trivial.

Misc:
- We should finish trimming VirtualFile's scope to be truly limited to core data path read & write.
  Abstractions for reading & writing pageserver config, location config, heatmaps, etc, should use
  APIs in a different package (`VirtualFile::crashsafe_overwrite` and `VirtualFile::read_to_string`
  are good entrypoints for cleanup.) https://github.com/neondatabase/neon/issues/11809

# Appendix

## Why Kernel Page Cache Is Ineffective At Tenant High Density

In the Motivation section, we stated:

> - **The kernel page cache ineffective** at high tenant density anyways (#tenants/pageserver instance).

The reason is that the  Pageserver workload sent from Computes is whatever is a Compute cache(s) miss.
That's either sequential scans or random reads.
A random read workload simply causes cache thrashing because a packed Pageserver NVMe drive (`im4gn.2xlarge`) has ~100x more capacity than DRAM available.
It is complete waste to have the kernel page cache cache data blocks in this case.
Sequential read workloads *can* benefit iff those pages have been updated recently (=no image layer yet) and together in time/LSN space.
In such cases, the WAL records of those updates likely sit on the same delta layer block.
When Compute does a sequential scan, it sends a series of single-page requests for these individual pages.
When Pageserver processes the second request in such a series, it goes to the same delta layer block and have a kernel page cache hit.
This dependence on kernel page cache for sequential scan performance is significant, but the solution is at a higher level than generic data block caching.
We can either add a small per-connection LRU cache for such delta layer blocks.
Or we can merge those sequential requests into a larger vectored get request, which is designed to never read a block twice.
This amortizes the read latency for our delta layer block across the vectored get batch size (which currently is up to 32).

There are Pageserver-internal workloads that do sequential access (compaction, image layer generation), but these
1. are not latency-critical and can do batched access outside of the `page_service` protocol constraints (image layer generation)
2. don't actually need to reconstruct images and therefore can use totally different access methods (=> compaction can use k-way merge iterators with their own internal buffering / prefetching).

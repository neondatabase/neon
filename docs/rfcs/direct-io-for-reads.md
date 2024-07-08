# Direct IO For Pageserver

## Summary

This document is a proposal and implementation plan for direct IO in Pageserver.

## Terminology

**kernel page cache**: the kernel's page cache is a write-back cache for filesystem contents.
The cached unit is memory-page-sized & aligned chunks of the files that are being cached (typically 4k).
The cache lives in kernel memory and is not directly accessible through userspace.

**Buffered IO**: the application's read/write system calls go through the kernel page cache.
For example, a 10 byte sized read or write to offset 5000 in a file will load the file contents
at offset `[4096,8192)` into a free page in the kernel page cache. If necessary, it will evict
other pages to make room (cf eviction). Then, the kernel performs a memory-to-memory copy of 10 bytes
from/to the offset `4` (`5000 = 4096 + 4`) within the cached page. If it's a write, the kernel keeps
track of the fact that the page is now "dirty" in some ancillary structure.

**Writeback**: a buffered read/write syscall returns after the memory-to-memory copy. The moficiations
made by e.g. write system calls are not even *issued* to disk, let alone durable. Instead, the kernel
asynchronously writes back dirtied pages based on a variety of conditions. For us, the most relevant
ones are a) explicit request by userspace (`fsync`) and b) memory pressure.

**Memory pressure**: the kernel page cache is a best effort service and a user of spare memory capacity.
The kernel page allocator will take pages used by page cache if there is no other free memory available.
Before reusing a page like that, the page has to be written back (writeback, see above).
The far-reaching consequence of this is that **any allocation of anonymous memory can do IO** if the only
way to get that memory is by eviction & re-using a dirty page cache page.
Notably, this includes a simple `malloc` in userspace, because eventually that boils down to `mmap(..., MAP_ANON, ...)`.

**Direct IO** allows application's read/write system calls to bypass the kernel page cache. The filesystem
is still involved because it is ultimately in charge of mapping the concept of files & offsets within them
to sectors on block devices.

**"buffered" vs "direct"**: the central distinction between buffered and direct IO is about who allocates and
fills the IO buffers, and who controls when exactly the IOs are issued. In buffered IO, it's the syscall handlers,
kernel page cache, and memory management subsystems (cf "writeback"). In direct IO, all of it is done by
the application.
It takes more effort by the application to program with direct instead of buffered IO.
The return is precise control over and a clear distinction between consumption/modification of memory vs disk.

**Pageserver PageCache**: Pageserver has an additioanl `PageCache` (referred to as PageCache from here on, as opposed to "kernel page cache").
Its caching unit is 8KiB which is the Postgres page size.
Currently, it is tiny (128MiB), very much like Postgres's `shared_buffers`.
A miss in PageCache is filled from the filesystem using buffered IO, issued through the `VirtualFile` layer in Pageserver.

**VirtualFIle** is Pageserver's abstraction for file IO, very similar to the faciltiy in Postgres that bears the same name.
Its historical purpose appears to be working around open file descriptor limitations, which is practically irrelevant on Linux.
However, the faciltiy in Pageserver is useful as an intermediary layer for metrics and abstracts over the different kinds of
IO engines that Pageserver supports (`std-fs` vs `tokio-epoll-uring`).

## History Of Caching In Pageserver

For multiple years, Pageserver's `PageCache` was used for all data path read _and write_ IO.
It performed write-back to the kernel using buffered IO.

We converted it into a read-only cache of immutable data in [PR 4994](https://github.com/neondatabase/neon/pull/4994).

The introduction of `tokio-epoll-uring` required converting the code base to used owned IO buffers.
The `PageCache` pages are usable as owned IO buffers.

We then introduced vectored `Timeline::get` (cf RFC 30).
The implementation bypasses PS `PageCache` for delta and image layer data block reads.
(The disk btree embedded in delta & image layers is still `PageCache`'d).

Most recently, and still ongoing, is [Epic: Bypass PageCache for user data blocks #7386](https://github.com/neondatabase/neon/issues/7386).
The goal there is to eliminate the remaining caching of user data blocks in PS `PageCache`.

The outcome of the above will be that
1. all data blocks are read through VirtualFile and
2. all indirect blocks (=disk btree blocks) are cached in the PS `PageCache`.
The norm will be very low baseline replacement rates in PS `PageCache`.
High baseline replacement rates will be treated as a signal of resource exhaustion (page cache insufficient to host working set of the PS).
It will be remediated by the storage controller, migrating tenants away to relieve pressure.

In the future, we may elminate the `PageCache` even for indirect blocks.
For example with an LRU cache that has as unit the entire disk btree content
instead of individual blocks.

##


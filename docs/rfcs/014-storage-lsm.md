# Why LSM trees?

In general, an LSM tree has the nice property that random updates are
fast, but the disk writes are sequential. When a new file is created,
it is immutable. New files are created and old ones are deleted, but
existing files are never modified. That fits well with storing the
files on S3.

Currently, we create a lot of small files. That is mostly a problem
with S3, because each GET/PUT operation is expensive, and LIST
operation only returns 1000 objects at a time, and isn't free
either. Currently, the files are "archived" together into larger
checkpoint files before they're uploaded to S3 to alleviate that
problem, but garbage collecting data from the archive files would be
difficult and we have not implemented it. This proposal addresses that
problem.


# Overview


```
^ LSN
|
|      Memtable:     +-----------------------------+
|                    |                             |
|                    +-----------------------------+
|
|
|            L0:     +-----------------------------+
|                    |                             |
|                    +-----------------------------+
|
|                    +-----------------------------+
|                    |                             |
|                    +-----------------------------+
|
|                    +-----------------------------+
|                    |                             |
|                    +-----------------------------+
|
|                    +-----------------------------+
|                    |                             |
|                    +-----------------------------+
|
|
|           L1:      +-------+ +-----+ +--+  +-+
|                    |       | |     | |  |  | |
|                    |       | |     | |  |  | |
|                    +-------+ +-----+ +--+  +-+
|
|                       +----+ +-----+ +--+  +----+
|                       |    | |     | |  |  |    |
|                       |    | |     | |  |  |    |
|                       +----+ +-----+ +--+  +----+
|
+--------------------------------------------------------------> Page ID


+---+
|   |   Layer file
+---+
```


# Memtable

When new WAL arrives, it is first put into the Memtable. Despite the
name, the Memtable is not a purely in-memory data structure. It can
spill to a temporary file on disk if the system is low on memory, and
is accessed through a buffer cache.

If the page server crashes, the Memtable is lost. It is rebuilt by
processing again the WAL that's newer than the latest layer in L0.

The size of the Memtable is configured by the "checkpoint distance"
setting. Because anything that hasn't been flushed to disk and
uploaded to S3 yet needs to be kept in the safekeeper, the "checkpoint
distance" also determines the amount of WAL that needs to kept in the
safekeeper.

# L0

When the Memtable fills up, it is written out to a new file in L0. The
files are immutable; when a file is created, it is never
modified. Each file in L0 is roughly 1 GB in size (*). Like the
Memtable, each file in L0 covers the whole key range.

When enough files have been accumulated in L0, compaction
starts. Compaction processes all the files in L0 and reshuffles the
data to create a new set of files in L1.


(*) except in corner cases like if we want to shut down the page
server and want to flush out the memtable to disk even though it's not
full yet.


# L1

L1 consists of ~ 1 GB files like L0. But each file covers only part of
the overall key space, and a larger range of LSNs. This speeds up
searches. When you're looking for a given page, you need to check all
the files in L0, to see if they contain a page version for the requested
page. But in L1, you only need to check the files whose key range covers
the requested page. This is particularly important at cold start, when
checking a file means downloading it from S3.

Partitioning by key range also helps with garbage collection. If only a
part of the database is updated, we will accumulate more files for
the hot part in L1, and old files can be removed without affecting the
cold part.


# Image layers

So far, we've only talked about delta layers. In addition to the delta
layers, we create image layers, when "enough" WAL has been accumulated
for some part of the database. Each image layer covers a 1 GB range of
key space. It contains images of the pages at a single LSN, a snapshot
if you will.

The exact heuristic for what "enough" means is not clear yet. Maybe
create a new image layer when 10 GB of WAL has been accumulated for a
1 GB segment.

The image layers limit the number of layers that a search needs to
check. That put a cap on read latency, and it also allows garbage
collecting layers that are older than the GC horizon.


# Partitioning scheme

When compaction happens and creates a new set of files in L1, how do
we partition the data into the files?

- Goal is that each file is ~ 1 GB in size
- Try to match partition boundaries at relation boundaries. (See [1]
  for how PebblesDB does this, and for why that's important)
- Greedy algorithm

# Additional Reading

[1] Paper on PebblesDB and how it does partitioning.
https://www.cs.utexas.edu/~rak/papers/sosp17-pebblesdb.pdf

# Postgres core changes

This lists all the changes that have been made to the PostgreSQL
source tree, as a somewhat logical set of patches. The long-term goal
is to eliminate all these changes, by submitting patches to upstream
and refactoring code into extensions, so that you can run unmodified
PostgreSQL against Neon storage.

In Neon, we run PostgreSQL in the compute nodes, but we also run a special WAL redo process in the
page server. We currently use the same binary for both, with --wal-redo runtime flag to launch it in
the WAL redo mode. Some PostgreSQL changes are needed in the compute node, while others are just for
the WAL redo process.

In addition to core PostgreSQL changes, there is a Neon extension in the pgxn/neon directory that
hooks into the smgr interface, and rmgr extension in pgxn/neon_rmgr. The extensions are loaded into
the Postgres processes with shared_preload_libraries. Most of the Neon-specific code is in the
extensions, and for any new features, that is preferred over modifying core PostgreSQL code.

Below is a list of all the PostgreSQL source code changes, categorized into changes needed for
compute, and changes needed for the WAL redo process:

# Changes for Compute node

## Prefetching

There are changes in many places to perform prefetching, for example for sequential scans. Neon
doesn't benefit from OS readahead, and the latency to pageservers is quite high compared to local
disk, so prefetching is critical for performance, also for sequential scans.

### How to get rid of the patch

Upcoming "streaming read" work in v17 might simplify this. And async I/O work in v18 will hopefully
do more.


## Add t_cid to heap WAL records

```
 src/backend/access/heap/heapam.c                            |   26 +-
 src/include/access/heapam_xlog.h                            |    6 +-
```

We have added a new t_cid field to heap WAL records. This changes the WAL record format, making Neon WAL format incompatible with vanilla PostgreSQL!

### Problem we're trying to solve

The problem is that the XLOG_HEAP_INSERT record does not include the command id of the inserted row. And same with deletion/update. So in the primary, a row is inserted with current xmin + cmin. But in the replica, the cmin is always set to 1. That works in PostgreSQL, because the command id is only relevant to the inserting transaction itself. After commit/abort, no one cares about it anymore. But with Neon, we rely on WAL replay to reconstruct the page, even while the original transaction is still running.

### How to get rid of the patch

Bite the bullet and submit the patch to PostgreSQL, to add the t_cid to the WAL records. It makes the WAL records larger, which could make this unpopular in the PostgreSQL community. However, it might simplify some logical decoding code; Andres Freund briefly mentioned in PGCon 2022 discussion on Heikki's Neon presentation that logical decoding currently needs to jump through some hoops to reconstruct the same information.

Update from Heikki (2024-04-17): I tried to write an upstream patch for that, to use the t_cid field for logical decoding, but it was not as straightforward as it first sounded.

### Alternatives
Perhaps we could write an extra WAL record with the t_cid information, when a page is evicted that contains rows that were touched a transaction that's still running. However, that seems very complicated.

## Mark index builds that use buffer manager without logging explicitly

```
 src/backend/access/gin/gininsert.c                          |    7 +
 src/backend/access/gist/gistbuild.c                         |   15 +-
 src/backend/access/spgist/spginsert.c                       |    8 +-

also some changes in src/backend/storage/smgr/smgr.c
```

pgvector 0.6.0 also needs a similar change, which would be very nice to get rid of too.

When a GIN index is built, for example, it is built by inserting the entries into the index more or
less normally, but without WAL-logging anything. After the index has been built, we iterate through
all pages and write them to the WAL. That doesn't work for Neon, because if a page is not WAL-logged
and is evicted from the buffer cache, it is lost. We have an check to catch that in the Neon
extension. To fix that, we've added a few functions to track explicitly when we're performing such
an operation: `smgr_start_unlogged_build`, `smgr_finish_unlogged_build_phase_1` and
`smgr_end_unlogged_build`.


### How to get rid of the patch

I think it would make sense to be more explicit about that in PostgreSQL too. So extract these
changes to a patch and post to pgsql-hackers.

Perhaps we could deduce that an unlogged index build has started when we see a page being evicted
with zero LSN. How to be sure it's an unlogged index build rather than a bug? Currently we have a
check for that and PANIC if we see page with zero LSN being evicted. And how do we detect when the
index build has finished? See https://github.com/neondatabase/neon/pull/7440 for an attempt at that.

## Track last-written page LSN

```
 src/backend/commands/dbcommands.c                           |   17 +-

Also one call to SetLastWrittenPageLSN() in spginsert.c, maybe elsewhere too
```

Whenever a page is evicted from the buffer cache, we remember its LSN, so that we can use the same
LSN in the GetPage@LSN request when reading the page back from the page server. The value is
conservative: it would be correct to always use the last-inserted LSN, but it would be slow because
then the page server would need to wait for the recent WAL to be streamed and processed, before
responding to any GetPage@LSN request.

The last-written page LSN is mostly tracked in the smgrwrite() function, without core code changes,
but there are a few exceptions where we've had to add explicit calls to the Neon-specific
SetLastWrittenPageLSN() function.

There's an open PR to track the LSN in a more-fine grained fashion:
https://github.com/neondatabase/postgres/pull/177

PostgreSQL v15 introduces a new method to do CREATE DATABASE that WAL-logs the database instead of
relying copying files and checkpoint. With that method, we probably won't need any special handling.
The old method is still available, though.

### How to get rid of the patch

Wait until v15?


## Allow startup without reading checkpoint record

In Neon, the compute node is stateless. So when we are launching compute node, we need to provide
some dummy PG_DATADIR. Relation pages can be requested on demand from page server. But Postgres
still need some non-relational data: control and configuration files, SLRUs,...  It is currently
implemented using basebackup (do not mix with pg_basebackup) which is created by pageserver. It
includes in this tarball config/control files, SLRUs and required directories.

As pageserver does not have the original WAL segments, the basebackup tarball includes an empty WAL
segment to bootstrap the WAL writing, but it doesn't contain the checkpoint record.  There are some
changes in xlog.c, to allow starting the compute node without reading the last checkpoint record
from WAL.

This includes code to read the `zenith.signal` file, which tells the startup code the LSN to start
at. When the `zenith.signal` file is present, the startup uses that LSN instead of the last
checkpoint's LSN. The system is known to be consistent at that LSN, without any WAL redo.


### How to get rid of the patch

???


### Alternatives

Include a fake checkpoint record in the tarball. Creating fake WAL is a bit risky, though; I'm
afraid it might accidentally get streamed to the safekeepers and overwrite or corrupt the real WAL.

## Disable sequence caching

```
diff --git a/src/backend/commands/sequence.c b/src/backend/commands/sequence.c
index 0415df9ccb..9f9db3c8bc 100644
--- a/src/backend/commands/sequence.c
+++ b/src/backend/commands/sequence.c
@@ -53,7 +53,9 @@
  * so we pre-log a few fetches in advance. In the event of
  * crash we can lose (skip over) as many values as we pre-logged.
  */
-#define SEQ_LOG_VALS   32
+/* Neon XXX: to ensure sequence order of sequence in Zenith we need to WAL log each sequence update. */
+/* #define SEQ_LOG_VALS        32 */
+#define SEQ_LOG_VALS   0
```

Due to performance reasons Postgres don't want to log each fetching of a value from a sequence, so
it pre-logs a few fetches in advance. In the event of crash we can lose (skip over) as many values
as we pre-logged. But with Neon, because page with sequence value can be evicted from buffer cache,
we can get a gap in sequence values even without crash.

### How to get rid of the patch

Maybe we can just remove it, and accept the gaps. Or add some special handling for sequence
relations in the Neon extension, to WAL log the sequence page when it's about to be evicted. It
would be weird if the sequence moved backwards though, think of PITR.

Or add a GUC for the amount to prefix to PostgreSQL, and force it to 1 in Neon.


## Make smgr interface available to extensions

```
 src/backend/storage/smgr/smgr.c                             |  203 +++---
 src/include/storage/smgr.h                                  |   72 +-
```

### How to get rid of the patch

Submit to upstream. This could be useful for the Disk Encryption patches too, or for compression.

We have submitted this to upstream, but it's moving at glacial a speed.
https://commitfest.postgresql.org/47/4428/

## Added relpersistence argument to smgropen()

```
 src/backend/access/heap/heapam_handler.c                    |    2 +-
 src/backend/catalog/storage.c                               |   10 +-
 src/backend/commands/tablecmds.c                            |    2 +-
 src/backend/storage/smgr/md.c                               |    4 +-
 src/include/utils/rel.h                                     |    3 +-
```

Neon needs to treat unlogged relations differently from others, so the smgrread(), smgrwrite() etc.
implementations need to know the 'relpersistence' of the relation. To get that information where
it's needed, we added the 'relpersistence' field to smgropen().

### How to get rid of the patch

Maybe 'relpersistence' would be useful in PostgreSQL for debugging purposes? Or simply for the
benefit of extensions like Neon. Should consider this in the patch to make smgr API usable to
extensions.

## Alternatives

Currently in Neon, unlogged tables live on local disk in the compute node, and are wiped away on
compute node restart. One alternative would be to instead WAL-log even unlogged tables, essentially
ignoring the UNLOGGED option. Or prohibit UNLOGGED tables completely. But would we still need the
relpersistence argument to handle index builds? See item on "Mark index builds that use buffer
manager without logging explicitly".

## Use smgr and dbsize_hook for size calculations

```
 src/backend/utils/adt/dbsize.c                              |   61 +-
```

In PostgreSQL, the rel and db-size functions scan the data directory directly. That won't work in Neon.

### How to get rid of the patch

Send patch to PostgreSQL, to use smgr API functions for relation size calculation instead. Maybe as
part of the general smgr API patch.



# WAL redo process changes

Pageserver delegates complex WAL decoding duties to Postgres, which means that the latter might fall
victim to carefully designed malicious WAL records and start doing harmful things to the system.  To
prevent this, the redo functions are executed in a separate process that is sandboxed with Linux
Secure Computing mode (see seccomp(2) man page).

As an alternative to having a separate WAL redo process, we could rewrite all redo handlers in Rust
This is infeasible. However, it would take a lot of effort to rewrite them, ensure that you've done
the rewrite correctly, and once you've done that, it would be a lot of ongoing maintenance effort to
keep the rewritten code in sync over time, across new PostgreSQL versions. That's why we want to
leverage PostgreSQL code.

Another alternative would be to harden all the PostgreSQL WAL redo functions so that it would be
safe to call them directly from Rust code, without needing the security sandbox. That's not feasible
for similar reasons as rewriting them in Rust.


## Don't replay change in XLogReadBufferForRedo that are not for the target page we're replaying

```
 src/backend/access/gin/ginxlog.c                            |   19 +-

Also some changes in xlog.c and xlogutils.c

Example:

@@ -415,21 +416,27 @@ ginRedoSplit(XLogReaderState *record)
        if (!isLeaf)
                ginRedoClearIncompleteSplit(record, 3);
 
-       if (XLogReadBufferForRedo(record, 0, &lbuffer) != BLK_RESTORED)
+       action = XLogReadBufferForRedo(record, 0, &lbuffer);
+       if (action != BLK_RESTORED && action != BLK_DONE)
                elog(ERROR, "GIN split record did not contain a full-page image of left page");
```

### Problem we're trying to solve

In PostgreSQL, if a WAL redo function calls XLogReadBufferForRead() for a page that has a full-page
image, it always succeeds. However, Neon WAL redo process is only concerned about replaying changes
to a singe page, so replaying any changes for other pages is a waste of cycles. We have modified
XLogReadBufferForRead() to return BLK_DONE for all other pages, to avoid the overhead. That is
unexpected by code like the above.

### How to get rid of the patch

Submit the changes to upstream, hope the community accepts them. There's no harm to PostgreSQL from
these changes, although it doesn't have any benefit either.

To make these changes useful to upstream PostgreSQL, we could implement a feature to look ahead the
WAL, and detect truncated relations. Even in PostgreSQL, it is a waste of cycles to replay changes
to pages that are later truncated away, so we could have XLogReadBufferForRedo() return BLK_DONE or
BLK_NOTFOUND for pages that are known to be truncated away later in the WAL stream.

### Alternatives

Maybe we could revert this optimization, and restore pages other than the target page too.

## Add predefined_sysidentifier flag to initdb

```
 src/backend/bootstrap/bootstrap.c                           |   13 +-
 src/bin/initdb/initdb.c                                     |    4 +

And some changes in xlog.c
```

This is used to help with restoring a database when you have all the WAL, all the way back to
initdb, but no backup. You can reconstruct the missing backup by running initdb again, with the same
sysidentifier.


### How to get rid of the patch

Ignore it. This is only needed for disaster recovery, so once we've eliminated all other Postgres
patches, we can just keep it around as a patch or as separate branch in a repo.


## pg_waldump flags to ignore errors

After creating a new project or branch in Neon, the first timeline can begin in the middle of a WAL segment. pg_waldump chokes on that, so we added some flags to make it possible to ignore errors.

### How to get rid of the patch

Like previous one, ignore it.



## Backpressure if pageserver doesn't ingest WAL fast enough

```
@@ -3200,6 +3202,7 @@ ProcessInterrupts(void)
                return;
        InterruptPending = false;
 
+retry:
        if (ProcDiePending)
        {
                ProcDiePending = false;
@@ -3447,6 +3450,13 @@ ProcessInterrupts(void)
 
        if (ParallelApplyMessagePending)
                HandleParallelApplyMessages();
+
+       /* Call registered callback if any */
+       if (ProcessInterruptsCallback)
+       {
+               if (ProcessInterruptsCallback())
+                       goto retry;
+       }
 }
```


### How to get rid of the patch

Submit a patch to upstream, for a hook in ProcessInterrupts. Could be useful for other extensions
too.


## SLRU on-demand download

```
 src/backend/access/transam/slru.c | 105 ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++-------------
 1 file changed, 92 insertions(+), 13 deletions(-)
```

### Problem we're trying to solve

Previously, SLRU files were included in the basebackup, but the total size of them can be large,
several GB, and downloading them all made the startup time too long.

### Alternatives

FUSE hook or LD_PRELOAD trick to intercept the reads on SLRU files


## WAL-log an all-zeros page as one large hole

- In XLogRecordAssemble()

### Problem we're trying to solve

This change was made in v16. Starting with v16, when PostgreSQL extends a relation, it first extends
it with zeros, and it can extend the relation more than one block at a time. The all-zeros page is WAL-ogged, but it's very wasteful to include 8 kB of zeros in the WAL for that. This hack was made so that we WAL logged a compact record with a whole-page "hole". However, PostgreSQL has assertions that prevent that such WAL records from being replayed, so this breaks compatibility such that unmodified PostreSQL cannot process Neon-generated WAL.

### How to get rid of the patch

Find another compact representation for a full-page image of an all-zeros page. A compressed image perhaps.


## Shut down walproposer after checkpointer

```
+                       /* Neon: Also allow walproposer background worker to be treated like a WAL sender, so that it's shut down last */
+                       if ((bp->bkend_type == BACKEND_TYPE_NORMAL || bp->bkend_type == BACKEND_TYPE_BGWORKER) &&
```

This changes was needed so that postmaster shuts down the walproposer process only after the shutdown checkpoint record is written. Otherwise, the shutdown record will never make it to the safekeepers.

### How to get rid of the patch

Do a bigger refactoring of the postmaster state machine, such that a background worker can specify
the shutdown ordering by itself. The postmaster state machine has grown pretty complicated, and
would benefit from a refactoring for the sake of readability anyway.


## EXPLAIN changes for prefetch and LFC

### How to get rid of the patch

Konstantin submitted a patch to -hackers already: https://commitfest.postgresql.org/47/4643/. Get that into a committable state.


## On-demand download of extensions

### How to get rid of the patch

FUSE or LD_PRELOAD trickery to intercept reads?


## Publication superuser checks

We have hacked CreatePublication so that also neon_superuser can create them.

### How to get rid of the patch

Create an upstream patch with more fine-grained privileges for publications CREATE/DROP that can be GRANTed to users.


## WAL log replication slots

### How to get rid of the patch

Utilize the upcoming v17 "slot sync worker", or a similar neon-specific background worker process, to periodically WAL-log the slots, or to export them somewhere else.


## WAL-log replication snapshots

### How to get rid of the patch

WAL-log them periodically, from a backgound worker.


## WAL-log relmapper files

Similarly to replications snapshot files, the CID mapping files generated during VACUUM FULL of a catalog table are WAL-logged

### How to get rid of the patch

WAL-log them periodically, from a backgound worker.


## XLogWaitForReplayOf()

??




# Not currently committed but proposed

## Disable ring buffer buffer manager strategies

### Why?

Postgres tries to avoid cache flushing by bulk operations (copy, seqscan, vacuum,...).
Even if there are free space in buffer cache, pages may be evicted.
Negative effect of it can be somehow compensated by file system cache, but in Neon,
cost of requesting page from page server is much higher.

### Alternatives?

Instead of just prohibiting ring buffer we may try to implement more flexible eviction policy,
for example copy evicted page from ring buffer to some other buffer if there is free space
in buffer cache.

## Disable marking page as dirty when hint bits are set.

### Why?

Postgres has to modify page twice: first time when some tuple is updated and second time when
hint bits are set. Wal logging hint bits updates requires FPI which significantly increase size of WAL.

### Alternatives?

Add special WAL record for setting page hints.

## Prewarming

### Why?

Short downtime (or, in other words, fast compute node restart time) is one of the key feature of Neon.
But overhead of request-response round-trip for loading pages on demand can make started node warm-up quite slow.
We can capture state of compute node buffer cache and send bulk request for this pages at startup.

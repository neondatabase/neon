1. Add t_cid to XLOG record
- Why?
  The cmin/cmax on a heap page is a real bummer. I don't see any other way to fix that than bite the bullet and modify the WAL-logging routine to include the cmin/cmax.

  To recap, the problem is that the XLOG_HEAP_INSERT record does not include the command id of the inserted row. And same with deletion/update. So in the primary, a row is inserted with current xmin + cmin. But in the replica, the cmin is always set to 1. That works, because the command id is only relevant to the inserting transaction itself. After commit/abort, no one cares abut it anymore.

- Alternatives?
  I don't know

2. Add PD_WAL_LOGGED.
- Why?
  Postgres sometimes writes data to the page before it is wal-logged. If such page ais swapped out, we  will loose this change. The problem is currently solved by setting PD_WAL_LOGGED bit in page header. When page without this bit set is written to the SMGR, then it is forced to be written to the WAL as FPI using log_newpage_copy() function.

  There was wrong assumption that it can happen only during construction of some exotic indexes (like gist). It is not true. The same situation can happen with COPY,VACUUM and when record hint bits are set.

- Discussion:
  https://discord.com/channels/869525774699462656/882681420986851359

- Alternatives:
  Do not store this flag in page header, but associate this bit with shared buffer. Logically it is more correct but in practice we will get not advantages: neither in space, neither in CPU overhead.


3. XLogReadBufferForRedo not always loads and pins requested buffer. So we need to add extra checks that buffer is really pinned. Also do not use BufferGetBlockNumber for buffer returned by XLogReadBufferForRedo.
- Why?
  XLogReadBufferForRedo is not pinning pages which are not requested by wal-redo. It is specific only for wal-redo Postgres.

- Alternatives?
  No


4. Eliminate reporting of some warnings related with hint bits, for example
"page is not marked all-visible but visibility map bit is set in relation".
- Why?
  Hint bit may be not WAL logged.

- Alternative?
  Always wal log any page changes.


5. Maintain last written LSN.
- Why?
  When compute node requests page from page server, we need to specify LSN. Ideally it should be LSN
  of WAL record performing last update of this pages. But we do not know it, because we do not have page.
  We can use current WAL flush position, but in this case there is high probability that page server
  will be blocked until this peace of WAL is delivered.
  As better approximation we can keep max LSN of written page. It will be better to take in account LSNs only of evicted pages,
  but SMGR API doesn't provide such knowledge.

- Alternatives?
  Maintain map of LSNs of evicted pages.


6. Launching Postgres without WAL.
- Why?
  According to Zenith architecture compute node is stateless. So when we are launching
  compute node, we need to provide some dummy PG_DATADIR. Relation pages
  can be requested on demand from page server. But Postgres still need some non-relational data:
  control and configuration files, SLRUs,...
  It is currently implemented  using basebackup (do not mix with pg_basebackup) which is created
  by pageserver. It includes in this tarball config/control files, SLRUs and required directories.
  As far as pageserver do not have original (non-scattered) WAL segments, it includes in
  this tarball dummy WAL segment which contains only SHUTDOWN_CHECKPOINT record at the beginning of segment,
  which redo field points to the end of wal. It allows to load checkpoint record in more or less
  standard way with minimal changes of Postgres, but then some special handling is needed,
  including restoring previous record position from zenith.signal file.
  Also we have to correctly initialize header of last WAL page (pointed by checkpoint.redo)
  to pass checks performed by XLogReader.

- Alternatives?
  We may not include fake WAL segment in tarball at all and modify xlog.c to load checkpoint record
  in special way. But it may only increase number of changes in xlog.c

7. Add redo_read_buffer_filter callback to XLogReadBufferForRedoExtended
- Why?
  We need a way in wal-redo Postgres to ignore pages which are not requested by pageserver.
  So wal-redo Postgres reconstructs only requested page and for all other returns BLK_DONE
  which means that recovery for them is not needed.

- Alternatives?
  No

8. Enforce WAL logging of sequence updates.
- Why?
  Due to performance reasons Postgres don't want to log each fetching of a value from a sequence,
  so we pre-log a few fetches in advance. In the event of crash we can lose
  (skip over) as many values as we pre-logged.
  But it doesn't work with Zenith because page with sequence value can be evicted from buffer cache
  and we will get a gap in sequence values even without crash.

- Alternatives:
  Do not try to preserve sequential order but avoid performance penalty.


9. Treat unlogged tables as normal (permanent) tables.
- Why?
  Unlogged tables are not transient, so them have to survive node restart (unlike temporary tables).
  But as far as compute node is stateless, we need to persist their data to storage node.
  And it can only be done through the WAL.

- Alternatives?
  * Store unlogged tables locally (violates requirement of stateless compute nodes).
  * Prohibit unlogged tables at all.


10. Support start Postgres in wal-redo mode
- Why?
  To be able to apply WAL record and reconstruct pages at page server.

- Alternatives?
  * Rewrite redo handlers in Rust
  * Do not reconstruct pages at page server at all and do it at compute node.


11. WAL proposer
- Why?
  WAL proposer is communicating with safekeeper and ensures WAL durability by quorum writes.
  It is currently implemented as patch to standard WAL sender.

- Alternatives?
  Can be moved to extension if some extra callbacks will be added to wal sender code.


12. Secure Computing BPF API wrapper.
- Why?
  Pageserver delegates complex WAL decoding duties to Postgres,
  which means that the latter might fall victim to carefully designed
  malicious WAL records and start doing harmful things to the system.
  To prevent this, it has been decided to limit possible interactions
  with the outside world using the Secure Computing BPF mode.

- Alternatives:
  * Rewrite redo handlers in Rust.
  * Add more checks to guarantee correctness of WAL records.
  * Move seccomp.c to extension
  * Many other discussed approaches to neutralize incorrect WAL records vulnerabilities.


13. Callbacks for replica feedbacks
- Why?
  Allowing waproposer to interact with walsender code.

- Alternatives
  Copy walsender code to walproposer.


14. Support multiple SMGR implementations.
- Why?
  Postgres provides abstract API for storage manager but it has only one implementation
  and provides no way to replace it with custom storage manager.

- Alternatives?
  None.


15. Calculate database size as sum of all database relations.
- Why?
  Postgres is calculating database size by traversing data directory
  but as far as Zenith compute node is stateless we can not do it.

- Alternatives?
  Send this request directly to pageserver and calculate real (physical) size
  of Zenith representation of database/timeline, rather than sum logical size of all relations.


-----------------------------------------------
Not currently committed but proposed:

1. Disable ring buffer buffer manager strategies
- Why?
  Postgres tries to avoid cache flushing by bulk operations (copy, seqscan, vacuum,...).
  Even if there are free space in buffer cache, pages may be evicted.
  Negative effect of it can be somehow compensated by file system cache, but in case of Zenith
  cost of requesting page from page server is much higher.

- Alternatives?
  Instead of just prohibiting ring buffer we may try to implement more flexible eviction policy,
  for example copy evicted page from ring buffer to some other buffer if there is free space
  in buffer cache.

2. Disable marking page as dirty when hint bits are set.
- Why?
  Postgres has to modify page twice: first time when some tuple is updated and second time when
  hint bits are set. Wal logging hint bits updates requires FPI which significantly increase size of WAL.

- Alternatives?
  Add special WAL record for setting page hints.

3. Prefetching
- Why?
  As far as pages in Zenith are loaded on demand, to reduce node startup time
  and also sppedup some massive queries we need some mechanism for bulk loading to
  reduce page request round-trip overhead.

  Currently Postgres is supporting prefetching only for bitmap scan.
  In Zenith we also use prefetch for sequential and index scan. For sequential scan we prefetch
  some number of following pages. For index scan we prefetch pages of heap relation addressed by TIDs.

4. Prewarming.
- Why?
  Short downtime (or, in other words, fast compute node restart time) is one of the key feature of Zenith.
  But overhead of request-response round-trip for loading pages on demand can make started node warm-up quite slow.
  We can capture state of compute node buffer cache and send bulk request for this pages at startup.

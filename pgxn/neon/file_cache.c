/*
 *
 * file_cache.c
 *
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  pgxn/neon/file_cache.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <sys/file.h>
#include <unistd.h>
#include <fcntl.h>

#include "neon_pgversioncompat.h"

#include "access/parallel.h"
#include "access/xlog.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "pagestore_client.h"
#include "common/hashfn.h"
#include "pgstat.h"
#include "port/pg_iovec.h"
#include "postmaster/bgworker.h"
#include RELFILEINFO_HDR
#include "storage/buf_internals.h"
#include "storage/fd.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/lwlock.h"
#include "storage/pg_shmem.h"
#include "utils/builtins.h"
#include "utils/dynahash.h"
#include "utils/guc.h"

#if PG_VERSION_NUM >= 150000
#include "access/xlogrecovery.h"
#endif

#include "hll.h"
#include "bitmap.h"
#include "neon.h"
#include "neon_perf_counters.h"

#define CriticalAssert(cond) do if (!(cond)) elog(PANIC, "LFC: assertion %s failed at %s:%d: ", #cond, __FILE__, __LINE__); while (0)

/*
 * Local file cache is used to temporary store relations pages in local file system.
 * All blocks of all relations are stored inside one file and addressed using shared hash map.
 * Currently LRU eviction policy based on L2 list is used as replacement algorithm.
 * As far as manipulation of L2-list requires global critical section, we are not using partitioned hash.
 * Also we are using exclusive lock even for read operation because LRU requires relinking element in L2 list.
 * If this lock become a bottleneck, we can consider other eviction strategies, for example clock algorithm.
 *
 * Cache is always reconstructed at node startup, so we do not need to save mapping somewhere and worry about
 * its consistency.

 *
 * ## Holes
 *
 * The LFC can be resized on the fly, up to a maximum size that's determined
 * at server startup (neon.max_file_cache_size). After server startup, we
 * expand the underlying file when needed, until it reaches the soft limit
 * (neon.file_cache_size_limit). If the soft limit is later reduced, we shrink
 * the LFC by punching holes in the underlying file with a
 * fallocate(FALLOC_FL_PUNCH_HOLE) call. The nominal size of the file doesn't
 * shrink, but the disk space it uses does.
 *
 * Each hole is tracked by a dummy FileCacheEntry, which are kept in the
 * 'holes' linked list. They are entered into the chunk hash table, with a
 * special key where the blockNumber is used to store the 'offset' of the
 * hole, and all other fields are zero. Holes are never looked up in the hash
 * table, we only enter them there to have a FileCacheEntry that we can keep
 * in the linked list. If the soft limit is raised again, we reuse the holes
 * before extending the nominal size of the file.
 */

/*-----------
 * Prewarming
 * ----------
 *
 * LFC prewarming happens with the use of one or more prewarm workers, which
 * should never access the same LFC chunk at the same time.
 *
 * The prewarm worker works with these invariants:
 *		1. There are no concurrent writes to the pages that the prewarm worker
 *			is writing to the chunk.
 *			This is guaranteed by these mechanisms: 
 *			 - Before starting to write, a prewarm worker marks the chunk as
 *			   "prewarm is writing to this", and then waits for all current
 *			   accessors to finish their activities. Therefore, all users
 *			   know that prewarm is active on the chunk.
 *			 - Prewarm workers will only write to as-of-yet unwritten blocks
 *			   in the chunk.
 *			 - Writers to the chunk will wait for the prewarm worker to finish
 *			   writing in the chunk before starting their own writes into
 *			   as-of-yet unwritten blocks in the chunk.
 *			 - Readers won't be blocked (but can decide to wait for prewarm
 *			   in case they need to read pages not yet in LFC, and prewarm is
 *			   active on the chunk)
 *		2. Pages are never overwritten by LFC prewarm
 *		3. Readers are never blocked
 *		4. Only potentially conflicting writers are blocked, *but are not ignored*.
 *
 * The only consideration is that in hot-standby mode, we *can not* ignore
 * writes to pages that are being prewarmed. Usually, that's not much of an
 * issue, but here we don't have a buffer that ensures synchronization between
 * the redo process and the contents of the page/buffer (which we'd otherwise
 * have). So, in REDO mode we'd have to make sure that the recovery process
 * DOES do redo for pages in chunks that are being prewarmed, even if that
 * means doing PS reads, to make sure we don't drop changes for pages we're
 * fetching through prewarm processes.
 *
 * The algorithm used is as follows:
 *
 * 1.  Select a chunk to prewarm.
 *     This requires the chunk to be present in LFC, or an empty chunk to be
 *     available in the LFC that we can then fill.
 *     If there are no empty chunks left, and the chunk we selected isn't
 *     present in the LFC, we skip this chunk as evicting data to prewarm other
 *     data may be detrimental to the workload.
 * 2.  Prepare the chunk: with exclusively locked LFC:
 *     1.  Note down which blocks are already written to the chunk.
 *         We don't have to read those blocks, as they're already the latest
 *         version.
 *         If we don't have any blocks left to fetch, we're done: restart at 1
 *     2.  Unlink the selected chunk from the LRU.
 *         Now it can't be removed through LRU eviction, so we know any new
 *         blocks written to the chunk will be the latest version of said pages.
 * 3.  Determine which blocks to retrieve (= selection from 1, with selection
 *     from 2.1 removed)
 * 4.  Prefetch the to-be-retrieved blocks of the chunk.
 * 5.  Read all to-be-retrieved blocks of the chunk into local memory.
 * 6.  With exclusively locked LFC:
 *     1.  Mark chunk as LFC_PREWARM_STARTED
 *     2.  Copy this backend's prewarmer worker number (if relevant) into new
 *         `prewarm_worker_field` on chunk (this needs to be only a few bits,
 *         e.g. 8, as we only will likely have very few workers concurrently
 *         prewarming the LFC),
 *     3.  Copy the LFC entry's refcount into per-prewarmworker state's
 *         'wait_count', reduced by 1 (to account for the bgworker itself).
 *     Now the chunk we're going to put data into is pinned (has been removed from the LRU, can't be reclaimed) and no new backends will start write-IO on pages that aren't present in the chunk due to the LFC_PREWARM_STARTED flag.
 * 7.  Normal backends: Writes for not-yet-present pages that get to that chunk
 *     start waiting on (new) condition variable `prewarm_done` in the
 *     reported prewarm worker's worker state, until the lfc chunk doesn't have
 *     `LFC_PREWARM_STARTED` set or the page is present in the LFC chunk. All
 *     other writes and reads can continue as usual (those pages are not
 *     touched by the prewarm worker).
 * 8.  Normal backends: When the chunk is released:
 *     1.  If LFC_PREWARM_STARTED is not set, we're done.
 *     2.  If LFC_PREWARM_STARTED was already set when the chunk was acquired,
 *         we're done.
 *     3.  Reduce the current chunk's prewarm worker's wait count by 1.
 *     4.  If the current chunk's prewarm worker's wait count is 0, signal
 *         chunkrelease.
 * 9.  Prewarmer:
 *     1.  wait on new condition variable `chunkrelease` until the wait_count
 *         is 0 (no more active concurrent writes)
 *     2.  Write out own data into pages that aren't yet present in the chunk
 * 10. Prewarmer; holding LFC lock:
 *     1.  Set 'written' bit for valid data.
 *     2.  Unset LFC_PREWARM_STARTED
 *     3.  Release and link chunk as normal.
 * 11. Signal condition variable `prewarm_done`.
 */

/* Local file storage allocation chunk.
 * Should be power of two. Using larger than page chunks can
 * 1. Reduce hash-map memory footprint: 8TB database contains billion pages
 *    and size of hash entry is 40 bytes, so we need 40Gb just for hash map.
 *    1Mb chunks can reduce hash map size to 320Mb.
 * 2. Improve access locality, subsequent pages will be allocated together improving seqscan speed
 */
#define BLOCKS_PER_CHUNK	128 /* 1Mb chunk */
/*
 * Smaller chunk seems to be better for OLTP workload
 */
// #define BLOCKS_PER_CHUNK	8 /* 64kb chunk */
#define MB					((uint64)1024*1024)

#define SIZE_MB_TO_CHUNKS(size) ((uint32)((size) * MB / BLCKSZ / BLOCKS_PER_CHUNK))
#define CHUNK_BITMAP_SIZE ((BLOCKS_PER_CHUNK + 31) / 32)

typedef struct FileCacheEntry
{
	BufferTag	key;
	uint32		hash;
	uint32		offset;
	uint32		access_count : 31; /* limited to (2^18 - 1) by MAX_BACKENDS */
	bool		prewarm_active : 1;
	uint32		bitmap[CHUNK_BITMAP_SIZE];
	dlist_node	list_node;		/* LRU/holes list node */
} FileCacheEntry;

/*----------
 * 		wait_count
 * 			Number of chunk accesses the backend is waiting for to complete
 *		wait_write
 *			Signaled whenever the prewarm process is done with a chunk
 *		backend_io
 *			Prewarmer sleeps on this CV when it is waiting for concurrent
 *			accesses to the chunk to finish their work.
 */
typedef struct FileCacheWorkerShared
{
	uint64				wait_time;		/* total time spent waiting for
										 * concurrent writes on LFC chunks */
	int					wait_count;		/* Number of other backends prewarm is
										 * still waiting for */
	ConditionVariable	prewarm_done;	/* Backends can wait on this for
										 * prewarm to complete */
	ConditionVariable	chunk_release;	/* Prewarm worker waits for signal on
										 * this CV for concurrent writes */
} FileCacheWorkerShared;

typedef struct FileCacheControl
{
	uint64		generation;		/* generation is needed to handle correct hash
								 * reenabling */
	uint32		size;			/* size of cache file in chunks */
	uint32		used;			/* number of used chunks */
	uint32		used_pages;		/* number of used pages */
	uint32		limit;			/* shared copy of lfc_size_limit */
	uint64		hits;
	uint64		misses;
	uint64		writes;			/* number of writes issued */
	uint64		time_read;		/* time spent reading (us) */
	uint64		time_write;		/* time spent writing (us) */
	uint64		time_prewarm;	/* time spent waiting for prewarm (us) */
	uint32		prewarm_total_chunks;
	uint32		prewarm_curr_chunk;
	uint32		prewarmed_pages;
	uint32		skipped_pages;
	dlist_head	lru;			/* double linked list for LRU replacement
								 * algorithm */
	dlist_head  holes;			/* double linked list of punched holes */
	FileCacheWorkerShared worker; /* future: support multiple parallel workers */
	HyperLogLogState wss_estimation; /* estimation of working set size */
} FileCacheControl;

typedef struct FileCacheStateEntry
{
	BufferTag	key;
	uint32		bitmap[CHUNK_BITMAP_SIZE];
} FileCacheStateEntry;

static HTAB *lfc_hash;
static int	lfc_desc = 0;
static LWLockId lfc_lock;
static int	lfc_max_size;
static int	lfc_size_limit;
static int	lfc_prewarm_limit;
static int	lfc_prewarm_batch;
static char *lfc_path;
static FileCacheControl *lfc_ctl;
static shmem_startup_hook_type prev_shmem_startup_hook;
#if PG_VERSION_NUM>=150000
static shmem_request_hook_type prev_shmem_request_hook;
#endif

#define LFC_ENABLED() (lfc_ctl->limit != 0)

/*
 * Local file cache is optional and Neon can work without it.
 * In case of any any errors with this cache, we should disable it but to not throw error.
 * Also we should allow  re-enable it if source of failure (lack of disk space, permissions,...) is fixed.
 * All cache content should be invalidated to avoid reading of stale or corrupted data
 */
static void
lfc_disable(char const *op)
{
	int			fd;

	elog(WARNING, "LFC: failed to %s local file cache at %s: %m, disabling local file cache", op, lfc_path);

	/* Invalidate hash */
	LWLockAcquire(lfc_lock, LW_EXCLUSIVE);

	if (LFC_ENABLED())
	{
		HASH_SEQ_STATUS status;
		FileCacheEntry *entry;

		hash_seq_init(&status, lfc_hash);
		while ((entry = hash_seq_search(&status)) != NULL)
		{
			hash_search_with_hash_value(lfc_hash, &entry->key, entry->hash, HASH_REMOVE, NULL);
		}
		lfc_ctl->generation += 1;
		lfc_ctl->size = 0;
		lfc_ctl->used = 0;
		lfc_ctl->limit = 0;
		dlist_init(&lfc_ctl->lru);
		dlist_init(&lfc_ctl->holes);

		if (lfc_desc > 0)
		{
			int			rc;

			/*
			 * If the reason of error is ENOSPC, then truncation of file may
			 * help to reclaim some space
			 */
			pgstat_report_wait_start(WAIT_EVENT_NEON_LFC_TRUNCATE);
			rc = ftruncate(lfc_desc, 0);
			pgstat_report_wait_end();

			if (rc < 0)
				elog(WARNING, "LFC: failed to truncate local file cache %s: %m", lfc_path);
		}
	}

	/*
	 * We need to use unlink to to avoid races in LFC write, because it is not
	 * protectedby
	 */
	unlink(lfc_path);

	fd = BasicOpenFile(lfc_path, O_RDWR | O_CREAT | O_TRUNC);
	if (fd < 0)
		elog(WARNING, "LFC: failed to recreate local file cache %s: %m", lfc_path);
	else
		close(fd);

	LWLockRelease(lfc_lock);

	if (lfc_desc > 0)
		close(lfc_desc);

	lfc_desc = -1;
}

/*
 * This check is done without obtaining lfc_lock, so it is unreliable
 */
static bool
lfc_maybe_disabled(void)
{
	return !lfc_ctl || !LFC_ENABLED();
}

static bool
lfc_ensure_opened(void)
{
	bool		enabled = !lfc_maybe_disabled();

	/* Open cache file if not done yet */
	if (lfc_desc <= 0 && enabled)
	{
		lfc_desc = BasicOpenFile(lfc_path, O_RDWR);

		if (lfc_desc < 0)
		{
			lfc_disable("open");
			return false;
		}
	}
	return enabled;
}

static void
lfc_shmem_startup(void)
{
	bool		found;
	static HASHCTL info;

	if (prev_shmem_startup_hook)
	{
		prev_shmem_startup_hook();
	}

	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);

	lfc_ctl = (FileCacheControl *) ShmemInitStruct("lfc", sizeof(FileCacheControl), &found);
	if (!found)
	{
		int			fd;
		uint32		n_chunks = SIZE_MB_TO_CHUNKS(lfc_max_size);

		lfc_lock = (LWLockId) GetNamedLWLockTranche("lfc_lock");
		info.keysize = sizeof(BufferTag);
		info.entrysize = sizeof(FileCacheEntry);

		/*
		 * n_chunks+1 because we add new element to hash table before eviction
		 * of victim
		 */
		lfc_hash = ShmemInitHash("lfc_hash",
								 n_chunks + 1, n_chunks + 1,
								 &info,
								 HASH_ELEM | HASH_BLOBS);
		memset(lfc_ctl, 0, sizeof *lfc_ctl);
		dlist_init(&lfc_ctl->lru);
		dlist_init(&lfc_ctl->holes);

		/* Initialize hyper-log-log structure for estimating working set size */
		initSHLL(&lfc_ctl->wss_estimation);

		/* Recreate file cache on restart */
		fd = BasicOpenFile(lfc_path, O_RDWR | O_CREAT | O_TRUNC);
		if (fd < 0)
		{
			elog(WARNING, "LFC: failed to create local file cache %s: %m", lfc_path);
			lfc_ctl->limit = 0;
		}
		else
		{
			close(fd);
			lfc_ctl->limit = SIZE_MB_TO_CHUNKS(lfc_size_limit);
		}
	}
	LWLockRelease(AddinShmemInitLock);
}

static void
lfc_shmem_request(void)
{
#if PG_VERSION_NUM>=150000
	if (prev_shmem_request_hook)
		prev_shmem_request_hook();
#endif

	RequestAddinShmemSpace(sizeof(FileCacheControl) + hash_estimate_size(SIZE_MB_TO_CHUNKS(lfc_max_size) + 1, sizeof(FileCacheEntry)));
	RequestNamedLWLockTranche("lfc_lock", 1);
}

static bool
is_normal_backend(void)
{
	/*
	 * Stats collector detach shared memory, so we should not try to access
	 * shared memory here. Parallel workers first assign default value (0), so
	 * not perform truncation in parallel workers. The Postmaster can handle
	 * SIGHUP and it has access to shared memory (UsedShmemSegAddr != NULL),
	 * but has no PGPROC.
	 */
	return lfc_ctl && MyProc && UsedShmemSegAddr && !IsParallelWorker();
}

static bool
lfc_check_limit_hook(int *newval, void **extra, GucSource source)
{
	if (*newval > lfc_max_size)
	{
		elog(ERROR, "LFC: neon.file_cache_size_limit can not be larger than neon.max_file_cache_size");
		return false;
	}
	return true;
}

static void
lfc_change_limit_hook(int newval, void *extra)
{
	uint32		new_size = SIZE_MB_TO_CHUNKS(newval);

	if (!is_normal_backend())
		return;

	if (!lfc_ensure_opened())
		return;

	LWLockAcquire(lfc_lock, LW_EXCLUSIVE);

	while (new_size < lfc_ctl->used && !dlist_is_empty(&lfc_ctl->lru))
	{
		/*
		 * Shrink cache by throwing away least recently accessed chunks and
		 * returning their space to file system
		 */
		FileCacheEntry *victim = dlist_container(FileCacheEntry, list_node, dlist_pop_head_node(&lfc_ctl->lru));
		FileCacheEntry *hole;
		uint32		offset = victim->offset;
		uint32		hash;
		bool		found;
		BufferTag	holetag;

		CriticalAssert(victim->access_count == 0);
#ifdef FALLOC_FL_PUNCH_HOLE
		if (fallocate(lfc_desc, FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE, (off_t) victim->offset * BLOCKS_PER_CHUNK * BLCKSZ, BLOCKS_PER_CHUNK * BLCKSZ) < 0)
			neon_log(LOG, "Failed to punch hole in file: %m");
#endif
		/* We remove the old entry, and re-enter a hole to the hash table */
		for (int i = 0; i < BLOCKS_PER_CHUNK; i++)
		{
			lfc_ctl->used_pages -= (victim->bitmap[i >> 5] >> (i & 31)) & 1;
		}
		hash_search_with_hash_value(lfc_hash, &victim->key, victim->hash, HASH_REMOVE, NULL);

		memset(&holetag, 0, sizeof(holetag));
		holetag.blockNum = offset;
		hash = get_hash_value(lfc_hash, &holetag);
		hole = hash_search_with_hash_value(lfc_hash, &holetag, hash, HASH_ENTER, &found);
		hole->hash = hash;
		hole->offset = offset;
		hole->access_count = 0;
		CriticalAssert(!found);
		dlist_push_tail(&lfc_ctl->holes, &hole->list_node);

		lfc_ctl->used -= 1;
	}
	lfc_ctl->limit = new_size;
	if (new_size == 0) {
		lfc_ctl->generation += 1;
	}
	neon_log(DEBUG1, "set local file cache limit to %d", new_size);

	LWLockRelease(lfc_lock);
}

void
lfc_init(void)
{
	/*
	 * In order to create our shared memory area, we have to be loaded via
	 * shared_preload_libraries.
	 */
	if (!process_shared_preload_libraries_in_progress)
		neon_log(ERROR, "Neon module should be loaded via shared_preload_libraries");


	DefineCustomIntVariable("neon.max_file_cache_size",
							"Maximal size of Neon local file cache",
							NULL,
							&lfc_max_size,
							0,	/* disabled by default */
							0,
							INT_MAX,
							PGC_POSTMASTER,
							GUC_UNIT_MB,
							NULL,
							NULL,
							NULL);

	DefineCustomIntVariable("neon.file_cache_size_limit",
							"Current limit for size of Neon local file cache",
							NULL,
							&lfc_size_limit,
							0,	/* disabled by default */
							0,
							INT_MAX,
							PGC_SIGHUP,
							GUC_UNIT_MB,
							lfc_check_limit_hook,
							lfc_change_limit_hook,
							NULL);

	DefineCustomStringVariable("neon.file_cache_path",
							   "Path to local file cache (can be raw device)",
							   NULL,
							   &lfc_path,
							   "file.cache",
							   PGC_POSTMASTER,
							   0,
							   NULL,
							   NULL,
							   NULL);

	DefineCustomIntVariable("neon.file_cache_prewarm_limit",
							"Maximal number of prewarmed pages",
							NULL,
							&lfc_prewarm_limit,
							0,	/* disabled by default */
							0,
							INT_MAX,
							PGC_SIGHUP,
							0,
							NULL,
							NULL,
							NULL);

	DefineCustomIntVariable("neon.file_cache_prewarm_batch",
							"Number of pages retrivied by prewarm from page server",
							NULL,
							&lfc_prewarm_batch,
							64,
							1,
							INT_MAX,
							PGC_SIGHUP,
							0,
							NULL,
							NULL,
							NULL);

	if (lfc_max_size == 0)
		return;

	prev_shmem_startup_hook = shmem_startup_hook;
	shmem_startup_hook = lfc_shmem_startup;
#if PG_VERSION_NUM>=150000
	prev_shmem_request_hook = shmem_request_hook;
	shmem_request_hook = lfc_shmem_request;
#else
	lfc_shmem_request();
#endif
}

static FileCacheStateEntry*
lfc_get_state(size_t* n_entries)
{
	size_t max_entries = *n_entries;
	size_t i = 0;
	FileCacheStateEntry* fs;

	if (lfc_maybe_disabled() || max_entries == 0)	/* fast exit if file cache is disabled */
		return NULL;

	fs = (FileCacheStateEntry*)palloc(sizeof(FileCacheStateEntry) * max_entries);

	LWLockAcquire(lfc_lock, LW_SHARED);

	if (LFC_ENABLED())
	{
		dlist_iter	iter;
		dlist_reverse_foreach(iter, &lfc_ctl->lru)
		{
			FileCacheEntry *entry = dlist_container(FileCacheEntry, list_node, iter.cur);
			memcpy(&fs[i].key, &entry->key, sizeof entry->key);
			memcpy(fs[i].bitmap, entry->bitmap, sizeof entry->bitmap);
			if (++i == max_entries)
				break;
		}
		elog(LOG, "LFC: save state of %ld chunks", (long)i);
	}

	LWLockRelease(lfc_lock);

	*n_entries = i;
	return fs;
}

/*
 * Release the given entry, and handle any prewarm worker signaling
 * that might be required.
 *
 * When called, the lfc_lock must be held exclusively. This function releases
 * that lock.
 */
static inline void
release_entry(FileCacheEntry *entry, bool prewarm_was_active)
{
	bool		signal = false;

	Assert(entry->key.blockNum % BLOCKS_PER_CHUNK == 0);
	CriticalAssert(entry->access_count > 0);

	Assert(LWLockHeldByMeInMode(lfc_lock, LW_EXCLUSIVE));

	dlist_push_head(&lfc_ctl->lru, &entry->list_node);
	entry->access_count--;

	if (unlikely(entry->prewarm_active) && !prewarm_was_active)
	{
		Assert(lfc_ctl->worker.wait_count >= 1);

		lfc_ctl->worker.wait_count--;

		if (lfc_ctl->worker.wait_count == 0)
			signal = true;
	}

	LWLockRelease(lfc_lock);

	if (signal)
	{
		ConditionVariableBroadcast(&lfc_ctl->worker.chunk_release);
	}
}

/*
 * Get an FileCacheEntry to write data into.
 *
 * It returns NULL if it can't find a matching entry, and can't allocate one.
 * It also returns NULL if the generation changed while allocating the entry.
 *
 * It waits for the LFC prewarm worker to finish its job if it detects the
 * prewarm operation might conflict with this backend's write operation.
 *
 * When it returns, the lfc_lock is held in exclusive mode.
 */
static inline FileCacheEntry *
lfc_entry_for_write(BufferTag *key, bool no_replace, bool *prewarm_active,
					const uint32 *bitmap)
{
	bool		found = false;
	uint32		hash;

	FileCacheEntry *entry;
	Assert(!LWLockHeldByMe(lfc_lock));

	Assert(key->blockNum % BLOCKS_PER_CHUNK == 0);

	hash = get_hash_value(lfc_hash, key);

	LWLockAcquire(lfc_lock, LW_EXCLUSIVE);

	if (!LFC_ENABLED())
	{
		LWLockRelease(lfc_lock);
		return NULL;
	}

	/*
	 * We use find-or-insert if there's space and if we don't have any holes
	 * in the LFC we can still fill.
	 */
	if (lfc_ctl->limit > lfc_ctl->size && dlist_is_empty(&lfc_ctl->holes))
		entry = hash_search_with_hash_value(lfc_hash, key, hash,
											HASH_ENTER_NULL, &found);
	else
		entry = hash_search_with_hash_value(lfc_hash, key, hash,
											HASH_FIND, &found);

	if (!found)
	{
		/* only when newly entered, only when size < limit */
		if (entry != NULL)
		{
			Assert(dlist_is_empty(&lfc_ctl->holes));

			lfc_ctl->used++;
			entry->key = *key;
			entry->hash = hash;
			entry->offset = lfc_ctl->size++;
			entry->access_count = 0;
			entry->prewarm_active = false;
			memset(entry->bitmap, 0, CHUNK_BITMAP_SIZE * sizeof(uint32));
			memset(&entry->list_node, 0, sizeof(dlist_node));
			neon_log(DEBUG2, "Allocated new space");
		}
		else
		{
			/*
			 * lfc_hash is full, and we didn't want to add, or have space for,
			 * a new entry. Instead, find a replacement from either the list
			 * with holes (if our page usage is within the limit), or the LRU
			 * (if the caller allowed us to do that).
			 */
			if (lfc_ctl->limit > lfc_ctl->used &&
				!dlist_is_empty(&lfc_ctl->holes))
			{
				lfc_ctl->used++;
				entry = dlist_container(FileCacheEntry, list_node,
										dlist_pop_head_node(&lfc_ctl->holes));
				neon_log(DEBUG2, "Fill hole");
			}
			else if (!no_replace && !dlist_is_empty(&lfc_ctl->lru))
			{
				entry = dlist_container(FileCacheEntry, list_node,
										dlist_pop_head_node(&lfc_ctl->lru));
				neon_log(DEBUG2, "Swap file cache page");
			}

			Assert(entry->access_count == 0);

			if (entry != NULL)
			{
				lfc_ctl->used_pages -= pg_popcount((char *) entry->bitmap,
												   CHUNK_BITMAP_SIZE * sizeof(uint32));
				hash_search_with_hash_value(lfc_hash, &entry->key, entry->hash,
											HASH_REMOVE, &found);
				Assert(found);

				hash_search_with_hash_value(lfc_hash, key, hash, HASH_ENTER,
											&found);
				Assert(found);

				entry->key = *key;
				entry->hash = hash;
				/* offset is retained from the old version of the entry */
				entry->access_count = 0;
				entry->prewarm_active = false;
				memset(entry->bitmap, 0, CHUNK_BITMAP_SIZE * sizeof(uint32));
				memset(&entry->list_node, 0, sizeof(dlist_node));
			}
		}
	}

	/*-------
	 * We've tried to get a page as much as we possibly can:
	 *		- We tried adding a new chunk with HASH_ENTER_NULL;
	 *		- There are no LFC chunks that are not already being accessed by
	 *			another backend;
	 *		- There are no LFC chunks that were in the 'holes' list;
	 *
	 * So, we've exhausted places to pull entries from, if we don't have one
	 * yet, then we won't ever get one during this lock. So, return.
	 */
	if (entry == NULL)
		return NULL;

	/* increment access count, and unlink if needed */
	if (entry->access_count++ == 0)
		dlist_delete(&entry->list_node);

	if (entry->prewarm_active)
	{
		bool		conflict = false;

		/*
		 * Prewarm is active on this chunk.
		 * 
		 * If it's active on pages we're about to write (i.e. those pages
		 * haven't been written to yet), then we wait for prewarm to finish
		 * processing this chunk. Otherwise (i.e. the pages have been written
		 * to already) just continue on as usual.
		 */

		for (uint32 i = 0; i < CHUNK_BITMAP_SIZE; i++)
		{
			/* if we're about to write into an empty spot, we conflict */
			if (~(entry->bitmap[i]) & bitmap[i])
			{
				conflict = true;
				break;
			}
		}

		if (conflict)
		{
			uint64		generation;
			instr_time	start, end;
			ConditionVariable *cv = &lfc_ctl->worker.prewarm_done;
			generation = lfc_ctl->generation;
			LWLockRelease(lfc_lock);
			ConditionVariablePrepareToSleep(cv);

			INSTR_TIME_SET_CURRENT(start);
			while (entry->prewarm_active)
			{
				/* These IOs shouldn't take very long */
				ConditionVariableTimedSleep(cv, 100,
											WAIT_EVENT_NEON_LFC_PREWARM_IO);
			}
			ConditionVariableCancelSleep();
			INSTR_TIME_SET_CURRENT(end);
			INSTR_TIME_SUBTRACT(end, start);

			LWLockAcquire(lfc_lock, LW_EXCLUSIVE);

			lfc_ctl->time_prewarm += INSTR_TIME_GET_MICROSEC(end);

			/* LFC got disabled in the meantime */
			if (!LFC_ENABLED())
				return NULL;

			/* LFC generation changed, can't do much about that */
			if (unlikely(lfc_ctl->generation != generation))
				return NULL;
		}
	}

	*prewarm_active = entry->prewarm_active;

	return entry;
}

typedef struct LFCPrewarmChunk {
	FileCacheEntry	   *cacheEntry;		/* LFC entry of work item */
	FileCacheStateEntry *stateEntry;	/* prewarm request of work item */
	int			npages;					/* bit count in .stateEntry */
	int			prefetched;				/* num getpage requests already sent */
	int			received;				/* pages already in .pages */
	int			loaded;					/* pages loaded */
	PGAlignedBlock	*pages;				/* pages read from disk; IO_ALIGNed */
	void	   *alloc;					/* allocation containing *pages */
	BlockNumber	blknos[FLEXIBLE_ARRAY_MEMBER];	/* block numbers */
} LFCPrewarmChunk;

#define PREWARM_CHUNK_SIZE(nmemb) ( \
	offsetof(LFCPrewarmChunk, blknos) + (nmemb) * sizeof(BlockNumber) \
)

typedef struct LFCPrewarmWorkerState {
	uint64		pages_prefetched;	/* neon_prefetch() called */
	uint64		pages_read;			/* neon_read_at_lsn() called */
	uint64		pages_loaded;		/* actually written to LFC */
	int			max_io_depth;		/* max diff between prefetched and read */
	LFCPrewarmChunk *chunk;			/* current chunk we're working on */
} LFCPrewarmWorkerState;

static void lfcp_cleanup(int, Datum);
static LFCPrewarmChunk *lfcp_preprocess_chunk(FileCacheStateEntry *entry);
static void lfcp_prefetch_for_chunk(LFCPrewarmWorkerState *state);
static bool lfcp_read_chunk(LFCPrewarmWorkerState *state);
static void lfcp_load_chunk(LFCPrewarmWorkerState *state);
static void lfcp_release_chunk(LFCPrewarmChunk *chunk, uint64 generation);


/*
 * Prewarm LFC cache to the specified state.
 *
 * Prewarming can interfere with accesses to the pages by other backends. Usually access to LFC is protected by shared buffers: when Postgres
 * is reading page, it pins shared buffer and enforces that only one backend is reading it, while other are waiting for read completion.
 *
 * But it is not true for prewarming: backend can fetch page itself, modify and then write it to LFC. At the
 * same time `lfc_prewarm` tries to write deteriorated image of this page in LFC. To increase concurrency, access to LFC files (both read and write)
 * is performed without holding locks. So it can happen that two or more processes write different content to the same location in the LFC file.
 * Certainly we can not rely on disk content in this case.
 *
 * To solve this problem we use two flags in LFC entry: `prewarm_requested` and `prewarm_started`. First is set before prewarm is actually started.
 * `lfc_prewarm` writes to LFC file only if this flag is set. This flag is cleared if any other backend performs write to this LFC chunk.
 * In this case data loaded by `lfc_prewarm` is considered to be deteriorated and should be just ignored.
 *
 * But as far as write to LFC is performed without holding lock, there is no guarantee that no such write is in progress.
 * This is why second flag is used: `prewarm_started`. It is set by `lfc_prewarm` when is starts writing page and cleared when write is completed.
 * Any other backend writing to LFC should abandon it's write to LFC file (just not mark page as loaded in bitmap) if this flag is set.
 * So neither `lfc_prewarm`, neither backend are saving page in LFC in this case - it is just skipped.
 */

static void
lfc_prewarm(FileCacheStateEntry* fs, size_t numrestore)
{
	LFCPrewarmWorkerState state;

	if (!lfc_ensure_opened())
		return;

	if (numrestore == 0 || fs == NULL)
	{
		elog(LOG, "LFC: prewarm is disabled");
		return;
	}
	state.max_io_depth = Max(1, maintenance_io_concurrency);
	state.pages_prefetched = 0;
	state.pages_read = 0;
	state.pages_loaded = 0;

	PG_ENSURE_ERROR_CLEANUP(lfcp_cleanup, PointerGetDatum(&state));
	while (numrestore > 0)
	{
		CHECK_FOR_INTERRUPTS();
		/* Prefetching 1..3 */
		state.chunk = lfcp_preprocess_chunk(fs);

		/* Pump prefetch state and read data */
		while (state.chunk != NULL)
		{
			/* Prewarming 4 */
			lfcp_prefetch_for_chunk(&state);

			/* Prewarming 5 */
			if (lfcp_read_chunk(&state))
			{
				/* Prewarming steps 6, 9..11 */
				lfcp_load_chunk(&state);
			}
			CHECK_FOR_INTERRUPTS();
		}

		fs++;
		numrestore--;
	}
	PG_END_ENSURE_ERROR_CLEANUP(lfcp_cleanup, PointerGetDatum(&state));
}

static void
lfcp_cleanup(int code, Datum arg)
{
	LFCPrewarmWorkerState *state =
		(LFCPrewarmWorkerState *) DatumGetPointer(arg);
	LFCPrewarmChunk *chunk = state->chunk;

	if (chunk)
	{
		FileCacheEntry *entry = chunk->cacheEntry;
		if (entry)
		{
			bool	maybe_waiters = entry->prewarm_active;
			LWLockAcquire(lfc_lock, LW_EXCLUSIVE);
			entry->prewarm_active = false;
			release_entry(chunk->cacheEntry, false);

			/* signal any waiters */
			if (maybe_waiters)
				ConditionVariableSignal(&lfc_ctl->worker.prewarm_done);
		}
	}
}

/* Takes care of steps 1 through 3 of the prewarm system */
static LFCPrewarmChunk *
lfcp_preprocess_chunk(FileCacheStateEntry *fcsentry)
{
	uint32		bitmap[CHUNK_BITMAP_SIZE];
	bool		prewarm_conflict;
	int			j = 0;
	int			npages;
	LFCPrewarmChunk *pwchunk;
	FileCacheEntry *fcentry;

	/* Prewarming step 1, enter step 2; 2.2 */
	fcentry = lfc_entry_for_write(&fcsentry->key, true, &prewarm_conflict, fcsentry->bitmap);

	/* Chunk not found, and we're already at capacity */
	if (!fcentry)
	{
		LWLockRelease(lfc_lock);
		return NULL;
	}

	/* Prewarming 2.1 */
	for (int i = 0; i < CHUNK_BITMAP_SIZE; i++)
	{
		bitmap[i] = ~(fcentry->bitmap[i]) & fcsentry->bitmap[i];
	}

	npages = pg_popcount((const char *) bitmap,
						 sizeof(uint32) * CHUNK_BITMAP_SIZE);

	/*
	 * Break out of the loop and release resources when we don't have any
	 * pages left to prewarm.
	 */
	if (unlikely(npages == 0))
	{
		release_entry(fcentry, prewarm_conflict);
		return NULL;
	}

	/* Prewarming exit 2 */
	LWLockRelease(lfc_lock);

	pwchunk = palloc0(PREWARM_CHUNK_SIZE(npages));
	pwchunk->npages = npages;
	pwchunk->stateEntry = fcsentry;
	pwchunk->cacheEntry = fcentry;

	/* Prewarming 3 */
	for (int i = 0; i < BLOCKS_PER_CHUNK; i++)
	{
		if (bitmap[i >> 5] & 1 << (i % 32))
		{
			pwchunk->blknos[j++] = fcsentry->key.blockNum + i;
		}
	}

	Assert(j == pwchunk->npages);

	return pwchunk;
}

static void
lfcp_prefetch_for_chunk(LFCPrewarmWorkerState *state)
{
	LFCPrewarmChunk *chunk = state->chunk;
	int			io_depth = state->pages_prefetched - state->pages_read;
	int			ios_scheduled = chunk->npages - chunk->prefetched;

	if (io_depth < state->max_io_depth && ios_scheduled > 0)
	{
		BufferTag	tag;
		tag = chunk->stateEntry->key;

		for (int i = chunk->prefetched;
			 io_depth < state->max_io_depth && i < chunk->npages;
			 i++, io_depth++, state->pages_prefetched++, chunk->prefetched++)
		{
			tag.blockNum = chunk->blknos[i];
			prefetch_page(BufTagGetNRelFileInfo(tag), tag.forkNum,
						  tag.blockNum);
		}
	}
	Assert(chunk->npages >= chunk->prefetched);
	Assert(chunk->prefetched >= chunk->received);
	Assert(chunk->received >= 0);
}

/*
 * Read the next block from PS.
 * Returns true if this PrewarmChunk now has all requested pages.
 */
static bool
lfcp_read_chunk(LFCPrewarmWorkerState *state)
{
	LFCPrewarmChunk *chunk = state->chunk;
	int			io_depth = state->pages_prefetched - state->pages_read;
	int			chunk_ios_active = chunk->npages - chunk->prefetched;

	if (chunk_ios_active > 0)
	{
		BufferTag	tag;

		Assert(io_depth >= chunk_ios_active);

		tag = chunk->stateEntry->key;

		if (chunk->received == 0)
		{
			Assert(chunk->alloc == NULL);
#if PG_MAJORVERSION_NUM > 15
			chunk->pages = chunk->alloc = 
				MemoryContextAllocAligned(CurrentMemoryContext,
										  chunk->npages * BLCKSZ,
										  PG_IO_ALIGN_SIZE,
										  0);
#else
			chunk->alloc = palloc((chunk->npages) * BLCKSZ);
			chunk->pages = (PGAlignedBlock *)
				TYPEALIGN((uintptr_t) chunk->alloc, PG_IO_ALIGN_SIZE);
#endif
		}

		Assert(chunk->pages != NULL);
		tag.blockNum = chunk->blknos[chunk->received];

		read_page(BufTagGetNRelFileInfo(tag), tag.forkNum, tag.blockNum,
				  chunk->pages[chunk->received].data);

		state->pages_read++;
		chunk->received++;
	}

	Assert(chunk_ios_active <= (state->pages_prefetched - state->pages_read));
	Assert(chunk->npages >= chunk->prefetched);
	Assert(chunk->prefetched >= chunk->received);
	Assert(chunk->received >= 0);

	/* Are all pages ready to load into the LFC now? */
	return chunk->npages == chunk->received;
}

/*
 * Load this chunk into LFC.
 * Handles prewarm 9..11
 */
static void
lfcp_load_chunk(LFCPrewarmWorkerState *state)
{
	LFCPrewarmChunk *chunk = state->chunk;
	FileCacheEntry *fcentry = chunk->cacheEntry;
	uint64		generation;

	LWLockAcquire(lfc_lock, LW_EXCLUSIVE);

	/* handle error condition */
	if (!LFC_ENABLED())
	{
		LWLockRelease(lfc_lock);
		lfcp_release_chunk(chunk, lfc_ctl->generation);
		state->chunk = NULL;
		return;
	}

	/* Prewarming 6.1 */
	chunk->cacheEntry->prewarm_active = true;
	/* Prewarming 6.3 */
	lfc_ctl->worker.wait_count = chunk->cacheEntry->access_count - 1;
	generation = lfc_ctl->generation;

	/* Prewarming exit 6 */
	LWLockRelease(lfc_lock);

	Assert(chunk->npages > 0);

	/* Prewarming 9.1 */
	if (lfc_ctl->worker.wait_count > 0)
	{
		instr_time	start, end;
		int		max_loops = lfc_ctl->worker.wait_count;

		INSTR_TIME_SET_CURRENT(start);
		ConditionVariablePrepareToSleep(&lfc_ctl->worker.chunk_release);

		while (lfc_ctl->worker.wait_count > 0 && max_loops > 0)
		{
			max_loops--;
			ConditionVariableTimedSleep(&lfc_ctl->worker.chunk_release,
										10, WAIT_EVENT_NEON_LFC_PREWARM_IO);
		}

		ConditionVariableCancelSleep();
		INSTR_TIME_SET_CURRENT(end);

		if (lfc_ctl->worker.wait_count > 0)
			elog(ERROR, "Prewarm waiting for too long; %d readers remaining after %0.3f seconds",
				 lfc_ctl->worker.wait_count, INSTR_TIME_GET_DOUBLE(end));

		INSTR_TIME_SUBTRACT(end, start);
		lfc_ctl->worker.wait_time += INSTR_TIME_GET_MICROSEC(end);
	}

	/* Prewarming 9.2 */
	do {
		int		i;
		int		start = chunk->loaded;
		int		writes_remaining = chunk->npages - chunk->loaded;
		BlockNumber base = chunk->blknos[chunk->loaded];

		/*
		 * Merge IOs
		 * 
		 * "i" will be the number of blocks written starting with `chunk->loaded`
		 */
		for (i = 0; i < writes_remaining; i++)
		{
			if (chunk->blknos[start + i] != base + i)
				break;
			if (fcentry->bitmap[(start + i) / 32] & 1 << ((start + i) % 32))
				break;
		}

		/*
		 * If the current block was written after we started fetching blocks,
		 * then i == 0. Else, write the combined pages in a single IO.
		 */
		if (i > 0)
		{
			size_t	len = i * BLCKSZ;
			off_t	file_off =
				((off_t) fcentry->offset) * BLOCKS_PER_CHUNK * BLCKSZ +
				((off_t) start * BLCKSZ);
			char   *dataptr = chunk->pages[start].data;

			do {
				/* no 'pwritev' necessary, nor possible */
				int	len_written = (int) pwrite(lfc_desc, dataptr, len,
											   file_off);

				if (len_written < 0)
				{
					elog(ERROR, "LFC Prewarm: Error writing data to LFC: %m");
				}
				file_off += len_written;
				dataptr += len_written;
				len -= len_written;
			} while (len > 0);

			chunk->loaded += i;
		}
		else
		{
			/*
			 * Current page was written to after we started prewarming, but
			 * before we received all pages for this chunk.
			 */
			Assert(fcentry->bitmap[start / 32] & 1 << (start % 32));
			chunk->loaded += 1;
		}
	} while (chunk->loaded < chunk->npages);

	/* Prewarming 10 and 11 */
	/* handles signalling ready-marking of data */
	lfcp_release_chunk(chunk, generation);
	state->chunk = NULL;
}

/* Handles prewarming step 10 and 11  */
static void
lfcp_release_chunk(LFCPrewarmChunk *chunk, uint64 generation)
{
	/* Prewarming 10 entry */
	LWLockAcquire(lfc_lock, LW_EXCLUSIVE);
	/* Prewarming 10.2 */
	chunk->cacheEntry->prewarm_active = false;

	/* Handle error condition */
	if (lfc_ctl->generation != generation)
	{
		release_entry(chunk->cacheEntry, false);
		return;
	}

	/* Prewarming 10.1 */
	if (chunk->loaded == chunk->npages)
	{
		/* mark pages 'written' for this chunk */
		for (int i = 0; i < CHUNK_BITMAP_SIZE; i++)
		{
			chunk->cacheEntry->bitmap[i] |= chunk->stateEntry->bitmap[i];
		}
	}
	/* Prewarming 10.3; exit 10 */
	release_entry(chunk->cacheEntry, false);

	/* Prewarming 11 */
	ConditionVariableSignal(&lfc_ctl->worker.prewarm_done);

	/* Cleanup */
	pfree(chunk->alloc);
	pfree(chunk);
}

/*
 * Check if page is present in the cache.
 * Returns true if page is found in local cache.
 */
bool
lfc_cache_contains(NRelFileInfo rinfo, ForkNumber forkNum, BlockNumber blkno)
{
	BufferTag	tag;
	FileCacheEntry *entry;
	int			chunk_offs = blkno & (BLOCKS_PER_CHUNK - 1);
	bool		found = false;
	uint32		hash;

	if (lfc_maybe_disabled())	/* fast exit if file cache is disabled */
		return false;

	CopyNRelFileInfoToBufTag(tag, rinfo);
	tag.forkNum = forkNum;
	tag.blockNum = blkno & ~(BLOCKS_PER_CHUNK - 1);

	CriticalAssert(BufTagGetRelNumber(&tag) != InvalidRelFileNumber);
	hash = get_hash_value(lfc_hash, &tag);

	LWLockAcquire(lfc_lock, LW_SHARED);
	if (LFC_ENABLED())
	{
		entry = hash_search_with_hash_value(lfc_hash, &tag, hash, HASH_FIND, NULL);
		found = entry != NULL && (entry->bitmap[chunk_offs >> 5] & (1 << (chunk_offs & 31))) != 0;
	}
	LWLockRelease(lfc_lock);
	return found;
}

/*
 * Check if page is present in the cache.
 * Returns true if page is found in local cache.
 */
int
lfc_cache_containsv(NRelFileInfo rinfo, ForkNumber forkNum, BlockNumber blkno,
					int nblocks, bits8 *bitmap)
{
	BufferTag	tag;
	FileCacheEntry *entry;
	uint32		chunk_offs;
	int			found = 0;
	uint32		hash;
	int			i = 0;

	if (lfc_maybe_disabled())	/* fast exit if file cache is disabled */
		return 0;

	CopyNRelFileInfoToBufTag(tag, rinfo);
	tag.forkNum = forkNum;

	CriticalAssert(BufTagGetRelNumber(&tag) != InvalidRelFileNumber);

	tag.blockNum = (blkno + i) & ~(BLOCKS_PER_CHUNK - 1);
	hash = get_hash_value(lfc_hash, &tag);
	chunk_offs = (blkno + i) & (BLOCKS_PER_CHUNK - 1);

	LWLockAcquire(lfc_lock, LW_SHARED);

	while (true)
	{
		int		this_chunk = Min(nblocks, BLOCKS_PER_CHUNK - chunk_offs);
		if (LFC_ENABLED())
		{
			entry = hash_search_with_hash_value(lfc_hash, &tag, hash, HASH_FIND, NULL);

			if (entry != NULL)
			{
				for (; chunk_offs < BLOCKS_PER_CHUNK && i < nblocks; chunk_offs++, i++)
				{
					if ((entry->bitmap[chunk_offs >> 5] & 
						(1 << (chunk_offs & 31))) != 0)
					{
						BITMAP_SET(bitmap, i);
						found++;
					}
				}
			}
			else
			{
				i += this_chunk;
			}
		}
		else
		{
			LWLockRelease(lfc_lock);
			return found;
		}

		/*
		 * Break out of the iteration before doing expensive stuff for
		 * a next iteration
		 */
		if (i + 1 >= nblocks)
			break;

		/*
		 * Prepare for the next iteration. We don't unlock here, as that'd
		 * probably be more expensive than the gains it'd get us.
		 */
		tag.blockNum = (blkno + i) & ~(BLOCKS_PER_CHUNK - 1);
		hash = get_hash_value(lfc_hash, &tag);
		chunk_offs = (blkno + i) & (BLOCKS_PER_CHUNK - 1);
	}

	LWLockRelease(lfc_lock);

#if USE_ASSERT_CHECKING
	do {
		int count = 0;

		for (int j = 0; j < nblocks; j++)
		{
			if (BITMAP_ISSET(bitmap, j))
				count++;
		}

		Assert(count == found);
	} while (false);
#endif

	return found;
}

/*
 * Evict a page (if present) from the local file cache
 */
void
lfc_evict(NRelFileInfo rinfo, ForkNumber forkNum, BlockNumber blkno)
{
	BufferTag	tag;
	FileCacheEntry *entry;
	bool		found;
	int			chunk_offs = blkno & (BLOCKS_PER_CHUNK - 1);
	uint32		hash;

	if (lfc_maybe_disabled())	/* fast exit if file cache is disabled */
		return;

	CopyNRelFileInfoToBufTag(tag, rinfo);
	tag.forkNum = forkNum;
	tag.blockNum = (blkno & ~(BLOCKS_PER_CHUNK - 1));

	CriticalAssert(BufTagGetRelNumber(&tag) != InvalidRelFileNumber);
	hash = get_hash_value(lfc_hash, &tag);

	LWLockAcquire(lfc_lock, LW_EXCLUSIVE);

	if (!LFC_ENABLED())
	{
		LWLockRelease(lfc_lock);
		return;
	}

	entry = hash_search_with_hash_value(lfc_hash, &tag, hash, HASH_FIND, &found);

	if (!found)
	{
		/* nothing to do */
		LWLockRelease(lfc_lock);
		return;
	}

	/* remove the page from the cache */
	entry->bitmap[chunk_offs >> 5] &= ~(1 << (chunk_offs & (32 - 1)));

	if (entry->access_count == 0)
	{
		/*
		 * If the chunk has no live entries, we can position the chunk to be
		 * recycled first.
		 */
		if (entry->bitmap[chunk_offs >> 5] == 0)
		{
			bool		has_remaining_pages = false;

			for (int i = 0; i < CHUNK_BITMAP_SIZE; i++)
			{
				if (entry->bitmap[i] != 0)
				{
					has_remaining_pages = true;
					break;
				}
			}

			/*
			 * Put the entry at the position that is first to be reclaimed when we
			 * have no cached pages remaining in the chunk
			 */
			if (!has_remaining_pages)
			{
				dlist_delete(&entry->list_node);
				dlist_push_head(&lfc_ctl->lru, &entry->list_node);
			}
		}
	}

	/*
	 * Done: apart from empty chunks, we don't move chunks in the LRU when
	 * they're empty because eviction isn't usage.
	 */

	LWLockRelease(lfc_lock);
}

/*
 * Try to read pages from local cache.
 * Returns the number of pages read from the local cache, and sets bits in
 * 'read' for the pages which were read. This may scribble over buffers not
 * marked in 'read', so be careful with operation ordering.
 *
 * In case of error local file cache is disabled (lfc->limit is set to zero),
 * and -1 is returned. Note that 'read' and the buffers may be touched and in
 * an otherwise invalid state.
 *
 * If the mask argument is supplied, bits will be set at the offsets of pages
 * that were present and read from the LFC.
 */
int
lfc_readv_select(NRelFileInfo rinfo, ForkNumber forkNum, BlockNumber blkno,
				 void **buffers, BlockNumber nblocks, bits8 *mask)
{
	BufferTag	tag;
	FileCacheEntry *entry;
	ssize_t		rc;
	uint32		hash;
	uint64		generation;
	uint32		entry_offset;
	int			blocks_read = 0;
	int			buf_offset = 0;

	if (lfc_maybe_disabled())	/* fast exit if file cache is disabled */
		return 0;

	if (!lfc_ensure_opened())
		return 0;

	CopyNRelFileInfoToBufTag(tag, rinfo);
	tag.forkNum = forkNum;

	CriticalAssert(BufTagGetRelNumber(&tag) != InvalidRelFileNumber);

	/* 
	 * For every chunk that has blocks we're interested in, we
	 * 1. get the chunk header
	 * 2. Check if the chunk actually has the blocks we're interested in
	 * 3. Read the blocks we're looking for (in one preadv), assuming they exist
	 * 4. Update the statistics for the read call.
	 *
	 * If there is an error, we do an early return.
	 */
	while (nblocks > 0)
	{
		struct iovec iov[PG_IOV_MAX];
		int		chunk_offs = blkno & (BLOCKS_PER_CHUNK - 1);
		int		blocks_in_chunk = Min(nblocks, BLOCKS_PER_CHUNK - (blkno % BLOCKS_PER_CHUNK));
		int		iteration_hits = 0;
		int		iteration_misses = 0;
		bool	start_prewarm_active;
		uint64	io_time_us = 0;
		Assert(blocks_in_chunk > 0);

		for (int i = 0; i < blocks_in_chunk; i++)
		{
			iov[i].iov_base = buffers[buf_offset + i];
			iov[i].iov_len = BLCKSZ;
		}

		tag.blockNum = blkno - chunk_offs;
		hash = get_hash_value(lfc_hash, &tag);

		LWLockAcquire(lfc_lock, LW_EXCLUSIVE);

		/* We can return the blocks we've read before LFC got disabled;
		 * assuming we read any. */
		if (!LFC_ENABLED())
		{
			LWLockRelease(lfc_lock);
			return blocks_read;
		}

		entry = hash_search_with_hash_value(lfc_hash, &tag, hash, HASH_FIND, NULL);

		/* Approximate working set for the blocks assumed in this entry */
		for (int i = 0; i < blocks_in_chunk; i++)
		{
			tag.blockNum = blkno + i;
			addSHLL(&lfc_ctl->wss_estimation, hash_bytes((uint8_t const*)&tag, sizeof(tag)));
		}

		if (entry == NULL)
		{
			/* Pages are not cached */
			lfc_ctl->misses += blocks_in_chunk;
			pgBufferUsage.file_cache.misses += blocks_in_chunk;
			LWLockRelease(lfc_lock);

			buf_offset += blocks_in_chunk;
			nblocks -= blocks_in_chunk;
			blkno += blocks_in_chunk;

			continue;
		}

		/* Unlink entry from LRU list to pin it for the duration of IO operation */
		if (entry->access_count++ == 0)
			dlist_delete(&entry->list_node);

		generation = lfc_ctl->generation;
		entry_offset = entry->offset;
		start_prewarm_active = entry->prewarm_active;

		LWLockRelease(lfc_lock);

		/*
		 * Unlocked read is safe here, because all modifications on this data
		 * will set more bits, never remove them, at least not while the chunk
		 * hasn't been added back to the LRU (which won't happen while we
		 * don't release it).
		 */
		for (int i = 0; i < blocks_in_chunk; i++)
		{
			/*
			 * If the page is valid, we consider it "read".
			 * All other pages will be fetched separately by the next cache
			 */
			if (entry->bitmap[(chunk_offs + i) / 32] & (1 << ((chunk_offs + i) % 32)))
			{
				BITMAP_SET(mask, buf_offset + i);
				iteration_hits++;
			}
			else
				iteration_misses++;
		}

		Assert(iteration_hits + iteration_misses > 0);

		if (iteration_hits != 0)
		{
			pgstat_report_wait_start(WAIT_EVENT_NEON_LFC_READ);
			rc = preadv(lfc_desc, iov, blocks_in_chunk,
						((off_t) entry_offset * BLOCKS_PER_CHUNK + chunk_offs) * BLCKSZ);
			pgstat_report_wait_end();

			if (rc != (BLCKSZ * blocks_in_chunk))
			{
				lfc_disable("read");
				return -1;
			}
		}

		/* Return entry to the LFC */
		LWLockAcquire(lfc_lock, LW_EXCLUSIVE);

		if (unlikely(lfc_ctl->generation != generation))
		{
			/* generation mismatch, assume error condition */
			LWLockRelease(lfc_lock);
			return -1;
		}

		CriticalAssert(LFC_ENABLED());

		lfc_ctl->hits += iteration_hits;
		lfc_ctl->misses += iteration_misses;
		pgBufferUsage.file_cache.hits += iteration_hits;
		pgBufferUsage.file_cache.misses += iteration_misses;

		if (iteration_hits)
		{
			lfc_ctl->time_read += io_time_us;
			inc_page_cache_read_wait(io_time_us);
		}

		release_entry(entry, start_prewarm_active);

		buf_offset += blocks_in_chunk;
		nblocks -= blocks_in_chunk;
		blkno += blocks_in_chunk;
		blocks_read += iteration_hits;
	}

	return blocks_read;
}

/*
 * Put page in local file cache.
 * If cache is full then evict some other page.
 */
void
lfc_writev(NRelFileInfo rinfo, ForkNumber forkNum, BlockNumber blkno,
		   const void *const *buffers, BlockNumber nblocks)
{
	BufferTag	tag;
	FileCacheEntry *entry;
	ssize_t		rc;
	uint64		generation;
	uint32		entry_offset;
	int			buf_offset = 0;

	if (lfc_maybe_disabled())	/* fast exit if file cache is disabled */
		return;

	if (!lfc_ensure_opened())
		return;

	CopyNRelFileInfoToBufTag(tag, rinfo);
	tag.forkNum = forkNum;

	CriticalAssert(BufTagGetRelNumber(&tag) != InvalidRelFileNumber);

	/*
	 * For every chunk that has blocks we're interested in, we
	 * 1. get the chunk header
	 * 2. Check if the chunk actually has the blocks we're interested in
	 * 3. Read the blocks we're looking for (in one preadv), assuming they exist
	 * 4. Update the statistics for the read call.
	 *
	 * If there is an error, we do an early return.
	 */
	while (nblocks > 0)
	{
		struct iovec iov[PG_IOV_MAX];
		uint32		chunk_offs = blkno & (BLOCKS_PER_CHUNK - 1);
		uint32		blocks_in_chunk = Min(nblocks, BLOCKS_PER_CHUNK - (blkno % BLOCKS_PER_CHUNK));
		uint32		bitmap[CHUNK_BITMAP_SIZE];
		instr_time io_start, io_end;
		bool	prewarm_active;
		Assert(blocks_in_chunk > 0);

		for (int i = 0; i < blocks_in_chunk; i++)
		{
			iov[i].iov_base = unconstify(void *, buffers[buf_offset + i]);
			iov[i].iov_len = BLCKSZ;
			bitmap[(buf_offset + i) >> 5] |= 1 << ((buf_offset + i) % 32);
		}

		tag.blockNum = blkno & ~(BLOCKS_PER_CHUNK - 1);

		entry = lfc_entry_for_write(&tag, false, &prewarm_active, bitmap);

		if (!LFC_ENABLED() || !PointerIsValid(entry))
		{
			LWLockRelease(lfc_lock);
			return;
		}

		Assert(entry);
		generation = lfc_ctl->generation;
		entry_offset = entry->offset;
		LWLockRelease(lfc_lock);

		pgstat_report_wait_start(WAIT_EVENT_NEON_LFC_WRITE);
		INSTR_TIME_SET_CURRENT(io_start);
		rc = pwritev(lfc_desc, iov, blocks_in_chunk,
					 ((off_t) entry_offset * BLOCKS_PER_CHUNK + chunk_offs) * BLCKSZ);
		INSTR_TIME_SET_CURRENT(io_end);
		pgstat_report_wait_end();

		if (rc != BLCKSZ * blocks_in_chunk)
		{
			lfc_disable("write");
		}
		else
		{
			LWLockAcquire(lfc_lock, LW_EXCLUSIVE);

			if (lfc_ctl->generation == generation)
			{
				uint64	time_spent_us;
				CriticalAssert(LFC_ENABLED());
				/* Place entry to the head of LRU list */
				CriticalAssert(entry->access_count > 0);

				lfc_ctl->writes += blocks_in_chunk;
				INSTR_TIME_SUBTRACT(io_start, io_end);
				time_spent_us = INSTR_TIME_GET_MICROSEC(io_start);
				lfc_ctl->time_write += time_spent_us;
				inc_page_cache_write_wait(time_spent_us);

				for (int i = 0; i < blocks_in_chunk; i++)
				{
					lfc_ctl->used_pages += 1 - ((entry->bitmap[(chunk_offs + i) >> 5] >> ((chunk_offs + i) & 31)) & 1);
					entry->bitmap[(chunk_offs + i) >> 5] |=
						(1 << ((chunk_offs + i) & 31));
				}

				release_entry(entry, false);
			}
			else
			{
				LWLockRelease(lfc_lock);
			}
		}
		blkno += blocks_in_chunk;
		buf_offset += blocks_in_chunk;
		nblocks -= blocks_in_chunk;
	}
}

typedef struct
{
	TupleDesc	tupdesc;
} NeonGetStatsCtx;

#define NUM_NEON_GET_STATS_COLS	2

PG_FUNCTION_INFO_V1(neon_get_lfc_stats);
Datum
neon_get_lfc_stats(PG_FUNCTION_ARGS)
{
	FuncCallContext *funcctx;
	NeonGetStatsCtx *fctx;
	MemoryContext oldcontext;
	TupleDesc	tupledesc;
	Datum		result;
	HeapTuple	tuple;
	char const *key;
	uint64		value = 0;
	Datum		values[NUM_NEON_GET_STATS_COLS];
	bool		nulls[NUM_NEON_GET_STATS_COLS];

	if (SRF_IS_FIRSTCALL())
	{
		funcctx = SRF_FIRSTCALL_INIT();

		/* Switch context when allocating stuff to be used in later calls */
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		/* Create a user function context for cross-call persistence */
		fctx = (NeonGetStatsCtx *) palloc(sizeof(NeonGetStatsCtx));

		/* Construct a tuple descriptor for the result rows. */
		tupledesc = CreateTemplateTupleDesc(NUM_NEON_GET_STATS_COLS);

		TupleDescInitEntry(tupledesc, (AttrNumber) 1, "lfc_key",
						   TEXTOID, -1, 0);
		TupleDescInitEntry(tupledesc, (AttrNumber) 2, "lfc_value",
						   INT8OID, -1, 0);

		fctx->tupdesc = BlessTupleDesc(tupledesc);
		funcctx->user_fctx = fctx;

		/* Return to original context when allocating transient memory */
		MemoryContextSwitchTo(oldcontext);
	}

	funcctx = SRF_PERCALL_SETUP();

	/* Get the saved state */
	fctx = (NeonGetStatsCtx *) funcctx->user_fctx;

	switch (funcctx->call_cntr)
	{
		case 0:
			key = "file_cache_misses";
			if (lfc_ctl)
				value = lfc_ctl->misses;
			break;
		case 1:
			key = "file_cache_hits";
			if (lfc_ctl)
				value = lfc_ctl->hits;
			break;
		case 2:
			key = "file_cache_used";
			if (lfc_ctl)
				value = lfc_ctl->used;
			break;
		case 3:
			key = "file_cache_writes";
			if (lfc_ctl)
				value = lfc_ctl->writes;
			break;
		case 4:
			key = "file_cache_size";
			if (lfc_ctl)
				value = lfc_ctl->size;
			break;
		case 5:
			key = "file_cache_used_pages";
			if (lfc_ctl)
				value = lfc_ctl->used_pages;
			break;
		default:
			SRF_RETURN_DONE(funcctx);
	}
	values[0] = PointerGetDatum(cstring_to_text(key));
	nulls[0] = false;
	if (lfc_ctl)
	{
		nulls[1] = false;
		values[1] = Int64GetDatum(value);
	}
	else
		nulls[1] = true;

	tuple = heap_form_tuple(fctx->tupdesc, values, nulls);
	result = HeapTupleGetDatum(tuple);
	SRF_RETURN_NEXT(funcctx, result);
}


/*
 * Function returning data from the local file cache
 * relation node/tablespace/database/blocknum and access_counter
 */
PG_FUNCTION_INFO_V1(local_cache_pages);

/*
 * Record structure holding the to be exposed cache data.
 */
typedef struct
{
	uint32		pageoffs;
	Oid			relfilenode;
	Oid			reltablespace;
	Oid			reldatabase;
	ForkNumber	forknum;
	BlockNumber blocknum;
	uint16		accesscount;
} LocalCachePagesRec;

/*
 * Function context for data persisting over repeated calls.
 */
typedef struct
{
	TupleDesc	tupdesc;
	LocalCachePagesRec *record;
} LocalCachePagesContext;


#define NUM_LOCALCACHE_PAGES_ELEM	7

Datum
local_cache_pages(PG_FUNCTION_ARGS)
{
	FuncCallContext *funcctx;
	Datum		result;
	MemoryContext oldcontext;
	LocalCachePagesContext *fctx;	/* User function context. */
	TupleDesc	tupledesc;
	TupleDesc	expected_tupledesc;
	HeapTuple	tuple;

	if (SRF_IS_FIRSTCALL())
	{
		HASH_SEQ_STATUS status;
		FileCacheEntry *entry;
		uint32		n_pages = 0;

		funcctx = SRF_FIRSTCALL_INIT();

		/* Switch context when allocating stuff to be used in later calls */
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		/* Create a user function context for cross-call persistence */
		fctx = (LocalCachePagesContext *) palloc(sizeof(LocalCachePagesContext));

		/*
		 * To smoothly support upgrades from version 1.0 of this extension
		 * transparently handle the (non-)existence of the pinning_backends
		 * column. We unfortunately have to get the result type for that... -
		 * we can't use the result type determined by the function definition
		 * without potentially crashing when somebody uses the old (or even
		 * wrong) function definition though.
		 */
		if (get_call_result_type(fcinfo, NULL, &expected_tupledesc) != TYPEFUNC_COMPOSITE)
			neon_log(ERROR, "return type must be a row type");

		if (expected_tupledesc->natts != NUM_LOCALCACHE_PAGES_ELEM)
			neon_log(ERROR, "incorrect number of output arguments");

		/* Construct a tuple descriptor for the result rows. */
		tupledesc = CreateTemplateTupleDesc(expected_tupledesc->natts);
		TupleDescInitEntry(tupledesc, (AttrNumber) 1, "pageoffs",
						   INT8OID, -1, 0);
#if PG_MAJORVERSION_NUM < 16
		TupleDescInitEntry(tupledesc, (AttrNumber) 2, "relfilenode",
						   OIDOID, -1, 0);
#else
		TupleDescInitEntry(tupledesc, (AttrNumber) 2, "relfilenumber",
						   OIDOID, -1, 0);
#endif
		TupleDescInitEntry(tupledesc, (AttrNumber) 3, "reltablespace",
						   OIDOID, -1, 0);
		TupleDescInitEntry(tupledesc, (AttrNumber) 4, "reldatabase",
						   OIDOID, -1, 0);
		TupleDescInitEntry(tupledesc, (AttrNumber) 5, "relforknumber",
						   INT2OID, -1, 0);
		TupleDescInitEntry(tupledesc, (AttrNumber) 6, "relblocknumber",
						   INT8OID, -1, 0);
		TupleDescInitEntry(tupledesc, (AttrNumber) 7, "accesscount",
						   INT4OID, -1, 0);

		fctx->tupdesc = BlessTupleDesc(tupledesc);

		if (lfc_ctl)
		{
			LWLockAcquire(lfc_lock, LW_SHARED);

			if (LFC_ENABLED())
			{
				hash_seq_init(&status, lfc_hash);
				while ((entry = hash_seq_search(&status)) != NULL)
				{
					for (int i = 0; i < CHUNK_BITMAP_SIZE; i++)
						n_pages += pg_popcount32(entry->bitmap[i]);
				}
			}
		}
		fctx->record = (LocalCachePagesRec *)
			MemoryContextAllocHuge(CurrentMemoryContext,
								   sizeof(LocalCachePagesRec) * n_pages);

		/* Set max calls and remember the user function context. */
		funcctx->max_calls = n_pages;
		funcctx->user_fctx = fctx;

		/* Return to original context when allocating transient memory */
		MemoryContextSwitchTo(oldcontext);

		if (n_pages != 0)
		{
			/*
			 * Scan through all the cache entries, saving the relevant fields
			 * in the fctx->record structure.
			 */
			uint32		n = 0;

			hash_seq_init(&status, lfc_hash);
			while ((entry = hash_seq_search(&status)) != NULL)
			{
				for (int i = 0; i < BLOCKS_PER_CHUNK; i++)
				{
					if (entry->bitmap[i >> 5] & (1 << (i & 31)))
					{
						fctx->record[n].pageoffs = entry->offset * BLOCKS_PER_CHUNK + i;
						fctx->record[n].relfilenode = NInfoGetRelNumber(BufTagGetNRelFileInfo(entry->key));
						fctx->record[n].reltablespace = NInfoGetSpcOid(BufTagGetNRelFileInfo(entry->key));
						fctx->record[n].reldatabase = NInfoGetDbOid(BufTagGetNRelFileInfo(entry->key));
						fctx->record[n].forknum = entry->key.forkNum;
						fctx->record[n].blocknum = entry->key.blockNum + i;
						fctx->record[n].accesscount = entry->access_count;
						n += 1;
					}
				}
			}
			Assert(n_pages == n);
		}
		if (lfc_ctl)
			LWLockRelease(lfc_lock);
	}

	funcctx = SRF_PERCALL_SETUP();

	/* Get the saved state */
	fctx = funcctx->user_fctx;

	if (funcctx->call_cntr < funcctx->max_calls)
	{
		uint32		i = funcctx->call_cntr;
		Datum		values[NUM_LOCALCACHE_PAGES_ELEM];
		bool		nulls[NUM_LOCALCACHE_PAGES_ELEM] = {
			false, false, false, false, false, false, false
		};

		values[0] = Int64GetDatum((int64) fctx->record[i].pageoffs);
		values[1] = ObjectIdGetDatum(fctx->record[i].relfilenode);
		values[2] = ObjectIdGetDatum(fctx->record[i].reltablespace);
		values[3] = ObjectIdGetDatum(fctx->record[i].reldatabase);
		values[4] = ObjectIdGetDatum(fctx->record[i].forknum);
		values[5] = Int64GetDatum((int64) fctx->record[i].blocknum);
		values[6] = Int32GetDatum(fctx->record[i].accesscount);

		/* Build and return the tuple. */
		tuple = heap_form_tuple(fctx->tupdesc, values, nulls);
		result = HeapTupleGetDatum(tuple);

		SRF_RETURN_NEXT(funcctx, result);
	}
	else
		SRF_RETURN_DONE(funcctx);
}

PG_FUNCTION_INFO_V1(approximate_working_set_size_seconds);

Datum
approximate_working_set_size_seconds(PG_FUNCTION_ARGS)
{
	if (lfc_size_limit != 0)
	{
		int32 dc;
		time_t duration = PG_ARGISNULL(0) ? (time_t)-1 : PG_GETARG_INT32(0);
		LWLockAcquire(lfc_lock, LW_SHARED);
		dc = (int32) estimateSHLL(&lfc_ctl->wss_estimation, duration);
		LWLockRelease(lfc_lock);
		PG_RETURN_INT32(dc);
	}
	PG_RETURN_NULL();
}

PG_FUNCTION_INFO_V1(approximate_working_set_size);

Datum
approximate_working_set_size(PG_FUNCTION_ARGS)
{
	if (lfc_size_limit != 0)
	{
		int32 dc;
		bool reset = PG_GETARG_BOOL(0);
		LWLockAcquire(lfc_lock, reset ? LW_EXCLUSIVE : LW_SHARED);
		dc = (int32) estimateSHLL(&lfc_ctl->wss_estimation, (time_t)-1);
		if (reset)
			memset(lfc_ctl->wss_estimation.regs, 0, sizeof lfc_ctl->wss_estimation.regs);
		LWLockRelease(lfc_lock);
		PG_RETURN_INT32(dc);
	}
	PG_RETURN_NULL();
}

PG_FUNCTION_INFO_V1(get_local_cache_state);

Datum
get_local_cache_state(PG_FUNCTION_ARGS)
{
	size_t n_entries = PG_ARGISNULL(0) ? lfc_prewarm_limit : PG_GETARG_INT32(0);
	FileCacheStateEntry* fs = lfc_get_state(&n_entries);
	if (fs != NULL)
	{
		size_t size_in_bytes = sizeof(FileCacheStateEntry) * n_entries;
		bytea* res = (bytea*)palloc(VARHDRSZ + size_in_bytes);

		SET_VARSIZE(res, VARHDRSZ + size_in_bytes);
		memcpy(VARDATA(res), fs, size_in_bytes);
		pfree(fs);

		PG_RETURN_BYTEA_P(res);
	}
	PG_RETURN_NULL();
}

PG_FUNCTION_INFO_V1(prewarm_local_cache);

Datum
prewarm_local_cache(PG_FUNCTION_ARGS)
{
	bytea* state = PG_GETARG_BYTEA_PP(0);
	uint32 n_entries = VARSIZE_ANY_EXHDR(state)/sizeof(FileCacheStateEntry);
	FileCacheStateEntry* fs = (FileCacheStateEntry*)VARDATA_ANY(state);

	lfc_prewarm(fs, n_entries);

	PG_RETURN_NULL();
}

PG_FUNCTION_INFO_V1(get_prewarm_info);

Datum
get_prewarm_info(PG_FUNCTION_ARGS)
{
	Datum		values[4];
	bool		nulls[4];
	TupleDesc	tupdesc;

	if (lfc_size_limit == 0)
		PG_RETURN_NULL();

	tupdesc = CreateTemplateTupleDesc(4);
	TupleDescInitEntry(tupdesc, (AttrNumber) 1, "total_chunks", INT4OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 2, "curr_chunk", INT4OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 3, "prewarmed_pages", INT4OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 4, "skipped_pages", INT4OID, -1, 0);
	tupdesc = BlessTupleDesc(tupdesc);

	MemSet(nulls, 0, sizeof(nulls));
	LWLockAcquire(lfc_lock, LW_SHARED);
	values[0] = Int32GetDatum(lfc_ctl->prewarm_total_chunks);
	values[1] = Int32GetDatum(lfc_ctl->prewarm_curr_chunk);
	values[2] = Int32GetDatum(lfc_ctl->prewarmed_pages);
	values[3] = Int32GetDatum(lfc_ctl->skipped_pages);
	LWLockRelease(lfc_lock);

	PG_RETURN_DATUM(HeapTupleGetDatum(heap_form_tuple(tupdesc, values, nulls)));
}


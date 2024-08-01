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
#include "funcapi.h"
#include "miscadmin.h"
#include "pagestore_client.h"
#include "common/hashfn.h"
#include "pgstat.h"
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

#include "hll.h"

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
 */

/* Local file storage allocation chunk.
 * Should be power of two and not less than 32. Using larger than page chunks can
 * 1. Reduce hash-map memory footprint: 8TB database contains billion pages
 *    and size of hash entry is 40 bytes, so we need 40Gb just for hash map.
 *    1Mb chunks can reduce hash map size to 320Mb.
 * 2. Improve access locality, subsequent pages will be allocated together improving seqscan speed
 */
#define BLOCKS_PER_CHUNK	8 /* 64kb chunk */
#define MB					((uint64)1024*1024)

#define SIZE_MB_TO_CHUNKS(size) ((uint32)((size) * MB / BLCKSZ / BLOCKS_PER_CHUNK))
#define CHUNK_BITMAP_SIZE ((BLOCKS_PER_CHUNK + 31) / 32)

typedef struct FileCacheEntry
{
	BufferTag	key;
	uint32		hash;
	uint32		offset;
	uint32		access_count;
	uint32		bitmap[CHUNK_BITMAP_SIZE];
	dlist_node	lru_node;		/* LRU list node */
} FileCacheEntry;

typedef struct FileCacheControl
{
	uint64		generation;		/* generation is needed to handle correct hash
								 * reenabling */
	uint32		size;			/* size of cache file in chunks */
	uint32		used;			/* number of used chunks */
	uint32		limit;			/* shared copy of lfc_size_limit */
	uint64		hits;
	uint64		misses;
	uint64		writes;
	uint32		n_holes;
	uint32		first_hole;
 	dlist_head	lru;			/* double linked list for LRU replacement
								 * algorithm */
	HyperLogLogState wss_estimation; /* estimation of working set size */
	uint32		bitmap[FLEXIBLE_ARRAY_MEMBER];
} FileCacheControl;

static HTAB *lfc_hash;
static int	lfc_desc = 0;
static LWLockId lfc_lock;
static int	lfc_max_size;
static int	lfc_size_limit;
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

	elog(WARNING, "Failed to %s local file cache at %s: %m, disabling local file cache", op, lfc_path);

	/* Invalidate hash */
	LWLockAcquire(lfc_lock, LW_EXCLUSIVE);

	if (LFC_ENABLED())
	{
		HASH_SEQ_STATUS status;
		FileCacheEntry *entry;
		size_t n_chunks = SIZE_MB_TO_CHUNKS(lfc_max_size);

		hash_seq_init(&status, lfc_hash);
		while ((entry = hash_seq_search(&status)) != NULL)
		{
			hash_search_with_hash_value(lfc_hash, &entry->key, entry->hash, HASH_REMOVE, NULL);
		}
		memset(lfc_ctl->bitmap, 0, (n_chunks+31)/32*4);
		lfc_ctl->generation += 1;
		lfc_ctl->size = 0;
		lfc_ctl->used = 0;
		lfc_ctl->limit = 0;
		lfc_ctl->n_holes = 0;
		lfc_ctl->first_hole = 0;
		dlist_init(&lfc_ctl->lru);

		if (lfc_desc > 0)
		{
			/*
			 * If the reason of error is ENOSPC, then truncation of file may
			 * help to reclaim some space
			 */
			int			rc = ftruncate(lfc_desc, 0);

			if (rc < 0)
				elog(WARNING, "Failed to truncate local file cache %s: %m", lfc_path);
		}
	}

	/*
	 * We need to use unlink to to avoid races in LFC write, because it is not
	 * protectedby
	 */
	unlink(lfc_path);

	fd = BasicOpenFile(lfc_path, O_RDWR | O_CREAT | O_TRUNC);
	if (fd < 0)
		elog(WARNING, "Failed to recreate local file cache %s: %m", lfc_path);
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

static size_t
lfc_ctl_sizeof(void)
{
	size_t n_chunks = SIZE_MB_TO_CHUNKS(lfc_max_size);
	return offsetof(FileCacheControl, bitmap) + (n_chunks+31)/32*4;
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

	lfc_ctl = (FileCacheControl *) ShmemInitStruct("lfc", lfc_ctl_sizeof(), &found);
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
		memset(lfc_ctl->bitmap, 0, (n_chunks+31)/32*4);
		lfc_ctl->generation = 0;
		lfc_ctl->size = 0;
		lfc_ctl->used = 0;
		lfc_ctl->hits = 0;
		lfc_ctl->misses = 0;
		lfc_ctl->writes = 0;
		lfc_ctl->n_holes = 0;
		lfc_ctl->first_hole = 0;
		dlist_init(&lfc_ctl->lru);

		/* Initialize hyper-log-log structure for estimating working set size */
		initSHLL(&lfc_ctl->wss_estimation);

		/* Recreate file cache on restart */
		fd = BasicOpenFile(lfc_path, O_RDWR | O_CREAT | O_TRUNC);
		if (fd < 0)
		{
			elog(WARNING, "Failed to create local file cache %s: %m", lfc_path);
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

	RequestAddinShmemSpace(lfc_ctl_sizeof() + hash_estimate_size(SIZE_MB_TO_CHUNKS(lfc_max_size) + 1, sizeof(FileCacheEntry)));
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
		elog(ERROR, "neon.file_cache_size_limit can not be larger than neon.max_file_cache_size");
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
		FileCacheEntry *victim = dlist_container(FileCacheEntry, lru_node, dlist_pop_head_node(&lfc_ctl->lru));

		Assert(victim->access_count == 0);
#ifdef FALLOC_FL_PUNCH_HOLE
		if (fallocate(lfc_desc, FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE, (off_t) victim->offset * BLOCKS_PER_CHUNK * BLCKSZ, BLOCKS_PER_CHUNK * BLCKSZ) < 0)
			neon_log(LOG, "Failed to punch hole in file: %m");
#endif
		lfc_ctl->bitmap[victim->offset >> 5] |= 1 << (victim->offset & 31);
		if (lfc_ctl->n_holes == 0 || victim->offset < lfc_ctl->first_hole)
			lfc_ctl->first_hole = victim->offset;
		lfc_ctl->n_holes += 1;
		hash_search_with_hash_value(lfc_hash, &victim->key, victim->hash, HASH_REMOVE, NULL);
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

	/*
	 * If the chunk has no live entries, we can position the chunk to be
	 * recycled first.
	 */
	if (entry->bitmap[chunk_offs >> 5] == 0)
	{
		bool		has_remaining_pages;

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
			dlist_delete(&entry->lru_node);
			dlist_push_head(&lfc_ctl->lru, &entry->lru_node);
		}
	}

	/*
	 * Done: apart from empty chunks, we don't move chunks in the LRU when
	 * they're empty because eviction isn't usage.
	 */

	LWLockRelease(lfc_lock);
}

/*
 * Try to read page from local cache.
 * Returns true if page is found in local cache.
 * In case of error local file cache is disabled (lfc->limit is set to zero).
 */
bool
lfc_read(NRelFileInfo rinfo, ForkNumber forkNum, BlockNumber blkno,
		 char *buffer)
{
	BufferTag	tag;
	FileCacheEntry *entry;
	ssize_t		rc;
	int			chunk_offs = blkno & (BLOCKS_PER_CHUNK - 1);
	bool		result = true;
	uint32		hash;
	uint64		generation;
	uint32		entry_offset;

	if (lfc_maybe_disabled())	/* fast exit if file cache is disabled */
		return false;

	if (!lfc_ensure_opened())
		return false;

	CopyNRelFileInfoToBufTag(tag, rinfo);
	tag.forkNum = forkNum;
	tag.blockNum = blkno & ~(BLOCKS_PER_CHUNK - 1);
	hash = get_hash_value(lfc_hash, &tag);

	LWLockAcquire(lfc_lock, LW_EXCLUSIVE);

	if (!LFC_ENABLED())
	{
		LWLockRelease(lfc_lock);
		return false;
	}

	entry = hash_search_with_hash_value(lfc_hash, &tag, hash, HASH_FIND, NULL);

	/* Approximate working set */
	tag.blockNum = blkno;
	addSHLL(&lfc_ctl->wss_estimation, hash_bytes((uint8_t const*)&tag, sizeof(tag)));

	if (entry == NULL || (entry->bitmap[chunk_offs >> 5] & (1 << (chunk_offs & 31))) == 0)
	{
		/* Page is not cached */
		lfc_ctl->misses += 1;
		pgBufferUsage.file_cache.misses += 1;
		LWLockRelease(lfc_lock);
		return false;
	}
	/* Unlink entry from LRU list to pin it for the duration of IO operation */
	if (entry->access_count++ == 0)
		dlist_delete(&entry->lru_node);
	generation = lfc_ctl->generation;
	entry_offset = entry->offset;

	LWLockRelease(lfc_lock);

	rc = pread(lfc_desc, buffer, BLCKSZ, ((off_t) entry_offset * BLOCKS_PER_CHUNK + chunk_offs) * BLCKSZ);
	if (rc != BLCKSZ)
	{
		lfc_disable("read");
		return false;
	}

	/* Place entry to the head of LRU list */
	LWLockAcquire(lfc_lock, LW_EXCLUSIVE);

	if (lfc_ctl->generation == generation)
	{
		Assert(LFC_ENABLED());
		lfc_ctl->hits += 1;
		pgBufferUsage.file_cache.hits += 1;
		Assert(entry->access_count > 0);
		if (--entry->access_count == 0)
			dlist_push_tail(&lfc_ctl->lru, &entry->lru_node);
	}
	else
		result = false;

	LWLockRelease(lfc_lock);

	return result;
}

/*
 * Put page in local file cache.
 * If cache is full then evict some other page.
 */
void
#if PG_MAJORVERSION_NUM < 16
lfc_write(NRelFileInfo rinfo, ForkNumber forkNum, BlockNumber blkno, char *buffer)
#else
lfc_write(NRelFileInfo rinfo, ForkNumber forkNum, BlockNumber blkno, const void *buffer)
#endif
{
	BufferTag	tag;
	FileCacheEntry *entry;
	ssize_t		rc;
	bool		found;
	int			chunk_offs = blkno & (BLOCKS_PER_CHUNK - 1);
	uint32		hash;
	uint64		generation;
	uint32		entry_offset;

	if (lfc_maybe_disabled())	/* fast exit if file cache is disabled */
		return;

	if (!lfc_ensure_opened())
		return;

	tag.forkNum = forkNum;
	tag.blockNum = blkno & ~(BLOCKS_PER_CHUNK - 1);
	CopyNRelFileInfoToBufTag(tag, rinfo);
	hash = get_hash_value(lfc_hash, &tag);

	LWLockAcquire(lfc_lock, LW_EXCLUSIVE);

	if (!LFC_ENABLED())
	{
		LWLockRelease(lfc_lock);
		return;
	}

	entry = hash_search_with_hash_value(lfc_hash, &tag, hash, HASH_ENTER, &found);

	if (found)
	{
		/*
		 * Unlink entry from LRU list to pin it for the duration of IO
		 * operation
		 */
		if (entry->access_count++ == 0)
			dlist_delete(&entry->lru_node);
	}
	else
	{
		/*
		 * We have two choices if all cache pages are pinned (i.e. used in IO
		 * operations):
		 *
		 * 1) Wait until some of this operation is completed and pages is
		 * unpinned.
		 *
		 * 2) Allocate one more chunk, so that specified cache size is more
		 * recommendation than hard limit.
		 *
		 * As far as probability of such event (that all pages are pinned) is
		 * considered to be very very small: there are should be very large
		 * number of concurrent IO operations and them are limited by
		 * max_connections, we prefer not to complicate code and use second
		 * approach.
		 */
		if (lfc_ctl->used >= lfc_ctl->limit && !dlist_is_empty(&lfc_ctl->lru))
		{
			/* Cache overflow: evict least recently used chunk */
			FileCacheEntry *victim = dlist_container(FileCacheEntry, lru_node, dlist_pop_head_node(&lfc_ctl->lru));

			Assert(victim->access_count == 0);
			entry->offset = victim->offset; /* grab victim's chunk */
			hash_search_with_hash_value(lfc_hash, &victim->key, victim->hash, HASH_REMOVE, NULL);
			neon_log(DEBUG2, "Swap file cache page");
		}
		else
		{
			lfc_ctl->used += 1;
			if (lfc_ctl->n_holes != 0)
			{
				/* Reuse one of holes */
				size_t i = lfc_ctl->first_hole;
				entry->offset = i;
				lfc_ctl->bitmap[i >> 5] &= ~(1 << (i & 31));
				if (--lfc_ctl->n_holes != 0)
				{
					do {
						i += 1;
					} while (!(lfc_ctl->bitmap[i >> 5] & (1 << (i & 31))));
					lfc_ctl->first_hole = i;
				}
			}
			else
				entry->offset = lfc_ctl->size++;	/* allocate new chunk at end
													 * of file */
		}
		entry->access_count = 1;
		entry->hash = hash;
		memset(entry->bitmap, 0, sizeof entry->bitmap);
	}

	generation = lfc_ctl->generation;
	entry_offset = entry->offset;
	lfc_ctl->writes += 1;
	LWLockRelease(lfc_lock);

	rc = pwrite(lfc_desc, buffer, BLCKSZ, ((off_t) entry_offset * BLOCKS_PER_CHUNK + chunk_offs) * BLCKSZ);
	if (rc != BLCKSZ)
	{
		lfc_disable("write");
	}
	else
	{
		LWLockAcquire(lfc_lock, LW_EXCLUSIVE);

		if (lfc_ctl->generation == generation)
		{
			Assert(LFC_ENABLED());
			/* Place entry to the head of LRU list */
			Assert(entry->access_count > 0);
			if (--entry->access_count == 0)
				dlist_push_tail(&lfc_ctl->lru, &entry->lru_node);

			entry->bitmap[chunk_offs >> 5] |= (1 << (chunk_offs & 31));
		}

		LWLockRelease(lfc_lock);
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
	uint64		value;
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
			key = "file_cache_holes";
			if (lfc_ctl)
				value = lfc_ctl->n_holes;
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

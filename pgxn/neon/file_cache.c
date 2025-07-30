/*-------------------------------------------------------------------------
 *
 * file_cache.c
 *
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
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
#include "common/hashfn.h"
#include "pgstat.h"
#include "port/pg_iovec.h"
#include "postmaster/bgworker.h"
#include "postmaster/interrupt.h"
#include RELFILEINFO_HDR
#include "storage/buf_internals.h"
#include "storage/fd.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/lwlock.h"
#include "storage/pg_shmem.h"
#include "storage/procsignal.h"
#include "tcop/tcopprot.h"
#include "utils/builtins.h"
#include "utils/dynahash.h"
#include "utils/guc.h"

#if PG_VERSION_NUM >= 150000
#include "access/xlogrecovery.h"
#endif

#include "hll.h"
#include "bitmap.h"
#include "file_cache.h"
#include "neon.h"
#include "neon_lwlsncache.h"
#include "neon_perf_counters.h"
#include "neon_utils.h"
#include "pagestore_client.h"
#include "communicator.h"

#include "communicator/communicator_bindings.h"

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

/* Local file storage allocation chunk.
 * Should be power of two. Using larger than page chunks can
 * 1. Reduce hash-map memory footprint: 8TB database contains billion pages
 *    and size of hash entry is 40 bytes, so we need 40Gb just for hash map.
 *    1Mb chunks can reduce hash map size to 320Mb.
 * 2. Improve access locality, subsequent pages will be allocated together improving seqscan speed
 */
#define MAX_BLOCKS_PER_CHUNK_LOG  7 /* 1Mb chunk */
#define MAX_BLOCKS_PER_CHUNK	  (1 << MAX_BLOCKS_PER_CHUNK_LOG)

#define MB					((uint64)1024*1024)

#define SIZE_MB_TO_CHUNKS(size) ((uint32)((size) * MB / BLCKSZ >> lfc_chunk_size_log))
#define BLOCK_TO_CHUNK_OFF(blkno) ((blkno) & (lfc_blocks_per_chunk-1))

/*
 * Blocks are read or written to LFC file outside LFC critical section.
 * To synchronize access to such block, writer set state of such block to PENDING.
 * If some other backend (read or writer) see PENDING status, it change it to REQUESTED and start
 * waiting until status is changed on conditional variable.
 * When writer completes is operation, it checks if status is REQUESTED and if so, broadcast conditional variable,
 * waking up all backend waiting for access to this block.
 */
typedef enum FileCacheBlockState
{
	UNAVAILABLE, /* block is not present in cache */
	AVAILABLE,   /* block can be used */
	PENDING,     /* block is loaded */
	REQUESTED    /* some other backend is waiting for block to be loaded */
} FileCacheBlockState;


typedef struct FileCacheEntry
{
	BufferTag	key;
	uint32		hash;
	uint32		offset;
	uint32		access_count;
	dlist_node	list_node;		/* LRU/holes list node */
	uint32		state[FLEXIBLE_ARRAY_MEMBER]; /* two bits per block */
} FileCacheEntry;

#define FILE_CACHE_ENRTY_SIZE MAXALIGN(offsetof(FileCacheEntry, state) + (lfc_blocks_per_chunk*2+31)/32*4)
#define GET_STATE(entry, i) (((entry)->state[(i) / 16] >> ((i) % 16 * 2)) & 3)
#define SET_STATE(entry, i, new_state) (entry)->state[(i) / 16] = ((entry)->state[(i) / 16] & ~(3 << ((i) % 16 * 2))) | ((new_state) << ((i) % 16 * 2))

#define N_COND_VARS 	64
#define CV_WAIT_TIMEOUT	10

#define MAX_PREWARM_WORKERS 8

typedef struct PrewarmWorkerState
{
	uint32		prewarmed_pages;
	uint32		skipped_pages;
	TimestampTz completed;
} PrewarmWorkerState;

typedef struct FileCacheControl
{
	uint64		generation;		/* generation is needed to handle correct hash
								 * reenabling */
	uint32		size;			/* size of cache file in chunks */
	uint32		used;			/* number of used chunks */
	uint32		used_pages;		/* number of used pages */
	uint32		pinned;			/* number of pinned chunks */
	uint32		limit;			/* shared copy of lfc_size_limit */
	uint64		hits;
	uint64		misses;
	uint64		writes;			/* number of writes issued */
	uint64		time_read;		/* time spent reading (us) */
	uint64		time_write;		/* time spent writing (us) */
	uint64		resizes;        /* number of LFC resizes   */
	uint64		evicted_pages;	/* number of evicted pages */
	dlist_head	lru;			/* double linked list for LRU replacement
								 * algorithm */
	dlist_head  holes;          /* double linked list of punched holes */

	ConditionVariable cv[N_COND_VARS]; /* turnstile of condition variables */

	/*
	 * Estimation of working set size.
	 *
	 * This is not guarded by the lock. No locking is needed because all the
	 * writes to the "registers" are simple 64-bit stores, to update a
	 * timestamp. We assume that:
	 *
	 * - 64-bit stores are atomic. We could enforce that by using
	 *   pg_atomic_uint64 instead of TimestampTz as the datatype in hll.h, but
	 *   for now we just rely on it implicitly.
	 *
	 * - Even if they're not, and there is a race between two stores, it
	 *   doesn't matter much which one wins because they're both updating the
	 *   register with the current timestamp. Or you have a race between
	 *   resetting the register and updating it, in which case it also doesn't
	 *   matter much which one wins.
	 *
	 * - If they're not atomic, you might get an occasional "torn write" if
	 *   you're really unlucky, but we tolerate that too. It just means that
	 *   the estimate will be a little off, until the register is updated
	 *   again.
	 */
	HyperLogLogState wss_estimation;

	/* Prewarmer state */
	PrewarmWorkerState prewarm_workers[MAX_PREWARM_WORKERS];
	size_t n_prewarm_workers;
	size_t n_prewarm_entries;
	size_t total_prewarm_pages;
	size_t prewarm_batch;
	bool   prewarm_active;
	bool   prewarm_canceled;
	dsm_handle prewarm_lfc_state_handle;
} FileCacheControl;

#define FILE_CACHE_STATE_MAGIC 0xfcfcfcfc

#define FILE_CACHE_STATE_BITMAP(fcs)	((uint8*)&(fcs)->chunks[(fcs)->n_chunks])
#define FILE_CACHE_STATE_SIZE_FOR_CHUNKS(n_chunks)	(sizeof(FileCacheState) + (n_chunks)*sizeof(BufferTag) + (((n_chunks) * lfc_blocks_per_chunk)+7)/8)
#define FILE_CACHE_STATE_SIZE(fcs)		(sizeof(FileCacheState) + (fcs->n_chunks)*sizeof(BufferTag) + (((fcs->n_chunks) << fcs->chunk_size_log)+7)/8)

static HTAB *lfc_hash;
static int	lfc_desc = -1;
static LWLockId lfc_lock;
static int	lfc_max_size;
static int	lfc_size_limit;
static int	lfc_prewarm_limit;
static int	lfc_prewarm_batch;
static int	lfc_chunk_size_log = MAX_BLOCKS_PER_CHUNK_LOG;
static int	lfc_blocks_per_chunk = MAX_BLOCKS_PER_CHUNK;
static char *lfc_path;
static uint64 lfc_generation;
static FileCacheControl *lfc_ctl;
static bool lfc_do_prewarm;

bool lfc_store_prefetch_result;
bool lfc_prewarm_update_ws_estimation;

bool AmPrewarmWorker;

#define LFC_ENABLED() (lfc_ctl->limit != 0)

PGDLLEXPORT void lfc_prewarm_main(Datum main_arg);

/*
 * Close LFC file if opened.
 * All backends should close their LFC files once LFC is disabled.
 */
static void
lfc_close_file(void)
{
	if (lfc_desc >= 0)
	{
		close(lfc_desc);
		lfc_desc = -1;
	}
}

/*
 * Local file cache is optional and Neon can work without it.
 * In case of any any errors with this cache, we should disable it but to not throw error.
 * Also we should allow  re-enable it if source of failure (lack of disk space, permissions,...) is fixed.
 * All cache content should be invalidated to avoid reading of stale or corrupted data
 */
static void
lfc_switch_off(void)
{
	int			fd;

	if (LFC_ENABLED())
	{
		HASH_SEQ_STATUS status;
		FileCacheEntry *entry;

		/* Invalidate hash */
		hash_seq_init(&status, lfc_hash);
		while ((entry = hash_seq_search(&status)) != NULL)
		{
			hash_search_with_hash_value(lfc_hash, &entry->key, entry->hash, HASH_REMOVE, NULL);
		}
		lfc_ctl->generation += 1;
		lfc_ctl->size = 0;
		lfc_ctl->pinned = 0;
		lfc_ctl->used = 0;
		lfc_ctl->used_pages = 0;
		lfc_ctl->limit = 0;
		dlist_init(&lfc_ctl->lru);
		dlist_init(&lfc_ctl->holes);

		/*
		 * We need to use unlink to to avoid races in LFC write, because it is not
		 * protected by lock
		 */
		unlink(lfc_path);

		fd = BasicOpenFile(lfc_path, O_RDWR | O_CREAT | O_TRUNC);
		if (fd < 0)
			elog(WARNING, "LFC: failed to recreate local file cache %s: %m", lfc_path);
		else
			close(fd);

		/* Wakeup waiting backends */
		for (int i = 0; i < N_COND_VARS; i++)
			ConditionVariableBroadcast(&lfc_ctl->cv[i]);
	}
	lfc_close_file();
}

static void
lfc_disable(char const *op)
{
	elog(WARNING, "LFC: failed to %s local file cache at %s: %m, disabling local file cache", op, lfc_path);

	LWLockAcquire(lfc_lock, LW_EXCLUSIVE);
	lfc_switch_off();
	LWLockRelease(lfc_lock);
}

/*
 * This check is done without obtaining lfc_lock, so it is unreliable
 */
static bool
lfc_maybe_disabled(void)
{
	return !lfc_ctl || !LFC_ENABLED();
}

/*
 * Open LFC file if not opened yet or generation is changed.
 * Should be called under LFC lock.
 */
static bool
lfc_ensure_opened(void)
{
	if (lfc_generation != lfc_ctl->generation)
	{
		lfc_close_file();
		lfc_generation = lfc_ctl->generation;
	}
	/* Open cache file if not done yet */
	if (lfc_desc < 0)
	{
		lfc_desc = BasicOpenFile(lfc_path, O_RDWR);

		if (lfc_desc < 0)
		{
			lfc_disable("open");
			return false;
		}
	}
	return true;
}

void
LfcShmemInit(void)
{
	bool		found;
	static HASHCTL info;

	if (lfc_max_size <= 0)
		return;

	lfc_ctl = (FileCacheControl *) ShmemInitStruct("lfc", sizeof(FileCacheControl), &found);
	if (!found)
	{
		int			fd;
		uint32		n_chunks = SIZE_MB_TO_CHUNKS(lfc_max_size);

		lfc_lock = (LWLockId) GetNamedLWLockTranche("lfc_lock");
		info.keysize = sizeof(BufferTag);
		info.entrysize = FILE_CACHE_ENRTY_SIZE;

		/*
		 * n_chunks+1 because we add new element to hash table before eviction
		 * of victim
		 */
		lfc_hash = ShmemInitHash("lfc_hash",
								 n_chunks + 1, n_chunks + 1,
								 &info,
								 HASH_ELEM | HASH_BLOBS);
		memset(lfc_ctl, 0, sizeof(FileCacheControl));
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

		/* Initialize turnstile of condition variables */
		for (int i = 0; i < N_COND_VARS; i++)
			ConditionVariableInit(&lfc_ctl->cv[i]);

	}
}

void
LfcShmemRequest(void)
{
	if (lfc_max_size > 0)
	{
		RequestAddinShmemSpace(sizeof(FileCacheControl) + hash_estimate_size(SIZE_MB_TO_CHUNKS(lfc_max_size) + 1, FILE_CACHE_ENRTY_SIZE));
		RequestNamedLWLockTranche("lfc_lock", 1);
	}
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
lfc_check_chunk_size(int *newval, void **extra, GucSource source)
{
	if (*newval & (*newval - 1))
	{
		elog(ERROR, "LFC chunk size should be power of two");
		return false;
	}
	return true;
}

static void
lfc_change_chunk_size(int newval, void* extra)
{
	lfc_chunk_size_log = pg_ceil_log2_32(newval);
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

	if (!lfc_ctl || !is_normal_backend())
		return;

	LWLockAcquire(lfc_lock, LW_EXCLUSIVE);

	/* Open LFC file only if LFC was enabled or we are going to reenable it */
	if (newval == 0 && !LFC_ENABLED())
	{
		LWLockRelease(lfc_lock);
		/* File should be reopened if LFC is reenabled */
		lfc_close_file();
		return;
	}

	if (!lfc_ensure_opened())
	{
		LWLockRelease(lfc_lock);
		return;
	}

	if (lfc_ctl->limit != new_size)
	{
		lfc_ctl->resizes += 1;
	}

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
		if (fallocate(lfc_desc, FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE, (off_t) victim->offset * lfc_blocks_per_chunk * BLCKSZ, lfc_blocks_per_chunk * BLCKSZ) < 0)
			neon_log(LOG, "Failed to punch hole in file: %m");
#endif
		/* We remove the old entry, and re-enter a hole to the hash table */
		for (int i = 0; i < lfc_blocks_per_chunk; i++)
		{
			bool is_page_cached = GET_STATE(victim, i) == AVAILABLE;
			lfc_ctl->used_pages -= is_page_cached;
			lfc_ctl->evicted_pages += is_page_cached;
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
	if (new_size == 0)
		lfc_switch_off();
	else
		lfc_ctl->limit = new_size;

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


	DefineCustomBoolVariable("neon.store_prefetch_result_in_lfc",
							"Immediately store received prefetch result in LFC",
							NULL,
							&lfc_store_prefetch_result,
							false,
							PGC_SUSET,
							0,
							NULL,
							NULL,
							NULL);

	DefineCustomBoolVariable("neon.prewarm_update_ws_estimation",
							"Consider prewarmed pages for working set estimation",
							NULL,
							&lfc_prewarm_update_ws_estimation,
							true,
							PGC_SUSET,
							0,
							NULL,
							NULL,
							NULL);

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

	DefineCustomIntVariable("neon.file_cache_chunk_size",
							"LFC chunk size in blocks (should be power of two)",
							NULL,
							&lfc_blocks_per_chunk,
							MAX_BLOCKS_PER_CHUNK,
							1,
							MAX_BLOCKS_PER_CHUNK,
							PGC_POSTMASTER,
							GUC_UNIT_BLOCKS,
							lfc_check_chunk_size,
							lfc_change_chunk_size,
							NULL);

	DefineCustomIntVariable("neon.file_cache_prewarm_limit",
							"Maximal number of prewarmed chunks",
							NULL,
							&lfc_prewarm_limit,
							INT_MAX,	/* no limit by default */
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
}

/*
 * Dump a list of pages that are currently in the LFC
 *
 * This is used to get a snapshot that can be used to prewarm the LFC later.
 */
FileCacheState*
lfc_get_state(size_t max_entries)
{
	FileCacheState* fcs = NULL;

	if (lfc_maybe_disabled() || max_entries == 0)	/* fast exit if file cache is disabled */
		return NULL;

	LWLockAcquire(lfc_lock, LW_SHARED);

	if (LFC_ENABLED())
	{
		dlist_iter iter;
		size_t i = 0;
		uint8* bitmap;
		size_t n_pages = 0;
		size_t n_entries = Min(max_entries, lfc_ctl->used - lfc_ctl->pinned);
		size_t state_size = FILE_CACHE_STATE_SIZE_FOR_CHUNKS(n_entries);
		fcs = (FileCacheState*)palloc0(state_size);
		SET_VARSIZE(fcs, state_size);
		fcs->magic = FILE_CACHE_STATE_MAGIC;
		fcs->chunk_size_log = lfc_chunk_size_log;
		fcs->n_chunks = n_entries;
		bitmap = FILE_CACHE_STATE_BITMAP(fcs);

		dlist_reverse_foreach(iter, &lfc_ctl->lru)
		{
			FileCacheEntry *entry = dlist_container(FileCacheEntry, list_node, iter.cur);
			fcs->chunks[i] = entry->key;
			for (int j = 0; j < lfc_blocks_per_chunk; j++)
			{
				if (GET_STATE(entry, j) != UNAVAILABLE)
				{
					/* Validate the buffer tag before including it */
					BufferTag test_tag = entry->key;
					test_tag.blockNum += j;

					if (BufferTagIsValid(&test_tag))
					{
						BITMAP_SET(bitmap, i*lfc_blocks_per_chunk + j);
						n_pages += 1;
					}
					else
					{
						elog(ERROR, "LFC: Skipping invalid buffer tag during cache state capture: blockNum=%u", test_tag.blockNum);
					}
				}
			}
			if (++i == n_entries)
				break;
		}
		Assert(i == n_entries);
		fcs->n_pages = n_pages;
		Assert(pg_popcount((char*)bitmap, ((n_entries << lfc_chunk_size_log) + 7)/8) == n_pages);
		elog(LOG, "LFC: save state of %d chunks %d pages (validated)", (int)n_entries, (int)n_pages);
	}

	LWLockRelease(lfc_lock);

	return fcs;
}

/*
 * Prewarm LFC cache to the specified state. It uses lfc_prefetch function to load prewarmed page without hoilding shared buffer lock
 * and avoid race conditions with other backends.
 */
void
lfc_prewarm(FileCacheState* fcs, uint32 n_workers)
{
	size_t fcs_chunk_size_log;
	size_t n_entries;
	size_t prewarm_batch = Min(lfc_prewarm_batch, readahead_buffer_size);
	size_t fcs_size;
	uint32_t max_prefetch_pages;
	dsm_segment *seg;
	BackgroundWorkerHandle* bgw_handle[MAX_PREWARM_WORKERS];


	if (!lfc_ensure_opened())
		return;

	if (prewarm_batch == 0 || lfc_prewarm_limit == 0 || n_workers == 0)
	{
		elog(LOG, "LFC: prewarm is disabled");
		return;
	}

	if (n_workers > MAX_PREWARM_WORKERS)
	{
		elog(ERROR, "LFC: Too much prewarm workers, maximum is %d", MAX_PREWARM_WORKERS);
	}

	if (fcs == NULL || fcs->n_chunks == 0)
	{
		elog(LOG, "LFC: nothing to prewarm");
		return;
	}

	if (fcs->magic != FILE_CACHE_STATE_MAGIC)
	{
		elog(ERROR, "LFC: Invalid file cache state magic: %X", fcs->magic);
	}

	fcs_size = VARSIZE(fcs);
	if (FILE_CACHE_STATE_SIZE(fcs) != fcs_size)
	{
		elog(ERROR, "LFC: Invalid file cache state size: %u vs. %u", (unsigned)FILE_CACHE_STATE_SIZE(fcs), VARSIZE(fcs));
	}

	fcs_chunk_size_log = fcs->chunk_size_log;
	if (fcs_chunk_size_log > MAX_BLOCKS_PER_CHUNK_LOG)
	{
		elog(ERROR, "LFC: Invalid chunk size log: %u", fcs->chunk_size_log);
	}

	n_entries = Min(fcs->n_chunks, lfc_prewarm_limit);
	Assert(n_entries != 0);

	max_prefetch_pages = n_entries << fcs_chunk_size_log;
	if (fcs->n_pages > max_prefetch_pages) {
		elog(ERROR, "LFC: Number of pages in file cache state (%d) is more than the limit (%d)", fcs->n_pages, max_prefetch_pages);
	}

	LWLockAcquire(lfc_lock, LW_EXCLUSIVE);

	/* Do not prewarm more entries than LFC limit */
	if (lfc_ctl->limit <= lfc_ctl->size)
	{
		elog(LOG, "LFC: skip prewarm because LFC is already filled");
		LWLockRelease(lfc_lock);
		return;
	}

	if (lfc_ctl->prewarm_active)
	{
		LWLockRelease(lfc_lock);
		elog(ERROR, "LFC: skip prewarm because another prewarm is still active");
	}
	lfc_ctl->n_prewarm_entries = n_entries;
	lfc_ctl->n_prewarm_workers = n_workers;
	lfc_ctl->prewarm_active = true;
	lfc_ctl->prewarm_canceled = false;
	lfc_ctl->prewarm_batch = prewarm_batch;
	memset(lfc_ctl->prewarm_workers, 0, n_workers*sizeof(PrewarmWorkerState));

	LWLockRelease(lfc_lock);

	/* Calculate total number of pages to be prewarmed */
	lfc_ctl->total_prewarm_pages = fcs->n_pages;

	seg = dsm_create(fcs_size, 0);
	memcpy(dsm_segment_address(seg), fcs, fcs_size);
	lfc_ctl->prewarm_lfc_state_handle = dsm_segment_handle(seg);

	/* Spawn background workers */
	for (uint32 i = 0; i < n_workers; i++)
	{
		BackgroundWorker worker = {0};

		worker.bgw_flags = BGWORKER_SHMEM_ACCESS;
		worker.bgw_start_time = BgWorkerStart_ConsistentState;
		worker.bgw_restart_time = BGW_NEVER_RESTART;
		strcpy(worker.bgw_library_name, "neon");
		strcpy(worker.bgw_function_name, "lfc_prewarm_main");
		snprintf(worker.bgw_name, BGW_MAXLEN, "LFC prewarm worker %d", i+1);
		strcpy(worker.bgw_type, "LFC prewarm worker");
		worker.bgw_main_arg = Int32GetDatum(i);
		/* must set notify PID to wait for shutdown */
		worker.bgw_notify_pid = MyProcPid;

		if (!RegisterDynamicBackgroundWorker(&worker, &bgw_handle[i]))
		{
			ereport(LOG,
					(errcode(ERRCODE_INSUFFICIENT_RESOURCES),
					 errmsg("LFC: registering dynamic bgworker prewarm failed"),
					 errhint("Consider increasing the configuration parameter \"%s\".", "max_worker_processes")));
			n_workers = i;
			lfc_ctl->prewarm_canceled = true;
			break;
		}
	}

	for (uint32 i = 0; i < n_workers; i++)
	{
		bool interrupted;
		do
		{
			interrupted = false;
			PG_TRY();
			{
				BgwHandleStatus status = WaitForBackgroundWorkerShutdown(bgw_handle[i]);
				if (status != BGWH_STOPPED && status != BGWH_POSTMASTER_DIED)
				{
					elog(LOG, "LFC: Unexpected status of prewarm worker termination: %d", status);
				}
			}
			PG_CATCH();
			{
				elog(LOG, "LFC: cancel prewarm");
				lfc_ctl->prewarm_canceled = true;
				interrupted = true;
			}
			PG_END_TRY();
		} while (interrupted);

		if (!lfc_ctl->prewarm_workers[i].completed)
		{
			/* Background worker doesn't set completion time: it means that it was abnormally terminated */
			elog(LOG, "LFC: prewarm worker %d failed", i+1);
			/* Set completion time to prevent get_prewarm_info from considering this worker as active */
			lfc_ctl->prewarm_workers[i].completed = GetCurrentTimestamp();
		}
	}
	dsm_detach(seg);

	LWLockAcquire(lfc_lock, LW_EXCLUSIVE);
	lfc_ctl->prewarm_active = false;
	LWLockRelease(lfc_lock);
}

void
lfc_prewarm_main(Datum main_arg)
{
	size_t snd_idx = 0, rcv_idx = 0;
	size_t n_sent = 0, n_received = 0;
	size_t fcs_chunk_size_log;
	size_t max_prefetch_pages;
	size_t prewarm_batch;
	size_t n_workers;
	dsm_segment *seg;
	FileCacheState* fcs;
	uint8* bitmap;
	BufferTag tag;
	PrewarmWorkerState* ws;
	uint32 worker_id = DatumGetInt32(main_arg);

	AmPrewarmWorker = true;

	pqsignal(SIGTERM, die);
	BackgroundWorkerUnblockSignals();

	seg = dsm_attach(lfc_ctl->prewarm_lfc_state_handle);
	if (seg == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("could not map dynamic shared memory segment")));

	fcs = (FileCacheState*) dsm_segment_address(seg);
	prewarm_batch = lfc_ctl->prewarm_batch;
	fcs_chunk_size_log = fcs->chunk_size_log;
	n_workers = lfc_ctl->n_prewarm_workers;
	max_prefetch_pages = lfc_ctl->n_prewarm_entries << fcs_chunk_size_log;
	ws = &lfc_ctl->prewarm_workers[worker_id];
	bitmap = FILE_CACHE_STATE_BITMAP(fcs);

	/* enable prefetch in LFC */
	lfc_store_prefetch_result = true;
	lfc_do_prewarm = true; /* Flag for lfc_prefetch preventing replacement of existed entries if LFC cache is full */

	elog(LOG, "LFC: worker %d start prewarming", worker_id);
	while (!lfc_ctl->prewarm_canceled)
	{
		if (snd_idx < max_prefetch_pages)
		{
			if ((snd_idx >> fcs_chunk_size_log) % n_workers != worker_id)
			{
				/* If there are multiple workers, split chunks between them */
				snd_idx += 1 << fcs_chunk_size_log;
			}
			else
			{
				if (BITMAP_ISSET(bitmap, snd_idx))
				{
					tag = fcs->chunks[snd_idx >> fcs_chunk_size_log];
					tag.blockNum += snd_idx & ((1 << fcs_chunk_size_log) - 1);

					if (!BufferTagIsValid(&tag)) {
						elog(ERROR, "LFC: Invalid buffer tag: %u", tag.blockNum);
					}

					if (!lfc_cache_contains(BufTagGetNRelFileInfo(tag), tag.forkNum, tag.blockNum))
					{
						(void)communicator_prefetch_register_bufferv(tag, NULL, 1, NULL);
						n_sent += 1;
					}
					else
					{
						ws->skipped_pages += 1;
						BITMAP_CLR(bitmap, snd_idx);
					}
				}
				snd_idx += 1;
			}
		}
		if (n_sent >= n_received + prewarm_batch || snd_idx == max_prefetch_pages)
		{
			if (n_received == n_sent && snd_idx == max_prefetch_pages)
			{
				break;
			}
			if ((rcv_idx >> fcs_chunk_size_log) % n_workers != worker_id)
			{
				/* Skip chunks processed by other workers */
				rcv_idx += 1 << fcs_chunk_size_log;
				continue;
			}

			/* Locate next block to prefetch */
			while (!BITMAP_ISSET(bitmap, rcv_idx))
			{
				rcv_idx += 1;
			}
			tag = fcs->chunks[rcv_idx >> fcs_chunk_size_log];
			tag.blockNum += rcv_idx & ((1 << fcs_chunk_size_log) - 1);
			if (communicator_prefetch_receive(tag))
			{
				ws->prewarmed_pages += 1;
			}
			else
			{
				ws->skipped_pages += 1;
			}
			rcv_idx += 1;
			n_received += 1;
		}
	}
	/* No need to perform prefetch cleanup here because prewarm worker will be terminated and
	 * connection to PS dropped just after return from this function.
	 */
	Assert(n_sent == n_received || lfc_ctl->prewarm_canceled);
	elog(LOG, "LFC: worker %d complete prewarming: loaded %ld pages", worker_id, (long)n_received);
	lfc_ctl->prewarm_workers[worker_id].completed = GetCurrentTimestamp();
}

void
lfc_invalidate(NRelFileInfo rinfo, ForkNumber forkNum, BlockNumber nblocks)
{
	BufferTag	tag;
	FileCacheEntry *entry;
	uint32		hash;

	if (lfc_maybe_disabled())	/* fast exit if file cache is disabled */
		return;

	CopyNRelFileInfoToBufTag(tag, rinfo);
	tag.forkNum = forkNum;

	CriticalAssert(BufTagGetRelNumber(&tag) != InvalidRelFileNumber);

	LWLockAcquire(lfc_lock, LW_EXCLUSIVE);
	if (LFC_ENABLED())
	{
		for (BlockNumber blkno = 0; blkno < nblocks; blkno += lfc_blocks_per_chunk)
		{
			tag.blockNum = blkno;
			hash = get_hash_value(lfc_hash, &tag);
			entry = hash_search_with_hash_value(lfc_hash, &tag, hash, HASH_FIND, NULL);
			if (entry != NULL)
			{
				for (int i = 0; i < lfc_blocks_per_chunk; i++)
				{
					if (GET_STATE(entry, i) == AVAILABLE)
					{
						lfc_ctl->used_pages -= 1;
						SET_STATE(entry, i, UNAVAILABLE);
					}
				}
			}
		}
	}
	LWLockRelease(lfc_lock);
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
	int			chunk_offs = BLOCK_TO_CHUNK_OFF(blkno);
	bool		found = false;
	uint32		hash;

	if (lfc_maybe_disabled())	/* fast exit if file cache is disabled */
		return false;

	CopyNRelFileInfoToBufTag(tag, rinfo);
	tag.forkNum = forkNum;
	tag.blockNum = blkno - chunk_offs;

	CriticalAssert(BufTagGetRelNumber(&tag) != InvalidRelFileNumber);
	hash = get_hash_value(lfc_hash, &tag);

	LWLockAcquire(lfc_lock, LW_SHARED);
	if (LFC_ENABLED())
	{
		entry = hash_search_with_hash_value(lfc_hash, &tag, hash, HASH_FIND, NULL);
		found = entry != NULL && GET_STATE(entry, chunk_offs) != UNAVAILABLE;
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

	chunk_offs = BLOCK_TO_CHUNK_OFF(blkno);
	tag.blockNum = blkno - chunk_offs;
	hash = get_hash_value(lfc_hash, &tag);

	LWLockAcquire(lfc_lock, LW_SHARED);

	if (!LFC_ENABLED())
	{
		LWLockRelease(lfc_lock);
		return 0;
	}
	while (true)
	{
		int		this_chunk = Min(nblocks - i, lfc_blocks_per_chunk - chunk_offs);
		entry = hash_search_with_hash_value(lfc_hash, &tag, hash, HASH_FIND, NULL);

		if (entry != NULL)
		{
			for (; chunk_offs < lfc_blocks_per_chunk && i < nblocks; chunk_offs++, i++)
			{
				if (GET_STATE(entry, chunk_offs) != UNAVAILABLE)
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
		/*
		 * Break out of the iteration before doing expensive stuff for
		 * a next iteration
		 */
		if (i >= nblocks)
			break;

		/*
		 * Prepare for the next iteration. We don't unlock here, as that'd
		 * probably be more expensive than the gains it'd get us.
		 */
		chunk_offs = BLOCK_TO_CHUNK_OFF(blkno + i);
		tag.blockNum = (blkno + i) - chunk_offs;
		hash = get_hash_value(lfc_hash, &tag);
	}

	LWLockRelease(lfc_lock);

#ifdef USE_ASSERT_CHECKING
	{
		int count = 0;

		for (int j = 0; j < nblocks; j++)
		{
			if (BITMAP_ISSET(bitmap, j))
				count++;
		}

		Assert(count == found);
	}
#endif

	return found;
}

#if PG_MAJORVERSION_NUM >= 16
static PGIOAlignedBlock voidblock = {0};
#else
static PGAlignedBlock voidblock = {0};
#endif
#define SCRIBBLEPAGE (&voidblock.data)

/*
 * Try to read pages from local cache.
 * Returns the number of pages read from the local cache, and sets bits in
 * 'mask' for the pages which were read. This may scribble over buffers not
 * marked in 'mask', so be careful with operation ordering.
 *
 * In case of error local file cache is disabled (lfc->limit is set to zero),
 * and -1 is returned.
 *
 * If the mask argument is supplied, we'll only try to read those pages which
 * don't have their bits set on entry. At exit, pages which were successfully
 * read from LFC will have their bits set.
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
		return -1;

	CopyNRelFileInfoToBufTag(tag, rinfo);
	tag.forkNum = forkNum;

	CriticalAssert(BufTagGetRelNumber(&tag) != InvalidRelFileNumber);

	/* Update working set size estimate for the blocks */
	for (int i = 0; i < nblocks; i++)
	{
		tag.blockNum = blkno + i;
		addSHLL(&lfc_ctl->wss_estimation, hash_bytes((uint8_t const*)&tag, sizeof(tag)));
	}

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
		uint8	chunk_mask[MAX_BLOCKS_PER_CHUNK / 8] = {0};
		int		chunk_offs = BLOCK_TO_CHUNK_OFF(blkno);
		int		blocks_in_chunk = Min(nblocks, lfc_blocks_per_chunk - chunk_offs);
		int		iteration_hits = 0;
		int		iteration_misses = 0;
		uint64	io_time_us = 0;
		int		n_blocks_to_read = 0;
		int		iov_last_used = 0;
		int		first_block_in_chunk_read = -1;
		ConditionVariable* cv;

		Assert(blocks_in_chunk > 0);

		for (int i = 0; i < blocks_in_chunk; i++)
		{
			iov[i].iov_len = BLCKSZ;
			/* mask not set = we must do work */
			if (!BITMAP_ISSET(mask, buf_offset + i))
			{
				iov[i].iov_base = buffers[buf_offset + i];
				n_blocks_to_read++;
				iov_last_used = i + 1;

				if (first_block_in_chunk_read == -1)
				{
					first_block_in_chunk_read = i;
				}
			}
			/* mask set = we must do no work */
			else
			{
				/* don't scribble on pages we weren't requested to write to */
				iov[i].iov_base = SCRIBBLEPAGE;
			}
		}

		/* shortcut IO */
		if (n_blocks_to_read == 0)
		{
			buf_offset += blocks_in_chunk;
			nblocks -= blocks_in_chunk;
			blkno += blocks_in_chunk;
			continue;
		}

		/*
		 * The effective iov size must be >= the number of blocks we're about
		 * to read.
		 */
		Assert(iov_last_used - first_block_in_chunk_read >= n_blocks_to_read);

		tag.blockNum = blkno - chunk_offs;
		hash = get_hash_value(lfc_hash, &tag);
		cv = &lfc_ctl->cv[hash % N_COND_VARS];

		LWLockAcquire(lfc_lock, LW_EXCLUSIVE);

		/* We can return the blocks we've read before LFC got disabled;
		 * assuming we read any. */
		if (!LFC_ENABLED() || !lfc_ensure_opened())
		{
			LWLockRelease(lfc_lock);
			return blocks_read;
		}

		entry = hash_search_with_hash_value(lfc_hash, &tag, hash, HASH_FIND, NULL);
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
		{
			lfc_ctl->pinned += 1;
			dlist_delete(&entry->list_node);
		}
		generation = lfc_ctl->generation;
		entry_offset = entry->offset;

		for (int i = first_block_in_chunk_read; i < iov_last_used; i++)
		{
			FileCacheBlockState state = UNAVAILABLE;
			bool sleeping = false;

			/* no need to work on something we're not interested in */
			if (BITMAP_ISSET(mask, buf_offset + i))
				continue;

			while (lfc_ctl->generation == generation)
			{
				state = GET_STATE(entry, chunk_offs + i);
				if (state == PENDING) {
					SET_STATE(entry, chunk_offs + i, REQUESTED);
				} else if (state != REQUESTED) {
					break;
				}
				if (!sleeping)
				{
					ConditionVariablePrepareToSleep(cv);
					sleeping = true;
				}
				LWLockRelease(lfc_lock);
				ConditionVariableTimedSleep(cv, CV_WAIT_TIMEOUT, WAIT_EVENT_NEON_LFC_CV_WAIT);
				LWLockAcquire(lfc_lock, LW_EXCLUSIVE);
			}
			if (sleeping)
			{
				ConditionVariableCancelSleep();
			}
			if (state == AVAILABLE)
			{
				BITMAP_SET(chunk_mask, i);
				iteration_hits++;
			}
			else
				iteration_misses++;
		}
		LWLockRelease(lfc_lock);

		Assert(iteration_hits + iteration_misses > 0);

		if (iteration_hits != 0)
		{
			/* chunk offset (#
			   of pages) into the LFC file */
			off_t	first_read_offset = (off_t) entry_offset * lfc_blocks_per_chunk;
			int		nwrite = iov_last_used - first_block_in_chunk_read;
			/* offset of first IOV */
			first_read_offset += chunk_offs + first_block_in_chunk_read;

			pgstat_report_wait_start(WAIT_EVENT_NEON_LFC_READ);

			/* Read only the blocks we're interested in, limiting */
			rc = preadv(lfc_desc, &iov[first_block_in_chunk_read],
						nwrite, first_read_offset * BLCKSZ);
			pgstat_report_wait_end();

			if (rc != (BLCKSZ * nwrite))
			{
				lfc_disable("read");
				return -1;
			}
		}

		/* Place entry to the head of LRU list */
		LWLockAcquire(lfc_lock, LW_EXCLUSIVE);

		if (lfc_ctl->generation == generation)
		{
			CriticalAssert(LFC_ENABLED());
			lfc_ctl->hits += iteration_hits;
			lfc_ctl->misses += iteration_misses;
			pgBufferUsage.file_cache.hits += iteration_hits;
			pgBufferUsage.file_cache.misses += iteration_misses;

			if (iteration_hits)
			{
				lfc_ctl->time_read += io_time_us;
				inc_page_cache_read_wait(io_time_us);
				/*
				 * We successfully read the pages we know were valid when we
				 * started reading; now mark those pages as read
				 */
				for (int i = first_block_in_chunk_read; i < iov_last_used; i++)
				{
					if (BITMAP_ISSET(chunk_mask, i))
						BITMAP_SET(mask, buf_offset + i);
				}
			}

			CriticalAssert(entry->access_count > 0);
			if (--entry->access_count == 0)
			{
				lfc_ctl->pinned -= 1;
				dlist_push_tail(&lfc_ctl->lru, &entry->list_node);
			}
		}
		else
		{
			/* generation mismatch, assume error condition */
			lfc_close_file();
			LWLockRelease(lfc_lock);
			return -1;
		}

		LWLockRelease(lfc_lock);

		buf_offset += blocks_in_chunk;
		nblocks -= blocks_in_chunk;
		blkno += blocks_in_chunk;
		blocks_read += iteration_hits;
	}

	return blocks_read;
}

/*
 * Initialize new LFC hash entry, perform eviction if needed.
 * Returns false if there are no unpinned entries and chunk can not be added.
 */
static bool
lfc_init_new_entry(FileCacheEntry* entry, uint32 hash)
{
	/*-----------
	 * If the chunk wasn't already in the LFC then we have these
	 * options, in order of preference:
	 *
	 * Unless there is no space available, we can:
	 *  1. Use an entry from the `holes` list, and
	 *  2. Create a new entry.
	 * We can always, regardless of space in the LFC:
	 *  3. evict an entry from LRU, and
	 *  4. ignore the write operation (the least favorite option)
	 */
	if (lfc_ctl->used < lfc_ctl->limit)
	{
		if (!dlist_is_empty(&lfc_ctl->holes))
		{
			/* We can reuse a hole that was left behind when the LFC was shrunk previously */
			FileCacheEntry *hole = dlist_container(FileCacheEntry, list_node,
												   dlist_pop_head_node(&lfc_ctl->holes));
			uint32 offset = hole->offset;
			bool hole_found;

			hash_search_with_hash_value(lfc_hash, &hole->key,
										hole->hash, HASH_REMOVE, &hole_found);
			CriticalAssert(hole_found);

			lfc_ctl->used += 1;
			entry->offset = offset;			/* reuse the hole */
		}
		else
		{
			lfc_ctl->used += 1;
			entry->offset = lfc_ctl->size++;/* allocate new chunk at end
											 * of file */
		}
	}
	/*
	 * We've already used up all allocated LFC entries.
	 *
	 * If we can clear an entry from the LRU, do that.
	 * If we can't (e.g. because all other slots are being accessed)
	 * then we will remove this entry from the hash and continue
	 * on to the next chunk, as we may not exceed the limit.
	 *
	 * While prewarming LFC we do not want to replace existed entries,
	 * so we just stop prewarm is LFC cache is full.
	 */
	else if (!dlist_is_empty(&lfc_ctl->lru) && !lfc_do_prewarm)
	{
		/* Cache overflow: evict least recently used chunk */
		FileCacheEntry *victim = dlist_container(FileCacheEntry, list_node,
												 dlist_pop_head_node(&lfc_ctl->lru));

		for (int i = 0; i < lfc_blocks_per_chunk; i++)
		{
			bool is_page_cached = GET_STATE(victim, i) == AVAILABLE;
			lfc_ctl->used_pages -= is_page_cached;
			lfc_ctl->evicted_pages += is_page_cached;
		}

		CriticalAssert(victim->access_count == 0);
		entry->offset = victim->offset; /* grab victim's chunk */
		hash_search_with_hash_value(lfc_hash, &victim->key,
									victim->hash, HASH_REMOVE, NULL);
		neon_log(DEBUG2, "Swap file cache page");
	}
	else
	{
		/* Can't add this chunk - we don't have the space for it */
		hash_search_with_hash_value(lfc_hash, &entry->key, hash,
									HASH_REMOVE, NULL);
		lfc_ctl->prewarm_canceled = true; /* cancel prewarm if LFC limit is reached */
		return false;
	}

	entry->access_count = 1;
	entry->hash = hash;
	lfc_ctl->pinned += 1;

	for (int i = 0; i < lfc_blocks_per_chunk; i++)
		SET_STATE(entry, i, UNAVAILABLE);

	return true;
}

/*
 * Store received prefetch result in LFC cache.
 * Unlike lfc_read/lfc_write this call is is not protected by shared buffer lock.
 * So we should be ready that other backends will try to concurrently read or write this block.
 * We do not store prefetched block if it already exists in LFC or it's not_modified_since LSN is smaller
 * than current last written LSN (LwLSN).
 *
 * We can enforce correctness of storing page in LFC by the following steps:
 * 1. Check under LFC lock that page in not present in LFC.
 * 2. Check under LFC lock that LwLSN is not changed since prefetch request time (not_modified_since).
 * 3. Change page state to "Pending" under LFC lock to prevent all other backends to read or write this
 *    pages until this write is completed.
 * 4. Assume that some other backend creates new image of the page without reading it
 *    (because reads will be blocked because of 2). This version of the page is stored in shared buffer.
 *    Any attempt to throw away this page from shared buffer will be blocked, because Postgres first
 *    needs to save dirty page and write will be blocked because of 2.
 *    So any backend trying to access this page, will take it from shared buffer without accessing
 *    SMGR and LFC.
 * 5. After write completion we once again obtain LFC lock and wake-up all waiting backends.
 *    If there is some backend waiting to write new image of the page (4) then now it will be able to
 *    do it,overwriting old (prefetched) page image. As far as this write will be completed before
 *    shared buffer can be reassigned, not other backend can see old page image.
*/
bool
lfc_prefetch(NRelFileInfo rinfo, ForkNumber forknum, BlockNumber blkno,
			 const void* buffer, XLogRecPtr lsn)
{
	BufferTag	tag;
	FileCacheEntry *entry;
	ssize_t		rc;
	bool		found;
	uint32		hash;
	uint64		generation;
	uint32		entry_offset;
	instr_time io_start, io_end;
	ConditionVariable* cv;
	FileCacheBlockState state;
	XLogRecPtr lwlsn;

	int		chunk_offs = BLOCK_TO_CHUNK_OFF(blkno);

	if (lfc_maybe_disabled())	/* fast exit if file cache is disabled */
		return false;

	CopyNRelFileInfoToBufTag(tag, rinfo);
	CriticalAssert(BufTagGetRelNumber(&tag) != InvalidRelFileNumber);
	tag.forkNum = forknum;

	/* Update working set size estimate for the blocks */
	if (lfc_prewarm_update_ws_estimation)
	{
		tag.blockNum = blkno;
		addSHLL(&lfc_ctl->wss_estimation, hash_bytes((uint8_t const*)&tag, sizeof(tag)));
	}

	tag.blockNum = blkno - chunk_offs;
	hash = get_hash_value(lfc_hash, &tag);
	cv = &lfc_ctl->cv[hash % N_COND_VARS];

	LWLockAcquire(lfc_lock, LW_EXCLUSIVE);

	if (!LFC_ENABLED() || !lfc_ensure_opened())
	{
		LWLockRelease(lfc_lock);
		return false;
	}

	lwlsn = neon_get_lwlsn(rinfo, forknum, blkno);

	if (lwlsn > lsn)
	{
		elog(DEBUG1, "Skip LFC write for %u because LwLSN=%X/%X is greater than not_nodified_since LSN %X/%X",
			 blkno, LSN_FORMAT_ARGS(lwlsn), LSN_FORMAT_ARGS(lsn));
		LWLockRelease(lfc_lock);
		return false;
	}

	entry = hash_search_with_hash_value(lfc_hash, &tag, hash, HASH_ENTER, &found);
	if (found)
	{
		state = GET_STATE(entry, chunk_offs);
		if (state != UNAVAILABLE) {
			/* Do not rewrite existed LFC entry */
			LWLockRelease(lfc_lock);
			return false;
		}
		/*
		 * Unlink entry from LRU list to pin it for the duration of IO
		 * operation
		 */
		if (entry->access_count++ == 0)
		{
			lfc_ctl->pinned += 1;
			dlist_delete(&entry->list_node);
		}
	}
	else
	{
		if (!lfc_init_new_entry(entry, hash))
		{
			/*
			 * We can't process this chunk due to lack of space in LFC,
			 * so skip to the next one
			 */
			LWLockRelease(lfc_lock);
			return false;
		}
	}

	generation = lfc_ctl->generation;
	entry_offset = entry->offset;

	SET_STATE(entry, chunk_offs, PENDING);

	LWLockRelease(lfc_lock);

	pgstat_report_wait_start(WAIT_EVENT_NEON_LFC_WRITE);
	INSTR_TIME_SET_CURRENT(io_start);
	rc = pwrite(lfc_desc, buffer, BLCKSZ,
				((off_t) entry_offset * lfc_blocks_per_chunk + chunk_offs) * BLCKSZ);
	INSTR_TIME_SET_CURRENT(io_end);
	pgstat_report_wait_end();

	if (rc != BLCKSZ)
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

			lfc_ctl->writes += 1;
			INSTR_TIME_SUBTRACT(io_start, io_end);
			time_spent_us = INSTR_TIME_GET_MICROSEC(io_start);
			lfc_ctl->time_write += time_spent_us;
			inc_page_cache_write_wait(time_spent_us);

			if (--entry->access_count == 0)
			{
				lfc_ctl->pinned -= 1;
				dlist_push_tail(&lfc_ctl->lru, &entry->list_node);
			}

			state = GET_STATE(entry, chunk_offs);
			if (state == REQUESTED) {
				ConditionVariableBroadcast(cv);
			}
			if (state != AVAILABLE)
			{
				lfc_ctl->used_pages += 1;
				SET_STATE(entry, chunk_offs, AVAILABLE);
			}
		}
		else
		{
			lfc_close_file();
		}
		LWLockRelease(lfc_lock);
	}
	return true;
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
	bool		found;
	uint32		hash;
	uint64		generation;
	uint32		entry_offset;
	int			buf_offset = 0;

	if (lfc_maybe_disabled())	/* fast exit if file cache is disabled */
		return;

	CopyNRelFileInfoToBufTag(tag, rinfo);
	CriticalAssert(BufTagGetRelNumber(&tag) != InvalidRelFileNumber);
	tag.forkNum = forkNum;

	/* Update working set size estimate for the blocks */
	for (int i = 0; i < nblocks; i++)
	{
		tag.blockNum = blkno + i;
		addSHLL(&lfc_ctl->wss_estimation, hash_bytes((uint8_t const*)&tag, sizeof(tag)));
	}

	LWLockAcquire(lfc_lock, LW_EXCLUSIVE);

	if (!LFC_ENABLED() || !lfc_ensure_opened())
	{
		LWLockRelease(lfc_lock);
		return;
	}
	generation = lfc_ctl->generation;

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
		int		chunk_offs = BLOCK_TO_CHUNK_OFF(blkno);
		int		blocks_in_chunk = Min(nblocks, lfc_blocks_per_chunk - chunk_offs);
		instr_time io_start, io_end;
		ConditionVariable* cv;

		Assert(blocks_in_chunk > 0);

		for (int i = 0; i < blocks_in_chunk; i++)
		{
			iov[i].iov_base = unconstify(void *, buffers[buf_offset + i]);
			iov[i].iov_len = BLCKSZ;
		}

		tag.blockNum = blkno - chunk_offs;
		hash = get_hash_value(lfc_hash, &tag);
		cv = &lfc_ctl->cv[hash % N_COND_VARS];

		entry = hash_search_with_hash_value(lfc_hash, &tag, hash, HASH_ENTER, &found);
		if (found)
		{
			/*
			 * Unlink entry from LRU list to pin it for the duration of IO
			 * operation
			 */
			if (entry->access_count++ == 0)
			{
				lfc_ctl->pinned += 1;
				dlist_delete(&entry->list_node);
			}
		}
		else
		{
			if (!lfc_init_new_entry(entry, hash))
			{
				/*
				 * We can't process this chunk due to lack of space in LFC,
				 * so skip to the next one
				 */
				blkno += blocks_in_chunk;
				buf_offset += blocks_in_chunk;
				nblocks -= blocks_in_chunk;
				continue;
			}
		}

		entry_offset = entry->offset;

		for (int i = 0; i < blocks_in_chunk; i++)
		{
			FileCacheBlockState state = UNAVAILABLE;
			bool sleeping = false;
			while (lfc_ctl->generation == generation)
			{
				state = GET_STATE(entry, chunk_offs + i);
				if (state == PENDING) {
					SET_STATE(entry, chunk_offs + i, REQUESTED);
				} else if (state == UNAVAILABLE) {
					SET_STATE(entry, chunk_offs + i, PENDING);
					break;
				} else if (state == AVAILABLE) {
					break;
				}
				if (!sleeping)
				{
					ConditionVariablePrepareToSleep(cv);
					sleeping = true;
				}
				LWLockRelease(lfc_lock);
				ConditionVariableTimedSleep(cv, CV_WAIT_TIMEOUT, WAIT_EVENT_NEON_LFC_CV_WAIT);
				LWLockAcquire(lfc_lock, LW_EXCLUSIVE);
			}
			if (sleeping)
			{
				ConditionVariableCancelSleep();
			}
		}
		LWLockRelease(lfc_lock);

		pgstat_report_wait_start(WAIT_EVENT_NEON_LFC_WRITE);
		INSTR_TIME_SET_CURRENT(io_start);
		rc = pwritev(lfc_desc, iov, blocks_in_chunk,
					 ((off_t) entry_offset * lfc_blocks_per_chunk + chunk_offs) * BLCKSZ);
		INSTR_TIME_SET_CURRENT(io_end);
		pgstat_report_wait_end();

		if (rc != BLCKSZ * blocks_in_chunk)
		{
			lfc_disable("write");
			return;
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

				if (--entry->access_count == 0)
				{
					lfc_ctl->pinned -= 1;
					dlist_push_tail(&lfc_ctl->lru, &entry->list_node);
				}

				for (int i = 0; i < blocks_in_chunk; i++)
				{
					FileCacheBlockState state = GET_STATE(entry, chunk_offs + i);
					if (state == REQUESTED)
					{
						ConditionVariableBroadcast(cv);
					}
					if (state != AVAILABLE)
					{
						lfc_ctl->used_pages += 1;
						SET_STATE(entry, chunk_offs + i, AVAILABLE);
					}
				}
			}
			else
			{
				/* stop iteration if LFC was disabled */
				lfc_close_file();
				break;
			}
		}
		blkno += blocks_in_chunk;
		buf_offset += blocks_in_chunk;
		nblocks -= blocks_in_chunk;
	}
	LWLockRelease(lfc_lock);
}

/*
 * Return metrics about the LFC.
 *
 * The return format is a palloc'd array of LfcStatsEntrys. The size
 * of the returned array is returned in *num_entries.
 */
LfcStatsEntry *
lfc_get_stats(size_t *num_entries)
{
	LfcStatsEntry *entries;
	size_t		n = 0;

#define MAX_ENTRIES 10
	entries = palloc(sizeof(LfcStatsEntry) * MAX_ENTRIES);

	entries[n++] = (LfcStatsEntry) {"file_cache_chunk_size_pages", lfc_ctl == NULL,
									lfc_ctl ? lfc_blocks_per_chunk : 0 };
	entries[n++] = (LfcStatsEntry) {"file_cache_misses", lfc_ctl == NULL,
									lfc_ctl ? lfc_ctl->misses : 0};
	entries[n++] = (LfcStatsEntry) {"file_cache_hits", lfc_ctl == NULL,
									lfc_ctl ? lfc_ctl->hits : 0 };
	entries[n++] = (LfcStatsEntry) {"file_cache_used", lfc_ctl == NULL,
									lfc_ctl ? lfc_ctl->used : 0 };
	entries[n++] = (LfcStatsEntry) {"file_cache_writes", lfc_ctl == NULL,
									lfc_ctl ? lfc_ctl->writes : 0 };
	entries[n++] = (LfcStatsEntry) {"file_cache_size", lfc_ctl == NULL,
									lfc_ctl ? lfc_ctl->size : 0 };
	entries[n++] = (LfcStatsEntry) {"file_cache_used_pages", lfc_ctl == NULL,
									lfc_ctl ? lfc_ctl->used_pages : 0 };
	entries[n++] = (LfcStatsEntry) {"file_cache_evicted_pages", lfc_ctl == NULL,
									lfc_ctl ? lfc_ctl->evicted_pages : 0 };
	entries[n++] = (LfcStatsEntry) {"file_cache_limit", lfc_ctl == NULL,
									lfc_ctl ? lfc_ctl->limit : 0 };
	entries[n++] = (LfcStatsEntry) {"file_cache_chunks_pinned", lfc_ctl == NULL,
									lfc_ctl ? lfc_ctl->pinned : 0 };
	Assert(n <= MAX_ENTRIES);
#undef MAX_ENTRIES

	*num_entries = n;
	return entries;
}


/*
 * Function returning data from the local file cache
 * relation node/tablespace/database/blocknum and access_counter
 */
LocalCachePagesRec *
lfc_local_cache_pages(size_t *num_entries)
{
	HASH_SEQ_STATUS status;
	FileCacheEntry *entry;
	size_t		n_pages;
	size_t		n;
	LocalCachePagesRec *result;

	if (!lfc_ctl)
	{
		*num_entries = 0;
		return NULL;
	}

	LWLockAcquire(lfc_lock, LW_SHARED);
	if (!LFC_ENABLED())
	{
		LWLockRelease(lfc_lock);
		*num_entries = 0;
		return NULL;
	}

	/* Count the pages first */
	n_pages = 0;
	hash_seq_init(&status, lfc_hash);
	while ((entry = hash_seq_search(&status)) != NULL)
	{
		/* Skip hole tags */
		if (NInfoGetRelNumber(BufTagGetNRelFileInfo(entry->key)) != 0)
		{
			for (int i = 0; i < lfc_blocks_per_chunk; i++)
				n_pages += GET_STATE(entry, i) == AVAILABLE;
		}
	}

	if (n_pages == 0)
	{
		LWLockRelease(lfc_lock);
		*num_entries = 0;
		return NULL;
	}

	result = (LocalCachePagesRec *)
		MemoryContextAllocHuge(CurrentMemoryContext,
							   sizeof(LocalCachePagesRec) * n_pages);

	/*
	 * Scan through all the cache entries, saving the relevant fields
	 * in the result structure.
	 */
	n = 0;
	hash_seq_init(&status, lfc_hash);
	while ((entry = hash_seq_search(&status)) != NULL)
	{
		for (int i = 0; i < lfc_blocks_per_chunk; i++)
		{
			if (NInfoGetRelNumber(BufTagGetNRelFileInfo(entry->key)) != 0)
			{
				if (GET_STATE(entry, i) == AVAILABLE)
				{
					result[n].pageoffs = entry->offset * lfc_blocks_per_chunk + i;
					result[n].relfilenode = NInfoGetRelNumber(BufTagGetNRelFileInfo(entry->key));
					result[n].reltablespace = NInfoGetSpcOid(BufTagGetNRelFileInfo(entry->key));
					result[n].reldatabase = NInfoGetDbOid(BufTagGetNRelFileInfo(entry->key));
					result[n].forknum = entry->key.forkNum;
					result[n].blocknum = entry->key.blockNum + i;
					result[n].accesscount = entry->access_count;
					n += 1;
				}
			}
		}
	}
	Assert(n_pages == n);
	LWLockRelease(lfc_lock);

	*num_entries = n_pages;
	return result;
}

/*
 * Internal implementation of the approximate_working_set_size_seconds()
 * function.
 */
int32
lfc_approximate_working_set_size_seconds(time_t duration, bool reset)
{
	int32		dc;

	if (lfc_size_limit == 0)
		return -1;

	dc = (int32) estimateSHLL(&lfc_ctl->wss_estimation, duration);
	if (reset)
		memset(lfc_ctl->wss_estimation.regs, 0, sizeof lfc_ctl->wss_estimation.regs);
	return dc;
}

/*
 * Get metrics, for the built-in metrics exporter that's part of the communicator
 * process.
 *
 * NB: This is called from a Rust tokio task inside the communicator process.
 * Acquiring lwlocks, elog(), allocating memory or anything else non-trivial
 * is strictly prohibited here!
 */
struct LfcMetrics
callback_get_lfc_metrics_unsafe(void)
{
	struct LfcMetrics result = {
		.lfc_cache_size_limit = (int64) lfc_size_limit * 1024 * 1024,
		.lfc_hits = lfc_ctl ? lfc_ctl->hits : 0,
		.lfc_misses = lfc_ctl ? lfc_ctl->misses : 0,
		.lfc_used = lfc_ctl ? lfc_ctl->used : 0,
		.lfc_writes = lfc_ctl ? lfc_ctl->writes : 0,
	};

	if (lfc_ctl)
	{
		for (int minutes = 1; minutes <= 60; minutes++)
		{
			result.lfc_approximate_working_set_size_windows[minutes - 1] =
				lfc_approximate_working_set_size_seconds(minutes * 60, false);
		}
	}

	return result;
}


PG_FUNCTION_INFO_V1(get_local_cache_state);

Datum
get_local_cache_state(PG_FUNCTION_ARGS)
{
	size_t max_entries = PG_ARGISNULL(0) ? lfc_prewarm_limit : PG_GETARG_INT32(0);
	FileCacheState* fcs = lfc_get_state(max_entries);
	if (fcs != NULL)
		PG_RETURN_BYTEA_P((bytea*)fcs);
	else
		PG_RETURN_NULL();
}

PG_FUNCTION_INFO_V1(prewarm_local_cache);

Datum
prewarm_local_cache(PG_FUNCTION_ARGS)
{
	bytea* state = PG_GETARG_BYTEA_PP(0);
	uint32 n_workers =  PG_GETARG_INT32(1);
	FileCacheState* fcs = (FileCacheState*)state;

	lfc_prewarm(fcs, n_workers);

	PG_RETURN_NULL();
}

PG_FUNCTION_INFO_V1(get_prewarm_info);

Datum
get_prewarm_info(PG_FUNCTION_ARGS)
{
	Datum		values[4];
	bool		nulls[4];
	TupleDesc	tupdesc;
	uint32 prewarmed_pages = 0;
	uint32 skipped_pages = 0;
	uint32 active_workers = 0;
	uint32 total_pages;
	size_t n_workers;

	if (lfc_size_limit == 0)
		PG_RETURN_NULL();

	LWLockAcquire(lfc_lock, LW_SHARED);
	if (!lfc_ctl || lfc_ctl->n_prewarm_workers == 0)
	{
		LWLockRelease(lfc_lock);
		PG_RETURN_NULL();
	}
	n_workers = lfc_ctl->n_prewarm_workers;
	total_pages = lfc_ctl->total_prewarm_pages;
	for (size_t i = 0; i < n_workers; i++)
	{
		PrewarmWorkerState* ws = &lfc_ctl->prewarm_workers[i];
		prewarmed_pages += ws->prewarmed_pages;
		skipped_pages += ws->skipped_pages;
		active_workers += ws->completed != 0;
	}
	LWLockRelease(lfc_lock);

	tupdesc = CreateTemplateTupleDesc(4);
	TupleDescInitEntry(tupdesc, (AttrNumber) 1, "total_pages", INT4OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 2, "prewarmed_pages", INT4OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 3, "skipped_pages", INT4OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 4, "active_workers", INT4OID, -1, 0);
	tupdesc = BlessTupleDesc(tupdesc);

	MemSet(nulls, 0, sizeof(nulls));

	values[0] = Int32GetDatum(total_pages);
	values[1] = Int32GetDatum(prewarmed_pages);
	values[2] = Int32GetDatum(skipped_pages);
	values[3] = Int32GetDatum(active_workers);

	PG_RETURN_DATUM(HeapTupleGetDatum(heap_form_tuple(tupdesc, values, nulls)));
}

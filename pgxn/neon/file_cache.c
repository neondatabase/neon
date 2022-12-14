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

#include <sys/file.h>
#include <unistd.h>
#include <fcntl.h>

#include "postgres.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "pagestore_client.h"
#include "access/parallel.h"
#include "postmaster/bgworker.h"
#include "storage/relfilenode.h"
#include "storage/buf_internals.h"
#include "storage/latch.h"
#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "utils/dynahash.h"
#include "utils/guc.h"
#include "storage/fd.h"
#include "storage/pg_shmem.h"
#include "storage/buf_internals.h"

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
#define CHUNK_SIZE 128 /* 1Mb chunk */
#define MB         ((uint64)1024*1024)

typedef struct FileCacheEntry
{
	BufferTag key;
	uint32    offset;
	uint32    access_count;
	uint32    bitmap[CHUNK_SIZE/32];
	/* LRU list */
	struct FileCacheEntry* next;
	struct FileCacheEntry* prev;
} FileCacheEntry;

typedef struct FileCacheControl
{
	uint32 size; /* size of cache file in chunks */
	FileCacheEntry lru;
} FileCacheControl;

static HTAB* lfc_hash;
static int   lfc_desc;
static LWLockId lfc_lock;
static int   lfc_max_size;
static int   lfc_size_limit;
static char* lfc_path;
static  FileCacheControl* lfc_ctl;
static shmem_startup_hook_type prev_shmem_startup_hook;

/*
 * Prune L2 list
 */
static void
lfc_prune(FileCacheEntry* entry)
{
	entry->next = entry->prev = entry;
}

/*
 * Unlink from LRU list
 */
static void
lfc_unlink(FileCacheEntry* entry)
{
	entry->next->prev = entry->prev;
	entry->prev->next = entry->next;
}

/*
 * Link to the head of LRU list
 */
static void
lfc_link(FileCacheEntry* after, FileCacheEntry* entry)
{
	entry->next = after->next;
	entry->prev = after;
	after->next->prev = entry;
	after->next = entry;
}

/*
 * Check if list is empty
 */
static bool
lfc_is_empty(FileCacheEntry* entry)
{
	return entry->next == entry;
}

static void
lfc_shmem_startup(void)
{
	bool found;
	static HASHCTL info;

	if (prev_shmem_startup_hook)
	{
		prev_shmem_startup_hook();
	}

	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);

	lfc_ctl = (FileCacheControl*)ShmemInitStruct("lfc", sizeof(FileCacheControl), &found);
	if (!found)
	{
		uint32 lfc_size = (uint32)(lfc_max_size*MB/BLCKSZ/CHUNK_SIZE);
		lfc_lock = (LWLockId)GetNamedLWLockTranche("lfc_lock");
		info.keysize = sizeof(BufferTag);
		info.entrysize = sizeof(FileCacheEntry);
		lfc_hash = ShmemInitHash("lfc_hash",
								 /* lfc_size+1 because we add new element to hash table before eviction of victim */
								 lfc_size+1, lfc_size+1,
								 &info,
								 HASH_ELEM | HASH_BLOBS);
		lfc_ctl->size = 0;
		lfc_prune(&lfc_ctl->lru);

		/* Remove file cache on restart */
		(void)unlink(lfc_path);
	}
	LWLockRelease(AddinShmemInitLock);
}

bool
lfc_check_limit_hook(int *newval, void **extra, GucSource source)
{
	if (*newval > lfc_max_size)
	{
		elog(ERROR, "neon.file_cache_size_limit can not be larger than neon.max_file_cache_size");
		return false;
	}
	return true;
}

void
lfc_change_limit_hook(int newval, void *extra)
{
	uint32 new_size = (uint32)(newval*MB/BLCKSZ/CHUNK_SIZE);
	if (!lfc_ctl || !UsedShmemSegAddr || IsParallelWorker())
		return;

	/* Open cache file if not done yet */
	if (lfc_desc == 0)
	{
		lfc_desc = BasicOpenFile(lfc_path, O_RDWR|O_CREAT);
		if (lfc_desc < 0) {
			elog(LOG, "Failed to open file cache %s: %m", lfc_path);
			lfc_size_limit = 0; /* disable file cache */
			return;
		}
	}
	LWLockAcquire(lfc_lock, LW_EXCLUSIVE);
	while (new_size < lfc_ctl->size && !lfc_is_empty(&lfc_ctl->lru))
	{
		/* Shrink cache by throwing away least recently accessed chunks and returning their space to file system */
		FileCacheEntry* victim = lfc_ctl->lru.prev;
		Assert(victim->access_count == 0);
		lfc_unlink(victim);
#ifdef FALLOC_FL_PUNCH_HOLE
		if (fallocate(lfc_desc, FALLOC_FL_PUNCH_HOLE|FALLOC_FL_KEEP_SIZE, (off_t)victim->offset*CHUNK_SIZE*BLCKSZ, CHUNK_SIZE*BLCKSZ) < 0)
			elog(LOG, "Failed to punch hole in file: %m");
#endif
		hash_search(lfc_hash, &victim->key, HASH_REMOVE, NULL);
		lfc_ctl->size -= 1;
	}
	elog(LOG, "set local file cache limit to %d", new_size);
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
		elog(ERROR, "Neon module should be loaded via shared_preload_libraries");

	DefineCustomIntVariable("neon.max_file_cache_size",
							"Maximal size of Neon local file cache",
							NULL,
							&lfc_max_size,
							0, /* disabled by default */
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
							0, /* disabled by default */
							0,
							INT_MAX,
							PGC_SIGHUP,
							GUC_UNIT_MB,
							NULL,
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

	RequestAddinShmemSpace(sizeof(FileCacheControl) + hash_estimate_size(lfc_max_size*MB/BLCKSZ/CHUNK_SIZE+1, sizeof(FileCacheEntry)));
	RequestNamedLWLockTranche("lfc_lock", 1);

	prev_shmem_startup_hook = shmem_startup_hook;
	shmem_startup_hook = lfc_shmem_startup;
}

bool
lfc_cached(RelFileNode rnode, ForkNumber forkNum, BlockNumber blkno)
{
	BufferTag tag;
	FileCacheEntry* entry;
	int chunk_offs = blkno & (CHUNK_SIZE-1);

	if (lfc_size_limit == 0) /* fast exit if file cache is disabled */
		return false;

	tag.rnode = rnode;
	tag.forkNum = forkNum;
	tag.blockNum = blkno & ~(CHUNK_SIZE-1);

	LWLockAcquire(lfc_lock, LW_SHARED);
	entry = hash_search(lfc_hash, &tag, HASH_FIND, NULL);
	if (entry == NULL || (entry->bitmap[chunk_offs >> 5] & (1 << (chunk_offs & 31))) == 0)
	{
		/* Page is not cached */
		LWLockRelease(lfc_lock);
		return false;
	}
	LWLockRelease(lfc_lock);
	return true;
}

/*
 * Try to read page from local cache.
 * Returns true if page is found in local cache.
 * In case of error lfc_size_limit is set to zero to disable any further opera-tins with cache.
 */
bool
lfc_read(RelFileNode rnode, ForkNumber forkNum, BlockNumber blkno,
		 char *buffer)
{
	BufferTag tag;
	FileCacheEntry* entry;
	ssize_t rc;
	int chunk_offs = blkno & (CHUNK_SIZE-1);

	if (lfc_size_limit == 0) /* fast exit if file cache is disabled */
		return false;

	tag.rnode = rnode;
	tag.forkNum = forkNum;
	tag.blockNum = blkno & ~(CHUNK_SIZE-1);

	LWLockAcquire(lfc_lock, LW_EXCLUSIVE);
	entry = hash_search(lfc_hash, &tag, HASH_FIND, NULL);
	if (entry == NULL || (entry->bitmap[chunk_offs >> 5] & (1 << (chunk_offs & 31))) == 0)
	{
		/* Page is not cached */
		LWLockRelease(lfc_lock);
		return false;
	}
	/* Unlink entry from LRU list to pin it for the duration of IO operation */
	if (entry->access_count++ == 0)
		lfc_unlink(entry);
	LWLockRelease(lfc_lock);

	/* Open cache file if not done yet */
	if (lfc_desc == 0)
	{
		lfc_desc = BasicOpenFile(lfc_path, O_RDWR|O_CREAT);
		if (lfc_desc < 0) {
			elog(LOG, "Failed to open file cache %s: %m", lfc_path);
			lfc_size_limit = 0; /* disable file cache */
			return false;
		}
	}

	rc = pread(lfc_desc, buffer, BLCKSZ, ((off_t)entry->offset*CHUNK_SIZE + chunk_offs)*BLCKSZ);
	if (rc != BLCKSZ)
	{
		elog(INFO, "Failed to read file cache: %m");
		lfc_size_limit = 0; /* disable file cache */
		return false;
	}

	/* Place entry to the head of LRU list */
	LWLockAcquire(lfc_lock, LW_EXCLUSIVE);
	Assert(entry->access_count > 0);
	if (--entry->access_count == 0)
		lfc_link(&lfc_ctl->lru, entry);
	LWLockRelease(lfc_lock);

	return true;
}

/*
 * Put page in local file cache.
 * If cache is full then evict some other page.
 */
void
lfc_write(RelFileNode rnode, ForkNumber forkNum, BlockNumber blkno,
		  char *buffer)
{
	BufferTag tag;
	FileCacheEntry* entry;
	ssize_t rc;
	bool found;
	int chunk_offs = blkno & (CHUNK_SIZE-1);

	if (lfc_size_limit == 0) /* fast exit if file cache is disabled */
		return;

	tag.rnode = rnode;
	tag.forkNum = forkNum;
	tag.blockNum = blkno & ~(CHUNK_SIZE-1);

	LWLockAcquire(lfc_lock, LW_EXCLUSIVE);
	entry = hash_search(lfc_hash, &tag, HASH_ENTER, &found);

	if (found)
	{
		/* Unlink entry from LRU list to pin it for the duration of IO operation */
		if (entry->access_count++ == 0)
			lfc_unlink(entry);
	}
	else
	{
		/*
		 * We have two choices if all cache pages are pinned (i.e. used in IO operations):
		 * 1. Wait until some of this operation is completed and pages is unpinned
		 * 2. Allocate one more chunk, so that specified cache size is more than recommendation than hard limit.
		 * As far as probability of such event (that all pages are pinned) is considered to be very very small:
		 * there are should be very large number of concurrent IO operations and them are limited by max_connections,
		 * we prefer not to complicate code and use second approach.
		 */
		if (lfc_ctl->size >= lfc_size_limit*MB/BLCKSZ/CHUNK_SIZE && !lfc_is_empty(&lfc_ctl->lru))
		{
			/* Cache overflow: evict least recently used chunk */
			FileCacheEntry* victim = lfc_ctl->lru.prev;
			Assert(victim->access_count == 0);
			lfc_unlink(victim);
			entry->offset = victim->offset; /* grab victim's chunk */
			hash_search(lfc_hash, &victim->key, HASH_REMOVE, NULL);
			elog(LOG, "Swap file cache page");
		}
		else
			entry->offset = lfc_ctl->size++; /* allocate new chunk at end of file */
		entry->access_count = 1;
		memset(entry->bitmap, 0, sizeof entry->bitmap);
	}
	entry->bitmap[chunk_offs >> 5] |= (1 << (chunk_offs & 31));

	LWLockRelease(lfc_lock);

	/* Open cache file if not done yet */
	if (lfc_desc == 0)
	{
		lfc_desc = BasicOpenFile(lfc_path, O_RDWR|O_CREAT);
		if (lfc_desc < 0) {
			elog(LOG, "Failed to open file cache %s: %m", lfc_path);
			lfc_size_limit = 0; /* disable file cache */
			return;
		}
	}

	rc = pwrite(lfc_desc, buffer, BLCKSZ, ((off_t)entry->offset*CHUNK_SIZE + chunk_offs)*BLCKSZ);
	if (rc != BLCKSZ)
	{
		elog(INFO, "Failed to write file cache: %m");
		lfc_size_limit = 0; /* disable file cache */
		return;
	}

	/* Place entry to the head of LRU list */
	LWLockAcquire(lfc_lock, LW_EXCLUSIVE);
	Assert(entry->access_count > 0);
	if (--entry->access_count == 0)
		lfc_link(&lfc_ctl->lru, entry);
	LWLockRelease(lfc_lock);
}


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

/*
 * Function returning data from the local file cache
 * relation node/tablespace/database/blocknum and access_counter
 */
PG_FUNCTION_INFO_V1(local_cache_pages);

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
		FileCacheEntry* entry;
		uint32 n_pages = 0;
		uint32 i;

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
			elog(ERROR, "return type must be a row type");

		if (expected_tupledesc->natts != NUM_LOCALCACHE_PAGES_ELEM)
			elog(ERROR, "incorrect number of output arguments");

		/* Construct a tuple descriptor for the result rows. */
		tupledesc = CreateTemplateTupleDesc(expected_tupledesc->natts);
		TupleDescInitEntry(tupledesc, (AttrNumber) 1, "pageoffs",
						   INT8OID, -1, 0);
		TupleDescInitEntry(tupledesc, (AttrNumber) 2, "relfilenode",
						   OIDOID, -1, 0);
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

		LWLockAcquire(lfc_lock, LW_SHARED);

        hash_seq_init(&status, lfc_hash);
        while ((entry = hash_seq_search(&status)) != NULL)
		{
			for (int i = 0; i < CHUNK_SIZE; i++)
				n_pages += (entry->bitmap[i >> 5] & (1 << (i & 31))) != 0;
		}
		fctx->record = (LocalCachePagesRec *)
			MemoryContextAllocHuge(CurrentMemoryContext,
								   sizeof(LocalCachePagesRec) * n_pages);

		/* Set max calls and remember the user function context. */
		funcctx->max_calls = n_pages;
		funcctx->user_fctx = fctx;

		/* Return to original context when allocating transient memory */
		MemoryContextSwitchTo(oldcontext);

		/*
		 * Scan through all the buffers, saving the relevant fields in the
		 * fctx->record structure.
		 *
		 * We don't hold the partition locks, so we don't get a consistent
		 * snapshot across all buffers, but we do grab the buffer header
		 * locks, so the information of each buffer is self-consistent.
		 */
		n_pages = 0;
        hash_seq_init(&status, lfc_hash);
        while ((entry = hash_seq_search(&status)) != NULL)
		{
			for (int i = 0; i < CHUNK_SIZE; i++)
			{
				if (entry->bitmap[i >> 5] & (1 << (i & 31)))
				{
					fctx->record[n_pages].pageoffs = entry->offset*CHUNK_SIZE + i;
					fctx->record[n_pages].relfilenode = entry->key.rnode.relNode;
					fctx->record[n_pages].reltablespace = entry->key.rnode.spcNode;
					fctx->record[n_pages].reldatabase = entry->key.rnode.dbNode;
					fctx->record[n_pages].forknum = entry->key.forkNum;
					fctx->record[n_pages].blocknum = entry->key.blockNum + i;
					fctx->record[n_pages].accesscount = entry->access_count;
					n_pages += 1;
				}
			}
		}
		Assert(n_pages == funcctx->max_calls);
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

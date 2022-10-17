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
#include "miscadmin.h"
#include "pgstat.h"
#include "pagestore_client.h"
#include "postmaster/bgworker.h"
#include "storage/relfilenode.h"
#include "storage/buf_internals.h"
#include "storage/latch.h"
#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "utils/dynahash.h"
#include "utils/guc.h"
#include "storage/fd.h"
#include <storage/buf_internals.h>

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
static int   lfc_size;
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

void
lfc_init(void)
{
	/*
	 * In order to create our shared memory area, we have to be loaded via
	 * shared_preload_libraries.
	 */
	if (!process_shared_preload_libraries_in_progress)
		elog(ERROR, "Neon module should be loaded via shared_preload_libraries");

	DefineCustomIntVariable("neon.file_cache_size",
							"Maximal size of neon local file cache (chunks)",
							NULL,
							&lfc_size,
							0, /* disabled by default */
							0,
							INT_MAX,
							PGC_POSTMASTER,
							0,
							NULL,
							NULL,
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

	if (lfc_size == 0)
		return;

	RequestAddinShmemSpace(sizeof(FileCacheControl) + hash_estimate_size(lfc_size+1, sizeof(FileCacheEntry)));
	RequestNamedLWLockTranche("lfc_lock", 1);

	prev_shmem_startup_hook = shmem_startup_hook;
	shmem_startup_hook = lfc_shmem_startup;
}

/*
 * Try to read page from local cache.
 * Returns true if page is found in local cache.
 * In case of error lfc_size is set to zero to disable any further opera-tins with cache.
 */
bool
lfc_read(RelFileNode rnode, ForkNumber forkNum, BlockNumber blkno,
		 char *buffer)
{
	BufferTag tag;
	FileCacheEntry* entry;
	ssize_t rc;
	int chunk_offs = blkno & (CHUNK_SIZE-1);

	if (lfc_size == 0) /* fast exit if file cache is disabled */
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
			lfc_size = 0; /* disable file cache */
			return false;
		}
	}

	rc = pread(lfc_desc, buffer, BLCKSZ, ((off_t)entry->offset*CHUNK_SIZE + chunk_offs)*BLCKSZ);
	if (rc != BLCKSZ)
	{
		elog(INFO, "Failed to read file cache: %m");
		lfc_size = 0; /* disable file cache */
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

	if (lfc_size == 0) /* fast exit if file cache is disabled */
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
		if (lfc_ctl->size >= (uint64)lfc_size && !lfc_is_empty(&lfc_ctl->lru))
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
			lfc_size = 0; /* disable file cache */
			return;
		}
	}

	rc = pwrite(lfc_desc, buffer, BLCKSZ, ((off_t)entry->offset*CHUNK_SIZE + chunk_offs)*BLCKSZ);
	if (rc != BLCKSZ)
	{
		elog(INFO, "Failed to write file cache: %m");
		lfc_size = 0; /* disable file cache */
		return;
	}

	/* Place entry to the head of LRU list */
	LWLockAcquire(lfc_lock, LW_EXCLUSIVE);
	Assert(entry->access_count > 0);
	if (--entry->access_count == 0)
		lfc_link(&lfc_ctl->lru, entry);
	LWLockRelease(lfc_lock);
}

/*-------------------------------------------------------------------------
 *
 * relsize_cache.c
 *      Relation size cache for better zentih performance.
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  contrib/neon/relsize_cache.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "neon_pgversioncompat.h"

#include "pagestore_client.h"
#include RELFILEINFO_HDR
#include "storage/smgr.h"
#include "storage/lwlock.h"
#include "storage/ipc.h"
#include "storage/shmem.h"
#include "catalog/pg_tablespace_d.h"
#include "utils/dynahash.h"
#include "utils/guc.h"

#if PG_VERSION_NUM >= 150000
#include "miscadmin.h"
#endif

typedef struct
{
	NRelFileInfo rinfo;
	ForkNumber	forknum;
} RelTag;

typedef struct
{
	RelTag		tag;
	BlockNumber size : 31;
	BlockNumber unlogged : 1;
	dlist_node	lru_node;		/* LRU list node */
} RelSizeEntry;

typedef struct
{
	size_t      size;
	uint64		hits;
	uint64		misses;
	uint64		writes;
	dlist_head	lru;			/* double linked list for LRU replacement
								 * algorithm */
} RelSizeHashControl;

static HTAB *relsize_hash;
static LWLockId relsize_lock;
static int	relsize_hash_size;
static RelSizeHashControl* relsize_ctl;
static shmem_startup_hook_type prev_shmem_startup_hook = NULL;
#if PG_VERSION_NUM >= 150000
static shmem_request_hook_type prev_shmem_request_hook = NULL;
static void relsize_shmem_request(void);
#endif

/*
 * Size of a cache entry is 36 bytes. So this default will take about 2.3 MB,
 * which seems reasonable.
 */
#define DEFAULT_RELSIZE_HASH_SIZE (64 * 1024)

static void
neon_smgr_shmem_startup(void)
{
	static HASHCTL info;
	bool found;

	if (prev_shmem_startup_hook)
		prev_shmem_startup_hook();

	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);
	relsize_ctl = (RelSizeHashControl *) ShmemInitStruct("relsize_hash", sizeof(RelSizeHashControl), &found);
	if (!found)
	{
		relsize_lock = (LWLockId) GetNamedLWLockTranche("neon_relsize");
		info.keysize = sizeof(RelTag);
		info.entrysize = sizeof(RelSizeEntry);
		relsize_hash = ShmemInitHash("neon_relsize",
									 relsize_hash_size, relsize_hash_size,
									 &info,
									 HASH_ELEM | HASH_BLOBS);
		LWLockRelease(AddinShmemInitLock);
		relsize_ctl->size = 0;
		relsize_ctl->hits = 0;
		relsize_ctl->misses = 0;
		relsize_ctl->writes = 0;
		dlist_init(&relsize_ctl->lru);
	}
}

bool
get_cached_relsize(NRelFileInfo rinfo, ForkNumber forknum, BlockNumber *size)
{
	bool		found = false;

	if (relsize_hash_size > 0)
	{
		RelTag		tag;
		RelSizeEntry *entry;

		tag.rinfo = rinfo;
		tag.forknum = forknum;
		LWLockAcquire(relsize_lock, LW_SHARED);
		entry = hash_search(relsize_hash, &tag, HASH_FIND, NULL);
		if (entry != NULL)
		{
			*size = entry->size;
			relsize_ctl->hits += 1;
			found = true;
			if (!entry->unlogged) /* entries of relation involved in unlogged build are pinned */
			{
				/* Move entry to the LRU list tail */
				dlist_delete(&entry->lru_node);
				dlist_push_tail(&relsize_ctl->lru, &entry->lru_node);
			}
		}
		else
		{
			relsize_ctl->misses += 1;
		}
		LWLockRelease(relsize_lock);
	}
	return found;
}

/*
 * Cache relation size.
 * Returns true if it happens during unlogged build.
 * In this case lock is not released.
 */
bool
set_cached_relsize(NRelFileInfo rinfo, ForkNumber forknum, BlockNumber new_size, BlockNumber* old_size)
{
	bool unlogged = false;
	if (relsize_hash_size > 0)
	{
		RelTag		tag;
		RelSizeEntry *entry;
		bool		found = false;

		tag.rinfo = rinfo;
		tag.forknum = forknum;
		LWLockAcquire(relsize_lock, LW_EXCLUSIVE);
		/*
		 * This should actually never happen! Below we check if hash is full and delete least recently user item in this case.
		 * But for further safety we also perform check here.
		 */
		while ((entry = hash_search(relsize_hash, &tag, HASH_ENTER_NULL, &found)) == NULL)
		{
			if (dlist_is_empty(&relsize_ctl->lru))
			{
				elog(FATAL, "No more free relsize cache entries");
			}
			else
			{
				RelSizeEntry *victim = dlist_container(RelSizeEntry, lru_node, dlist_pop_head_node(&relsize_ctl->lru));
				hash_search(relsize_hash, &victim->tag, HASH_REMOVE, NULL);
				Assert(relsize_ctl->size > 0);
				relsize_ctl->size -= 1;
			}
		}
		if (old_size)
		{
			*old_size = found ? entry->size : 0;
		}
		entry->size = new_size;
		if (!found)
		{
			entry->unlogged = false;
			if (relsize_ctl->size+1 == relsize_hash_size)
			{
				/*
				 * Remove least recently used elment from the hash.
				 * Hash size after is becomes `relsize_hash_size-1`.
				 * But it is not considered to be a problem, because size of this hash is expecrted large enough and +-1 doesn't matter.
				 */
				if (dlist_is_empty(&relsize_ctl->lru))
				{
					elog(FATAL, "No more free relsize cache entries");
				}
				else
				{
					RelSizeEntry *victim = dlist_container(RelSizeEntry, lru_node, dlist_pop_head_node(&relsize_ctl->lru));
					hash_search(relsize_hash, &victim->tag, HASH_REMOVE, NULL);
				}
			}
			else
			{
				relsize_ctl->size += 1;
			}
		}
		else if (entry->unlogged) /* entries of relation involved in unlogged build are pinned */
		{
			dlist_delete(&entry->lru_node);
		}

		if (!entry->unlogged) /* entries of relation involved in unlogged build are pinned */
		{
			dlist_push_tail(&relsize_ctl->lru, &entry->lru_node);
		}
		else
		{
			Assert(old_size);
			unlogged = true;
		}
		relsize_ctl->writes += 1;
		if (!unlogged)
		{
			LWLockRelease(relsize_lock);
		}
	}
	return unlogged;
}

void
update_cached_relsize(NRelFileInfo rinfo, ForkNumber forknum, BlockNumber size)
{
	if (relsize_hash_size > 0)
	{
		RelTag		tag;
		RelSizeEntry *entry;
		bool		found;

		tag.rinfo = rinfo;
		tag.forknum = forknum;
		LWLockAcquire(relsize_lock, LW_EXCLUSIVE);
		entry = hash_search(relsize_hash, &tag, HASH_ENTER, &found);
		if (!found) {
			entry->unlogged = false;
			entry->size = size;

			if (relsize_ctl->size+1 == relsize_hash_size)
			{
				if (dlist_is_empty(&relsize_ctl->lru))
				{
					elog(FATAL, "No more free relsize cache entries");
				}
				else
				{
					RelSizeEntry *victim = dlist_container(RelSizeEntry, lru_node, dlist_pop_head_node(&relsize_ctl->lru));
					hash_search(relsize_hash, &victim->tag, HASH_REMOVE, NULL);
				}
			}
			else
			{
				relsize_ctl->size += 1;
			}
		}
		else
		{
			if (entry->size < size)
				entry->size = size;

			if (!entry->unlogged) /* entries of relation involved in unlogged build are pinned */
			{
				dlist_delete(&entry->lru_node);
			}
		}
		relsize_ctl->writes += 1;
		if (!entry->unlogged) /* entries of relation involved in unlogged build are pinned */
		{
			dlist_push_tail(&relsize_ctl->lru, &entry->lru_node);
		}
		LWLockRelease(relsize_lock);
	}
}

void
forget_cached_relsize(NRelFileInfo rinfo, ForkNumber forknum)
{
	if (relsize_hash_size > 0)
	{
		RelTag		tag;
		RelSizeEntry *entry;
		tag.rinfo = rinfo;
		tag.forknum = forknum;
		LWLockAcquire(relsize_lock, LW_EXCLUSIVE);
		entry = hash_search(relsize_hash, &tag, HASH_REMOVE, NULL);
		if (entry)
		{
			if (!entry->unlogged)
			{
				/* Entried of relations involved in unlogged build are pinned */
				dlist_delete(&entry->lru_node);
			}
			relsize_ctl->size -= 1;
		}
		LWLockRelease(relsize_lock);
	}
}

/*
 * This function starts unlogged build if it was not yet started.
 * The criteria for starting iunlogged build is writing page without normal LSN.
 * It can happen in any backend when page is evicted from shared buffers.
 * Or can not happen at all if index fits in shared buffers.
 *
 * If this function really starts unlogged build, then it returns true, remove entry from LRU list
 * protecting it from eviction until the end of unlogged build.
 * Also it keeps lock on relsize hash. This lock should be later released using resume_unlogged_build().
 * It allows caller to perform some actions
 * in critical section, for example right now it create relation on the disk using mdcreate
 */
bool
start_unlogged_build(NRelFileInfo rinfo, ForkNumber forknum, BlockNumber blocknum, BlockNumber* relsize)
{
	bool start = false;
	if (relsize_hash_size > 0)
	{
		RelTag		tag;
		RelSizeEntry *entry;
		bool		found;

		tag.rinfo = rinfo;
		tag.forknum = forknum;
		LWLockAcquire(relsize_lock, LW_EXCLUSIVE);
		entry = hash_search(relsize_hash, &tag, HASH_ENTER, &found);
		if (!found) {
			*relsize = 0;
			entry->size = blocknum + 1;
			start = true;

			if (relsize_ctl->size+1 == relsize_hash_size)
			{
				if (dlist_is_empty(&relsize_ctl->lru))
				{
					elog(FATAL, "No more free relsize cache entries");
				}
				else
				{
					RelSizeEntry *victim = dlist_container(RelSizeEntry, lru_node, dlist_pop_head_node(&relsize_ctl->lru));
					hash_search(relsize_hash, &victim->tag, HASH_REMOVE, NULL);
				}
			}
			else
			{
				relsize_ctl->size += 1;
			}
		}
		else
		{
			start = !entry->unlogged;

			*relsize = entry->size;
			if (entry->size <= blocknum)
			{
				entry->size = blocknum + 1;
			}

			if (start)
			{
				/* relation involved in unlogged build are pinned until the end of the build */
				dlist_delete(&entry->lru_node);
			}
		}
		entry->unlogged = true;
		relsize_ctl->writes += 1;

		/*
		 * We are not putting entry in LRU least to prevent it fro eviction until the end of unlogged build
		 */

		if (start)
			elog(LOG, "Start unlogged build for %u/%u/%u.%u",
				 RelFileInfoFmt(rinfo), forknum);
	}
	return start;
}

/*
 * Check if unlogged build is in progress.
 * If so, true is returned and lock on relsize cache is hold.
 * It should be later released by calling resume_unlogged_build().
 * It allows to read page from local file without risk that it is removed by stop_unlogged_build by some other backend.
 */
bool
is_unlogged_build(NRelFileInfo rinfo, ForkNumber forknum, BlockNumber* relsize)
{
	bool		unlogged = false;

	if (relsize_hash_size > 0)
	{
		RelTag		tag;
		RelSizeEntry *entry;

		tag.rinfo = rinfo;
		tag.forknum = forknum;
		LWLockAcquire(relsize_lock, LW_SHARED);
		entry = hash_search(relsize_hash, &tag, HASH_FIND, NULL);
		if (entry != NULL)
		{
			unlogged = entry->unlogged;
			*relsize = entry->size;
			relsize_ctl->hits += 1;
		}
		else
		{
			relsize_ctl->misses += 1;
		}
		if (!unlogged)
			LWLockRelease(relsize_lock);
	}
	return unlogged;
}

/*
 * Check if relation is extended during unlogged build.
 * If it is unlogged build, true is returned and lock on relsize cache is hold.
 * It should be later released by calling resume_unlogged_build().
 * It allows to atomically extend local file.
 */
bool
is_unlogged_build_extend(NRelFileInfo rinfo, ForkNumber forknum, BlockNumber blocknum, BlockNumber* relsize)
{
	bool		unlogged = false;

	if (relsize_hash_size > 0)
	{
		RelTag		tag;
		RelSizeEntry *entry;

		tag.rinfo = rinfo;
		tag.forknum = forknum;

		LWLockAcquire(relsize_lock, LW_SHARED);
		entry = hash_search(relsize_hash, &tag, HASH_FIND, NULL);
		if (entry != NULL)
		{
			if (entry->size <= blocknum)
			{
				/* Very rare case: it can happen only if relation is thrown away from relcache before unlogged build is detected */
				/* Repeat search under exclusive lock */
				LWLockRelease(relsize_lock);
				LWLockAcquire(relsize_lock, LW_EXCLUSIVE);
				entry = hash_search(relsize_hash, &tag, HASH_FIND, NULL);
				if (entry == NULL)
				{
					relsize_ctl->misses += 1;
					LWLockRelease(relsize_lock);
					return false;
				}
			}
			unlogged = entry->unlogged;
			*relsize = entry->size;
			if (entry->size <= blocknum)
			{
				entry->size = blocknum + 1;
			}
			relsize_ctl->hits += 1;
		}
		else
		{
			relsize_ctl->misses += 1;
		}
		if (!unlogged)
			LWLockRelease(relsize_lock);
	}
	return unlogged;
}

/*
 * Check if unlogged build is in progress and if so, clear the flag and return entry to LRU list.
 * If it was unlogged build, true is returned and lock on relsize cache is hold.
 * It should be later released by calling resume_unlogged_build().
 * It allows to atomically unlink local file.
 */
bool
stop_unlogged_build(NRelFileInfo rinfo, ForkNumber forknum)
{
	bool		unlogged = false;

	if (relsize_hash_size > 0)
	{
		RelTag		tag;
		RelSizeEntry *entry;

		tag.rinfo = rinfo;
		tag.forknum = forknum;
		LWLockAcquire(relsize_lock, LW_EXCLUSIVE);
		entry = hash_search(relsize_hash, &tag, HASH_FIND, NULL);
		if (entry != NULL)
		{
			unlogged = entry->unlogged;
			entry->unlogged = false;
			relsize_ctl->hits += 1;
			if (unlogged)
			{
				elog(LOG, "Stop unlogged build for %u/%u/%u.%u",
					 RelFileInfoFmt(rinfo), forknum);
				/* Return entry to the LRU list */
				dlist_push_tail(&relsize_ctl->lru, &entry->lru_node);
			}
		}
		else
		{
			relsize_ctl->misses += 1;
		}
		LWLockRelease(relsize_lock);
	}
	return unlogged;
}

/*
 * Release lock obtained by start_unlogged_build or is_unlogged-build functions
 */
void
resume_unlogged_build(void)
{
	if (relsize_hash_size > 0)
		LWLockRelease(relsize_lock);
}


void
relsize_hash_init(void)
{
	DefineCustomIntVariable("neon.relsize_hash_size",
							"Sets the maximum number of cached relation sizes for neon",
							NULL,
							&relsize_hash_size,
							DEFAULT_RELSIZE_HASH_SIZE,
							0,
							INT_MAX,
							PGC_POSTMASTER,
							0,
							NULL, NULL, NULL);

	if (relsize_hash_size > 0)
	{
#if PG_VERSION_NUM >= 150000
		prev_shmem_request_hook = shmem_request_hook;
		shmem_request_hook = relsize_shmem_request;
#else
		RequestAddinShmemSpace(hash_estimate_size(relsize_hash_size, sizeof(RelSizeEntry)));
		RequestNamedLWLockTranche("neon_relsize", 1);
#endif

		prev_shmem_startup_hook = shmem_startup_hook;
		shmem_startup_hook = neon_smgr_shmem_startup;
	}
}

#if PG_VERSION_NUM >= 150000
/*
 * shmem_request hook: request additional shared resources.  We'll allocate or
 * attach to the shared resources in neon_smgr_shmem_startup().
 */
static void
relsize_shmem_request(void)
{
	if (prev_shmem_request_hook)
		prev_shmem_request_hook();

	RequestAddinShmemSpace(sizeof(RelSizeHashControl) + hash_estimate_size(relsize_hash_size, sizeof(RelSizeEntry)));
	RequestNamedLWLockTranche("neon_relsize", 1);
}
#endif

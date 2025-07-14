/*-------------------------------------------------------------------------
 *
 * relkind_cache.c
 *      Cache to track the relkind of relations
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "neon.h"
#include "miscadmin.h"
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

/*
 * The main goal of this cache is to avoid calls of mdexists in neon_write,
 * which is needed to distinguish unlogged relations.
 *
 * This hash is also used to mark relation during unlogged build.
 * It has limited size, implementing eviction based on LRU algorithm.
 * Relations involved in unlogged build are pinned in the cache (assuming that
 * number of concurrent unlogged build is small.
 *
 * Another task of this hash is to prevent race condition during unlogged build termination.
 * Some backend may want to evict page which backenf performing unlogged build can complete it and unlinking local files.
 * We are using shared lock which is hold during all write operation. As far as lock is shared is doesn't prevent concurrent writes.
 * Exclusive lock is taken by unlogged_build_end to change relation kind.
 */

typedef struct
{
	size_t      size;
	uint64		hits;
	uint64		misses;
	uint64		pinned;
	slock_t		mutex;
	dlist_head	lru;			/* double linked list for LRU replacement
								 * algorithm */
} RelKindHashControl;

static HTAB *relkind_hash;
static int	relkind_hash_size;
static RelKindHashControl* relkind_ctl;
static shmem_startup_hook_type prev_shmem_startup_hook = NULL;
#if PG_VERSION_NUM >= 150000
static shmem_request_hook_type prev_shmem_request_hook = NULL;
static void relkind_shmem_request(void);
#endif

LWLockId finish_unlogged_build_lock;

/*
 * Size of a cache entry is 32 bytes. So this default will take about 2 MB,
 * which seems reasonable.
 */
#define DEFAULT_RELKIND_HASH_SIZE (64 * 1024)


/*
 * Callback for shared memory intialization
 */
static void
relkind_cache_startup(void)
{
	static HASHCTL info;
	bool found;

	if (prev_shmem_startup_hook)
		prev_shmem_startup_hook();

	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);
	relkind_ctl = (RelKindHashControl *) ShmemInitStruct("relkind_hash", sizeof(RelKindHashControl), &found);
	if (!found)
	{
		/*
		 * In the worst case, the hash needs to be large enough for the case that all backends are performing an unlogged index build at the same time.
		 * Or actually twice that, because while performing an unlogged index build, each backend can also be trying to write out a page for another
		 * relation and hence hold one more entry in the cache pinned.
		 */
		size_t hash_size = Max(2 * MaxBackends, relkind_hash_size);
		finish_unlogged_build_lock = (LWLockId) GetNamedLWLockTranche("neon_relkind");
		info.keysize = sizeof(NRelFileInfo);
		info.entrysize = sizeof(RelKindEntry);
		relkind_hash = ShmemInitHash("neon_relkind",
									 hash_size, hash_size,
									 &info,
									 HASH_ELEM | HASH_BLOBS);
		SpinLockInit(&relkind_ctl->mutex);
		relkind_ctl->size = 0;
		relkind_ctl->hits = 0;
		relkind_ctl->misses = 0;
		relkind_ctl->pinned = 0;
		dlist_init(&relkind_ctl->lru);
	}
	LWLockRelease(AddinShmemInitLock);
}

/*
 * Lookup existed entry or create new one
 */
static RelKindEntry*
get_pinned_entry(NRelFileInfo rinfo)
{
	bool found;
	RelKindEntry* entry = hash_search(relkind_hash, &rinfo, HASH_ENTER_NULL, &found);

	if (entry == NULL)
	{
		if (dlist_is_empty(&relkind_ctl->lru))
		{
			neon_log(PANIC, "Not unpinned relkind entries");
		}
		else
		{
			bool found;
			/*
			 * Remove least recently used element from the hash.
			 */
			RelKindEntry *victim = dlist_container(RelKindEntry, lru_node, dlist_pop_head_node(&relkind_ctl->lru));
			Assert(victim->access_count == 0);
			hash_search(relkind_hash, &victim->rel, HASH_REMOVE, &found);
			Assert(found);
			Assert(relkind_ctl->size > 0);
			relkind_ctl->size -= 1;
		}
		entry = hash_search(relkind_hash, &rinfo, HASH_ENTER_NULL, &found);
		Assert(!found);
	}
	if (!found)
	{
		entry->relkind = RELKIND_UNKNOWN; /* information about relation kind is not yet available */
		relkind_ctl->pinned += 1;
		entry->access_count = 1;
		relkind_ctl->size += 1;
	}
	else if (entry->access_count++ == 0)
	{
		dlist_delete(&entry->lru_node);
		relkind_ctl->pinned += 1;
	}
	return entry;
}

/*
 * Unpin entry and place it at the end of LRU list
 */
static void
unpin_entry(RelKindEntry *entry)
{
	Assert(entry->access_count != 0);
	if (--entry->access_count == 0)
	{
		Assert(relkind_ctl->pinned != 0);
		relkind_ctl->pinned -= 1;
		dlist_push_tail(&relkind_ctl->lru, &entry->lru_node);
	}
}

/*
 * Intialize new entry. This function is used by neon_start_unlogged_build to mark relation involved in unlogged build.
 * In case of overflow removes least recently used entry.
 * Return pinned entry. It will be released by unpin_cached_relkind at the end of unlogged build.
 */
RelKindEntry*
pin_cached_relkind(NRelFileInfo rinfo, RelKind relkind)
{
	RelKindEntry *entry;

	/* Use spinlock to prevent concurrent hash modifitcation */
	SpinLockAcquire(&relkind_ctl->mutex);
	entry = get_pinned_entry(rinfo);
	entry->relkind = relkind;
	SpinLockRelease(&relkind_ctl->mutex);
	return entry;
}

/*
 * Lookup entry and create new one if not exists. This function is called by neon_write to detenmine if changes should be written to the local disk.
 * In case of overflow removes least recently used entry.
 * If entry is found and its relkind is known, then it is stored in provided location and NULL is returned.
 * If entry is not found then new one is created, pinned and returned. Entry should be updated using store_cached_relkind.
 * Shared lock is obtained if relation is involved in inlogged build.
 */
RelKind
get_cached_relkind(NRelFileInfo rinfo)
{
	RelKindEntry *entry;
	RelKind relkind = RELKIND_UNKNOWN;

	SpinLockAcquire(&relkind_ctl->mutex);
	entry = hash_search(relkind_hash, &rinfo, HASH_FIND, NULL);
	if (entry != NULL)
	{
		/* Do pin+unpin entry to move it to the end of LRU list */
		if (entry->access_count++ == 0)
		{
			dlist_delete(&entry->lru_node);
			relkind_ctl->pinned += 1;
		}
		relkind = entry->relkind;
		unpin_entry(entry);
	}
	SpinLockRelease(&relkind_ctl->mutex);
	return relkind;
}


/*
 * Store relation kind as a result of mdexists check. Unpin entry.
 */
void
set_cached_relkind(NRelFileInfo rinfo, RelKind relkind)
{
	RelKindEntry *entry;

	SpinLockAcquire(&relkind_ctl->mutex);
	/* Do pin+unpin entry to move it to the end of LRU list */
	entry = get_pinned_entry(rinfo);
	Assert(entry->relkind == RELKIND_UNKNOWN || entry->relkind == relkind);
	entry->relkind = relkind;
	unpin_entry(entry);
	SpinLockRelease(&relkind_ctl->mutex);
}

void
unpin_cached_relkind(RelKindEntry* entry)
{
	if (entry)
	{
		SpinLockAcquire(&relkind_ctl->mutex);
		unpin_entry(entry);
		SpinLockRelease(&relkind_ctl->mutex);
	}
}

void
forget_cached_relkind(NRelFileInfo rinfo)
{
	RelKindEntry *entry;
	SpinLockAcquire(&relkind_ctl->mutex);
	entry = hash_search(relkind_hash, &rinfo, HASH_REMOVE, NULL);
	if (entry)
	{
		Assert(entry->access_count == 0);
		dlist_delete(&entry->lru_node);
		relkind_ctl->size -= 1;
	}
	SpinLockRelease(&relkind_ctl->mutex);
}




void
relkind_hash_init(void)
{
	DefineCustomIntVariable("neon.relkind_hash_size",
							"Sets the maximum number of cached relation kinds for neon",
							NULL,
							&relkind_hash_size,
							DEFAULT_RELKIND_HASH_SIZE,
							1,
							INT_MAX,
							PGC_POSTMASTER,
							0,
							NULL, NULL, NULL);

#if PG_VERSION_NUM >= 150000
	prev_shmem_request_hook = shmem_request_hook;
	shmem_request_hook = relkind_shmem_request;
#else
	RequestAddinShmemSpace(hash_estimate_size(relkind_hash_size, sizeof(RelKindEntry)));
	RequestNamedLWLockTranche("neon_relkind", 1);
#endif

	prev_shmem_startup_hook = shmem_startup_hook;
	shmem_startup_hook = relkind_cache_startup;
}

#if PG_VERSION_NUM >= 150000
/*
 * shmem_request hook: request additional shared resources.  We'll allocate or
 * attach to the shared resources in neon_smgr_shmem_startup().
 */
static void
relkind_shmem_request(void)
{
	if (prev_shmem_request_hook)
		prev_shmem_request_hook();

	RequestAddinShmemSpace(sizeof(RelKindHashControl) + hash_estimate_size(relkind_hash_size, sizeof(RelKindEntry)));
	RequestNamedLWLockTranche("neon_relkind", 1);
}
#endif

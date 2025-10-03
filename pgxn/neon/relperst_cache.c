/*-------------------------------------------------------------------------
 *
 * relperst_cache.c
 *      Cache to track the relpersistence of relations
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

#include "miscadmin.h"

/*
 * The main goal of this cache is to avoid repeated calls of mdexists in neon_write,
 * which is needed to distinguish unlogged relations.
 * It has a fixed size, implementing eviction with the LRU algorithm.
 *
 * This hash is also used to mark a relation during an unlogged build.
 * Relations involved in unlogged build are pinned in the cache and never evicted. (Relying 
 * on the fact that the number of concurrent unlogged builds is small). Evicting a page
 * belonging to an unlogged build involves an extra locking step to eliminate a race condition
 * between unlogged build completing and deleted the local file, at the same time that
 * another backend is evicting a page belonging to it. See how `finish_unlogged_build_lock`
 * is used in `neon_write`
 */

typedef struct
{
	size_t      size;
	uint64		hits;
	uint64		misses;
	uint64		pinned;
	dlist_head	lru;			/* double linked list for LRU replacement
								 * algorithm */
} NeonRelPersistenceHashControl;

/*
 * Size of a cache entry is 32 bytes. So this default will take about 0.5 MB,
 * which seems reasonable.
 */
#define DEFAULT_RELPERST_HASH_SIZE (16 * 1024)
#define MAX_RELPERST_HASH_SIZE (1024 * 1024)


static HTAB *relperst_hash;
static int	relperst_hash_size = DEFAULT_RELPERST_HASH_SIZE;
static NeonRelPersistenceHashControl* relperst_ctl;

/* Protects unlogged build completing while another backend is writing to it */ 
LWLockId finish_unlogged_build_lock;
/* Protects 'relperst_hash' */
static LWLockId relperst_hash_lock;

/*
 * Shared memory registration
 */
void
RelperstCacheShmemRequest(void)
{
	RequestAddinShmemSpace(sizeof(NeonRelPersistenceHashControl) + hash_estimate_size(relperst_hash_size, sizeof(NeonRelPersistenceEntry)));
	RequestNamedLWLockTranche("neon_relperst", 2);
}

/*
 * Initialize shared memory
 */
void
RelperstCacheShmemInit(void)
{
	static HASHCTL info;
	bool found;

	relperst_ctl = (NeonRelPersistenceHashControl *) ShmemInitStruct("relperst_hash", sizeof(NeonRelPersistenceHashControl), &found);
	if (!found)
	{
		/*
		 * In the worst case, the hash needs to be large enough for the case that all backends are performing an unlogged index build at the same time.
		 * Or actually twice that, because while performing an unlogged index build, each backend can also be trying to write out a page for another
		 * relation and hence hold one more entry in the cache pinned. Use MaxConnections instead of MaxBackends because only normal backends can perform unlogged build.
		 */
		size_t hash_size = Max(2 * MaxConnections, relperst_hash_size);
		relperst_hash_lock = (LWLockId) GetNamedLWLockTranche("neon_relperst");
		finish_unlogged_build_lock = (LWLockId)(GetNamedLWLockTranche("neon_relperst") + 1);
		info.keysize = sizeof(NRelFileInfo);
		info.entrysize = sizeof(NeonRelPersistenceEntry);
		relperst_hash = ShmemInitHash("neon_relperst",
									 hash_size, hash_size,
									 &info,
									 HASH_ELEM | HASH_BLOBS);
		relperst_ctl->size = 0;
		relperst_ctl->hits = 0;
		relperst_ctl->misses = 0;
		relperst_ctl->pinned = 0;
		dlist_init(&relperst_ctl->lru);
	}
}

/*
 * Lookup existing entry or create a new one
 */
static NeonRelPersistenceEntry*
get_pinned_entry(NRelFileInfo rinfo)
{
	bool found;
	NeonRelPersistenceEntry* entry = hash_search(relperst_hash, &rinfo, HASH_ENTER_NULL, &found);

	if (entry == NULL)
	{
		if (dlist_is_empty(&relperst_ctl->lru))
		{
			/* Cannot happen, because we size the hash table to be large enough for the worst case */
			neon_log(PANIC, "No unpinned relperst entries");
		}
		else
		{
			/*
			 * Remove least recently used element from the hash.
			 */
			NeonRelPersistenceEntry *victim = dlist_container(NeonRelPersistenceEntry, lru_node, dlist_pop_head_node(&relperst_ctl->lru));
			Assert(victim->access_count == 0);
			hash_search(relperst_hash, &victim->rel, HASH_REMOVE, &found);
			Assert(found);
			Assert(relperst_ctl->size > 0);
			relperst_ctl->size -= 1;
		}
		entry = hash_search(relperst_hash, &rinfo, HASH_ENTER_NULL, &found);
		Assert(!found);
	}
	if (!found)
	{
		/* the caller will fill this in by calling set_cached_relperst() later */
		entry->relperst = NEON_RELPERSISTENCE_UNKNOWN;
		relperst_ctl->pinned += 1;
		entry->access_count = 1;
		relperst_ctl->size += 1;
	}
	else if (entry->access_count++ == 0)
	{
		dlist_delete(&entry->lru_node);
		relperst_ctl->pinned += 1;
	}
	return entry;
}

/*
 * Unpin entry and place it at the end of LRU list
 */
static void
unpin_entry(NeonRelPersistenceEntry *entry)
{
	Assert(entry->access_count != 0);
	if (--entry->access_count == 0)
	{
		Assert(relperst_ctl->pinned != 0);
		relperst_ctl->pinned -= 1;
		dlist_push_tail(&relperst_ctl->lru, &entry->lru_node);
	}
}

/*
 * Get existed or intialize new entry. This function is used by neon_start_unlogged_build to mark relation involved in unlogged build.
 * In case of overflow removes least recently used entry.
 * Return pinned entry. It will be released by unpin_cached_relperst at the end of unlogged build.
 */
NeonRelPersistenceEntry*
pin_cached_relperst(NRelFileInfo rinfo, NeonRelPersistence relperst)
{
	NeonRelPersistenceEntry *entry;

	LWLockAcquire(relperst_hash_lock, LW_EXCLUSIVE);

	entry = get_pinned_entry(rinfo);
	entry->relperst = relperst;

	LWLockRelease(relperst_hash_lock);
	return entry;
}

/*
 * Lookup entry or create new one if not exists. This function is called by neon_write to detenmine if changes should be written to the local disk.
 * In case of overflow removes least recently used entry.
 * If relation in involved in unlogged build, the caller should obtain shared lock on `finish_unlogged_build_lock` and recheck
 * state under lock.
 */
NeonRelPersistence
get_cached_relperst(NRelFileInfo rinfo)
{
	NeonRelPersistenceEntry *entry;
	NeonRelPersistence relperst = NEON_RELPERSISTENCE_UNKNOWN;

	/* we don't modify the hash table, but need an exclusive lock to manipulate the LRU list */
	LWLockAcquire(relperst_hash_lock, LW_EXCLUSIVE);

	entry = hash_search(relperst_hash, &rinfo, HASH_FIND, NULL);
	if (entry != NULL)
	{
		/* Do pin+unpin entry to move it to the end of LRU list */
		if (entry->access_count++ == 0)
		{
			dlist_delete(&entry->lru_node);
			relperst_ctl->pinned += 1;
		}
		relperst = entry->relperst;
		unpin_entry(entry);
	}
	LWLockRelease(relperst_hash_lock);
	return relperst;
}


/*
 * Store relation kind as a result of mdexists check.
 */
void
set_cached_relperst(NRelFileInfo rinfo, NeonRelPersistence relperst)
{
	NeonRelPersistenceEntry *entry;

	LWLockAcquire(relperst_hash_lock, LW_EXCLUSIVE);

	/* Do pin+unpin entry to move it to the end of LRU list */
	entry = get_pinned_entry(rinfo);
	Assert(entry->relperst == NEON_RELPERSISTENCE_UNKNOWN || entry->relperst == relperst);
	entry->relperst = relperst;
	unpin_entry(entry);

	LWLockRelease(relperst_hash_lock);
}

/* Release a pin that was acquired earlier with pin_cached_relperst() */
void
unpin_cached_relperst(NeonRelPersistenceEntry* entry)
{
	if (entry)
	{
		LWLockAcquire(relperst_hash_lock, LW_EXCLUSIVE);
		unpin_entry(entry);
		LWLockRelease(relperst_hash_lock);
	}
}

void
forget_cached_relperst(NRelFileInfo rinfo)
{
	NeonRelPersistenceEntry *entry;

	LWLockAcquire(relperst_hash_lock, LW_EXCLUSIVE);

	entry = hash_search(relperst_hash, &rinfo, HASH_REMOVE, NULL);
	if (entry)
	{
		Assert(entry->access_count == 0);
		dlist_delete(&entry->lru_node);
		relperst_ctl->size -= 1;
	}

	LWLockRelease(relperst_hash_lock);
}




void
relperst_hash_init(void)
{
	DefineCustomIntVariable("neon.relperst_hash_size",
							"Sets the maximum number of cached relation persistence for neon",
							NULL,
							&relperst_hash_size,
							DEFAULT_RELPERST_HASH_SIZE,
							1,
							MAX_RELPERST_HASH_SIZE,
							PGC_POSTMASTER,
							0,
							NULL, NULL, NULL);
}

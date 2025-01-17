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
	BlockNumber size;
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
		/* We need exclusive lock here because of LRU list manipulation */
		LWLockAcquire(relsize_lock, LW_EXCLUSIVE);
		entry = hash_search(relsize_hash, &tag, HASH_FIND, NULL);
		if (entry != NULL)
		{
			*size = entry->size;
			relsize_ctl->hits += 1;
			found = true;
			/* Move entry to the LRU list tail */
			dlist_delete(&entry->lru_node);
			dlist_push_tail(&relsize_ctl->lru, &entry->lru_node);
		}
		else
		{
			relsize_ctl->misses += 1;
		}
		LWLockRelease(relsize_lock);
	}
	return found;
}

void
set_cached_relsize(NRelFileInfo rinfo, ForkNumber forknum, BlockNumber size)
{
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
			RelSizeEntry *victim = dlist_container(RelSizeEntry, lru_node, dlist_pop_head_node(&relsize_ctl->lru));
			hash_search(relsize_hash, &victim->tag, HASH_REMOVE, NULL);
			Assert(relsize_ctl->size > 0);
			relsize_ctl->size -= 1;
		}
		entry->size = size;
		if (!found)
		{
			if (++relsize_ctl->size == relsize_hash_size)
			{
				/*
				 * Remove least recently used elment from the hash.
				 * Hash size after is becomes `relsize_hash_size-1`.
				 * But it is not considered to be a problem, because size of this hash is expecrted large enough and +-1 doesn't matter.
				 */
				RelSizeEntry *victim = dlist_container(RelSizeEntry, lru_node, dlist_pop_head_node(&relsize_ctl->lru));
				hash_search(relsize_hash, &victim->tag, HASH_REMOVE, NULL);
				relsize_ctl->size -= 1;
			}
		}
		else
		{
			dlist_delete(&entry->lru_node);
		}
		dlist_push_tail(&relsize_ctl->lru, &entry->lru_node);
		relsize_ctl->writes += 1;
		LWLockRelease(relsize_lock);
	}
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
		if (!found || entry->size < size)
			entry->size = size;
		if (!found)
		{
			if (++relsize_ctl->size == relsize_hash_size)
			{
				RelSizeEntry *victim = dlist_container(RelSizeEntry, lru_node, dlist_pop_head_node(&relsize_ctl->lru));
				hash_search(relsize_hash, &victim->tag, HASH_REMOVE, NULL);
				relsize_ctl->size -= 1;
			}
		}
		else
		{
			dlist_delete(&entry->lru_node);
		}
		relsize_ctl->writes += 1;
		dlist_push_tail(&relsize_ctl->lru, &entry->lru_node);
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
			dlist_delete(&entry->lru_node);
			relsize_ctl->size -= 1;
		}
		LWLockRelease(relsize_lock);
	}
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

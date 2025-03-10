#include "postgres.h"

#include "neon_lwlc.h"
#if PG_MAJORVERSION_NUM >= 17

#include "access/xlog.h"
#include "storage/ipc.h"
#include "storage/shmem.h"
#include "storage/buf_internals.h"
#include "utils/guc.h"
#include "utils/hsearch.h"



typedef struct LastWrittenLsnCacheEntry
{
	BufferTag	key;
	XLogRecPtr	lsn;
	/* double linked list for LRU replacement algorithm */
	dlist_node	lru_node;
} LastWrittenLsnCacheEntry;

typedef struct LwLsnCacheCtl {
	int lastWrittenLsnCacheSize;
	/*
	* Maximal last written LSN for pages not present in lastWrittenLsnCache
	*/
	XLogRecPtr  maxLastWrittenLsn;

	/*
	* Double linked list to implement LRU replacement policy for last written LSN cache.
	* Access to this list as well as to last written LSN cache is protected by 'LastWrittenLsnLock'.
	*/
	dlist_head lastWrittenLsnLRU;
} LwLsnCacheCtl;


/*
 * Cache of last written LSN for each relation page.
 * Also to provide request LSN for smgrnblocks, smgrexists there is pseudokey=InvalidBlockId which stores LSN of last
 * relation metadata update.
 * Size of the cache is limited by GUC variable lastWrittenLsnCacheSize ("lsn_cache_size"),
 * pages are replaced using LRU algorithm, based on L2-list.
 * Access to this cache is protected by 'LastWrittenLsnLock'.
 */
static HTAB *lastWrittenLsnCache;

LwLsnCacheCtl* LwLsnCache;

static int lwlsn_cache_size; 


static void
lwlc_register_gucs(void)
{
	DefineCustomIntVariable("neon.last_written_lsn_cache_size",
							"Size of last written LSN cache used by Neon",
							NULL,
							&lwlsn_cache_size,
							(128*1024), 1024, INT_MAX,
							PGC_POSTMASTER,
							0, /* plain units */
							NULL, NULL, NULL);
}

static XLogRecPtr SetLastWrittenLSNForBlockRangeInternal(XLogRecPtr lsn,
														 NRelFileInfo rlocator,
														 ForkNumber forknum,
														 BlockNumber from,
														 BlockNumber n_blocks);

/* All the necessary hooks are defined here */
 // TODO: Need to see how to define this
void lwlc_pre_recovery_start_hook(const ControlFileData* controlFile);

// Note: these are the previous hooks
static get_lwlsn_hook_type prev_get_lwlsn_hook = NULL;
static get_lwlsn_v_hook_type prev_get_lwlsn_v_hook = NULL;
static set_lwlsn_block_range_hook_type prev_set_lwlsn_block_range_hook = NULL;
static set_lwlsn_block_v_hook_type prev_set_lwlsn_block_v_hook = NULL;
static set_lwlsn_block_hook_type prev_set_lwlsn_block_hook = NULL;
static set_lwlsn_relation_hook_type prev_set_lwlsn_relation_hook = NULL;
static set_lwlsn_db_hook_type prev_set_lwlsn_db_hook = NULL;

static shmem_startup_hook_type prev_shmem_startup_hook;
static shmem_request_hook_type prev_shmem_request_hook;

static void shmemrequest(void);
static void shmeminit(void);

void
init_lwlc(void)
{
	if (!process_shared_preload_libraries_in_progress)
		return;
	
	lwlc_register_gucs();

	prev_shmem_startup_hook = shmem_startup_hook;
	shmem_startup_hook = shmeminit;
	prev_shmem_request_hook = shmem_request_hook;
	shmem_request_hook = shmemrequest;
	
	prev_get_lwlsn_hook = get_lwlsn_hook;
	get_lwlsn_hook = neon_get_lwlsn;
	prev_get_lwlsn_v_hook = get_lwlsn_v_hook;
	get_lwlsn_v_hook = neon_get_lwlsn_v;
	prev_set_lwlsn_block_range_hook = set_lwlsn_block_range_hook;
	set_lwlsn_block_range_hook = neon_set_lwlsn_block_range;
	prev_set_lwlsn_block_v_hook = set_lwlsn_block_v_hook;
	set_lwlsn_block_v_hook = neon_set_lwlsn_block_v;
	prev_set_lwlsn_block_hook = set_lwlsn_block_hook;
	set_lwlsn_block_hook = neon_set_lwlsn_block;
	prev_set_lwlsn_relation_hook = set_lwlsn_relation_hook;
	set_lwlsn_relation_hook = neon_set_lwlsn_relation;
	prev_set_lwlsn_db_hook = set_lwlsn_db_hook;
	set_lwlsn_db_hook = neon_set_lwlsn_db;
}

static void shmemrequest(void) {
	Size requested_size = sizeof(LwLsnCacheCtl);

	#if PG_VERSION_NUM >= 150000
		if (prev_shmem_request_hook)
			prev_shmem_request_hook();
	#endif

		requested_size += hash_estimate_size(lwlsn_cache_size, sizeof(LastWrittenLsnCacheEntry));

		RequestAddinShmemSpace(requested_size);
}

static void shmeminit(void) {
	static HASHCTL info;
	bool found;
	if (prev_shmem_startup_hook) {
		prev_shmem_startup_hook();
	}
	if (lwlsn_cache_size > 0)
	{
		info.keysize = sizeof(BufferTag);
		info.entrysize = sizeof(LastWrittenLsnCacheEntry);
		lastWrittenLsnCache = ShmemInitHash("last_written_lsn_cache",
			lwlsn_cache_size, lwlsn_cache_size,
										&info,
										HASH_ELEM | HASH_BLOBS);
		LwLsnCache = ShmemInitStruct("neon/LwLsnCacheCtl", sizeof(LwLsnCacheCtl), &found);
		// Now set the size in the struct
		LwLsnCache->lastWrittenLsnCacheSize = lwlsn_cache_size;
		if (found) {
			return;
		}
	}
	dlist_init(&LwLsnCache->lastWrittenLsnLRU);
    LwLsnCache->maxLastWrittenLsn = GetRedoRecPtr();
}

/*
 * neon_get_lwlsn -- Returns maximal LSN of written page.
 * It returns an upper bound for the last written LSN of a given page,
 * either from a cached last written LSN or a global maximum last written LSN.
 * If rnode is InvalidOid then we calculate maximum among all cached LSN and maxLastWrittenLsn.
 * If cache is large enough, iterating through all hash items may be rather expensive.
 * But neon_get_lwlsn(InvalidOid) is used only by neon_dbsize which is not performance critical.
 */
XLogRecPtr
neon_get_lwlsn(NRelFileInfo rlocator, ForkNumber forknum, BlockNumber blkno)
{
	XLogRecPtr lsn;
	LastWrittenLsnCacheEntry* entry;

	Assert(LwLsnCache->lastWrittenLsnCacheSize != 0);

	LWLockAcquire(LastWrittenLsnLock, LW_SHARED);

	/* Maximal last written LSN among all non-cached pages */
	lsn = LwLsnCache->maxLastWrittenLsn;

	if (rlocator.relNumber != InvalidOid)
	{
		BufferTag key;
		key.spcOid = rlocator.spcOid;
		key.dbOid = rlocator.dbOid;
		key.relNumber = rlocator.relNumber;
		key.forkNum = forknum;
		key.blockNum = blkno;
		entry = hash_search(lastWrittenLsnCache, &key, HASH_FIND, NULL);
		if (entry != NULL)
			lsn = entry->lsn;
		else
		{
			LWLockRelease(LastWrittenLsnLock);
			LWLockAcquire(LastWrittenLsnLock, LW_EXCLUSIVE);
			/*
			 * In case of statements CREATE TABLE AS SELECT... or INSERT FROM SELECT... we are fetching data from source table
			 * and storing it in destination table. It cause problems with prefetch last-written-lsn is known for the pages of
			 * source table (which for example happens after compute restart). In this case we get get global value of
			 * last-written-lsn which is changed frequently as far as we are writing pages of destination table.
			 * As a result request-lsn for the prefetch and request-let when this page is actually needed are different
			 * and we got exported prefetch request. So it actually disarms prefetch.
			 * To prevent that, we re-insert the page with the latest LSN, so that it's
			 * less likely the LSN for this page will get evicted from the LwLsnCache
			 * before the page is read.
			 */
			 lsn = SetLastWrittenLSNForBlockRangeInternal(lsn, rlocator, forknum, blkno, 1);
		}
	}
	else
	{
		HASH_SEQ_STATUS seq;
		/* Find maximum of all cached LSNs */
		hash_seq_init(&seq, lastWrittenLsnCache);
		while ((entry = (LastWrittenLsnCacheEntry *) hash_seq_search(&seq)) != NULL)
		{
			if (entry->lsn > lsn)
				lsn = entry->lsn;
		}
	}
	LWLockRelease(LastWrittenLsnLock);

	return lsn;
}

/*
 * GetLastWrittenLSN -- Returns maximal LSN of written page.
 * It returns an upper bound for the last written LSN of a given page,
 * either from a cached last written LSN or a global maximum last written LSN.
 * If rnode is InvalidOid then we calculate maximum among all cached LSN and maxLastWrittenLsn.
 * If cache is large enough, iterating through all hash items may be rather expensive.
 * But GetLastWrittenLSN(InvalidOid) is used only by neon_dbsize which is not performance critical.
 */
void
neon_get_lwlsn_v(NRelFileInfo relfilenode, ForkNumber forknum,
				   BlockNumber blkno, int nblocks, XLogRecPtr *lsns)
{
	LastWrittenLsnCacheEntry* entry;
	XLogRecPtr lsn;

	Assert(LwLsnCache->lastWrittenLsnCacheSize != 0);
	Assert(nblocks > 0);
	Assert(PointerIsValid(lsns));

	LWLockAcquire(LastWrittenLsnLock, LW_SHARED);

	if (relfilenode.relNumber != InvalidOid)
	{
		BufferTag key;
		bool missed_keys = false;

		key.spcOid = relfilenode.spcOid;
		key.dbOid = relfilenode.dbOid;
		key.relNumber = relfilenode.relNumber;
		key.forkNum = forknum;

		for (int i = 0; i < nblocks; i++)
		{
			/* Maximal last written LSN among all non-cached pages */
			key.blockNum = blkno + i;

			entry = hash_search(lastWrittenLsnCache, &key, HASH_FIND, NULL);
			if (entry != NULL)
			{
 				lsns[i] = entry->lsn;
			}
			else
			{
				/* Mark this block's LSN as missing - we'll update the LwLSN for missing blocks in bulk later */
				lsns[i] = InvalidXLogRecPtr;
				missed_keys = true;
			}
		}

		/*
		 * If we had any missing LwLSN entries, we add the missing ones now.
		 * By doing the insertions in one batch, we decrease lock contention.
		 */
		if (missed_keys)
		{
			LWLockRelease(LastWrittenLsnLock);
			LWLockAcquire(LastWrittenLsnLock, LW_EXCLUSIVE);

			lsn = LwLsnCache->maxLastWrittenLsn;

			for (int i = 0; i < nblocks; i++)
			{
				if (lsns[i] == InvalidXLogRecPtr)
				{
					lsns[i] = lsn;
					SetLastWrittenLSNForBlockRangeInternal(lsn, relfilenode, forknum, blkno + i, 1);
				}
			}
		}
	}
	else
	{
		HASH_SEQ_STATUS seq;
		lsn = LwLsnCache->maxLastWrittenLsn;
		/* Find maximum of all cached LSNs */
		hash_seq_init(&seq, lastWrittenLsnCache);
		while ((entry = (LastWrittenLsnCacheEntry *) hash_seq_search(&seq)) != NULL)
		{
			if (entry->lsn > lsn)
				lsn = entry->lsn;
		}

		for (int i = 0; i < nblocks; i++)
			lsns[i] = lsn;
	}
	LWLockRelease(LastWrittenLsnLock);
}

/*
 * Guts for SetLastWrittenLSNForBlockRange.
 * Caller must ensure LastWrittenLsnLock is held in exclusive mode.
 */
static XLogRecPtr
SetLastWrittenLSNForBlockRangeInternal(XLogRecPtr lsn,
									   NRelFileInfo rlocator,
									   ForkNumber forknum,
									   BlockNumber from,
									   BlockNumber n_blocks)
{
	if (rlocator.relNumber == InvalidOid)
	{
		if (lsn > LwLsnCache->maxLastWrittenLsn)
		LwLsnCache->maxLastWrittenLsn = lsn;
		else
			lsn = LwLsnCache->maxLastWrittenLsn;
	}
	else
	{
		LastWrittenLsnCacheEntry* entry;
		BufferTag key;
		bool found;
		BlockNumber i;

		key.spcOid = rlocator.spcOid;
		key.dbOid = rlocator.dbOid;
		key.relNumber = rlocator.relNumber;
		key.forkNum = forknum;
		for (i = 0; i < n_blocks; i++)
		{
			key.blockNum = from + i;
			entry = hash_search(lastWrittenLsnCache, &key, HASH_ENTER, &found);
			if (found)
			{
				if (lsn > entry->lsn)
					entry->lsn = lsn;
				else
					lsn = entry->lsn;
				/* Unlink from LRU list */
				dlist_delete(&entry->lru_node);
			}
			else
			{
				entry->lsn = lsn;
				if (hash_get_num_entries(lastWrittenLsnCache) > LwLsnCache->lastWrittenLsnCacheSize)
				{
					/* Replace least recently used entry */
					LastWrittenLsnCacheEntry* victim = dlist_container(LastWrittenLsnCacheEntry, lru_node, dlist_pop_head_node(&LwLsnCache->lastWrittenLsnLRU));
					/* Adjust max LSN for not cached relations/chunks if needed */
					if (victim->lsn > LwLsnCache->maxLastWrittenLsn)
					LwLsnCache->maxLastWrittenLsn = victim->lsn;

					hash_search(lastWrittenLsnCache, victim, HASH_REMOVE, NULL);
				}
			}
			/* Link to the end of LRU list */
			dlist_push_tail(&LwLsnCache->lastWrittenLsnLRU, &entry->lru_node);
		}
	}
	return lsn;
}

/*
 * SetLastWrittenLSNForBlockRange -- Set maximal LSN of written page range.
 * We maintain cache of last written LSNs with limited size and LRU replacement
 * policy. Keeping last written LSN for each page allows to use old LSN when
 * requesting pages of unchanged or appended relations. Also it is critical for
 * efficient work of prefetch in case massive update operations (like vacuum or remove).
 *
 * rlocator.relNumber can be InvalidOid, in this case maxLastWrittenLsn is updated.
 * SetLastWrittenLsn with dummy rlocator is used by createdb and dbase_redo functions.
 */
XLogRecPtr
neon_set_lwlsn_block_range(XLogRecPtr lsn, NRelFileInfo rlocator, ForkNumber forknum, BlockNumber from, BlockNumber n_blocks)
{
	if (lsn == InvalidXLogRecPtr || n_blocks == 0 || LwLsnCache->lastWrittenLsnCacheSize == 0)
		return lsn;

	LWLockAcquire(LastWrittenLsnLock, LW_EXCLUSIVE);
	lsn = SetLastWrittenLSNForBlockRangeInternal(lsn, rlocator, forknum, from, n_blocks);
	LWLockRelease(LastWrittenLsnLock);

	return lsn;
}

/*
 * neon_set_lwlsn_block_v -- Set maximal LSN of pages to their respective
 * LSNs.
 *
 * We maintain cache of last written LSNs with limited size and LRU replacement
 * policy. Keeping last written LSN for each page allows to use old LSN when
 * requesting pages of unchanged or appended relations. Also it is critical for
 * efficient work of prefetch in case massive update operations (like vacuum or remove).
 */
XLogRecPtr
neon_set_lwlsn_block_v(const XLogRecPtr *lsns, NRelFileInfo relfilenode,
						   ForkNumber forknum, BlockNumber blockno,
						   int nblocks)
{
	LastWrittenLsnCacheEntry* entry;
	BufferTag	key;
	bool		found;
	XLogRecPtr	max = InvalidXLogRecPtr;

	if (lsns == NULL || nblocks == 0 || LwLsnCache->lastWrittenLsnCacheSize == 0 ||
		relfilenode.relNumber == InvalidOid)
		return InvalidXLogRecPtr;

	key.relNumber = relfilenode.relNumber;
	key.dbOid = relfilenode.dbOid;
	key.spcOid = relfilenode.spcOid;
	key.forkNum = forknum;

	LWLockAcquire(LastWrittenLsnLock, LW_EXCLUSIVE);

	for (int i = 0; i < nblocks; i++)
	{
		XLogRecPtr	lsn = lsns[i];

		key.blockNum = blockno + i;
		entry = hash_search(lastWrittenLsnCache, &key, HASH_ENTER, &found);
		if (found)
		{
			if (lsn > entry->lsn)
				entry->lsn = lsn;
			else
				lsn = entry->lsn;
			/* Unlink from LRU list */
			dlist_delete(&entry->lru_node);
		}
		else
		{
			entry->lsn = lsn;
			if (hash_get_num_entries(lastWrittenLsnCache) > LwLsnCache->lastWrittenLsnCacheSize)
			{
				/* Replace least recently used entry */
				LastWrittenLsnCacheEntry* victim = dlist_container(LastWrittenLsnCacheEntry, lru_node, dlist_pop_head_node(&LwLsnCache->lastWrittenLsnLRU));
				/* Adjust max LSN for not cached relations/chunks if needed */
				if (victim->lsn > LwLsnCache->maxLastWrittenLsn)
				LwLsnCache->maxLastWrittenLsn = victim->lsn;

				hash_search(lastWrittenLsnCache, victim, HASH_REMOVE, NULL);
			}
		}
		/* Link to the end of LRU list */
		dlist_push_tail(&LwLsnCache->lastWrittenLsnLRU, &entry->lru_node);
		max = Max(max, lsn);
	}

	LWLockRelease(LastWrittenLsnLock);

	return max;
}

/*
 * SetLastWrittenLSNForBlock -- Set maximal LSN for block
 */
XLogRecPtr
neon_set_lwlsn_block(XLogRecPtr lsn, NRelFileInfo rlocator, ForkNumber forknum, BlockNumber blkno)
{
	return neon_set_lwlsn_block_range(lsn, rlocator, forknum, blkno, 1);
}

/*
 * neon_set_lwlsn_relation -- Set maximal LSN for relation metadata
 */
XLogRecPtr
neon_set_lwlsn_relation(XLogRecPtr lsn, NRelFileInfo rlocator, ForkNumber forknum)
{
	return neon_set_lwlsn_block(lsn, rlocator, forknum, REL_METADATA_PSEUDO_BLOCKNO);
}

/*
 * neon_set_lwlsn_db -- Set maximal LSN for the whole database
 */
XLogRecPtr
neon_set_lwlsn_db(XLogRecPtr lsn)
{
	NRelFileInfo dummyNode = {InvalidOid, InvalidOid, InvalidOid};
	return neon_set_lwlsn_block(lsn, dummyNode, MAIN_FORKNUM, 0);
}

#else /* ! PG_MAJORVERSION_NUM == 17 */

#endif

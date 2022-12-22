/*
 * LastWrittenLsn cache
 * 
 * It contains 2 systems:
 * 
 * 1. A hashmap cache that contains evicted pages' buffer tags with their
 *	  LSNs, with eviction by lowest LSN made efficient by a pairingheap
 *	  constructed in that cache
 * 2. A single watermark LSN that is the highest LSN evicted from the cache in
 * 	  1). 
 */

#include "postgres.h"

#include "access/xlog.h"
#include "catalog/pg_control.h"
#include "lib/pairingheap.h"
#include "storage/ipc.h"
#include "storage/shmem.h"
#include "storage/lwlock.h"
#include "utils/guc.h"
#include "utils/hsearch.h"

#if PG_VERSION_NUM >= 150000
#include "access/xlogrecovery.h"
#endif

#include "neon.h"
#include "neon_lwlc.h"
#include "miscadmin.h"

static shmem_startup_hook_type prev_shmem_startup_hook;
#if PG_VERSION_NUM >= 150000
static shmem_request_hook_type prev_shmem_request_hook = NULL;
static void relsize_shmem_request(void);
#endif

typedef enum LwLsnCacheRequest {
	LWLSN_BLOCK,
	LWLSN_RELFORK,
	LWLSN_DB,
	LWLSN_CLUSTER,
} LwLsnCacheRequest;

typedef struct LwLsnCacheEntryKeyData {
	RelFileNode	rnode;
	ForkNumber	forkNum;
	BlockNumber	blockNum;
} LwLsnCacheEntryKeyData;

typedef struct LwLsnCacheEntryData {
	LwLsnCacheEntryKeyData key;
	XLogRecPtr	lsn;
	pairingheap_node pheapnode;
} LwLsnCacheEntryData;

typedef struct LwLsnCacheData {
	pairingheap	pheap;
	bool		initialized;
	int			n_cached_entries;
	volatile XLogRecPtr	highWaterMark;
	volatile XLogRecPtr lowWaterMark;
} LwLsnCacheData;

int lsn_cache_size;

LwLsnCacheData *LwLsnCache;

LWLockPadded *LwLsnCacheLockTranche;
#define LwLsnLockTrancheName "neon/LwLsnCache"
#define LwLsnMetadataLock (&LwLsnCacheLockTranche[0].lock)

#define NUM_LWLSN_LOCKS 1

#define INSTALL_HOOK(name, function) \
do { \
	prev_##name = (name); \
	name = &(function); \
} while (false)

#define FORWARD_HOOK_CALL(name, ...) \
do { \
	if (prev_##name) \
		prev_##name(__VA_ARGS__); \
} while (false)



HTAB *LwLsnCacheTable;
xlog_recovery_hook_type prev_xlog_pre_recovery_start_hook = NULL;

void lwlc_pre_recovery_start_hook(const ControlFileData* controlFile);

static void lwlc_register_gucs(void);
static void lwlc_request_shmem(void);
static void lwlc_setup_shmem(void);
static int lwlc_pheap_comparefunc(const pairingheap_node *a,
								  const pairingheap_node *b,
								  void *arg);

static XLogRecPtr lwlc_lookup_last_lsn(LwLsnCacheRequest lookup_type,
									   RelFileNode node, ForkNumber fork,
									   BlockNumber blkno,
									   XLogRecPtr *effective);
static bool lwlc_should_insert(XLogRecPtr lsn);
static void lwlc_insert_last_lsn(XLogRecPtr lsn,
								 RelFileNode node, ForkNumber fork,
								 BlockNumber min_blkno, BlockNumber max_blockno);

xlog_set_lwlf_block_hook_type		prev_xlog_set_lwlf_block_hook = NULL;
xlog_set_lwlf_blockrange_hook_type	prev_xlog_set_lwlf_blockrange_hook = NULL;
xlog_set_lwlf_relation_hook_type	prev_xlog_set_lwlf_relation_hook = NULL;
xlog_set_lwlf_database_hook_type	prev_xlog_set_lwlf_database_hook = NULL;
xlog_set_lwlf_dbcluster_hook_type	prev_xlog_set_lwlf_dbcluster_hook = NULL;

void
pg_init_lwlc(void)
{
	if (!process_shared_preload_libraries_in_progress)
		return;

	lwlc_register_gucs();

	INSTALL_HOOK(shmem_startup_hook, lwlc_setup_shmem);

	INSTALL_HOOK(xlog_set_lwlf_block_hook, neon_set_lwlf_block_hook);
	INSTALL_HOOK(xlog_set_lwlf_blockrange_hook, neon_set_lwlf_blockrange_hook);
	INSTALL_HOOK(xlog_set_lwlf_relation_hook, neon_set_lwlf_relation_hook);
	INSTALL_HOOK(xlog_set_lwlf_database_hook, neon_set_lwlf_database_hook);
	INSTALL_HOOK(xlog_set_lwlf_dbcluster_hook, neon_set_lwlf_dbcluster_hook);

#if PG_VERSION_NUM >= 150000
	INSTALL_HOOK(shmem_request_hook, lwlc_request_shmem);
#else
	lwlc_request_shmem();
#endif

	INSTALL_HOOK(xlog_pre_recovery_start_hook, lwlc_pre_recovery_start_hook);
}

static void
lwlc_register_gucs(void)
{
	DefineCustomIntVariable("neon.lwlsn_cache_size",
							"Size of last written LSN cache used by Neon",
							NULL,
							&lsn_cache_size,
							(128*1024), -1, INT_MAX,
							PGC_POSTMASTER,
							0, /* plain units */
							NULL, NULL, NULL);
}

static void
lwlc_request_shmem(void)
{
	Size requested_size = sizeof(LwLsnCacheData);

#if PG_VERSION_NUM >= 150000
	if (prev_shmem_request_hook)
		prev_shmem_request_hook();
#endif

	requested_size += hash_estimate_size(lsn_cache_size, sizeof(LwLsnCacheEntryData));

	RequestAddinShmemSpace(requested_size);

	RequestNamedLWLockTranche(LwLsnLockTrancheName, NUM_LWLSN_LOCKS);
}

static void
lwlc_setup_shmem(void)
{
	static HASHCTL info;
	XLogRecPtr	startup_lsn;
	bool		found;

	if (prev_shmem_startup_hook)
		prev_shmem_startup_hook();

	info.keysize = sizeof(LwLsnCacheEntryKeyData);
	info.entrysize = sizeof(LwLsnCacheEntryData);

	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);

	LwLsnCache = ShmemInitStruct("neon/LwLsnCache", sizeof(LwLsnCacheData), &found);

	LwLsnCacheLockTranche = GetNamedLWLockTranche(LwLsnLockTrancheName);

	LwLsnCacheTable = ShmemInitHash("neon/LwLsnHashTable",
									lsn_cache_size, lsn_cache_size,
									&info,
									HASH_ELEM | HASH_BLOBS);

	if (found)
	{
		LWLockRelease(AddinShmemInitLock);
		return;
	}

	MemSet(LwLsnCache, 0, sizeof(LwLsnCacheData));

	/*
	 * XXX: Manual modification of these fields, because no shared allocation
	 * method exists for pheap.
	 */
	LwLsnCache->pheap.ph_compare = &lwlc_pheap_comparefunc;
	LwLsnCache->pheap.ph_arg = NULL;
	LwLsnCache->pheap.ph_root = NULL;
	LwLsnCache->n_cached_entries = 0;

	/*
	 * We will initiate the high/low watermarks with startup parameters when
	 * the startup process completes, so those are intentionally left blank.
	 */
	LwLsnCache->initialized = false;

	LWLockRelease(AddinShmemInitLock);
}

void
lwlc_pre_recovery_start_hook(const ControlFileData* controlFile)
{
	XLogRecPtr startup_lsn;

	FORWARD_HOOK_CALL(xlog_pre_recovery_start_hook, controlFile);

	Assert(LwLsnCache != NULL);
	Assert(LwLsnCache->initialized == false);
	LWLockAcquire(LwLsnMetadataLock, LW_EXCLUSIVE);

	startup_lsn = controlFile->checkPointCopy.redo;

	Assert(startup_lsn != InvalidXLogRecPtr);
	Assert(LwLsnCache->n_cached_entries == 0);

	LwLsnCache->highWaterMark = startup_lsn;
	LwLsnCache->lowWaterMark = startup_lsn;
	LwLsnCache->initialized = true;

	LWLockRelease(LwLsnMetadataLock);
}




int lwlc_pheap_comparefunc(const pairingheap_node *a, const pairingheap_node *b, void *arg) {
	const LwLsnCacheEntryData *a_entry;
	const LwLsnCacheEntryData *b_entry;

	a_entry = pairingheap_const_container(LwLsnCacheEntryData, pheapnode, a);
	b_entry = pairingheap_const_container(LwLsnCacheEntryData, pheapnode, b);

	if (a_entry->lsn < b_entry->lsn)
		return -1;
	else if (a_entry->lsn > b_entry->lsn)
		return 1;
	else
		return 0;
}


XLogRecPtr
lwlc_get_max_evicted(void)
{
	return LwLsnCache->highWaterMark;
}


static XLogRecPtr
lwlc_lookup_last_lsn(LwLsnCacheRequest lookup_type, RelFileNode node,
					 ForkNumber fork, BlockNumber blkno,
					 XLogRecPtr *effective)
{
	uint32		block_hash,
				relfork_hash,
				db_hash,
				cluster_hash;
	bool		found;
	XLogRecPtr	lsn;
	XLogRecPtr	local_effective;
	LwLsnCacheEntryKeyData key = {0};
	LwLsnCacheEntryData *entry;

	if (lsn_cache_size <= 0)
		return InvalidXLogRecPtr;

	/*
	 * By now we should have run recovery initialization in the startup process,
	 * so this must be initialized.
	 */
	Assert(LwLsnCache->initialized);

	switch (lookup_type) {
	case LWLSN_BLOCK:
		MemSet(&key, 0, sizeof(LwLsnCacheEntryKeyData));
		key.blockNum = blkno;
		key.rnode = node;
		key.forkNum = fork;
		block_hash = get_hash_value(LwLsnCacheTable, &key);
		/* fallthrough */
	case LWLSN_RELFORK:
		MemSet(&key, 0, sizeof(LwLsnCacheEntryKeyData));
		key.rnode = node;
		key.forkNum = fork;
		key.blockNum = InvalidBlockNumber;
		relfork_hash = get_hash_value(LwLsnCacheTable, &key);
		/* fallthrough */
	case LWLSN_DB:
		MemSet(&key, 0, sizeof(LwLsnCacheEntryKeyData));
		key.rnode.dbNode = node.dbNode;
		key.rnode.spcNode = InvalidOid;
		key.rnode.relNode = InvalidOid;
		key.blockNum = InvalidBlockNumber;
		key.forkNum = InvalidForkNumber;
		db_hash = get_hash_value(LwLsnCacheTable, &key);
		/* fallthrough */
	case LWLSN_CLUSTER:
		MemSet(&key, 0, sizeof(LwLsnCacheEntryKeyData));
		key.rnode.dbNode = InvalidOid;
		key.rnode.spcNode = InvalidOid;
		key.rnode.relNode = InvalidOid;
		key.blockNum = InvalidBlockNumber;
		key.forkNum = InvalidForkNumber;
		cluster_hash = get_hash_value(LwLsnCacheTable, &key);
		/* fallthrough */
	}

	LWLockAcquire(LwLsnMetadataLock, LW_SHARED);

	entry = (LwLsnCacheEntryData *)
		hash_search_with_hash_value(LwLsnCacheTable, &key, block_hash,
									HASH_FIND,
									NULL);

	switch (lookup_type) {
	case LWLSN_BLOCK:
		MemSet(&key, 0, sizeof(LwLsnCacheEntryKeyData));
		key.blockNum = blkno;
		key.rnode = node;
		key.forkNum = fork;
		entry = (LwLsnCacheEntryData *)
			hash_search_with_hash_value(LwLsnCacheTable, &key, block_hash,
										HASH_FIND,
										NULL);
		if (entry)
			break;
		/* fallthrough */
	case LWLSN_RELFORK:
		MemSet(&key, 0, sizeof(LwLsnCacheEntryKeyData));
		key.rnode = node;
		key.forkNum = fork;
		key.blockNum = InvalidBlockNumber;
		entry = (LwLsnCacheEntryData *)
			hash_search_with_hash_value(LwLsnCacheTable, &key, relfork_hash,
										HASH_FIND,
										NULL);
		if (entry)
			break;
		/* fallthrough */
	case LWLSN_DB:
		MemSet(&key, 0, sizeof(LwLsnCacheEntryKeyData));
		key.rnode.dbNode = node.dbNode;
		key.rnode.spcNode = InvalidOid;
		key.rnode.relNode = InvalidOid;
		key.blockNum = InvalidBlockNumber;
		key.forkNum = InvalidForkNumber;
		entry = (LwLsnCacheEntryData *)
			hash_search_with_hash_value(LwLsnCacheTable, &key, db_hash,
										HASH_FIND,
										NULL);
		if (entry)
			break;
		/* fallthrough */
	case LWLSN_CLUSTER:
		MemSet(&key, 0, sizeof(LwLsnCacheEntryKeyData));
		key.rnode.dbNode = InvalidOid;
		key.rnode.spcNode = InvalidOid;
		key.rnode.relNode = InvalidOid;
		key.blockNum = InvalidBlockNumber;
		key.forkNum = InvalidForkNumber;
		entry = (LwLsnCacheEntryData *)
			hash_search_with_hash_value(LwLsnCacheTable, &key, db_hash,
										HASH_FIND,
										NULL);
	}
	
	if (entry)
		lsn = entry->lsn;
	else
	{
		/*
		 * It can't have been modified and evicted after low watermark, as it
		 * would be in the cache. Thus, it was modified before lowWaterMark,
		 * which means lowWaterMark should be sufficient here.
		 */
		lsn = LwLsnCache->lowWaterMark;
	}
	LWLockRelease(LwLsnMetadataLock);

	if (effective != NULL)
		*effective = LwLsnCache->highWaterMark;

	return lsn;
}


static bool
lwlc_should_insert(XLogRecPtr lsn)
{
	if (lsn_cache_size <= 0)
		return false;
	return LwLsnCache->lowWaterMark < lsn;
}

static void
lwlc_insert_last_lsn(XLogRecPtr lsn,
					 RelFileNode node, ForkNumber fork,
					 BlockNumber start, BlockNumber n_blocks)
{
	uint32		hash;
	bool		found;
	XLogRecPtr	highmark;
	LwLsnCacheEntryKeyData key;
	LwLsnCacheEntryData *entry;

	/* early exit */
	if (lsn_cache_size <= 0)
		return;

	Assert(n_blocks != 0);

	/* Unreachable in assert-enabled builds, but fail silently otherwise */
	if (n_blocks == 0)
		return;

	/* don't put in effort if we wouldn't insert the entries anyway */
	if (lsn <= LwLsnCache->lowWaterMark)
		return;

	MemSet(&key, 0, sizeof(key));

	/* update the max known valid Lsn */
	highmark = Max(lsn, ProcLastRecPtr);

	key.rnode = node;
	key.forkNum = fork;
	key.blockNum = start;

	hash = get_hash_value(LwLsnCacheTable, &key);

	LWLockAcquire(LwLsnMetadataLock, LW_EXCLUSIVE);

	while (true)
	{
		if (lsn <= LwLsnCache->lowWaterMark)
		{
			/* XXX: Alternatively update 2nd level eviction cache when implemented? */
			break;
		}

		if(LwLsnCache->highWaterMark < highmark)
			LwLsnCache->highWaterMark = highmark;

		entry = (LwLsnCacheEntryData *)
			hash_search_with_hash_value(LwLsnCacheTable, &key, hash,
										HASH_ENTER,
										&found);

		if (found)
		{
			if (lsn > entry->lsn)
			{
				pairingheap_remove(&LwLsnCache->pheap, &entry->pheapnode);
				entry->lsn = lsn;
				pairingheap_add(&LwLsnCache->pheap, &entry->pheapnode);
			}
			else
			{
				/* nothing to do */
			}
		}
		else
		{
			/* entry was newly inserted, populate the entry with data */
			entry->lsn = lsn;

			/* add it to the pairingheap */
			pairingheap_add(&LwLsnCache->pheap, &entry->pheapnode);

			/* if we are about to have too many entries in the pheap, try shrinking it */
			if (hash_get_num_entries(LwLsnCacheTable) >= lsn_cache_size)
			{
				XLogRecPtr evicted_lsn;
				LwLsnCacheEntryKeyData evicted_key;

				entry = pairingheap_container(LwLsnCacheEntryData,
											  pheapnode,
											  pairingheap_remove_first(&LwLsnCache->pheap));
				evicted_key = entry->key;
				evicted_lsn = entry->lsn;

				if (LwLsnCache->lowWaterMark > entry->lsn)
					elog(FATAL, "LwLsn cache corruption - evicted lsn is smaller "
								"than highest eviction");
				/*
				 * lowWaterMark is the lowest LSN that *could* still be in the
				 * cache. So, if we evict LSN > lowWatermark, that becomes the
				 * next low watermark.
				 */
				if (LwLsnCache->lowWaterMark < evicted_lsn)
					LwLsnCache->lowWaterMark = evicted_lsn;

				hash_search(LwLsnCacheTable, &evicted_key, HASH_REMOVE, &found);

				if (evicted_lsn >= lsn)
					break; /* new insertions are useless */

				Assert(found);
			}
			else
			{
				/* nothing to do */
			}
		}

		if (--n_blocks == 0)
			break;

		start++;

		key.blockNum = start;
		hash = get_hash_value(LwLsnCacheTable, &key);
	}

	LWLockRelease(LwLsnMetadataLock);
}

XLogRecPtr
GetLastWrittenLsnForBuffer(RelFileNode node,
						   ForkNumber fork,
						   BlockNumber blkno,
						   XLogRecPtr *effective)
{
	Assert(OidIsValid(node.spcNode));
	/* node.dbNode may be InvalidOid, because shared catalogs have dbNode = 0 */
	Assert(OidIsValid(node.relNode));
	Assert(fork != InvalidForkNumber);
	Assert(BlockNumberIsValid(blkno));

	lwlc_lookup_last_lsn(LWLSN_BLOCK, node, fork, blkno, effective);
}

void
neon_set_lwlf_block_hook(XLogRecPtr lsn, RelFileNode node, ForkNumber fork, BlockNumber blkno)
{
	FORWARD_HOOK_CALL(xlog_set_lwlf_block_hook, lsn, node, fork, blkno);

	Assert(OidIsValid(node.spcNode));
	/* node.dbNode may be InvalidOid, because shared catalogs have dbNode = 0 */
	Assert(OidIsValid(node.relNode));
	Assert(fork != InvalidForkNumber);
	Assert(BlockNumberIsValid(blkno));

	if (lwlc_should_insert(lsn))
		lwlc_insert_last_lsn(lsn, node, fork, blkno, 1);
}

XLogRecPtr
GetLastWrittenLsnForRelFileNode(RelFileNode node,
								ForkNumber fork,
								XLogRecPtr *effective)
{
	Assert(OidIsValid(node.spcNode));
	/* node.dbNode may be InvalidOid, because shared catalogs have dbNode = 0 */
	Assert(OidIsValid(node.relNode));
	Assert(fork != InvalidForkNumber);

	lwlc_lookup_last_lsn(LWLSN_RELFORK, node, fork, InvalidBlockNumber, effective);
}

void
neon_set_lwlf_relation_hook(XLogRecPtr lsn, RelFileNode node, ForkNumber fork)
{
	FORWARD_HOOK_CALL(xlog_set_lwlf_relation_hook, lsn, node, fork);

	Assert(OidIsValid(node.spcNode));
	/* node.dbNode may be InvalidOid, because shared catalogs have dbNode = 0 */
	Assert(OidIsValid(node.relNode));
	Assert(fork != InvalidForkNumber);

	if (lwlc_should_insert(lsn))
		lwlc_insert_last_lsn(lsn, node, fork, InvalidBlockNumber, 1);

	/*
	 * A table's size has updated, thus the database is resized, thus we need
	 * to do requests at _at least_ this lsn for the latest (and greatest)
	 * database size estimates.
	 */
	if (OidIsValid(node.dbNode))
		neon_set_lwlf_database_hook(lsn, node.dbNode);
	else
		neon_set_lwlf_dbcluster_hook(lsn);
}

void
neon_set_lwlf_blockrange_hook(XLogRecPtr lsn,
							  RelFileNode node, ForkNumber fork,
							  BlockNumber start, BlockNumber n_blocks)
{
	FORWARD_HOOK_CALL(xlog_set_lwlf_blockrange_hook, lsn, node, fork, start, n_blocks);

	Assert(OidIsValid(node.spcNode));
	/* node.dbNode may be InvalidOid, because shared catalogs have dbNode = 0 */
	Assert(OidIsValid(node.relNode));
	Assert(fork != InvalidForkNumber);

	if (lwlc_should_insert(lsn))
		lwlc_insert_last_lsn(lsn, node, fork, start, n_blocks);

	/*
	 * A table's size has updated, thus the database is resized, thus we need
	 * to do requests at _at least_ this lsn for the latest (and greatest)
	 * database size estimates.
	 */
	neon_set_lwlf_database_hook(lsn, node.dbNode);
}

XLogRecPtr
GetLastWrittenLsnForDatabase(Oid datoid,
							 XLogRecPtr *effective)
{
	RelFileNode node = {0};

	/*
	 * XXX: Although RelFileNode.dbNode may be 0, I don't think we can reach
	 * this code for shared relations?
	 */
	Assert(OidIsValid(datoid));

	node.dbNode = datoid;

	lwlc_lookup_last_lsn(LWLSN_DB, node, InvalidForkNumber, InvalidBlockNumber, effective);
}

void
neon_set_lwlf_database_hook(XLogRecPtr lsn, Oid dboid)
{
	RelFileNode node = {0};

	FORWARD_HOOK_CALL(xlog_set_lwlf_database_hook, lsn, dboid);

	/*
	 * XXX: Although RelFileNode.dbNode may be 0, I don't think we can reach
	 * this code for shared relations?
	 */
	Assert(OidIsValid(dboid));

	node.dbNode = dboid;

	if (lwlc_should_insert(lsn))
		lwlc_insert_last_lsn(lsn, node, InvalidForkNumber, InvalidBlockNumber, 1);
}

XLogRecPtr
GetLastWrittenLsnForDbCluster(XLogRecPtr *effective)
{
	RelFileNode node = {
		.dbNode = InvalidOid,
		.relNode = InvalidOid,
		.spcNode = InvalidOid
	};

	lwlc_lookup_last_lsn(LWLSN_CLUSTER, node, InvalidForkNumber, InvalidBlockNumber, effective);
}

void
neon_set_lwlf_dbcluster_hook(XLogRecPtr lsn)
{
	RelFileNode node = {0};

	FORWARD_HOOK_CALL(xlog_set_lwlf_dbcluster_hook, lsn);

	if (lwlc_should_insert(lsn))
		lwlc_insert_last_lsn(lsn, node, InvalidForkNumber, InvalidBlockNumber, 1);
}


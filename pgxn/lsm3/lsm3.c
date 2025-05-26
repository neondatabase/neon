#include "postgres.h"
#include "access/attnum.h"
#include "utils/relcache.h"
#include "access/reloptions.h"
#include "access/nbtree.h"
#include "access/table.h"
#include "access/relation.h"
#include "access/relscan.h"
#include "access/xact.h"
#include "access/xloginsert.h"
#include "commands/defrem.h"
#include "funcapi.h"
#include "utils/rel.h"
#include "nodes/makefuncs.h"
#include "catalog/dependency.h"
#include "catalog/pg_operator.h"
#include "catalog/index.h"
#include "catalog/namespace.h"
#include "catalog/storage.h"
#include "utils/lsyscache.h"
#include "utils/typcache.h"
#include "utils/builtins.h"
#include "utils/index_selfuncs.h"
#include "utils/rel.h"
#include "miscadmin.h"
#include "tcop/utility.h"
#include "postmaster/bgworker.h"
#include "pgstat.h"
#include "executor/executor.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/lock.h"
#include "storage/lmgr.h"
#include "storage/proc.h"
#include "storage/procarray.h"

#include "lsm3.h"

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

PG_FUNCTION_INFO_V1(lsm3_handler);
PG_FUNCTION_INFO_V1(lsm3_btree_wrapper);
PG_FUNCTION_INFO_V1(lsm3_get_merge_count);
PG_FUNCTION_INFO_V1(lsm3_start_merge);
PG_FUNCTION_INFO_V1(lsm3_wait_merge_completion);
PG_FUNCTION_INFO_V1(lsm3_top_index_size);

extern void	_PG_init(void);
extern void	_PG_fini(void);

PGDLLEXPORT void lsm3_merger_main(Datum arg);

/* Lsm3 dictionary (hashtable with control data for all indexes) */
static HTAB*          Lsm3Dict;
static LWLock*        Lsm3DictLock;
static List*          Lsm3ReleasedLocks;
static List*          Lsm3Entries;
static bool           Lsm3InsideCopy;

/* Kind of relation optioms for Lsm3 index */
static relopt_kind    Lsm3ReloptKind;

/* Lsm3 kooks */
static ProcessUtility_hook_type PreviousProcessUtilityHook = NULL;
static shmem_startup_hook_type  PreviousShmemStartupHook = NULL;
#if PG_VERSION_NUM>=150000
static shmem_request_hook_type  PreviousShmemRequestHook = NULL;
#endif
static ExecutorFinish_hook_type PreviousExecutorFinish = NULL;

/* Lsm3 GUCs */
static int Lsm3MaxIndexes;
static int Lsm3TopIndexSize;

/* Background worker termination flag */
static volatile bool Lsm3Cancel;

#if PG_VERSION_NUM<160000
typedef PGAlignedBlock PGIOAlignedBlock;
#endif

static void
lsm3_shmem_request(void)
{
#if PG_VERSION_NUM>=150000
	if (PreviousShmemRequestHook)
		PreviousShmemRequestHook();
#endif

	RequestAddinShmemSpace(hash_estimate_size(Lsm3MaxIndexes, sizeof(Lsm3DictEntry)));
	RequestNamedLWLockTranche("lsm3", 1);
}

static void
lsm3_shmem_startup(void)
{
	HASHCTL info;

	if (PreviousShmemStartupHook)
	{
		PreviousShmemStartupHook();
    }
	memset(&info, 0, sizeof(info));
	info.keysize = sizeof(Oid);
	info.entrysize = sizeof(Lsm3DictEntry);
	Lsm3Dict = ShmemInitHash("lsm3 hash",
							 Lsm3MaxIndexes, Lsm3MaxIndexes,
							 &info,
							 HASH_ELEM | HASH_BLOBS);
	Lsm3DictLock = &(GetNamedLWLockTranche("lsm3"))->lock;
}

/* Initialize Lsm3 control data entry */
static void
lsm3_init_entry(Lsm3DictEntry* entry, Relation index)
{
	SpinLockInit(&entry->spinlock);
	entry->active_index = 0;
	entry->merger = NULL;
	entry->merge_in_progress = false;
	entry->start_merge = false;
	entry->n_merges = 0;
	entry->n_inserts = 0;
	entry->top[0] = entry->top[1] = InvalidOid;
	entry->access_count[0] = entry->access_count[1] = 0;
	entry->heap = index->rd_index->indrelid;
	entry->db_id = MyDatabaseId;
	entry->user_id = GetUserId();
	entry->top_index_size = index->rd_options ? ((Lsm3Options*)index->rd_options)->top_index_size : 0;
}

/* Get B-Tree index size (number of blocks) */
static BlockNumber
lsm3_get_index_size(Oid relid)
{
       Relation index = index_open(relid, AccessShareLock);
       BlockNumber size = RelationGetNumberOfBlocks(index);
	   index_close(index, AccessShareLock);
       return size;
}

/* Lookup or create Lsm3 control data for this index */
static Lsm3DictEntry*
lsm3_get_entry(Relation index)
{
	Lsm3DictEntry* entry;
	bool found = true;
	LWLockAcquire(Lsm3DictLock, LW_SHARED);
	entry = (Lsm3DictEntry*)hash_search(Lsm3Dict, &RelationGetRelid(index), HASH_FIND, &found);
	if (entry == NULL)
	{
		/* We need exclusive lock to create new entry */
		LWLockRelease(Lsm3DictLock);
		LWLockAcquire(Lsm3DictLock, LW_EXCLUSIVE);
		entry = (Lsm3DictEntry*)hash_search(Lsm3Dict, &RelationGetRelid(index), HASH_ENTER, &found);
	}
	if (!found)
	{
		char* relname = RelationGetRelationName(index);
		lsm3_init_entry(entry, index);
		for (int i = 0; i < 2; i++)
		{
			char* topidxname = psprintf("%s_top%d", relname, i);
			entry->top[i] = get_relname_relid(topidxname, RelationGetNamespace(index));
			if (entry->top[i] == InvalidOid)
			{
				elog(ERROR, "Lsm3: failed to lookup %s index", topidxname);
			}
		}
		entry->active_index = lsm3_get_index_size(entry->top[0]) >= lsm3_get_index_size(entry->top[1]) ? 0 : 1;
	}
	LWLockRelease(Lsm3DictLock);
	return entry;
}

/* Launch merger bgworker */
static void
lsm3_launch_bgworker(Lsm3DictEntry* entry)
{
	BackgroundWorker worker;
	BackgroundWorkerHandle *handle;
	pid_t bgw_pid;

	MemSet(&worker, 0, sizeof(worker));
	snprintf(worker.bgw_name, sizeof(worker.bgw_name), "lsm3-merger-%d", entry->base);
	snprintf(worker.bgw_type, sizeof(worker.bgw_type), "lsm3-merger-%d", entry->base);
	worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
	worker.bgw_start_time = BgWorkerStart_ConsistentState;
	worker.bgw_restart_time = BGW_NEVER_RESTART;
	strcpy(worker.bgw_function_name, "lsm3_merger_main");
	strcpy(worker.bgw_library_name, "lsm3");
	worker.bgw_main_arg = PointerGetDatum(entry);
	worker.bgw_notify_pid = MyProcPid;

	if (!RegisterDynamicBackgroundWorker(&worker, &handle))
	{
		elog(ERROR, "Lsm3: failed to start background worker");
	}
	if (WaitForBackgroundWorkerStartup(handle, &bgw_pid) != BGWH_STARTED)
	{
		elog(ERROR, "Lsm3: startup of background worker is failed");
	}
	entry->merger = BackendPidGetProc(bgw_pid);
	for (int n_attempts = 0; entry->merger == NULL || n_attempts < 100; n_attempts++)
	{
		pg_usleep(10000); /* wait background worker to be registered in procarray */
		entry->merger = BackendPidGetProc(bgw_pid);
	}
	if (entry->merger == NULL)
	{
		elog(ERROR, "Lsm3: background worker %d is crashed", bgw_pid);
	}
}

/* Cancel merger bgwroker */
static void
lsm3_merge_cancel(int sig)
{
	Lsm3Cancel = true;
	SetLatch(MyLatch);
}

/* Truncate top index */
static void
lsm3_truncate_index(Oid index_oid, Oid heap_oid)
{
	Relation index = index_open(index_oid, AccessExclusiveLock);
	Relation heap = table_open(heap_oid, AccessShareLock); /* heap is actually not used, because we will not load data to top indexes */
	IndexInfo* indexInfo = BuildDummyIndexInfo(index);
	RelationTruncate(index, 0);
	elog(LOG, "Lsm3: truncate index %s", RelationGetRelationName(index));
	index_build(heap, index, indexInfo, true, false);
	index_close(index, AccessExclusiveLock);
	table_close(heap, AccessShareLock);
}

#define INSERT_FLAGS UNIQUE_CHECK_NO, false

/* Merge top index into base index */
static void
lsm3_merge_indexes(Oid dst_oid, Oid src_oid, Oid heap_oid)
{
	Relation top_index = index_open(src_oid, AccessShareLock);
	Relation heap = table_open(heap_oid, AccessShareLock);
	Relation base_index = index_open(dst_oid, RowExclusiveLock);
	IndexScanDesc scan;
	bool ok;
	Oid  save_am = base_index->rd_rel->relam;

	elog(LOG, "Lsm3: merge top index %s with size %d blocks", RelationGetRelationName(top_index), RelationGetNumberOfBlocks(top_index));

	base_index->rd_rel->relam = BTREE_AM_OID;
	scan = index_beginscan(heap, top_index, SnapshotAny, 0, 0);
	scan->xs_want_itup = true;
	btrescan(scan, NULL, 0, 0, 0);
	for (ok = _bt_first(scan, ForwardScanDirection); ok; ok = _bt_next(scan, ForwardScanDirection))
	{
		IndexTuple itup = scan->xs_itup;
		if (BTreeTupleIsPosting(itup))
		{
			/* Some dirty coding here related with handling of posting items (index deduplication).
			 * If index tuple is posting item, we need to transfer it to normal index tuple.
			 * Posting list is representing by index tuple with INDEX_ALT_TID_MASK bit set in t_info and
			 * BT_IS_POSTING bit in TID offset, following by array of TIDs.
			 * We need to store right TID (taken from xs_heaptid) and correct index tuple length
			 * (not including size of TIDs array), clearing INDEX_ALT_TID_MASK.
			 * For efficiency reasons let's do it in place, saving and restoring original values after insertion is done.
			 */
			ItemPointerData save_tid = itup->t_tid;
			unsigned short save_info = itup->t_info;
			itup->t_info = (save_info & ~(INDEX_SIZE_MASK | INDEX_ALT_TID_MASK)) + BTreeTupleGetPostingOffset(itup);
			itup->t_tid = scan->xs_heaptid;
			_bt_doinsert(base_index, itup, INSERT_FLAGS, heap); /* lsm3 index is not unique so need not to heck for duplica
tes */
			itup->t_tid = save_tid;
			itup->t_info = save_info;
		}
		else
		{
			_bt_doinsert(base_index, itup, INSERT_FLAGS, heap); /* lsm3 index is not unique so need not to heck for duplica
tes */
		}
	}
	index_endscan(scan);
	base_index->rd_rel->relam = save_am;
	index_close(top_index, AccessShareLock);
	index_close(base_index, RowExclusiveLock);
	table_close(heap, AccessShareLock);
}

/* Lsm3 index options.
 */
static bytea *
lsm3_options(Datum reloptions, bool validate)
{
	static const relopt_parse_elt tab[] = {
		{"fillfactor", RELOPT_TYPE_INT, offsetof(BTOptions, fillfactor)},
		{"vacuum_cleanup_index_scale_factor", RELOPT_TYPE_REAL,
		offsetof(BTOptions, vacuum_cleanup_index_scale_factor)},
		{"deduplicate_items", RELOPT_TYPE_BOOL,
		 offsetof(BTOptions, deduplicate_items)},
		{"top_index_size", RELOPT_TYPE_INT, offsetof(Lsm3Options, top_index_size)},
		{"unique", RELOPT_TYPE_BOOL, offsetof(Lsm3Options, unique)}
	};
	return (bytea *) build_reloptions(reloptions, validate, Lsm3ReloptKind,
									  sizeof(Lsm3Options), tab, lengthof(tab));
}

/* Main function of merger bgwroker */
void
lsm3_merger_main(Datum arg)
{
	Lsm3DictEntry* entry = (Lsm3DictEntry*)DatumGetPointer(arg);
	char	   *appname;

	pqsignal(SIGINT,  lsm3_merge_cancel);
	pqsignal(SIGQUIT, lsm3_merge_cancel);
	pqsignal(SIGTERM, lsm3_merge_cancel);

	/* We're now ready to receive signals */
	BackgroundWorkerUnblockSignals();

	BackgroundWorkerInitializeConnectionByOid(entry->db_id, entry->user_id, 0);

	appname = psprintf("lsm3 merger for %d", entry->base);
	pgstat_report_appname(appname);
	pfree(appname);

	while (!Lsm3Cancel)
	{
		int merge_index= -1;
		int wr;
		pgstat_report_activity(STATE_IDLE, "waiting");
		wr = WaitLatch(MyLatch, WL_LATCH_SET | WL_EXIT_ON_PM_DEATH, -1L, PG_WAIT_EXTENSION);

		if ((wr & WL_POSTMASTER_DEATH) || Lsm3Cancel)
		{
			break;
		}

		ResetLatch(MyLatch);

		/* Check if merge is requested under spinlock */
		SpinLockAcquire(&entry->spinlock);
		if (entry->start_merge)
		{
			merge_index = 1 - entry->active_index; /* at this moment active index should already by swapped */
			entry->start_merge = false;
		}
		SpinLockRelease(&entry->spinlock);

		if (merge_index >= 0)
		{
			StartTransactionCommand();
			{
				pgstat_report_activity(STATE_RUNNING, "merging");
				lsm3_merge_indexes(entry->base, entry->top[merge_index], entry->heap);

				pgstat_report_activity(STATE_RUNNING, "truncate");
				lsm3_truncate_index(entry->top[merge_index], entry->heap);
			}
			CommitTransactionCommand();

			SpinLockAcquire(&entry->spinlock);
			entry->merge_in_progress = false; /* mark merge as completed */
			SpinLockRelease(&entry->spinlock);
		}
	}
	entry->merger = NULL;
}

/* Build index tuple comparator context */
static SortSupport
lsm3_build_sortkeys(Relation index)
{
	int	keysz = IndexRelationGetNumberOfKeyAttributes(index);
	SortSupport	sortKeys = (SortSupport) palloc0(keysz * sizeof(SortSupportData));
	BTScanInsert inskey = _bt_mkscankey(index, NULL);
	Oid          save_am = index->rd_rel->relam;

	index->rd_rel->relam = BTREE_AM_OID;

	for (int i = 0; i < keysz; i++)
	{
		SortSupport sortKey = &sortKeys[i];
		ScanKey		scanKey = &inskey->scankeys[i];
		int16		strategy;

		sortKey->ssup_cxt = CurrentMemoryContext;
		sortKey->ssup_collation = scanKey->sk_collation;
		sortKey->ssup_nulls_first =
			(scanKey->sk_flags & SK_BT_NULLS_FIRST) != 0;
		sortKey->ssup_attno = scanKey->sk_attno;
		/* Abbreviation is not supported here */
		sortKey->abbreviate = false;

		Assert(sortKey->ssup_attno != 0);

		strategy = (scanKey->sk_flags & SK_BT_DESC) != 0 ?
			BTGreaterStrategyNumber : BTLessStrategyNumber;

		PrepareSortSupportFromIndexRel(index, strategy, sortKey);
	}
	index->rd_rel->relam = save_am;
	return sortKeys;
}

/* Compare index tuples */
static int
lsm3_compare_index_tuples(IndexScanDesc scan1, IndexScanDesc scan2, SortSupport sortKeys)
{
	int n_keys = IndexRelationGetNumberOfKeyAttributes(scan1->indexRelation);

	for (int i = 1; i <= n_keys; i++)
	{
		Datum	datum[2];
		bool	isNull[2];
		int 	result;

		datum[0] = index_getattr(scan1->xs_itup, i, scan1->xs_itupdesc, &isNull[0]);
		datum[1] = index_getattr(scan2->xs_itup, i, scan2->xs_itupdesc, &isNull[1]);
		result = ApplySortComparator(datum[0], isNull[0],
									 datum[1], isNull[1],
									 &sortKeys[i - 1]);
		if (result != 0)
		{
			return result;
		}
	}
	return ItemPointerCompare(&scan1->xs_heaptid, &scan2->xs_heaptid);
}

/*
 * Lsm3 access methods implementation
 */

static IndexBuildResult *
lsm3_build(Relation heap, Relation index, IndexInfo *indexInfo)
{
	bool found;
	Lsm3DictEntry* entry;
	LWLockAcquire(Lsm3DictLock, LW_EXCLUSIVE);
	elog(LOG, "lsm3_build %s", index->rd_rel->relname.data);
	entry = hash_search(Lsm3Dict, &RelationGetRelid(index), HASH_ENTER, &found); /* Setting Lsm3Entry indicates to utility hook that Lsm3 index was created */
	if (!found)
	{
		lsm3_init_entry(entry, index);
	}
	{
		MemoryContext old_context = MemoryContextSwitchTo(TopMemoryContext);
		Lsm3Entries = lappend(Lsm3Entries, entry);
		MemoryContextSwitchTo(old_context);
	}
	entry->am_id = index->rd_rel->relam;
	index->rd_rel->relam = BTREE_AM_OID;
	LWLockRelease(Lsm3DictLock); /* Release lock set by lsm3_build */
	return btbuild(heap, index, indexInfo);
}

/*
 * Grab previously release self locks (to let merger to proceed).
 */
static void
lsm3_reacquire_locks(void)
{
	if (Lsm3ReleasedLocks)
	{
		ListCell* cell;
		foreach (cell, Lsm3ReleasedLocks)
		{
			Oid indexOid = lfirst_oid(cell);
			LockRelationOid(indexOid, RowExclusiveLock);
		}
		list_free(Lsm3ReleasedLocks);
		Lsm3ReleasedLocks = NULL;
	}
}

/* Insert in active top index, on overflow swap active indexes and initiate merge to base index */
static bool
lsm3_insert(Relation rel, Datum *values, bool *isnull,
			ItemPointer ht_ctid, Relation heapRel,
			IndexUniqueCheck checkUnique,
			bool indexUnchanged,
			IndexInfo *indexInfo)
{
	Lsm3DictEntry* entry = lsm3_get_entry(rel);

	int active_index;
	uint64 n_merges; /* used to check if merge was initiated by somebody else */
	Relation index;
	Oid  save_am;
	bool overflow;
	int top_index_size = entry->top_index_size ? entry->top_index_size : Lsm3TopIndexSize;
	bool is_initialized = true;

	/* Obtain current active index and increment access counter under spinlock */
	SpinLockAcquire(&entry->spinlock);
	active_index = entry->active_index;
	if (entry->top[active_index])
		entry->access_count[active_index] += 1;
	else
		is_initialized = false;
	n_merges = entry->n_merges;
	SpinLockRelease(&entry->spinlock);

	if (!is_initialized)
	{
		bool res;
		save_am = rel->rd_rel->relam;
		rel->rd_rel->relam = BTREE_AM_OID;
		res = btinsert(rel, values, isnull, ht_ctid, heapRel, checkUnique,
			 indexUnchanged,
			 indexInfo);
		rel->rd_rel->relam = save_am;
		return res;
	}
	/* Do insert in top index */
	index = index_open(entry->top[active_index], RowExclusiveLock);
	index->rd_rel->relam = BTREE_AM_OID;
	save_am = index->rd_rel->relam;
	btinsert(index, values, isnull, ht_ctid, heapRel, checkUnique,
			 indexUnchanged,
			 indexInfo);
	index_close(index, RowExclusiveLock);
	index->rd_rel->relam = save_am;

	overflow = !entry->merge_in_progress /* do not check for overflow if merge was already initiated */
 		&& (entry->n_inserts % LSM3_CHECK_TOP_INDEX_SIZE_PERIOD) == 0 /* perform check only each N-th insert  */
		&& RelationGetNumberOfBlocks(index)*(BLCKSZ/1024) > top_index_size;

	SpinLockAcquire(&entry->spinlock);
	/* If merge was not initiated before by somebody else, then do it */
	if (overflow && !entry->merge_in_progress && entry->n_merges == n_merges)
	{
		Assert(entry->active_index == active_index);
		entry->merge_in_progress = true;
		entry->active_index ^= 1; /* swap top indexes */
		entry->n_merges += 1;
	}
	Assert(entry->access_count[active_index] > 0);
	entry->access_count[active_index] -= 1;
	entry->n_inserts += 1;
	if (entry->merge_in_progress)
	{
		LOCKTAG		tag;
		SET_LOCKTAG_RELATION(tag,
							 MyDatabaseId,
							 entry->top[1-active_index]);
		/* Holding lock on non-ative index prevent merger bgworker from truncation this index */
#if PG_VERSION_NUM>=170000
		if (LockHeldByMe(&tag, RowExclusiveLock, false))
#else
		if (LockHeldByMe(&tag, RowExclusiveLock))
#endif
		{
			/* Copy locks all indexes and hold this locks until end of copy.
			 * We can not just release lock, because otherwise CopyFrom produces
			 * "you don't own a lock of type" warning.
			 * So just try to periodically release this lock and let merger grab it.
			 */
			if (!Lsm3InsideCopy ||
				(entry->n_inserts % LSM3_CHECK_TOP_INDEX_SIZE_PERIOD) == 0) /* release lock only each N-th insert  */

			{
				LockRelease(&tag, RowExclusiveLock, false);
				Lsm3ReleasedLocks = lappend_oid(Lsm3ReleasedLocks, entry->top[1-active_index]);
			}
		}

		/* If all inserts in previous active index are completed then we can start merge */
		if (entry->active_index != active_index && entry->access_count[active_index] == 0)
		{
			entry->start_merge = true;
			if (entry->merger == NULL) /* lazy start of bgworker */
			{
				lsm3_launch_bgworker(entry);
			}
			SetLatch(&entry->merger->procLatch);
		}
	}
	SpinLockRelease(&entry->spinlock);

	/* We have to require released locks because othervise CopyFrom will produce warning */
	if (Lsm3InsideCopy && Lsm3ReleasedLocks)
	{
		pg_usleep(1); /* give merge thread a chance to grab the lock before we require it */
		lsm3_reacquire_locks();
	}
	return false;
}

static IndexScanDesc
lsm3_beginscan(Relation rel, int nkeys, int norderbys)
{
	IndexScanDesc scan;
	Lsm3ScanOpaque* so;
	int i;

	/* no order by operators allowed */
	Assert(norderbys == 0);

	/* get the scan */
	scan = RelationGetIndexScan(rel, nkeys, norderbys);
	scan->xs_itupdesc = RelationGetDescr(rel);
	so = (Lsm3ScanOpaque*)palloc(sizeof(Lsm3ScanOpaque));
	so->entry = lsm3_get_entry(rel);
	so->sortKeys = lsm3_build_sortkeys(rel);
	for (i = 0; i < 2; i++)
	{
		if (so->entry->top[i])
		{
			so->top_index[i] = index_open(so->entry->top[i], AccessShareLock);
			so->scan[i] = btbeginscan(so->top_index[i], nkeys, norderbys);
		}
		else
		{
			so->top_index[i] = NULL;
			so->scan[i] = NULL;
		}
	}
	so->scan[2] = btbeginscan(rel, nkeys, norderbys);
	for (i = 0; i < 3; i++)
	{
		if (so->scan[i])
		{
			so->eof[i] = false;
			so->scan[i]->xs_want_itup = true;
			so->scan[i]->parallel_scan = NULL;
		}
	}
	so->unique = rel->rd_options ? ((Lsm3Options*)rel->rd_options)->unique : false;
	so->curr_index = -1;
	scan->opaque = so;

	return scan;
}

static void
lsm3_rescan(IndexScanDesc scan, ScanKey scankey, int nscankeys,
			ScanKey orderbys, int norderbys)
{
	Lsm3ScanOpaque* so = (Lsm3ScanOpaque*) scan->opaque;

	so->curr_index = -1;
	for (int i = 0; i < 3; i++)
	{
		if (so->scan[i])
		{
			btrescan(so->scan[i], scankey, nscankeys, orderbys, norderbys);
			so->eof[i] = false;
		}
	}
}

static void
lsm3_endscan(IndexScanDesc scan)
{
	Lsm3ScanOpaque* so = (Lsm3ScanOpaque*) scan->opaque;

	for (int i = 0; i < 3; i++)
	{
		if (so->scan[i])
		{
			btendscan(so->scan[i]);
			if (i < 2)
			{
				index_close(so->top_index[i], AccessShareLock);
			}
		}
	}
	pfree(so);
}


static bool
lsm3_gettuple(IndexScanDesc scan, ScanDirection dir)
{
	Lsm3ScanOpaque* so = (Lsm3ScanOpaque*) scan->opaque;
	int min = -1;
	int curr = so->curr_index;
	/* We start with active top index, then merging index and last of all: largest base index */
	int try_index_order[3] = {so->entry->active_index, 1-so->entry->active_index, 2};

	/* btree indexes are never lossy */
	scan->xs_recheck = false;

	if (curr >= 0) /* lazy advance of current index */
	{
		so->eof[curr] = !_bt_next(so->scan[curr], dir); /* move forward current index */
	}

	for (int j = 0; j < 3; j++)
	{
		int i = try_index_order[j];
		BTScanOpaque bto = (BTScanOpaque)so->scan[i]->opaque;
		so->scan[i]->xs_snapshot = scan->xs_snapshot;
		if (!so->eof[i] && !BTScanPosIsValid(bto->currPos))
		{
			so->eof[i] = !_bt_first(so->scan[i], dir);
			if (!so->eof[i] && so->unique && scan->numberOfKeys == scan->indexRelation->rd_index->indnkeyatts)
			{
				/* If index is marked as unique and we perform lookup using all index keys,
				 * then we can stop after locating first occurrence.
				 * If make it possible to avoid lookups of all three indexes.
				 */
				elog(DEBUG1, "Lsm3: lookup %d indexes", j+1);
				while (++j < 3) /* prevent search of all remanining indexes */
				{
					so->eof[try_index_order[j]] = true;
				}
				min = i;
				break;
			}
		}
		if (!so->eof[i])
		{
			if (min < 0)
			{
				min = i;
			}
			else
			{
				int result = lsm3_compare_index_tuples(so->scan[i], so->scan[min], so->sortKeys);
				if (result == 0)
				{
					/* Duplicate: it can happen during merge when same tid is both in top and base index */
					so->eof[i] = !_bt_next(so->scan[i], dir); /* just skip one of entries */
				}
				else if ((result < 0) == ScanDirectionIsForward(dir))
				{
					min = i;
				}
			}
		}
	}
	if (min < 0) /* all indexes are traversed */
	{
		return false;
	}
	else
	{
		scan->xs_heaptid = so->scan[min]->xs_heaptid; /* copy TID */
		if (scan->xs_want_itup) {
			scan->xs_itup = so->scan[min]->xs_itup;
		}
		so->curr_index = min; /*will be advance at next call of gettuple */
		return true;
	}
}

static int64
lsm3_getbitmap(IndexScanDesc scan, TIDBitmap *tbm)
{
	Lsm3ScanOpaque* so = (Lsm3ScanOpaque*)scan->opaque;
	int64 ntids = 0;
	for (int i = 0; i < 3; i++)
	{
		if (so->scan[i])
		{
			so->scan[i]->xs_snapshot = scan->xs_snapshot;
			ntids += btgetbitmap(so->scan[i], tbm);
		}
	}
	return ntids;
}

Datum
lsm3_handler(PG_FUNCTION_ARGS)
{
	IndexAmRoutine *amroutine = makeNode(IndexAmRoutine);

	amroutine->amstrategies = BTMaxStrategyNumber;
	amroutine->amsupport = BTNProcs;
	amroutine->amoptsprocnum = BTOPTIONS_PROC;
	amroutine->amcanorder = true;
	amroutine->amcanorderbyop = false;
	amroutine->amcanbackward = true;
	amroutine->amcanunique = false;   /* We can't check that index is unique without accessing base index */
	amroutine->amcanmulticol = true;
	amroutine->amoptionalkey = true;
	amroutine->amsearcharray = false; /* TODO: not sure if it will work correctly with merge */
	amroutine->amsearchnulls = true;
	amroutine->amstorage = false;
	amroutine->amclusterable = true;
	amroutine->ampredlocks = true;
	amroutine->amcanparallel = false; /* TODO: parallel scac is not supported yet */
	amroutine->amcaninclude = true;
	amroutine->amusemaintenanceworkmem = false;
	amroutine->amparallelvacuumoptions = 0;
	amroutine->amkeytype = InvalidOid;

	amroutine->ambuild = lsm3_build;
	amroutine->ambuildempty = btbuildempty;
	amroutine->aminsert = lsm3_insert;
	amroutine->ambulkdelete = btbulkdelete;
	amroutine->amvacuumcleanup = btvacuumcleanup;
	amroutine->amcanreturn = btcanreturn;
	amroutine->amcostestimate = btcostestimate;
	amroutine->amoptions = lsm3_options;
	amroutine->amproperty = btproperty;
	amroutine->ambuildphasename = btbuildphasename;
	amroutine->amvalidate = btvalidate;
	amroutine->ambeginscan = lsm3_beginscan;
	amroutine->amrescan = lsm3_rescan;
	amroutine->amgettuple = lsm3_gettuple;
	amroutine->amgetbitmap = lsm3_getbitmap;
	amroutine->amendscan = lsm3_endscan;
	amroutine->ammarkpos = NULL;  /*  When do we need index_markpos? Can we live without it? */
	amroutine->amrestrpos = NULL;
	amroutine->amestimateparallelscan = NULL;
	amroutine->aminitparallelscan = NULL;
	amroutine->amparallelrescan = NULL;

	PG_RETURN_POINTER(amroutine);
}

/*
 * Access methods for B-Tree wrapper: actually we only want to disable inserts.
 */

/* We do not need to load data in top top index: just initialize index metadata */
static IndexBuildResult *
lsm3_build_empty(Relation heap, Relation index, IndexInfo *indexInfo)
{
	PGIOAlignedBlock metapage;
	XLogRecPtr lsn;
	Buffer buf;

#if PG_VERSION_NUM>=150000
	RelationGetSmgr(index);
#else
	RelationOpenSmgr(index);
#endif

	/* Construct metapage. */
	_bt_initmetapage(metapage.data, BTREE_METAPAGE, 0, _bt_allequalimage(index, false));

	/*
	 * Write the page and log it.  It might seem that an immediate sync would
	 * be sufficient to guarantee that the file exists on disk, but recovery
	 * itself might remove it while replaying, for example, an
	 * XLOG_DBASE_CREATE or XLOG_TBLSPC_CREATE record.  Therefore, we need
	 * this even when wal_level=minimal.
	 */
	PageSetChecksumInplace(metapage.data, BTREE_METAPAGE);
	PageSetLSN((Page)metapage.data, GetXLogInsertRecPtr());
	smgrextend(index->rd_smgr, MAIN_FORKNUM, BTREE_METAPAGE,
			   (char *) metapage.data, true);

#if PG_VERSION_NUM>=160000
	lsn = log_newpage(&index->rd_smgr->smgr_rlocator.locator, MAIN_FORKNUM,
					  BTREE_METAPAGE, metapage.data, true);
	SetLastWrittenLSNForBlock(lsn, index->rd_smgr->smgr_rlocator.locator, MAIN_FORKNUM, BTREE_METAPAGE);
	SetLastWrittenLSNForRelation(lsn, index->rd_smgr->smgr_rlocator.locator, MAIN_FORKNUM);
#else
	lsn = log_newpage(&index->rd_smgr->smgr_rnode.node, MAIN_FORKNUM,
					  BTREE_METAPAGE, metapage.data, true);
	SetLastWrittenLSNForBlock(lsn, index->rd_smgr->smgr_rnode.node, MAIN_FORKNUM, BTREE_METAPAGE);
	SetLastWrittenLSNForRelation(lsn, index->rd_smgr->smgr_rnode.node, MAIN_FORKNUM);
#endif

	buf = _bt_getbuf(index, BTREE_METAPAGE, BT_WRITE);
	PageSetLSN(BufferGetPage(buf), lsn);
	_bt_relbuf(index, buf);

	/*
	 * An immediate sync is required even if we xlog'd the page, because the
	 * write did not go through shared_buffers and therefore a concurrent
	 * checkpoint may have moved the redo pointer past our xlog record.
	 */
	smgrimmedsync(index->rd_smgr, MAIN_FORKNUM);

	RelationCloseSmgr(index);


	return (IndexBuildResult *) palloc0(sizeof(IndexBuildResult));
}

static bool
lsm3_dummy_insert(Relation rel, Datum *values, bool *isnull,
				  ItemPointer ht_ctid, Relation heapRel,
				  IndexUniqueCheck checkUnique,
#if PG_VERSION_NUM>=140000
				  bool indexUnchanged,
#endif
				  IndexInfo *indexInfo)
{
	return false;
}

Datum
lsm3_btree_wrapper(PG_FUNCTION_ARGS)
{
	IndexAmRoutine *amroutine = makeNode(IndexAmRoutine);

	amroutine->amstrategies = BTMaxStrategyNumber;
	amroutine->amsupport = BTNProcs;
	amroutine->amoptsprocnum = BTOPTIONS_PROC;
	amroutine->amcanorder = true;
	amroutine->amcanorderbyop = false;
	amroutine->amcanbackward = true;
	amroutine->amcanunique = false;
	amroutine->amcanmulticol = true;
	amroutine->amoptionalkey = true;
	amroutine->amsearcharray = true;
	amroutine->amsearchnulls = true;
	amroutine->amstorage = false;
	amroutine->amclusterable = true;
	amroutine->ampredlocks = true;
	amroutine->amcanparallel = false;
	amroutine->amcaninclude = true;
	amroutine->amusemaintenanceworkmem = false;
	amroutine->amparallelvacuumoptions = 0;
	amroutine->amkeytype = InvalidOid;

	amroutine->ambuild = lsm3_build_empty;
	amroutine->ambuildempty = btbuildempty;
	amroutine->aminsert = lsm3_dummy_insert;
	amroutine->ambulkdelete = btbulkdelete;
	amroutine->amvacuumcleanup = btvacuumcleanup;
	amroutine->amcanreturn = btcanreturn;
	amroutine->amcostestimate = btcostestimate;
	amroutine->amoptions = lsm3_options;
	amroutine->amproperty = btproperty;
	amroutine->ambuildphasename = btbuildphasename;
	amroutine->amvalidate = btvalidate;
	amroutine->ambeginscan = btbeginscan;
	amroutine->amrescan = btrescan;
	amroutine->amgettuple = btgettuple;
	amroutine->amgetbitmap = btgetbitmap;
	amroutine->amendscan = btendscan;
	amroutine->ammarkpos = NULL;
	amroutine->amrestrpos = NULL;
	amroutine->amestimateparallelscan = NULL;
	amroutine->aminitparallelscan = NULL;
	amroutine->amparallelrescan = NULL;

	PG_RETURN_POINTER(amroutine);
}

/*
 * Utulity hook handling creation of Lsm3 indexes
 */
static void
lsm3_process_utility(PlannedStmt *plannedStmt,
					 const char *queryString,
					 bool readOnlyTree,
					 ProcessUtilityContext context,
					 ParamListInfo paramListInfo,
					 QueryEnvironment *queryEnvironment,
					 DestReceiver *destReceiver,
					 QueryCompletion *completionTag
	)
{
    Node *parseTree = plannedStmt->utilityStmt;
	DropStmt* drop  = NULL;
	ObjectAddresses *drop_objects = NULL;
	List* drop_oids = NULL;
	ListCell* cell;

	Lsm3Entries = NULL; /* Reset entry to check it after utility statement execution */
	Lsm3InsideCopy = false;
	if (IsA(parseTree, DropStmt))
	{
		drop = (DropStmt*)parseTree;
		if (drop->removeType == OBJECT_INDEX)
		{
			foreach (cell, drop->objects)
			{
				RangeVar* rv = makeRangeVarFromNameList((List *) lfirst(cell));
				Relation index = relation_openrv(rv, ExclusiveLock);
				if (index->rd_indam->ambuild  == lsm3_build)
				{
					Lsm3DictEntry* entry = lsm3_get_entry(index);
					if (drop_objects == NULL)
					{
						drop_objects = new_object_addresses();
					}
					for (int i = 0; i < 2; i++)
					{
						if (entry->top[i])
						{
							ObjectAddress obj;
							obj.classId = RelationRelationId;
							obj.objectId = entry->top[i];
							obj.objectSubId = 0;
							add_exact_object_address(&obj, drop_objects);
						}
					}
					drop_oids = lappend_oid(drop_oids, RelationGetRelid(index));
				}
				relation_close(index, ExclusiveLock);
			}
		}
	}
	else if (IsA(parseTree, CopyStmt))
	{
		Lsm3InsideCopy = true;
	}

	(PreviousProcessUtilityHook ? PreviousProcessUtilityHook : standard_ProcessUtility)
		(plannedStmt,
		 queryString,
		 readOnlyTree,
		 context,
		 paramListInfo,
		 queryEnvironment,
		 destReceiver,
		 completionTag);

	if (Lsm3Entries)
	{
		foreach (cell, Lsm3Entries)
		{
			Lsm3DictEntry* entry = (Lsm3DictEntry*)lfirst(cell);
			Oid top_index[2];
			if (IsA(parseTree, IndexStmt)) /* This is Lsm3 creation statement */
			{
				IndexStmt* stmt = (IndexStmt*)parseTree;
				char* originIndexName = stmt->idxname;
				char* originAccessMethod = stmt->accessMethod;

				for (int i = 0; i < 2; i++)
				{
					if (stmt->concurrent)
					{
						PushActiveSnapshot(GetTransactionSnapshot());
					}
					stmt->accessMethod = "lsm3_btree_wrapper";
					stmt->idxname = psprintf("%s_top%d", get_rel_name(entry->base), i);
					top_index[i] = DefineIndex(entry->heap,
											   stmt,
											   InvalidOid,
											   InvalidOid,
											   InvalidOid,
#if PG_VERSION_NUM>=160000
											   -1,
#endif
											   false,
											   false,
											   false,
											   false,
											   true).objectId;
				}
				stmt->accessMethod = originAccessMethod;
				stmt->idxname = originIndexName;
			}
			else
			{
				for (int i = 0; i < 2; i++)
				{
					top_index[i] = entry->top[i];
					if (top_index[i] == InvalidOid)
					{
						char* topidxname = psprintf("%s_top%d", get_rel_name(entry->base), i);
						top_index[i] = get_relname_relid(topidxname, get_rel_namespace(entry->base));
						if (top_index[i] == InvalidOid)
						{
							elog(ERROR, "Lsm3: failed to lookup %s index", topidxname);
						}
					}
				}
			}
			if (ActiveSnapshotSet())
			{
				PopActiveSnapshot();
			}
			CommitTransactionCommand();
			StartTransactionCommand();
			/*  Mark top index as invalid to prevent planner from using it in queries */
			for (int i = 0; i < 2; i++)
			{
				index_set_state_flags(top_index[i], INDEX_DROP_CLEAR_VALID);
			}
			SpinLockAcquire(&entry->spinlock);
			for (int i = 0; i < 2; i++)
			{
				entry->top[i] = top_index[i];
			}
			SpinLockRelease(&entry->spinlock);
			{
				Relation index = index_open(entry->base, AccessShareLock);
				index->rd_rel->relam = entry->am_id;
				index_close(index, AccessShareLock);
			}
		}
		list_free(Lsm3Entries);
		Lsm3Entries = NULL;
	}
	else if (drop_objects)
	{
		performMultipleDeletions(drop_objects, drop->behavior, 0);
		LWLockAcquire(Lsm3DictLock, LW_EXCLUSIVE);
		foreach (cell, drop_oids)
		{
			hash_search(Lsm3Dict, &lfirst_oid(cell), HASH_REMOVE, NULL);
		}
		LWLockRelease(Lsm3DictLock);
	}
}

/*
 * Executor finish hook to reclaim released locks on non-active top indexes
 * to avoid "you don't own a lock of type RowExclusiveLock" warning
 */
static void
lsm3_executor_finish(QueryDesc *queryDesc)
{
	lsm3_reacquire_locks();
	Lsm3InsideCopy = false;
	if (PreviousExecutorFinish)
		PreviousExecutorFinish(queryDesc);
	else
		standard_ExecutorFinish(queryDesc);

}


void
_PG_init(void)
{
	if (!process_shared_preload_libraries_in_progress)
	{
		elog(ERROR, "Lsm3: this extension should be loaded via shared_preload_libraries");
	}
	DefineCustomIntVariable("lsm3.top_index_size",
                            "Size of top index B-Tree (kb)",
							NULL,
							&Lsm3TopIndexSize,
							64*1024,
							BLCKSZ/1024,
							INT_MAX,
							PGC_SIGHUP,
							GUC_UNIT_KB,
							NULL,
							NULL,
							NULL);

	DefineCustomIntVariable("lsm3.max_indexes",
                            "Maximal number of Lsm3 indexes.",
							NULL,
							&Lsm3MaxIndexes,
							1024,
							1,
							INT_MAX,
							PGC_POSTMASTER,
							0,
							NULL,
							NULL,
							NULL);

	Lsm3ReloptKind = add_reloption_kind();

	add_bool_reloption(Lsm3ReloptKind, "unique",
					   "Index contains no duplicates",
					   false, AccessExclusiveLock);
	add_int_reloption(Lsm3ReloptKind, "top_index_size",
					  "Size of top index (kb)",
					  0, 0, INT_MAX, AccessExclusiveLock);
	add_int_reloption(Lsm3ReloptKind, "fillfactor",
					  "Packs btree index pages only to this percentage",
					  BTREE_DEFAULT_FILLFACTOR, BTREE_MIN_FILLFACTOR, 100, ShareUpdateExclusiveLock);
	add_real_reloption(Lsm3ReloptKind, "vacuum_cleanup_index_scale_factor",
					  "Packs btree index pages only to this percentage",
					  -1, 0.0, 1e10, ShareUpdateExclusiveLock);
	add_bool_reloption(Lsm3ReloptKind, "deduplicate_items",
					   "Enables \"deduplicate items\" feature for this btree index",
					   true, AccessExclusiveLock);

	PreviousShmemStartupHook = shmem_startup_hook;
	shmem_startup_hook = lsm3_shmem_startup;

#if PG_VERSION_NUM>=150000
	PreviousShmemRequestHook = shmem_request_hook;
	shmem_request_hook = lsm3_shmem_request;
#else
	lsm3_shmem_request();
#endif

	PreviousProcessUtilityHook = ProcessUtility_hook;
    ProcessUtility_hook = lsm3_process_utility;

	PreviousExecutorFinish = ExecutorFinish_hook;
	ExecutorFinish_hook = lsm3_executor_finish;
}

Datum
lsm3_get_merge_count(PG_FUNCTION_ARGS)
{
	Oid	relid = PG_GETARG_OID(0);
	Relation index = index_open(relid, AccessShareLock);
	Lsm3DictEntry* entry = lsm3_get_entry(index);
	index_close(index, AccessShareLock);
	if (entry == NULL)
		PG_RETURN_NULL();
	else
		PG_RETURN_INT64(entry->n_merges);
}


Datum
lsm3_start_merge(PG_FUNCTION_ARGS)
{
	Oid	relid = PG_GETARG_OID(0);
	Relation index = index_open(relid, AccessShareLock);
	Lsm3DictEntry* entry = lsm3_get_entry(index);
	index_close(index, AccessShareLock);

	SpinLockAcquire(&entry->spinlock);
	if (!entry->merge_in_progress)
	{
		entry->merge_in_progress = true;
		entry->active_index ^= 1;
		entry->n_merges += 1;
		if (entry->access_count[1-entry->active_index] == 0)
		{
			entry->start_merge = true;
			if (entry->merger == NULL) /* lazy start of bgworker */
			{
				lsm3_launch_bgworker(entry);
			}
			SetLatch(&entry->merger->procLatch);
		}
	}
	SpinLockRelease(&entry->spinlock);
	PG_RETURN_NULL();
}

Datum
lsm3_wait_merge_completion(PG_FUNCTION_ARGS)
{
	Oid	relid = PG_GETARG_OID(0);
	Relation index = index_open(relid, AccessShareLock);
	Lsm3DictEntry* entry = lsm3_get_entry(index);
	index_close(index, AccessShareLock);

	while (entry->merge_in_progress)
	{
		pg_usleep(1000000); /* one second */
	}
	PG_RETURN_NULL();
}

Datum
lsm3_top_index_size(PG_FUNCTION_ARGS)
{
	Oid	relid = PG_GETARG_OID(0);
	Relation index = index_open(relid, AccessShareLock);
	Lsm3DictEntry* entry = lsm3_get_entry(index);
	index_close(index, AccessShareLock);
	PG_RETURN_INT64((uint64)lsm3_get_index_size(lsm3_get_index_size(entry->top[entry->active_index]))*BLCKSZ);
}

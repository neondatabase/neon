/*-------------------------------------------------------------------------
 *
 * neon.c
 *	  Utility functions to expose neon specific information to user
 *
 * IDENTIFICATION
 *	 contrib/neon/neon.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "fmgr.h"

#include "miscadmin.h"
#include "access/xact.h"
#include "access/xlog.h"
#include "storage/buf_internals.h"
#include "storage/bufmgr.h"
#include "catalog/pg_type.h"
#include "postmaster/bgworker.h"
#include "postmaster/interrupt.h"
#include "replication/slot.h"
#include "replication/walsender.h"
#include "storage/procsignal.h"
#include "tcop/tcopprot.h"
#include "funcapi.h"
#include "access/htup_details.h"
#include "utils/pg_lsn.h"
#include "utils/guc.h"
#include "utils/wait_event.h"

#include "extension_server.h"
#include "neon.h"
#include "walproposer.h"
#include "pagestore_client.h"
#include "control_plane_connector.h"
#include "walsender_hooks.h"

PG_MODULE_MAGIC;
void		_PG_init(void);

static int	logical_replication_max_snap_files = 300;
bool primary_is_running = false;

static void
InitLogicalReplicationMonitor(void)
{
	BackgroundWorker bgw;

	DefineCustomIntVariable(
							"neon.logical_replication_max_snap_files",
							"Maximum allowed logical replication .snap files",
							NULL,
							&logical_replication_max_snap_files,
							300, 0, INT_MAX,
							PGC_SIGHUP,
							0,
							NULL, NULL, NULL);

	memset(&bgw, 0, sizeof(bgw));
	bgw.bgw_flags = BGWORKER_SHMEM_ACCESS;
	bgw.bgw_start_time = BgWorkerStart_RecoveryFinished;
	snprintf(bgw.bgw_library_name, BGW_MAXLEN, "neon");
	snprintf(bgw.bgw_function_name, BGW_MAXLEN, "LogicalSlotsMonitorMain");
	snprintf(bgw.bgw_name, BGW_MAXLEN, "Logical replication monitor");
	snprintf(bgw.bgw_type, BGW_MAXLEN, "Logical replication monitor");
	bgw.bgw_restart_time = 5;
	bgw.bgw_notify_pid = 0;
	bgw.bgw_main_arg = (Datum) 0;

	RegisterBackgroundWorker(&bgw);
}

static int
LsnDescComparator(const void *a, const void *b)
{
	XLogRecPtr	lsn1 = *((const XLogRecPtr *) a);
	XLogRecPtr	lsn2 = *((const XLogRecPtr *) b);

	if (lsn1 < lsn2)
		return 1;
	else if (lsn1 == lsn2)
		return 0;
	else
		return -1;
}

/*
 * Look at .snap files and calculate minimum allowed restart_lsn of slot so that
 * next gc would leave not more than logical_replication_max_snap_files; all
 * slots having lower restart_lsn should be dropped.
 */
static XLogRecPtr
get_num_snap_files_lsn_threshold(void)
{
	DIR		   *dirdesc;
	struct dirent *de;
	char	   *snap_path = "pg_logical/snapshots/";
	int			lsns_allocated = 1024;
	int			lsns_num = 0;
	XLogRecPtr *lsns;
	XLogRecPtr	cutoff;

	if (logical_replication_max_snap_files < 0)
		return 0;

	lsns = palloc(sizeof(XLogRecPtr) * lsns_allocated);

	/* find all .snap files and get their lsns */
	dirdesc = AllocateDir(snap_path);
	while ((de = ReadDir(dirdesc, snap_path)) != NULL)
	{
		XLogRecPtr	lsn;
		uint32		hi;
		uint32		lo;

		if (strcmp(de->d_name, ".") == 0 ||
			strcmp(de->d_name, "..") == 0)
			continue;

		if (sscanf(de->d_name, "%X-%X.snap", &hi, &lo) != 2)
		{
			ereport(LOG,
					(errmsg("could not parse file name as .snap file \"%s\"", de->d_name)));
			continue;
		}

		lsn = ((uint64) hi) << 32 | lo;
		elog(DEBUG5, "found snap file %X/%X", LSN_FORMAT_ARGS(lsn));
		if (lsns_allocated == lsns_num)
		{
			lsns_allocated *= 2;
			lsns = repalloc(lsns, sizeof(XLogRecPtr) * lsns_allocated);
		}
		lsns[lsns_num++] = lsn;
	}
	/* sort by lsn desc */
	qsort(lsns, lsns_num, sizeof(XLogRecPtr), LsnDescComparator);
	/* and take cutoff at logical_replication_max_snap_files */
	if (logical_replication_max_snap_files > lsns_num)
		cutoff = 0;
	/* have less files than cutoff */
	else
	{
		cutoff = lsns[logical_replication_max_snap_files - 1];
		elog(LOG, "ls_monitor: dropping logical slots with restart_lsn lower %X/%X, found %d .snap files, limit is %d",
			 LSN_FORMAT_ARGS(cutoff), lsns_num, logical_replication_max_snap_files);
	}
	pfree(lsns);
	FreeDir(dirdesc);
	return cutoff;
}

#define LS_MONITOR_CHECK_INTERVAL 10000 /* ms */

/*
 * Unused logical replication slots pins WAL and prevents deletion of snapshots.
 * WAL bloat is guarded by max_slot_wal_keep_size; this bgw removes slots which
 * need too many .snap files.
 */
PGDLLEXPORT void
LogicalSlotsMonitorMain(Datum main_arg)
{
	/* Establish signal handlers. */
	pqsignal(SIGUSR1, procsignal_sigusr1_handler);
	pqsignal(SIGHUP, SignalHandlerForConfigReload);
	pqsignal(SIGTERM, die);

	BackgroundWorkerUnblockSignals();

	for (;;)
	{
		XLogRecPtr	cutoff_lsn;

		/*
		 * If there are too many .snap files, just drop all logical slots to
		 * prevent aux files bloat.
		 */
		cutoff_lsn = get_num_snap_files_lsn_threshold();
		if (cutoff_lsn > 0)
		{
			for (int i = 0; i < max_replication_slots; i++)
			{
				char		slot_name[NAMEDATALEN];
				ReplicationSlot *s = &ReplicationSlotCtl->replication_slots[i];
				XLogRecPtr	restart_lsn;

				/* find the name */
				LWLockAcquire(ReplicationSlotControlLock, LW_SHARED);
				/* Consider only logical repliction slots */
				if (!s->in_use || !SlotIsLogical(s))
				{
					LWLockRelease(ReplicationSlotControlLock);
					continue;
				}

				/* do we need to drop it? */
				SpinLockAcquire(&s->mutex);
				restart_lsn = s->data.restart_lsn;
				SpinLockRelease(&s->mutex);
				if (restart_lsn >= cutoff_lsn)
				{
					LWLockRelease(ReplicationSlotControlLock);
					continue;
				}

				strlcpy(slot_name, s->data.name.data, NAMEDATALEN);
				elog(LOG, "ls_monitor: dropping slot %s with restart_lsn %X/%X below horizon %X/%X",
					 slot_name, LSN_FORMAT_ARGS(restart_lsn), LSN_FORMAT_ARGS(cutoff_lsn));
				LWLockRelease(ReplicationSlotControlLock);

				/* now try to drop it, killing owner before if any */
				for (;;)
				{
					pid_t		active_pid;

					SpinLockAcquire(&s->mutex);
					active_pid = s->active_pid;
					SpinLockRelease(&s->mutex);

					if (active_pid == 0)
					{
						/*
						 * Slot is releasted, try to drop it. Though of course
						 * it could have been reacquired, so drop can ERROR
						 * out. Similarly it could have been dropped in the
						 * meanwhile.
						 *
						 * In principle we could remove pg_try/pg_catch, that
						 * would restart the whole bgworker.
						 */
						ConditionVariableCancelSleep();
						PG_TRY();
						{
							ReplicationSlotDrop(slot_name, true);
							elog(LOG, "ls_monitor: slot %s dropped", slot_name);
						}
						PG_CATCH();
						{
							/* log ERROR and reset elog stack */
							EmitErrorReport();
							FlushErrorState();
							elog(LOG, "ls_monitor: failed to drop slot %s", slot_name);
						}
						PG_END_TRY();
						break;
					}
					else
					{
						/* kill the owner and wait for release */
						elog(LOG, "ls_monitor: killing slot %s owner %d", slot_name, active_pid);
						(void) kill(active_pid, SIGTERM);
						/* We shouldn't get stuck, but to be safe add timeout. */
						ConditionVariableTimedSleep(&s->active_cv, 1000, WAIT_EVENT_REPLICATION_SLOT_DROP);
					}
				}
			}
		}

		(void) WaitLatch(MyLatch,
						 WL_LATCH_SET | WL_EXIT_ON_PM_DEATH | WL_TIMEOUT,
						 LS_MONITOR_CHECK_INTERVAL,
						 PG_WAIT_EXTENSION);
		ResetLatch(MyLatch);
		CHECK_FOR_INTERRUPTS();
	}
}

void
_PG_init(void)
{
	/*
	 * Also load 'neon_rmgr'. This makes it unnecessary to list both 'neon'
	 * and 'neon_rmgr' in shared_preload_libraries.
	 */
#if PG_VERSION_NUM >= 160000
	load_file("$libdir/neon_rmgr", false);
#endif

	pg_init_libpagestore();
	pg_init_walproposer();
        WalSender_Custom_XLogReaderRoutines = NeonOnDemandXLogReaderRoutines;

	InitLogicalReplicationMonitor();

	InitControlPlaneConnector();

	pg_init_extension_server();

	DefineCustomBoolVariable(
		"neon.primary_is_running",
		"true if the primary was running at replica startup. false otherwise",
		NULL,
		&primary_is_running,
		false,
		PGC_POSTMASTER,
		0,
		NULL, NULL, NULL);
	/*
	 * Important: This must happen after other parts of the extension are
	 * loaded, otherwise any settings to GUCs that were set before the
	 * extension was loaded will be removed.
	 */
	EmitWarningsOnPlaceholders("neon");
}

PG_FUNCTION_INFO_V1(pg_cluster_size);
PG_FUNCTION_INFO_V1(backpressure_lsns);
PG_FUNCTION_INFO_V1(backpressure_throttling_time);

Datum
pg_cluster_size(PG_FUNCTION_ARGS)
{
	int64		size;

	size = GetNeonCurrentClusterSize();

	if (size == 0)
		PG_RETURN_NULL();

	PG_RETURN_INT64(size);
}

Datum
backpressure_lsns(PG_FUNCTION_ARGS)
{
	XLogRecPtr	writePtr;
	XLogRecPtr	flushPtr;
	XLogRecPtr	applyPtr;
	Datum		values[3];
	bool		nulls[3];
	TupleDesc	tupdesc;

	replication_feedback_get_lsns(&writePtr, &flushPtr, &applyPtr);

	tupdesc = CreateTemplateTupleDesc(3);
	TupleDescInitEntry(tupdesc, (AttrNumber) 1, "received_lsn", PG_LSNOID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 2, "disk_consistent_lsn", PG_LSNOID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 3, "remote_consistent_lsn", PG_LSNOID, -1, 0);
	tupdesc = BlessTupleDesc(tupdesc);

	MemSet(nulls, 0, sizeof(nulls));
	values[0] = LSNGetDatum(writePtr);
	values[1] = LSNGetDatum(flushPtr);
	values[2] = LSNGetDatum(applyPtr);

	PG_RETURN_DATUM(HeapTupleGetDatum(heap_form_tuple(tupdesc, values, nulls)));
}

Datum
backpressure_throttling_time(PG_FUNCTION_ARGS)
{
	PG_RETURN_UINT64(BackpressureThrottlingTime());
}

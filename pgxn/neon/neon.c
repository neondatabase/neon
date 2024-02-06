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

#include "neon.h"
#include "walproposer.h"
#include "pagestore_client.h"
#include "control_plane_connector.h"

PG_MODULE_MAGIC;
void		_PG_init(void);

static int	logical_replication_max_time_lag = 3600;

static void
InitLogicalReplicationMonitor(void)
{
	BackgroundWorker bgw;

	DefineCustomIntVariable(
		"neon.logical_replication_max_time_lag",
		"Threshold for dropping unused logical replication slots",
		NULL,
		&logical_replication_max_time_lag,
		3600, 0, INT_MAX,
		PGC_SIGHUP,
		GUC_UNIT_S,
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

typedef struct
{
	NameData    name;
	bool        dropped;
	XLogRecPtr  confirmed_flush_lsn;
	TimestampTz last_updated;
} SlotStatus;

/*
 * Unused logical replication slots pins WAL and prevents deletion of snapshots.
 */
PGDLLEXPORT void
LogicalSlotsMonitorMain(Datum main_arg)
{
	SlotStatus* slots;
	TimestampTz now, last_checked;

	/* Establish signal handlers. */
	pqsignal(SIGUSR1, procsignal_sigusr1_handler);
	pqsignal(SIGHUP, SignalHandlerForConfigReload);
	pqsignal(SIGTERM, die);

	BackgroundWorkerUnblockSignals();

	slots = (SlotStatus*)calloc(max_replication_slots, sizeof(SlotStatus));
	last_checked = GetCurrentTimestamp();

	for (;;)
	{
		(void) WaitLatch(MyLatch,
						 WL_LATCH_SET | WL_EXIT_ON_PM_DEATH | WL_TIMEOUT,
						 logical_replication_max_time_lag*1000/2,
						 PG_WAIT_EXTENSION);
		ResetLatch(MyLatch);
		CHECK_FOR_INTERRUPTS();

		now = GetCurrentTimestamp();

		if (now - last_checked > logical_replication_max_time_lag*USECS_PER_SEC)
		{
			int n_active_slots = 0;
			last_checked = now;

			LWLockAcquire(ReplicationSlotControlLock, LW_SHARED);
			for (int i = 0; i < max_replication_slots; i++)
			{
				ReplicationSlot *s = &ReplicationSlotCtl->replication_slots[i];

				/* Consider only logical repliction slots */
				if (!s->in_use || !SlotIsLogical(s))
					continue;

				if (s->active_pid != 0)
				{
					n_active_slots += 1;
					continue;
				}

				/* Check if there was some activity with the slot since last check */
				if (s->data.confirmed_flush != slots[i].confirmed_flush_lsn)
				{
					slots[i].confirmed_flush_lsn = s->data.confirmed_flush;
					slots[i].last_updated = now;
				}
				else if (now - slots[i].last_updated > logical_replication_max_time_lag*USECS_PER_SEC)
				{
					slots[i].name = s->data.name;
					slots[i].dropped = true;
				}
			}
			LWLockRelease(ReplicationSlotControlLock);

			/*
			 * If there are no active subscriptions, then no new snapshots are generated
			 * and so no need to force slot deletion.
			 */
			if (n_active_slots != 0)
			{
				for (int i = 0; i < max_replication_slots; i++)
				{
					if (slots[i].dropped)
					{
						elog(LOG, "Drop logical replication slot because it was not update more than %ld seconds",
							 (now - slots[i].last_updated)/USECS_PER_SEC);
						ReplicationSlotDrop(slots[i].name.data, true);
						slots[i].dropped = false;
					}
				}
			}
		}
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

	InitLogicalReplicationMonitor();

	InitControlPlaneConnector();

	pg_init_extension_server();

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

	size = GetZenithCurrentClusterSize();

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

#include "postgres.h"

#include <signal.h>
#include <unistd.h>
#include <sys/stat.h>
#include "access/xact.h"
#include "access/xlogdefs.h"
#include "access/xlogutils.h"
#include "access/xloginsert.h"
#if PG_VERSION_NUM >= 150000
#include "access/xlogrecovery.h"
#endif
#include "storage/fd.h"
#include "storage/latch.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "access/xlog.h"
#include "libpq/pqformat.h"
#include "replication/slot.h"
#include "replication/walreceiver.h"
#if PG_VERSION_NUM >= 160000
#include "replication/walsender_private.h"
#endif
#include "postmaster/bgworker.h"
#include "postmaster/interrupt.h"
#include "postmaster/postmaster.h"
#include "storage/pmsignal.h"
#include "storage/proc.h"
#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "storage/spin.h"
#include "tcop/tcopprot.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/ps_status.h"
#include "utils/timestamp.h"

#include "neon.h"
#include "walproposer.h"
#include "walproposer_utils.h"

char	   *wal_acceptors_list = "";
int			wal_acceptor_reconnect_timeout = 1000;
int			wal_acceptor_connection_timeout = 10000;

#define WAL_PROPOSER_SLOT_NAME "wal_proposer_slot"

static WalproposerShmemState * walprop_shared;

static void nwp_shmem_startup_hook(void);
static void nwp_register_gucs(void);
static void nwp_prepare_shmem(void);
static uint64 backpressure_lag_impl(void);
static bool backpressure_throttling_impl(void);
static void walprop_register_bgworker(void);

static process_interrupts_callback_t PrevProcessInterruptsCallback;
static shmem_startup_hook_type prev_shmem_startup_hook_type;
#if PG_VERSION_NUM >= 150000
static shmem_request_hook_type prev_shmem_request_hook = NULL;
static void walproposer_shmem_request(void);
#endif

/*
 * Initialize GUCs, bgworker, shmem and backpressure.
 */
void
pg_init_walproposer(void)
{
	if (!process_shared_preload_libraries_in_progress)
		return;

	nwp_register_gucs();

	nwp_prepare_shmem();

	delay_backend_us = &backpressure_lag_impl;
	PrevProcessInterruptsCallback = ProcessInterruptsCallback;
	ProcessInterruptsCallback = backpressure_throttling_impl;

	walprop_register_bgworker();
}

static void
nwp_register_gucs(void)
{
	DefineCustomStringVariable(
							   "neon.safekeepers",
							   "List of Neon WAL acceptors (host:port)",
							   NULL,	/* long_desc */
							   &wal_acceptors_list, /* valueAddr */
							   "",	/* bootValue */
							   PGC_POSTMASTER,
							   GUC_LIST_INPUT,	/* extensions can't use*
												 * GUC_LIST_QUOTE */
							   NULL, NULL, NULL);

	DefineCustomIntVariable(
							"neon.safekeeper_reconnect_timeout",
							"Walproposer reconnects to offline safekeepers once in this interval.",
							NULL,
							&wal_acceptor_reconnect_timeout,
							1000, 0, INT_MAX,	/* default, min, max */
							PGC_SIGHUP, /* context */
							GUC_UNIT_MS,	/* flags */
							NULL, NULL, NULL);

	DefineCustomIntVariable(
							"neon.safekeeper_connect_timeout",
							"Connection or connection attempt to safekeeper is terminated if no message is received (or connection attempt doesn't finish) within this period.",
							NULL,
							&wal_acceptor_connection_timeout,
							10000, 0, INT_MAX,
							PGC_SIGHUP,
							GUC_UNIT_MS,
							NULL, NULL, NULL);
}

/*  Check if we need to suspend inserts because of lagging replication. */
static uint64
backpressure_lag_impl(void)
{
	if (max_replication_apply_lag > 0 || max_replication_flush_lag > 0 || max_replication_write_lag > 0)
	{
		XLogRecPtr	writePtr;
		XLogRecPtr	flushPtr;
		XLogRecPtr	applyPtr;
#if PG_VERSION_NUM >= 150000
		XLogRecPtr	myFlushLsn = GetFlushRecPtr(NULL);
#else
		XLogRecPtr	myFlushLsn = GetFlushRecPtr();
#endif
		replication_feedback_get_lsns(&writePtr, &flushPtr, &applyPtr);
#define MB ((XLogRecPtr)1024 * 1024)

		elog(DEBUG2, "current flushLsn %X/%X PageserverFeedback: write %X/%X flush %X/%X apply %X/%X",
			 LSN_FORMAT_ARGS(myFlushLsn),
			 LSN_FORMAT_ARGS(writePtr),
			 LSN_FORMAT_ARGS(flushPtr),
			 LSN_FORMAT_ARGS(applyPtr));

		if ((writePtr != InvalidXLogRecPtr && max_replication_write_lag > 0 && myFlushLsn > writePtr + max_replication_write_lag * MB))
		{
			return (myFlushLsn - writePtr - max_replication_write_lag * MB);
		}

		if ((flushPtr != InvalidXLogRecPtr && max_replication_flush_lag > 0 && myFlushLsn > flushPtr + max_replication_flush_lag * MB))
		{
			return (myFlushLsn - flushPtr - max_replication_flush_lag * MB);
		}

		if ((applyPtr != InvalidXLogRecPtr && max_replication_apply_lag > 0 && myFlushLsn > applyPtr + max_replication_apply_lag * MB))
		{
			return (myFlushLsn - applyPtr - max_replication_apply_lag * MB);
		}
	}
	return 0;
}

/*
 * WalproposerShmemSize --- report amount of shared memory space needed
 */
Size
WalproposerShmemSize(void)
{
	return sizeof(WalproposerShmemState);
}

bool
WalproposerShmemInit(void)
{
	bool		found;

	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);
	walprop_shared = ShmemInitStruct("Walproposer shared state",
									 sizeof(WalproposerShmemState),
									 &found);

	if (!found)
	{
		memset(walprop_shared, 0, WalproposerShmemSize());
		SpinLockInit(&walprop_shared->mutex);
		pg_atomic_init_u64(&walprop_shared->backpressureThrottlingTime, 0);
	}
	LWLockRelease(AddinShmemInitLock);

	return found;
}

#define BACK_PRESSURE_DELAY 10000L // 0.01 sec

static bool
backpressure_throttling_impl(void)
{
	int64		lag;
	TimestampTz start,
				stop;
	bool		retry = PrevProcessInterruptsCallback
	? PrevProcessInterruptsCallback()
	: false;

	/*
	 * Don't throttle read only transactions or wal sender.
	 * Do throttle CREATE INDEX CONCURRENTLY, however. It performs some
	 * stages outside a transaction, even though it writes a lot of WAL. 
	 * Check PROC_IN_SAFE_IC flag to cover that case.
	 */
	if (am_walsender
		|| (!(MyProc->statusFlags & PROC_IN_SAFE_IC)
			&& !TransactionIdIsValid(GetCurrentTransactionIdIfAny())))
		return retry;

	/* Calculate replicas lag */
	lag = backpressure_lag_impl();
	if (lag == 0)
		return retry;

	/* Suspend writers until replicas catch up */
	set_ps_display("backpressure throttling");

	elog(DEBUG2, "backpressure throttling: lag %lu", lag);
	start = GetCurrentTimestamp();
	pg_usleep(BACK_PRESSURE_DELAY);
	stop = GetCurrentTimestamp();
	pg_atomic_add_fetch_u64(&walprop_shared->backpressureThrottlingTime, stop - start);
	return true;
}

uint64
BackpressureThrottlingTime(void)
{
	return pg_atomic_read_u64(&walprop_shared->backpressureThrottlingTime);
}

/*
 * Register a background worker proposing WAL to wal acceptors.
 */
static void
walprop_register_bgworker(void)
{
	BackgroundWorker bgw;

	/* If no wal acceptors are specified, don't start the background worker. */
	if (*wal_acceptors_list == '\0')
		return;

	memset(&bgw, 0, sizeof(bgw));
	bgw.bgw_flags = BGWORKER_SHMEM_ACCESS;
	bgw.bgw_start_time = BgWorkerStart_RecoveryFinished;
	snprintf(bgw.bgw_library_name, BGW_MAXLEN, "neon");
	snprintf(bgw.bgw_function_name, BGW_MAXLEN, "WalProposerMain");
	snprintf(bgw.bgw_name, BGW_MAXLEN, "WAL proposer");
	snprintf(bgw.bgw_type, BGW_MAXLEN, "WAL proposer");
	bgw.bgw_restart_time = 5;
	bgw.bgw_notify_pid = 0;
	bgw.bgw_main_arg = (Datum) 0;

	RegisterBackgroundWorker(&bgw);
}

/* shmem handling */

static void
nwp_prepare_shmem(void)
{
#if PG_VERSION_NUM >= 150000
	prev_shmem_request_hook = shmem_request_hook;
	shmem_request_hook = walproposer_shmem_request;
#else
	RequestAddinShmemSpace(WalproposerShmemSize());
#endif
	prev_shmem_startup_hook_type = shmem_startup_hook;
	shmem_startup_hook = nwp_shmem_startup_hook;
}

#if PG_VERSION_NUM >= 150000
/*
 * shmem_request hook: request additional shared resources.  We'll allocate or
 * attach to the shared resources in nwp_shmem_startup_hook().
 */
static void
walproposer_shmem_request(void)
{
	if (prev_shmem_request_hook)
		prev_shmem_request_hook();

	RequestAddinShmemSpace(WalproposerShmemSize());
}
#endif

static void
nwp_shmem_startup_hook(void)
{
	if (prev_shmem_startup_hook_type)
		prev_shmem_startup_hook_type();

	WalproposerShmemInit();
}

static WalproposerShmemState *
walprop_pg_get_shmem_state(void)
{
	Assert(walprop_shared != NULL);
	return walprop_shared;
}

void
replication_feedback_set(PageserverFeedback * rf)
{
	SpinLockAcquire(&walprop_shared->mutex);
	memcpy(&walprop_shared->feedback, rf, sizeof(PageserverFeedback));
	SpinLockRelease(&walprop_shared->mutex);
}

void
replication_feedback_get_lsns(XLogRecPtr *writeLsn, XLogRecPtr *flushLsn, XLogRecPtr *applyLsn)
{
	SpinLockAcquire(&walprop_shared->mutex);
	*writeLsn = walprop_shared->feedback.last_received_lsn;
	*flushLsn = walprop_shared->feedback.disk_consistent_lsn;
	*applyLsn = walprop_shared->feedback.remote_consistent_lsn;
	SpinLockRelease(&walprop_shared->mutex);
}

/*
 * Start walsender streaming replication
 */
static void
walprop_pg_start_streaming(XLogRecPtr startpos, TimeLineID timeline)
{
	StartReplicationCmd cmd;

	elog(LOG, "WAL proposer starts streaming at %X/%X",
		 LSN_FORMAT_ARGS(startpos));
	cmd.slotname = WAL_PROPOSER_SLOT_NAME;
	cmd.timeline = timeline;
	cmd.startpoint = startpos;
	StartProposerReplication(&cmd);
}

static void
walprop_pg_init_walsender(void)
{
	am_walsender = true;
	InitWalSender();
	InitProcessPhase2();

	/* Create replication slot for WAL proposer if not exists */
	if (SearchNamedReplicationSlot(WAL_PROPOSER_SLOT_NAME, false) == NULL)
	{
		ReplicationSlotCreate(WAL_PROPOSER_SLOT_NAME, false, RS_PERSISTENT, false);
		ReplicationSlotReserveWal();
		/* Write this slot to disk */
		ReplicationSlotMarkDirty();
		ReplicationSlotSave();
		ReplicationSlotRelease();
	}
}

static void
walprop_pg_init_standalone_sync_safekeepers(void)
{
	struct stat stat_buf;

#if PG_VERSION_NUM < 150000
	ThisTimeLineID = 1;
#endif

	/*
	 * Initialize postmaster_alive_fds as WaitEventSet checks them.
	 *
	 * Copied from InitPostmasterDeathWatchHandle()
	 */
	if (pipe(postmaster_alive_fds) < 0)
		ereport(FATAL,
				(errcode_for_file_access(),
					errmsg_internal("could not create pipe to monitor postmaster death: %m")));
	if (fcntl(postmaster_alive_fds[POSTMASTER_FD_WATCH], F_SETFL, O_NONBLOCK) == -1)
		ereport(FATAL,
				(errcode_for_socket_access(),
					errmsg_internal("could not set postmaster death monitoring pipe to nonblocking mode: %m")));

	ChangeToDataDir();

	/* Create pg_wal directory, if it doesn't exist */
	if (stat(XLOGDIR, &stat_buf) != 0)
	{
		ereport(LOG, (errmsg("creating missing WAL directory \"%s\"", XLOGDIR)));
		if (MakePGDirectory(XLOGDIR) < 0)
		{
			ereport(ERROR,
					(errcode_for_file_access(),
						errmsg("could not create directory \"%s\": %m",
							   XLOGDIR)));
			exit(1);
		}
	}
	BackgroundWorkerUnblockSignals();
}

static uint64
walprop_pg_init_bgworker(void)
{
#if PG_VERSION_NUM >= 150000
	TimeLineID	tli;
#endif

	/* Establish signal handlers. */
	pqsignal(SIGUSR1, procsignal_sigusr1_handler);
	pqsignal(SIGHUP, SignalHandlerForConfigReload);
	pqsignal(SIGTERM, die);

	BackgroundWorkerUnblockSignals();

	application_name = (char *) "walproposer";	/* for
												 * synchronous_standby_names */

#if PG_VERSION_NUM >= 150000
	/* FIXME pass proper tli to WalProposerInit ? */
	GetXLogReplayRecPtr(&tli);
#else
	GetXLogReplayRecPtr(&ThisTimeLineID);
#endif

	return GetSystemIdentifier();
}

static XLogRecPtr
walprop_pg_get_flush_rec_ptr(void)
{
#if PG_MAJORVERSION_NUM < 15
	return GetFlushRecPtr();
#else
	return GetFlushRecPtr(NULL);
#endif
}

/*
 * Temporary globally exported walproposer API for postgres.
 */
const walproposer_api walprop_pg = {
	.get_shmem_state = walprop_pg_get_shmem_state,
	.replication_feedback_set = replication_feedback_set,
	.start_streaming = walprop_pg_start_streaming,
	.init_walsender = walprop_pg_init_walsender,
	.init_standalone_sync_safekeepers = walprop_pg_init_standalone_sync_safekeepers,
	.init_bgworker = walprop_pg_init_bgworker,
	.get_flush_rec_ptr = walprop_pg_get_flush_rec_ptr
};

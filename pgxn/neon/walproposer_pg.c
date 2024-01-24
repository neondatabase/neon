/*
 * Implementation of postgres based walproposer disk and IO routines, i.e. the
 * real ones. The reason this is separate from walproposer.c is ability to
 * replace them with mocks, allowing to do simulation testing.
 *
 * Also contains initialization of postgres based walproposer.
 */

#include "postgres.h"

#include <signal.h>
#include <unistd.h>
#include <sys/stat.h>
#include "access/xact.h"
#include "access/xlog.h"
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
#include "replication/walsender_private.h"
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

#include "libpq-fe.h"

#include "libpqwalproposer.h"
#include "neon.h"
#include "neon_walreader.h"
#include "walproposer.h"

#define XLOG_HDR_SIZE (1 + 8 * 3)	/* 'w' + startPos + walEnd + timestamp */
#define XLOG_HDR_START_POS 1	/* offset of start position in wal sender*
								 * message header */

#define MB ((XLogRecPtr)1024 * 1024)

#define WAL_PROPOSER_SLOT_NAME "wal_proposer_slot"

char	   *wal_acceptors_list = "";
int			wal_acceptor_reconnect_timeout = 1000;
int			wal_acceptor_connection_timeout = 10000;

static AppendResponse quorumFeedback;
static WalproposerShmemState *walprop_shared;
static WalProposerConfig walprop_config;
static XLogRecPtr sentPtr = InvalidXLogRecPtr;
static const walproposer_api walprop_pg;

static void nwp_shmem_startup_hook(void);
static void nwp_register_gucs(void);
static void nwp_prepare_shmem(void);
static uint64 backpressure_lag_impl(void);
static bool backpressure_throttling_impl(void);
static void walprop_register_bgworker(void);

static void walprop_pg_init_standalone_sync_safekeepers(void);
static void walprop_pg_init_walsender(void);
static void walprop_pg_init_bgworker(void);
static TimestampTz walprop_pg_get_current_timestamp(WalProposer *wp);
static TimeLineID walprop_pg_get_timeline_id(void);
static void walprop_pg_load_libpqwalreceiver(void);

static process_interrupts_callback_t PrevProcessInterruptsCallback;
static shmem_startup_hook_type prev_shmem_startup_hook_type;
#if PG_VERSION_NUM >= 150000
static shmem_request_hook_type prev_shmem_request_hook = NULL;
static void walproposer_shmem_request(void);
#endif

static void StartProposerReplication(WalProposer *wp, StartReplicationCmd *cmd);
static void WalSndLoop(WalProposer *wp);
static void XLogBroadcastWalProposer(WalProposer *wp);

static void XLogWalPropWrite(WalProposer *wp, char *buf, Size nbytes, XLogRecPtr recptr);
static void XLogWalPropClose(XLogRecPtr recptr);

static void add_nwr_event_set(Safekeeper *sk, uint32 events);
static void update_nwr_event_set(Safekeeper *sk, uint32 events);
static void rm_safekeeper_event_set(Safekeeper *to_remove, bool is_sk);

static XLogRecPtr GetLogRepRestartLSN(WalProposer *wp);

static void
init_walprop_config(bool syncSafekeepers)
{
	walprop_config.neon_tenant = neon_tenant;
	walprop_config.neon_timeline = neon_timeline;
	walprop_config.safekeepers_list = wal_acceptors_list;
	walprop_config.safekeeper_reconnect_timeout = wal_acceptor_reconnect_timeout;
	walprop_config.safekeeper_connection_timeout = wal_acceptor_connection_timeout;
	walprop_config.wal_segment_size = wal_segment_size;
	walprop_config.syncSafekeepers = syncSafekeepers;
	if (!syncSafekeepers)
		walprop_config.systemId = GetSystemIdentifier();
	else
		walprop_config.systemId = 0;
	walprop_config.pgTimeline = walprop_pg_get_timeline_id();
}

/*
 * Entry point for `postgres --sync-safekeepers`.
 */
PGDLLEXPORT void
WalProposerSync(int argc, char *argv[])
{
	WalProposer *wp;

	init_walprop_config(true);
	walprop_pg_init_standalone_sync_safekeepers();
	walprop_pg_load_libpqwalreceiver();

	wp = WalProposerCreate(&walprop_config, walprop_pg);

	WalProposerStart(wp);
}

/*
 * WAL proposer bgworker entry point.
 */
PGDLLEXPORT void
WalProposerMain(Datum main_arg)
{
	WalProposer *wp;

	init_walprop_config(false);
	walprop_pg_init_bgworker();
	walprop_pg_load_libpqwalreceiver();

	wp = WalProposerCreate(&walprop_config, walprop_pg);
	wp->last_reconnect_attempt = walprop_pg_get_current_timestamp(wp);

	walprop_pg_init_walsender();
	WalProposerStart(wp);
}

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
static Size
WalproposerShmemSize(void)
{
	return sizeof(WalproposerShmemState);
}

static bool
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
	 * Don't throttle read only transactions or wal sender. Do throttle CREATE
	 * INDEX CONCURRENTLY, however. It performs some stages outside a
	 * transaction, even though it writes a lot of WAL. Check PROC_IN_SAFE_IC
	 * flag to cover that case.
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
walprop_pg_get_shmem_state(WalProposer *wp)
{
	Assert(walprop_shared != NULL);
	return walprop_shared;
}

void
replication_feedback_set(PageserverFeedback *rf)
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
walprop_pg_start_streaming(WalProposer *wp, XLogRecPtr startpos)
{
	StartReplicationCmd cmd;

	wpg_log(LOG, "WAL proposer starts streaming at %X/%X",
			LSN_FORMAT_ARGS(startpos));
	cmd.slotname = WAL_PROPOSER_SLOT_NAME;
	cmd.timeline = wp->greetRequest.timeline;
	cmd.startpoint = startpos;
	StartProposerReplication(wp, &cmd);
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

static void
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
}

static XLogRecPtr
walprop_pg_get_flush_rec_ptr(WalProposer *wp)
{
#if PG_MAJORVERSION_NUM < 15
	return GetFlushRecPtr();
#else
	return GetFlushRecPtr(NULL);
#endif
}

static TimestampTz
walprop_pg_get_current_timestamp(WalProposer *wp)
{
	return GetCurrentTimestamp();
}

static TimeLineID
walprop_pg_get_timeline_id(void)
{
#if PG_VERSION_NUM >= 150000
	/* FIXME don't use hardcoded timeline id */
	return 1;
#else
	return ThisTimeLineID;
#endif
}

static void
walprop_pg_load_libpqwalreceiver(void)
{
	load_file("libpqwalreceiver", false);
	if (WalReceiverFunctions == NULL)
		wpg_log(ERROR, "libpqwalreceiver didn't initialize correctly");
}

/* Helper function */
static bool
ensure_nonblocking_status(WalProposerConn *conn, bool is_nonblocking)
{
	/* If we're already correctly blocking or nonblocking, all good */
	if (is_nonblocking == conn->is_nonblocking)
		return true;

	/* Otherwise, set it appropriately */
	if (PQsetnonblocking(conn->pg_conn, is_nonblocking) == -1)
		return false;

	conn->is_nonblocking = is_nonblocking;
	return true;
}

/* Exported function definitions */
static char *
walprop_error_message(Safekeeper *sk)
{
	return PQerrorMessage(sk->conn->pg_conn);
}

static WalProposerConnStatusType
walprop_status(Safekeeper *sk)
{
	switch (PQstatus(sk->conn->pg_conn))
	{
		case CONNECTION_OK:
			return WP_CONNECTION_OK;
		case CONNECTION_BAD:
			return WP_CONNECTION_BAD;
		default:
			return WP_CONNECTION_IN_PROGRESS;
	}
}

WalProposerConn *
libpqwp_connect_start(char *conninfo)
{

	PGconn	   *pg_conn;
	WalProposerConn *conn;
	const char *keywords[3];
	const char *values[3];
	int			n;
	char	   *password = neon_auth_token;


	/*
	 * Connect using the given connection string. If the NEON_AUTH_TOKEN
	 * environment variable was set, use that as the password.
	 *
	 * The connection options are parsed in the order they're given, so when
	 * we set the password before the connection string, the connection string
	 * can override the password from the env variable. Seems useful, although
	 * we don't currently use that capability anywhere.
	 */
	n = 0;
	if (password)
	{
		keywords[n] = "password";
		values[n] = password;
		n++;
	}
	keywords[n] = "dbname";
	values[n] = conninfo;
	n++;
	keywords[n] = NULL;
	values[n] = NULL;
	n++;
	pg_conn = PQconnectStartParams(keywords, values, 1);

	/*
	 * "If the result is null, then libpq has been unable to allocate a new
	 * PGconn structure"
	 */
	if (!pg_conn)
		wpg_log(FATAL, "failed to allocate new PGconn object");

	/*
	 * And in theory this allocation can fail as well, but it's incredibly
	 * unlikely if we just successfully allocated a PGconn.
	 *
	 * palloc will exit on failure though, so there's not much we could do if
	 * it *did* fail.
	 */
	conn = palloc(sizeof(WalProposerConn));
	conn->pg_conn = pg_conn;
	conn->is_nonblocking = false;	/* connections always start in blocking
									 * mode */
	conn->recvbuf = NULL;
	return conn;
}

static void
walprop_connect_start(Safekeeper *sk)
{
	Assert(sk->conn == NULL);
	sk->conn = libpqwp_connect_start(sk->conninfo);

}

static WalProposerConnectPollStatusType
walprop_connect_poll(Safekeeper *sk)
{
	WalProposerConnectPollStatusType return_val;

	switch (PQconnectPoll(sk->conn->pg_conn))
	{
		case PGRES_POLLING_FAILED:
			return_val = WP_CONN_POLLING_FAILED;
			break;
		case PGRES_POLLING_READING:
			return_val = WP_CONN_POLLING_READING;
			break;
		case PGRES_POLLING_WRITING:
			return_val = WP_CONN_POLLING_WRITING;
			break;
		case PGRES_POLLING_OK:
			return_val = WP_CONN_POLLING_OK;
			break;

			/*
			 * There's a comment at its source about this constant being
			 * unused. We'll expect it's never returned.
			 */
		case PGRES_POLLING_ACTIVE:
			wpg_log(FATAL, "unexpected PGRES_POLLING_ACTIVE returned from PQconnectPoll");

			/*
			 * This return is never actually reached, but it's here to make
			 * the compiler happy
			 */
			return WP_CONN_POLLING_FAILED;

		default:
			Assert(false);
			return_val = WP_CONN_POLLING_FAILED;	/* keep the compiler quiet */
	}

	return return_val;
}

extern bool
libpqwp_send_query(WalProposerConn *conn, char *query)
{
	/*
	 * We need to be in blocking mode for sending the query to run without
	 * requiring a call to PQflush
	 */
	if (!ensure_nonblocking_status(conn, false))
		return false;

	/* PQsendQuery returns 1 on success, 0 on failure */
	if (!PQsendQuery(conn->pg_conn, query))
		return false;

	return true;
}

static bool
walprop_send_query(Safekeeper *sk, char *query)
{
	return libpqwp_send_query(sk->conn, query);
}

WalProposerExecStatusType
libpqwp_get_query_result(WalProposerConn *conn)
{

	PGresult   *result;
	WalProposerExecStatusType return_val;

	/* Marker variable if we need to log an unexpected success result */
	char	   *unexpected_success = NULL;

	/* Consume any input that we might be missing */
	if (!PQconsumeInput(conn->pg_conn))
		return WP_EXEC_FAILED;

	if (PQisBusy(conn->pg_conn))
		return WP_EXEC_NEEDS_INPUT;


	result = PQgetResult(conn->pg_conn);

	/*
	 * PQgetResult returns NULL only if getting the result was successful &
	 * there's no more of the result to get.
	 */
	if (!result)
	{
		wpg_log(WARNING, "[libpqwalproposer] Unexpected successful end of command results");
		return WP_EXEC_UNEXPECTED_SUCCESS;
	}

	/* Helper macro to reduce boilerplate */
#define UNEXPECTED_SUCCESS(msg) \
		return_val = WP_EXEC_UNEXPECTED_SUCCESS; \
		unexpected_success = msg; \
		break;


	switch (PQresultStatus(result))
	{
			/* "true" success case */
		case PGRES_COPY_BOTH:
			return_val = WP_EXEC_SUCCESS_COPYBOTH;
			break;

			/* Unexpected success case */
		case PGRES_EMPTY_QUERY:
			UNEXPECTED_SUCCESS("empty query return");
		case PGRES_COMMAND_OK:
			UNEXPECTED_SUCCESS("data-less command end");
		case PGRES_TUPLES_OK:
			UNEXPECTED_SUCCESS("tuples return");
		case PGRES_COPY_OUT:
			UNEXPECTED_SUCCESS("'Copy Out' response");
		case PGRES_COPY_IN:
			UNEXPECTED_SUCCESS("'Copy In' response");
		case PGRES_SINGLE_TUPLE:
			UNEXPECTED_SUCCESS("single tuple return");
		case PGRES_PIPELINE_SYNC:
			UNEXPECTED_SUCCESS("pipeline sync point");

			/* Failure cases */
		case PGRES_BAD_RESPONSE:
		case PGRES_NONFATAL_ERROR:
		case PGRES_FATAL_ERROR:
		case PGRES_PIPELINE_ABORTED:
			return_val = WP_EXEC_FAILED;
			break;

		default:
			Assert(false);
			return_val = WP_EXEC_FAILED;	/* keep the compiler quiet */
	}

	if (unexpected_success)
		wpg_log(WARNING, "[libpqwalproposer] Unexpected successful %s", unexpected_success);

	return return_val;
}

static WalProposerExecStatusType
walprop_get_query_result(Safekeeper *sk)
{
	return libpqwp_get_query_result(sk->conn);
}

static pgsocket
walprop_socket(Safekeeper *sk)
{
	return PQsocket(sk->conn->pg_conn);
}

static int
walprop_flush(Safekeeper *sk)
{
	return (PQflush(sk->conn->pg_conn));
}

/* Like libpqrcv_receive. *buf is valid until the next call. */
PGAsyncReadResult
libpqwp_async_read(WalProposerConn *conn, char **buf, int *amount)
{
	int			rawlen;

	if (conn->recvbuf != NULL)
	{
		PQfreemem(conn->recvbuf);
		conn->recvbuf = NULL;
	}

	/* Try to receive a CopyData message */
	rawlen = PQgetCopyData(conn->pg_conn, &conn->recvbuf, true);
	if (rawlen == 0)
	{
		/* Try consuming some data. */
		if (!PQconsumeInput(conn->pg_conn))
		{
			*amount = 0;
			*buf = NULL;
			return PG_ASYNC_READ_FAIL;
		}
		/* Now that we've consumed some input, try again */
		rawlen = PQgetCopyData(conn->pg_conn, &conn->recvbuf, true);
	}

	/*
	 * The docs for PQgetCopyData list the return values as: 0 if the copy is
	 * still in progress, but no "complete row" is available -1 if the copy is
	 * done -2 if an error occurred (> 0) if it was successful; that value is
	 * the amount transferred.
	 *
	 * The protocol we use between walproposer and safekeeper means that we
	 * *usually* wouldn't expect to see that the copy is done, but this can
	 * sometimes be triggered by the server returning an ErrorResponse (which
	 * also happens to have the effect that the copy is done).
	 */
	switch (rawlen)
	{
		case 0:
			*amount = 0;
			*buf = NULL;
			return PG_ASYNC_READ_TRY_AGAIN;
		case -1:
			{
				/*
				 * If we get -1, it's probably because of a server error; the
				 * safekeeper won't normally send a CopyDone message.
				 *
				 * We can check PQgetResult to make sure that the server
				 * failed; it'll always result in PGRES_FATAL_ERROR
				 */
				ExecStatusType status = PQresultStatus(PQgetResult(conn->pg_conn));

				if (status != PGRES_FATAL_ERROR)
					wpg_log(FATAL, "unexpected result status %d after failed PQgetCopyData", status);

				/*
				 * If there was actually an error, it'll be properly reported
				 * by calls to PQerrorMessage -- we don't have to do anything
				 * else
				 */
				*amount = 0;
				*buf = NULL;
				return PG_ASYNC_READ_FAIL;
			}
		case -2:
			*amount = 0;
			*buf = NULL;
			return PG_ASYNC_READ_FAIL;
		default:
			/* Positive values indicate the size of the returned result */
			*amount = rawlen;
			*buf = conn->recvbuf;
			return PG_ASYNC_READ_SUCCESS;
	}
}

/*
 * Receive a message from the safekeeper.
 *
 * On success, the data is placed in *buf. It is valid until the next call
 * to this function.
 */
static PGAsyncReadResult
walprop_async_read(Safekeeper *sk, char **buf, int *amount)
{
	return libpqwp_async_read(sk->conn, buf, amount);
}

static PGAsyncWriteResult
walprop_async_write(Safekeeper *sk, void const *buf, size_t size)
{
	int			result;

	/* If we aren't in non-blocking mode, switch to it. */
	if (!ensure_nonblocking_status(sk->conn, true))
		return PG_ASYNC_WRITE_FAIL;

	/*
	 * The docs for PQputcopyData list the return values as: 1 if the data was
	 * queued, 0 if it was not queued because of full buffers, or -1 if an
	 * error occurred
	 */
	result = PQputCopyData(sk->conn->pg_conn, buf, size);

	/*
	 * We won't get a result of zero because walproposer always empties the
	 * connection's buffers before sending more
	 */
	Assert(result != 0);

	switch (result)
	{
		case 1:
			/* good -- continue */
			break;
		case -1:
			return PG_ASYNC_WRITE_FAIL;
		default:
			wpg_log(FATAL, "invalid return %d from PQputCopyData", result);
	}

	/*
	 * After queueing the data, we still need to flush to get it to send. This
	 * might take multiple tries, but we don't want to wait around until it's
	 * done.
	 *
	 * PQflush has the following returns (directly quoting the docs): 0 if
	 * sucessful, 1 if it was unable to send all the data in the send queue
	 * yet -1 if it failed for some reason
	 */
	switch (result = PQflush(sk->conn->pg_conn))
	{
		case 0:
			return PG_ASYNC_WRITE_SUCCESS;
		case 1:
			return PG_ASYNC_WRITE_TRY_FLUSH;
		case -1:
			return PG_ASYNC_WRITE_FAIL;
		default:
			wpg_log(FATAL, "invalid return %d from PQflush", result);
	}
}

/*
 * This function is very similar to walprop_async_write. For more
 * information, refer to the comments there.
 */
static bool
walprop_blocking_write(Safekeeper *sk, void const *buf, size_t size)
{
	int			result;

	/* If we are in non-blocking mode, switch out of it. */
	if (!ensure_nonblocking_status(sk->conn, false))
		return false;

	if ((result = PQputCopyData(sk->conn->pg_conn, buf, size)) == -1)
		return false;

	Assert(result == 1);

	/* Because the connection is non-blocking, flushing returns 0 or -1 */

	if ((result = PQflush(sk->conn->pg_conn)) == -1)
		return false;

	Assert(result == 0);
	return true;
}

void
libpqwp_disconnect(WalProposerConn *conn)
{
	if (conn->recvbuf != NULL)
		PQfreemem(conn->recvbuf);
	PQfinish(conn->pg_conn);
	pfree(conn);
}

static void
walprop_finish(Safekeeper *sk)
{
	if (sk->conn)
	{
		libpqwp_disconnect(sk->conn);
		sk->conn = NULL;
	}

	/* free xlogreader */
	if (sk->xlogreader)
	{
		NeonWALReaderFree(sk->xlogreader);
		sk->xlogreader = NULL;
	}
	rm_safekeeper_event_set(sk, false);
}

/*
 * Subscribe for new WAL and stream it in the loop to safekeepers.
 *
 * At the moment, this never returns, but an ereport(ERROR) will take us back
 * to the main loop.
 */
static void
StartProposerReplication(WalProposer *wp, StartReplicationCmd *cmd)
{
	XLogRecPtr	FlushPtr;
	TimeLineID	currTLI;

#if PG_VERSION_NUM < 150000
	if (ThisTimeLineID == 0)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("IDENTIFY_SYSTEM has not been run before START_REPLICATION")));
#endif

	/*
	 * We assume here that we're logging enough information in the WAL for
	 * log-shipping, since this is checked in PostmasterMain().
	 *
	 * NOTE: wal_level can only change at shutdown, so in most cases it is
	 * difficult for there to be WAL data that we can still see that was
	 * written at wal_level='minimal'.
	 */

	if (cmd->slotname)
	{
		ReplicationSlotAcquire(cmd->slotname, true);
		if (SlotIsLogical(MyReplicationSlot))
			ereport(ERROR,
					(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
					 errmsg("cannot use a logical replication slot for physical replication")));

		/*
		 * We don't need to verify the slot's restart_lsn here; instead we
		 * rely on the caller requesting the starting point to use.  If the
		 * WAL segment doesn't exist, we'll fail later.
		 */
	}

	/*
	 * Select the timeline. If it was given explicitly by the client, use
	 * that. Otherwise use the timeline of the last replayed record, which is
	 * kept in ThisTimeLineID.
	 *
	 * Neon doesn't currently use PG Timelines, but it may in the future, so
	 * we keep this code around to lighten the load for when we need it.
	 */
#if PG_VERSION_NUM >= 150000
	FlushPtr = GetFlushRecPtr(&currTLI);
#else
	FlushPtr = GetFlushRecPtr();
	currTLI = ThisTimeLineID;
#endif

	/*
	 * When we first start replication the standby will be behind the primary.
	 * For some applications, for example synchronous replication, it is
	 * important to have a clear state for this initial catchup mode, so we
	 * can trigger actions when we change streaming state later. We may stay
	 * in this state for a long time, which is exactly why we want to be able
	 * to monitor whether or not we are still here.
	 */
	WalSndSetState(WALSNDSTATE_CATCHUP);

	/*
	 * Don't allow a request to stream from a future point in WAL that hasn't
	 * been flushed to disk in this server yet.
	 */
	if (FlushPtr < cmd->startpoint)
	{
		ereport(ERROR,
				(errmsg("requested starting point %X/%X is ahead of the WAL flush position of this server %X/%X",
						LSN_FORMAT_ARGS(cmd->startpoint),
						LSN_FORMAT_ARGS(FlushPtr))));
	}

	/* Start streaming from the requested point */
	sentPtr = cmd->startpoint;

	/* Initialize shared memory status, too */
	SpinLockAcquire(&MyWalSnd->mutex);
	MyWalSnd->sentPtr = sentPtr;
	SpinLockRelease(&MyWalSnd->mutex);

	SyncRepInitConfig();

	/* Infinite send loop, never returns */
	WalSndLoop(wp);

	WalSndSetState(WALSNDSTATE_STARTUP);

	if (cmd->slotname)
		ReplicationSlotRelease();
}

/*
 * Main loop that waits for LSN updates and calls the walproposer.
 * Synchronous replication sets latch in WalSndWakeup at walsender.c
 */
static void
WalSndLoop(WalProposer *wp)
{
	/* Clear any already-pending wakeups */
	ResetLatch(MyLatch);

	for (;;)
	{
		CHECK_FOR_INTERRUPTS();

		XLogBroadcastWalProposer(wp);

		if (MyWalSnd->state == WALSNDSTATE_CATCHUP)
			WalSndSetState(WALSNDSTATE_STREAMING);
		WalProposerPoll(wp);
	}
}

/*
 * Notify walproposer about the new WAL position.
 */
static void
XLogBroadcastWalProposer(WalProposer *wp)
{
	XLogRecPtr	startptr;
	XLogRecPtr	endptr;

	/* Start from the last sent position */
	startptr = sentPtr;

	/*
	 * Streaming the current timeline on a primary.
	 *
	 * Attempt to send all data that's already been written out and fsync'd to
	 * disk.  We cannot go further than what's been written out given the
	 * current implementation of WALRead().  And in any case it's unsafe to
	 * send WAL that is not securely down to disk on the primary: if the
	 * primary subsequently crashes and restarts, standbys must not have
	 * applied any WAL that got lost on the primary.
	 */
#if PG_VERSION_NUM >= 150000
	endptr = GetFlushRecPtr(NULL);
#else
	endptr = GetFlushRecPtr();
#endif

	/*
	 * Record the current system time as an approximation of the time at which
	 * this WAL location was written for the purposes of lag tracking.
	 *
	 * In theory we could make XLogFlush() record a time in shmem whenever WAL
	 * is flushed and we could get that time as well as the LSN when we call
	 * GetFlushRecPtr() above (and likewise for the cascading standby
	 * equivalent), but rather than putting any new code into the hot WAL path
	 * it seems good enough to capture the time here.  We should reach this
	 * after XLogFlush() runs WalSndWakeupProcessRequests(), and although that
	 * may take some time, we read the WAL flush pointer and take the time
	 * very close to together here so that we'll get a later position if it is
	 * still moving.
	 *
	 * Because LagTrackerWrite ignores samples when the LSN hasn't advanced,
	 * this gives us a cheap approximation for the WAL flush time for this
	 * LSN.
	 *
	 * Note that the LSN is not necessarily the LSN for the data contained in
	 * the present message; it's the end of the WAL, which might be further
	 * ahead.  All the lag tracking machinery cares about is finding out when
	 * that arbitrary LSN is eventually reported as written, flushed and
	 * applied, so that it can measure the elapsed time.
	 */
	LagTrackerWrite(endptr, GetCurrentTimestamp());

	/* Do we have any work to do? */
	Assert(startptr <= endptr);
	if (endptr <= startptr)
		return;

	WalProposerBroadcast(wp, startptr, endptr);
	sentPtr = endptr;

	/* Update shared memory status */
	{
		WalSnd	   *walsnd = MyWalSnd;

		SpinLockAcquire(&walsnd->mutex);
		walsnd->sentPtr = sentPtr;
		SpinLockRelease(&walsnd->mutex);
	}

	/* Report progress of XLOG streaming in PS display */
	if (update_process_title)
	{
		char		activitymsg[50];

		snprintf(activitymsg, sizeof(activitymsg), "streaming %X/%X",
				 LSN_FORMAT_ARGS(sentPtr));
		set_ps_display(activitymsg);
	}
}

/* Download WAL before basebackup for logical walsenders from sk, if needed */
static bool
WalProposerRecovery(WalProposer *wp, Safekeeper *sk)
{
	char	   *err;
	WalReceiverConn *wrconn;
	WalRcvStreamOptions options;
	char		conninfo[MAXCONNINFO];
	TimeLineID	timeline;
	XLogRecPtr	startpos;
	XLogRecPtr	endpos;
	uint64		download_range_mb;

	startpos = GetLogRepRestartLSN(wp);
	if (startpos == InvalidXLogRecPtr)
		return true;			/* recovery not needed */
	endpos = wp->propEpochStartLsn;

	timeline = wp->greetRequest.timeline;

	if (!neon_auth_token)
	{
		memcpy(conninfo, sk->conninfo, MAXCONNINFO);
	}
	else
	{
		int			written = 0;

		written = snprintf((char *) conninfo, MAXCONNINFO, "password=%s %s", neon_auth_token, sk->conninfo);
		if (written > MAXCONNINFO || written < 0)
			wpg_log(FATAL, "could not append password to the safekeeper connection string");
	}

#if PG_MAJORVERSION_NUM < 16
	wrconn = walrcv_connect(conninfo, false, "wal_proposer_recovery", &err);
#else
	wrconn = walrcv_connect(conninfo, false, false, "wal_proposer_recovery", &err);
#endif

	if (!wrconn)
	{
		ereport(WARNING,
				(errmsg("could not connect to WAL acceptor %s:%s: %s",
						sk->host, sk->port,
						err)));
		return false;
	}
	wpg_log(LOG,
			"start recovery for logical replication from %s:%s starting from %X/%08X till %X/%08X timeline "
			"%d",
			sk->host, sk->port, (uint32) (startpos >> 32),
			(uint32) startpos, (uint32) (endpos >> 32), (uint32) endpos, timeline);

	options.logical = false;
	options.startpoint = startpos;
	options.slotname = NULL;
	options.proto.physical.startpointTLI = timeline;

	if (walrcv_startstreaming(wrconn, &options))
	{
		XLogRecPtr	rec_start_lsn;
		XLogRecPtr	rec_end_lsn = 0;
		int			len;
		char	   *buf;
		pgsocket	wait_fd = PGINVALID_SOCKET;

		while ((len = walrcv_receive(wrconn, &buf, &wait_fd)) >= 0)
		{
			if (len == 0)
			{
				(void) WaitLatchOrSocket(
										 MyLatch, WL_EXIT_ON_PM_DEATH | WL_SOCKET_READABLE, wait_fd,
										 -1, WAIT_EVENT_WAL_RECEIVER_MAIN);
			}
			else
			{
				Assert(buf[0] == 'w' || buf[0] == 'k');
				if (buf[0] == 'k')
					continue;	/* keepalive */
				memcpy(&rec_start_lsn, &buf[XLOG_HDR_START_POS],
					   sizeof rec_start_lsn);
				rec_start_lsn = pg_ntoh64(rec_start_lsn);
				rec_end_lsn = rec_start_lsn + len - XLOG_HDR_SIZE;

				/* write WAL to disk */
				XLogWalPropWrite(sk->wp, &buf[XLOG_HDR_SIZE], len - XLOG_HDR_SIZE, rec_start_lsn);

				ereport(DEBUG1,
						(errmsg("Recover message %X/%X length %d",
								LSN_FORMAT_ARGS(rec_start_lsn), len)));
				if (rec_end_lsn >= endpos)
					break;
			}
		}
		ereport(LOG,
				(errmsg("end of replication stream at %X/%X: %m",
						LSN_FORMAT_ARGS(rec_end_lsn))));
		walrcv_disconnect(wrconn);

		/* failed to receive all WAL till endpos */
		if (rec_end_lsn < endpos)
			return false;
	}
	else
	{
		ereport(LOG,
				(errmsg("primary server contains no more WAL on requested timeline %u LSN %X/%08X",
						timeline, (uint32) (startpos >> 32), (uint32) startpos)));
		return false;
	}

	return true;
}

/*
 * These variables are used similarly to openLogFile/SegNo,
 * but for walproposer to write the XLOG during recovery. walpropFileTLI is the TimeLineID
 * corresponding the filename of walpropFile.
 */
static int	walpropFile = -1;
static TimeLineID walpropFileTLI = 0;
static XLogSegNo walpropSegNo = 0;

/*
 * Write XLOG data to disk.
 */
static void
XLogWalPropWrite(WalProposer *wp, char *buf, Size nbytes, XLogRecPtr recptr)
{
	int			startoff;
	int			byteswritten;

	/*
	 * Apart from walproposer, basebackup LSN page is also written out by
	 * postgres itself which writes WAL only in pages, and in basebackup it is
	 * inherently dummy (only safekeepers have historic WAL). Update WAL
	 * buffers here to avoid dummy page overwriting correct one we download
	 * here. Ugly, but alternatives are about the same ugly. We won't need
	 * that if we switch to on-demand WAL download from safekeepers, without
	 * writing to disk.
	 *
	 * https://github.com/neondatabase/neon/issues/5749
	 */
	if (!wp->config->syncSafekeepers)
		XLogUpdateWalBuffers(buf, recptr, nbytes);

	while (nbytes > 0)
	{
		int			segbytes;

		/* Close the current segment if it's completed */
		if (walpropFile >= 0 && !XLByteInSeg(recptr, walpropSegNo, wal_segment_size))
			XLogWalPropClose(recptr);

		if (walpropFile < 0)
		{
#if PG_VERSION_NUM >= 150000
			/* FIXME Is it ok to use hardcoded value here? */
			TimeLineID	tli = 1;
#else
			bool		use_existent = true;
#endif
			/* Create/use new log file */
			XLByteToSeg(recptr, walpropSegNo, wal_segment_size);
#if PG_VERSION_NUM >= 150000
			walpropFile = XLogFileInit(walpropSegNo, tli);
			walpropFileTLI = tli;
#else
			walpropFile = XLogFileInit(walpropSegNo, &use_existent, false);
			walpropFileTLI = ThisTimeLineID;
#endif
		}

		/* Calculate the start offset of the received logs */
		startoff = XLogSegmentOffset(recptr, wal_segment_size);

		if (startoff + nbytes > wal_segment_size)
			segbytes = wal_segment_size - startoff;
		else
			segbytes = nbytes;

		/* OK to write the logs */
		errno = 0;

		byteswritten = pg_pwrite(walpropFile, buf, segbytes, (off_t) startoff);
		if (byteswritten <= 0)
		{
			char		xlogfname[MAXFNAMELEN];
			int			save_errno;

			/* if write didn't set errno, assume no disk space */
			if (errno == 0)
				errno = ENOSPC;

			save_errno = errno;
			XLogFileName(xlogfname, walpropFileTLI, walpropSegNo, wal_segment_size);
			errno = save_errno;
			ereport(PANIC,
					(errcode_for_file_access(),
					 errmsg("could not write to log segment %s "
							"at offset %u, length %lu: %m",
							xlogfname, startoff, (unsigned long) segbytes)));
		}

		/* Update state for write */
		recptr += byteswritten;

		nbytes -= byteswritten;
		buf += byteswritten;
	}

	/*
	 * Close the current segment if it's fully written up in the last cycle of
	 * the loop.
	 */
	if (walpropFile >= 0 && !XLByteInSeg(recptr, walpropSegNo, wal_segment_size))
	{
		XLogWalPropClose(recptr);
	}
}

/*
 * Close the current segment.
 */
static void
XLogWalPropClose(XLogRecPtr recptr)
{
	Assert(walpropFile >= 0 && !XLByteInSeg(recptr, walpropSegNo, wal_segment_size));

	if (close(walpropFile) != 0)
	{
		char		xlogfname[MAXFNAMELEN];

		XLogFileName(xlogfname, walpropFileTLI, walpropSegNo, wal_segment_size);

		ereport(PANIC,
				(errcode_for_file_access(),
				 errmsg("could not close log segment %s: %m",
						xlogfname)));
	}

	walpropFile = -1;
}

static void
walprop_pg_wal_reader_allocate(Safekeeper *sk)
{
	char		log_prefix[64];

	snprintf(log_prefix, sizeof(log_prefix), WP_LOG_PREFIX "sk %s:%s nwr: ", sk->host, sk->port);
	Assert(!sk->xlogreader);
	sk->xlogreader = NeonWALReaderAllocate(wal_segment_size, sk->wp->propEpochStartLsn, sk->wp, log_prefix);
	if (sk->xlogreader == NULL)
		wpg_log(FATAL, "failed to allocate xlog reader");
}

static NeonWALReadResult
walprop_pg_wal_read(Safekeeper *sk, char *buf, XLogRecPtr startptr, Size count, char **errmsg)
{
	NeonWALReadResult res;

	res = NeonWALRead(sk->xlogreader,
					  buf,
					  startptr,
					  count,
					  walprop_pg_get_timeline_id());

	if (res == NEON_WALREAD_SUCCESS)
	{
		/*
		 * If we have the socket subscribed, but walreader doesn't need any
		 * events, it must mean that remote connection just closed hoping to
		 * do next read locally. Remove the socket then. It is important to do
		 * as otherwise next read might open another connection and we won't
		 * be able to distinguish whether we have correct socket added in wait
		 * event set.
		 */
		if (NeonWALReaderEvents(sk->xlogreader) == 0)
			rm_safekeeper_event_set(sk, false);
	}
	else if (res == NEON_WALREAD_ERROR)
	{
		*errmsg = NeonWALReaderErrMsg(sk->xlogreader);
	}

	return res;
}

static uint32
walprop_pg_wal_reader_events(Safekeeper *sk)
{
	return NeonWALReaderEvents(sk->xlogreader);
}

static WaitEventSet *waitEvents;

static void
walprop_pg_free_event_set(WalProposer *wp)
{
	if (waitEvents)
	{
		FreeWaitEventSet(waitEvents);
		waitEvents = NULL;
	}

	for (int i = 0; i < wp->n_safekeepers; i++)
	{
		wp->safekeeper[i].eventPos = -1;
		wp->safekeeper[i].nwrEventPos = -1;
		wp->safekeeper[i].nwrConnEstablished = false;
	}
}

static void
walprop_pg_init_event_set(WalProposer *wp)
{
	if (waitEvents)
		wpg_log(FATAL, "double-initialization of event set");

	/* for each sk, we have socket plus potentially socket for neon walreader */
	waitEvents = CreateWaitEventSet(TopMemoryContext, 2 + 2 * wp->n_safekeepers);
	AddWaitEventToSet(waitEvents, WL_LATCH_SET, PGINVALID_SOCKET,
					  MyLatch, NULL);
	AddWaitEventToSet(waitEvents, WL_EXIT_ON_PM_DEATH, PGINVALID_SOCKET,
					  NULL, NULL);

	for (int i = 0; i < wp->n_safekeepers; i++)
	{
		wp->safekeeper[i].eventPos = -1;
		wp->safekeeper[i].nwrEventPos = -1;
		wp->safekeeper[i].nwrConnEstablished = false;
	}
}

/* add safekeeper socket to wait event set */
static void
walprop_pg_add_safekeeper_event_set(Safekeeper *sk, uint32 events)
{
	Assert(sk->eventPos == -1);
	sk->eventPos = AddWaitEventToSet(waitEvents, events, walprop_socket(sk), NULL, sk);
}

/* add neon wal reader socket to wait event set */
static void
add_nwr_event_set(Safekeeper *sk, uint32 events)
{
	Assert(sk->nwrEventPos == -1);
	sk->nwrEventPos = AddWaitEventToSet(waitEvents, events, NeonWALReaderSocket(sk->xlogreader), NULL, sk);
	sk->nwrConnEstablished = NeonWALReaderIsRemConnEstablished(sk->xlogreader);
	wpg_log(DEBUG5, "sk %s:%s: added nwr socket events %d", sk->host, sk->port, events);
}

static void
walprop_pg_update_event_set(Safekeeper *sk, uint32 events)
{
	/* eventPos = -1 when we don't have an event */
	Assert(sk->eventPos != -1);

	ModifyWaitEvent(waitEvents, sk->eventPos, events, NULL);
}

/*
 * Update neon_walreader event.
 * Can be called when nwr socket doesn't exist, does nothing in this case.
 */
static void
update_nwr_event_set(Safekeeper *sk, uint32 events)
{
	/* eventPos = -1 when we don't have an event */
	if (sk->nwrEventPos != -1)
		ModifyWaitEvent(waitEvents, sk->nwrEventPos, events, NULL);
}


static void
walprop_pg_active_state_update_event_set(Safekeeper *sk)
{
	uint32		sk_events;
	uint32		nwr_events;

	Assert(sk->state == SS_ACTIVE);
	SafekeeperStateDesiredEvents(sk, &sk_events, &nwr_events);

	/*
	 * If we need to wait for neon_walreader, ensure we have up to date socket
	 * in the wait event set.
	 */
	if (sk->active_state == SS_ACTIVE_READ_WAL)
	{
		/*
		 * If conn is established and socket is thus stable, update the event
		 * directly; otherwise re-add it.
		 */
		if (sk->nwrConnEstablished)
		{
			Assert(sk->nwrEventPos != -1);
			update_nwr_event_set(sk, nwr_events);
		}
		else
		{
			rm_safekeeper_event_set(sk, false);
			add_nwr_event_set(sk, nwr_events);
		}
	}
	else
	{
		/*
		 * Hack: we should always set 0 here, but for random reasons
		 * WaitEventSet (WaitEventAdjustEpoll) asserts that there is at least
		 * some event. Since there is also no way to remove socket except
		 * reconstructing the whole set, SafekeeperStateDesiredEvents instead
		 * gives WL_SOCKET_CLOSED if socket exists. We never expect it to
		 * trigger.
		 *
		 * On PG 14 which doesn't have WL_SOCKET_CLOSED resort to event
		 * removal.
		 */
#if PG_VERSION_NUM >= 150000
		Assert(nwr_events == WL_SOCKET_CLOSED || nwr_events == 0);
		update_nwr_event_set(sk, WL_SOCKET_CLOSED);
#else							/* pg 14 */
		rm_safekeeper_event_set(sk, false);
#endif
	}
	walprop_pg_update_event_set(sk, sk_events);
}

static void
walprop_pg_rm_safekeeper_event_set(Safekeeper *to_remove)
{
	rm_safekeeper_event_set(to_remove, true);
}

/*
 * A hacky way to remove single event from the event set. Can be called if event
 * doesn't exist, does nothing in this case.
 *
 * Note: Internally, this completely reconstructs the event set. It should be
 * avoided if possible.
 *
 * If is_sk is true, socket of connection to safekeeper is removed; otherwise
 * socket of neon_walreader.
 */
static void
rm_safekeeper_event_set(Safekeeper *to_remove, bool is_sk)
{
	WalProposer *wp = to_remove->wp;

	wpg_log(DEBUG5, "sk %s:%s: removing event, is_sk %d",
			to_remove->host, to_remove->port, is_sk);

	/*
	 * Shortpath for exiting if have nothing to do. We never call this
	 * function with safekeeper socket not existing, but do that with neon
	 * walreader socket.
	 */
	if ((is_sk && to_remove->eventPos == -1) ||
		(!is_sk && to_remove->nwrEventPos == -1))
	{
		return;
	}

	/* Remove the existing event set, assign sk->eventPos = -1 */
	walprop_pg_free_event_set(wp);

	/* Re-initialize it without adding any safekeeper events */
	wp->api.init_event_set(wp);

	/*
	 * loop through the existing safekeepers. If they aren't the one we're
	 * removing, and if they have a socket we can use, re-add the applicable
	 * events.
	 */
	for (int i = 0; i < wp->n_safekeepers; i++)
	{
		Safekeeper *sk = &wp->safekeeper[i];

		/*
		 * If this safekeeper isn't offline, add events for it, except for the
		 * event requested to remove.
		 */
		if (sk->state != SS_OFFLINE)
		{
			uint32		sk_events;
			uint32		nwr_events;

			SafekeeperStateDesiredEvents(sk, &sk_events, &nwr_events);

			if (sk != to_remove || !is_sk)
			{
				/* will set sk->eventPos */
				wp->api.add_safekeeper_event_set(sk, sk_events);
			}
			if ((sk != to_remove || is_sk) && nwr_events)
			{
				add_nwr_event_set(sk, nwr_events);
			}
		}
	}
}

static int
walprop_pg_wait_event_set(WalProposer *wp, long timeout, Safekeeper **sk, uint32 *events)
{
	WaitEvent	event = {0};
	int			rc = 0;
	bool		late_cv_trigger = false;

	*sk = NULL;
	*events = 0;

#if PG_MAJORVERSION_NUM >= 16
	if (WalSndCtl != NULL)
		ConditionVariablePrepareToSleep(&WalSndCtl->wal_flush_cv);

	/*
	 * Now that we prepared the condvar, check flush ptr again -- it might
	 * have changed before we subscribed to cv so we missed the wakeup.
	 *
	 * Do that only when we're interested in new WAL: without sync-safekeepers
	 * and if election already passed.
	 */
	if (!wp->config->syncSafekeepers && wp->availableLsn != InvalidXLogRecPtr && GetFlushRecPtr(NULL) > wp->availableLsn)
	{
		ConditionVariableCancelSleep();
		ResetLatch(MyLatch);
		*events = WL_LATCH_SET;
		return 1;
	}
#endif

	/*
	 * Wait for a wait event to happen, or timeout: - Safekeeper socket can
	 * become available for READ or WRITE - Our latch got set, because *
	 * PG15-: We got woken up by a process triggering the WalSender * PG16+:
	 * WalSndCtl->wal_flush_cv was triggered
	 */
	rc = WaitEventSetWait(waitEvents, timeout,
						  &event, 1, WAIT_EVENT_WAL_SENDER_MAIN);
#if PG_MAJORVERSION_NUM >= 16
	if (WalSndCtl != NULL)
		late_cv_trigger = ConditionVariableCancelSleep();
#endif

	/*
	 * If wait is terminated by latch set (walsenders' latch is set on each
	 * wal flush). (no need for pm death check due to WL_EXIT_ON_PM_DEATH)
	 */
	if ((rc == 1 && event.events & WL_LATCH_SET) || late_cv_trigger)
	{
		/* Reset our latch */
		ResetLatch(MyLatch);
		*events = WL_LATCH_SET;
		return 1;
	}

	/*
	 * If the event contains something about the socket, it means we got an
	 * event from a safekeeper socket.
	 */
	if (rc == 1 && (event.events & (WL_SOCKET_MASK)))
	{
		*sk = (Safekeeper *) event.user_data;
		*events = event.events;
		return 1;
	}

	/* XXX: Can we have non-timeout event here? */
	*events = event.events;
	return rc;
}

static void
walprop_pg_finish_sync_safekeepers(WalProposer *wp, XLogRecPtr lsn)
{
	fprintf(stdout, "%X/%X\n", LSN_FORMAT_ARGS(lsn));
	exit(0);
}

/*
 * Choose most advanced PageserverFeedback and set it to *rf.
 */
static void
GetLatestNeonFeedback(PageserverFeedback *rf, WalProposer *wp)
{
	int			latest_safekeeper = 0;
	XLogRecPtr	last_received_lsn = InvalidXLogRecPtr;

	for (int i = 0; i < wp->n_safekeepers; i++)
	{
		if (wp->safekeeper[i].appendResponse.rf.last_received_lsn > last_received_lsn)
		{
			latest_safekeeper = i;
			last_received_lsn = wp->safekeeper[i].appendResponse.rf.last_received_lsn;
		}
	}

	rf->currentClusterSize = wp->safekeeper[latest_safekeeper].appendResponse.rf.currentClusterSize;
	rf->last_received_lsn = wp->safekeeper[latest_safekeeper].appendResponse.rf.last_received_lsn;
	rf->disk_consistent_lsn = wp->safekeeper[latest_safekeeper].appendResponse.rf.disk_consistent_lsn;
	rf->remote_consistent_lsn = wp->safekeeper[latest_safekeeper].appendResponse.rf.remote_consistent_lsn;
	rf->replytime = wp->safekeeper[latest_safekeeper].appendResponse.rf.replytime;

	wpg_log(DEBUG2, "GetLatestNeonFeedback: currentClusterSize %lu,"
			" last_received_lsn %X/%X, disk_consistent_lsn %X/%X, remote_consistent_lsn %X/%X, replytime %lu",
			rf->currentClusterSize,
			LSN_FORMAT_ARGS(rf->last_received_lsn),
			LSN_FORMAT_ARGS(rf->disk_consistent_lsn),
			LSN_FORMAT_ARGS(rf->remote_consistent_lsn),
			rf->replytime);
}

/*
 * Combine hot standby feedbacks from all safekeepers.
 */
static void
CombineHotStanbyFeedbacks(HotStandbyFeedback *hs, WalProposer *wp)
{
	hs->ts = 0;
	hs->xmin = InvalidFullTransactionId;
	hs->catalog_xmin = InvalidFullTransactionId;

	for (int i = 0; i < wp->n_safekeepers; i++)
	{
		elog(LOG, "hs.ts=%ld hs.xmin=%ld", wp->safekeeper[i].appendResponse.hs.ts, wp->safekeeper[i].appendResponse.hs.xmin);
 		if (wp->safekeeper[i].appendResponse.hs.ts != 0)
		{
			HotStandbyFeedback *skhs = &wp->safekeeper[i].appendResponse.hs;

			if (FullTransactionIdIsNormal(skhs->xmin)
				&& (!FullTransactionIdIsValid(hs->xmin) || FullTransactionIdPrecedes(skhs->xmin, hs->xmin)))
			{
				hs->xmin = skhs->xmin;
				hs->ts = skhs->ts;
			}
			if (FullTransactionIdIsNormal(skhs->catalog_xmin)
				&& (!FullTransactionIdIsValid(hs->catalog_xmin) || FullTransactionIdPrecedes(skhs->catalog_xmin, hs->catalog_xmin)))
			{
				hs->catalog_xmin = skhs->catalog_xmin;
				hs->ts = skhs->ts;
			}
		}
	}
}

/*
 * Based on commitLsn and safekeeper responses including pageserver feedback,
 * 1) Propagate cluster size received from ps to ensure the limit.
 * 2) Propagate pageserver LSN positions to ensure backpressure limits.
 * 3) Advance walproposer slot to commitLsn (releasing WAL & waking up waiters).
 * 4) Propagate hot standby feedback.
 *
 * None of that is functional in sync-safekeepers.
 */
static void
walprop_pg_process_safekeeper_feedback(WalProposer *wp, XLogRecPtr commitLsn)
{
	HotStandbyFeedback hsFeedback;
	XLogRecPtr	oldDiskConsistentLsn;

	if (wp->config->syncSafekeepers)
		return;

	oldDiskConsistentLsn = quorumFeedback.rf.disk_consistent_lsn;

	/* Get PageserverFeedback fields from the most advanced safekeeper */
	GetLatestNeonFeedback(&quorumFeedback.rf, wp);
	replication_feedback_set(&quorumFeedback.rf);
	SetZenithCurrentClusterSize(quorumFeedback.rf.currentClusterSize);

	if (commitLsn > quorumFeedback.flushLsn || oldDiskConsistentLsn != quorumFeedback.rf.disk_consistent_lsn)
	{
		if (commitLsn > quorumFeedback.flushLsn)
			quorumFeedback.flushLsn = commitLsn;

		/*
		 * Advance the replication slot to commitLsn. WAL before it is
		 * hardened and will be fetched from one of safekeepers by
		 * neon_walreader if needed.
		 *
		 * Also wakes up syncrep waiters.
		 */
		ProcessStandbyReply(
		/* write_lsn -  This is what durably stored in WAL service. */
							quorumFeedback.flushLsn,
		/* flush_lsn - This is what durably stored in WAL service. */
							quorumFeedback.flushLsn,

		/*
		 * apply_lsn - This is what processed and durably saved at*
		 * pageserver.
		 */
							quorumFeedback.rf.disk_consistent_lsn,
							walprop_pg_get_current_timestamp(wp), false);
	}

	CombineHotStanbyFeedbacks(&hsFeedback, wp);
	if (hsFeedback.ts != 0 && memcmp(&hsFeedback, &quorumFeedback.hs, sizeof hsFeedback) != 0)
	{
		quorumFeedback.hs = hsFeedback;
		elog(LOG, "ProcessStandbyHSFeedback(xmin=%d, catalog_xmin=%d", XidFromFullTransactionId(hsFeedback.xmin), XidFromFullTransactionId(hsFeedback.catalog_xmin));
		ProcessStandbyHSFeedback(hsFeedback.ts,
								 XidFromFullTransactionId(hsFeedback.xmin),
								 EpochFromFullTransactionId(hsFeedback.xmin),
								 XidFromFullTransactionId(hsFeedback.catalog_xmin),
								 EpochFromFullTransactionId(hsFeedback.catalog_xmin));
	} else
		elog(LOG, "Skip HSFeedback ts=%ld, xmin=%d, catalog_xmin=%d", hsFeedback.ts, XidFromFullTransactionId(hsFeedback.xmin), XidFromFullTransactionId(hsFeedback.catalog_xmin));
}

static XLogRecPtr
walprop_pg_get_redo_start_lsn(WalProposer *wp)
{
	return GetRedoStartLsn();
}

static bool
walprop_pg_strong_random(WalProposer *wp, void *buf, size_t len)
{
	return pg_strong_random(buf, len);
}

static void
walprop_pg_log_internal(WalProposer *wp, int level, const char *line)
{
	elog(FATAL, "unexpected log_internal message at level %d: %s", level, line);
}

static XLogRecPtr
GetLogRepRestartLSN(WalProposer *wp)
{
	FILE	   *f;
	XLogRecPtr	lrRestartLsn = InvalidXLogRecPtr;

	/* We don't need to do anything in syncSafekeepers mode. */
	if (wp->config->syncSafekeepers)
		return InvalidXLogRecPtr;

	/*
	 * If there are active logical replication subscription we need to provide
	 * enough WAL for their WAL senders based on th position of their
	 * replication slots.
	 */
	f = fopen("restart.lsn", "rb");
	if (f != NULL)
	{
		size_t		rc = fread(&lrRestartLsn, sizeof(lrRestartLsn), 1, f);

		fclose(f);
		if (rc == 1 && lrRestartLsn != InvalidXLogRecPtr)
		{
			uint64		download_range_mb;

			wpg_log(LOG, "logical replication restart LSN %X/%X", LSN_FORMAT_ARGS(lrRestartLsn));

			/*
			 * If we need to download more than a max_slot_wal_keep_size,
			 * don't do it to avoid risk of exploding pg_wal. Logical
			 * replication won't work until recreated, but at least compute
			 * would start; this also follows max_slot_wal_keep_size
			 * semantics.
			 */
			download_range_mb = (wp->propEpochStartLsn - lrRestartLsn) / MB;
			if (max_slot_wal_keep_size_mb > 0 && download_range_mb >= max_slot_wal_keep_size_mb)
			{
				wpg_log(WARNING, "not downloading WAL for logical replication since %X/%X as max_slot_wal_keep_size=%dMB",
						LSN_FORMAT_ARGS(lrRestartLsn), max_slot_wal_keep_size_mb);
				return InvalidXLogRecPtr;
			}

			/*
			 * start from the beginning of the segment to fetch page headers
			 * verifed by XLogReader
			 */
			lrRestartLsn = lrRestartLsn - XLogSegmentOffset(lrRestartLsn, wal_segment_size);
		}
	}
	return lrRestartLsn;
}

static const walproposer_api walprop_pg = {
	.get_shmem_state = walprop_pg_get_shmem_state,
	.start_streaming = walprop_pg_start_streaming,
	.get_flush_rec_ptr = walprop_pg_get_flush_rec_ptr,
	.get_current_timestamp = walprop_pg_get_current_timestamp,
	.conn_error_message = walprop_error_message,
	.conn_status = walprop_status,
	.conn_connect_start = walprop_connect_start,
	.conn_connect_poll = walprop_connect_poll,
	.conn_send_query = walprop_send_query,
	.conn_get_query_result = walprop_get_query_result,
	.conn_flush = walprop_flush,
	.conn_finish = walprop_finish,
	.conn_async_read = walprop_async_read,
	.conn_async_write = walprop_async_write,
	.conn_blocking_write = walprop_blocking_write,
	.recovery_download = WalProposerRecovery,
	.wal_reader_allocate = walprop_pg_wal_reader_allocate,
	.wal_read = walprop_pg_wal_read,
	.wal_reader_events = walprop_pg_wal_reader_events,
	.init_event_set = walprop_pg_init_event_set,
	.update_event_set = walprop_pg_update_event_set,
	.active_state_update_event_set = walprop_pg_active_state_update_event_set,
	.add_safekeeper_event_set = walprop_pg_add_safekeeper_event_set,
	.rm_safekeeper_event_set = walprop_pg_rm_safekeeper_event_set,
	.wait_event_set = walprop_pg_wait_event_set,
	.strong_random = walprop_pg_strong_random,
	.get_redo_start_lsn = walprop_pg_get_redo_start_lsn,
	.finish_sync_safekeepers = walprop_pg_finish_sync_safekeepers,
	.process_safekeeper_feedback = walprop_pg_process_safekeeper_feedback,
	.log_internal = walprop_pg_log_internal,
};

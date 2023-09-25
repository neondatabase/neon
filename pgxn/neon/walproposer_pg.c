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
#include "libpq-fe.h"

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

static TimestampTz
walprop_pg_get_current_timestamp(void)
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
		elog(ERROR, "libpqwalreceiver didn't initialize correctly");
}

/* Header in walproposer.h -- Wrapper struct to abstract away the libpq connection */
struct WalProposerConn
{
	PGconn	   *pg_conn;
	bool		is_nonblocking; /* whether the connection is non-blocking */
	char	   *recvbuf;		/* last received data from
								 * walprop_async_read */
};

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
walprop_error_message(WalProposerConn *conn)
{
	return PQerrorMessage(conn->pg_conn);
}

static WalProposerConnStatusType
walprop_status(WalProposerConn *conn)
{
	switch (PQstatus(conn->pg_conn))
	{
		case CONNECTION_OK:
			return WP_CONNECTION_OK;
		case CONNECTION_BAD:
			return WP_CONNECTION_BAD;
		default:
			return WP_CONNECTION_IN_PROGRESS;
	}
}

static WalProposerConn *
walprop_connect_start(char *conninfo, char *password)
{
	WalProposerConn *conn;
	PGconn	   *pg_conn;
	const char *keywords[3];
	const char *values[3];
	int			n;

	/*
	 * Connect using the given connection string. If the
	 * NEON_AUTH_TOKEN environment variable was set, use that as
	 * the password.
	 *
	 * The connection options are parsed in the order they're given, so
	 * when we set the password before the connection string, the
	 * connection string can override the password from the env variable.
	 * Seems useful, although we don't currently use that capability
	 * anywhere.
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
	 * Allocation of a PQconn can fail, and will return NULL. We want to fully
	 * replicate the behavior of PQconnectStart here.
	 */
	if (!pg_conn)
		return NULL;

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

static WalProposerConnectPollStatusType
walprop_connect_poll(WalProposerConn *conn)
{
	WalProposerConnectPollStatusType return_val;

	switch (PQconnectPoll(conn->pg_conn))
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
			elog(FATAL, "Unexpected PGRES_POLLING_ACTIVE returned from PQconnectPoll");

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

static bool
walprop_send_query(WalProposerConn *conn, char *query)
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

static WalProposerExecStatusType
walprop_get_query_result(WalProposerConn *conn)
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
		elog(WARNING, "[libpqwalproposer] Unexpected successful end of command results");
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
		elog(WARNING, "[libpqwalproposer] Unexpected successful %s", unexpected_success);

	return return_val;
}

static pgsocket
walprop_socket(WalProposerConn *conn)
{
	return PQsocket(conn->pg_conn);
}

static int
walprop_flush(WalProposerConn *conn)
{
	return (PQflush(conn->pg_conn));
}

static void
walprop_finish(WalProposerConn *conn)
{
	if (conn->recvbuf != NULL)
		PQfreemem(conn->recvbuf);
	PQfinish(conn->pg_conn);
	pfree(conn);
}

/*
 * Receive a message from the safekeeper.
 *
 * On success, the data is placed in *buf. It is valid until the next call
 * to this function.
 */
static PGAsyncReadResult
walprop_async_read(WalProposerConn *conn, char **buf, int *amount)
{
	int			result;

	if (conn->recvbuf != NULL)
	{
		PQfreemem(conn->recvbuf);
		conn->recvbuf = NULL;
	}

	/* Call PQconsumeInput so that we have the data we need */
	if (!PQconsumeInput(conn->pg_conn))
	{
		*amount = 0;
		*buf = NULL;
		return PG_ASYNC_READ_FAIL;
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
	switch (result = PQgetCopyData(conn->pg_conn, &conn->recvbuf, true))
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
					elog(FATAL, "unexpected result status %d after failed PQgetCopyData", status);

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
			*amount = result;
			*buf = conn->recvbuf;
			return PG_ASYNC_READ_SUCCESS;
	}
}

static PGAsyncWriteResult
walprop_async_write(WalProposerConn *conn, void const *buf, size_t size)
{
	int			result;

	/* If we aren't in non-blocking mode, switch to it. */
	if (!ensure_nonblocking_status(conn, true))
		return PG_ASYNC_WRITE_FAIL;

	/*
	 * The docs for PQputcopyData list the return values as: 1 if the data was
	 * queued, 0 if it was not queued because of full buffers, or -1 if an
	 * error occurred
	 */
	result = PQputCopyData(conn->pg_conn, buf, size);

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
			elog(FATAL, "invalid return %d from PQputCopyData", result);
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
	switch (result = PQflush(conn->pg_conn))
	{
		case 0:
			return PG_ASYNC_WRITE_SUCCESS;
		case 1:
			return PG_ASYNC_WRITE_TRY_FLUSH;
		case -1:
			return PG_ASYNC_WRITE_FAIL;
		default:
			elog(FATAL, "invalid return %d from PQflush", result);
	}
}

/*
 * This function is very similar to walprop_async_write. For more
 * information, refer to the comments there.
 */
static bool
walprop_blocking_write(WalProposerConn *conn, void const *buf, size_t size)
{
	int			result;

	/* If we are in non-blocking mode, switch out of it. */
	if (!ensure_nonblocking_status(conn, false))
		return false;

	if ((result = PQputCopyData(conn->pg_conn, buf, size)) == -1)
		return false;

	Assert(result == 1);

	/* Because the connection is non-blocking, flushing returns 0 or -1 */

	if ((result = PQflush(conn->pg_conn)) == -1)
		return false;

	Assert(result == 0);
	return true;
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
	.get_flush_rec_ptr = walprop_pg_get_flush_rec_ptr,
	.get_current_timestamp = walprop_pg_get_current_timestamp,
	.get_timeline_id = walprop_pg_get_timeline_id,
	.load_libpqwalreceiver = walprop_pg_load_libpqwalreceiver,
	.conn_error_message = walprop_error_message,
	.conn_status = walprop_status,
	.conn_connect_start = walprop_connect_start,
	.conn_connect_poll = walprop_connect_poll,
	.conn_send_query = walprop_send_query,
	.conn_get_query_result = walprop_get_query_result,
	.conn_socket = walprop_socket,
	.conn_flush = walprop_flush,
	.conn_finish = walprop_finish,
	.conn_async_read = walprop_async_read,
	.conn_async_write = walprop_async_write,
	.conn_blocking_write = walprop_blocking_write,
};

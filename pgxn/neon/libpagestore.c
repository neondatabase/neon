/*-------------------------------------------------------------------------
 *
 * libpagestore.c
 *	  Handles network communications with the remote pagestore.
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	 contrib/neon/libpqpagestore.c
 *
 *-------------------------------------------------------------------------
 */
#include <pthread.h>

#include "postgres.h"

#include "access/xlog.h"
#include "common/hashfn.h"
#include "fmgr.h"
#include "libpq-fe.h"
#include "libpq/libpq.h"
#include "libpq/pqformat.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "portability/instr_time.h"
#include "postmaster/bgworker.h"
#include "postmaster/interrupt.h"
#include "storage/buf_internals.h"
#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "storage/pg_shmem.h"
#include "storage/proc.h"
#include "storage/procsignal.h"
#include "utils/guc.h"

#include "neon.h"
#include "neon_perf_counters.h"
#include "neon_utils.h"
#include "pagestore_client.h"
#include "walproposer.h"

#ifdef __linux__
#include <sys/ioctl.h>
#include <linux/sockios.h>
#endif

#define PageStoreTrace DEBUG5

#define MIN_RECONNECT_INTERVAL_USEC 1000
#define MAX_RECONNECT_INTERVAL_USEC 1000000
#define RECEIVER_RETRY_DELAY_USEC   1000000
#define MAX_REQUEST_SIZE            1024
#define MAX_PS_QUERY_LENGTH         256

/* GUCs */
char	   *neon_timeline;
char	   *neon_tenant;
int32		max_cluster_size;
char	   *page_server_connstring;
char	   *neon_auth_token;

int			max_prefetch_distance = 128;
int			parallel_connections = 10;

int         neon_protocol_version = 3;

static int	stripe_size;

static pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;

#define CHAN_TO_SHARD(chan_no) ((chan_no) / parallel_connections)

void CommunicatorMain(Datum main_arg);

/* Produce error message in critical section for thgread safety */
#define neon_shard_log_cs(shard_no, tag, fmt, ...) do { 				\
		pthread_mutex_lock(&mutex);										\
		ereport(tag,													\
				(errmsg(NEON_TAG "[shard %d] " fmt, shard_no, ##__VA_ARGS__), \
				 errhidestmt(true), errhidecontext(true), errposition(0), internalerrposition(0))); \
		pthread_mutex_unlock(&mutex);									\
	} while (0)

typedef struct
{
	char		connstring[MAX_SHARDS][MAX_PAGESERVER_CONNSTRING_SIZE];
	size_t		num_shards;
} ShardMap;

static ShardMap shard_map;

#if PG_VERSION_NUM >= 150000
static shmem_request_hook_type prev_shmem_request_hook = NULL;
#endif
static shmem_startup_hook_type prev_shmem_startup_hook;

static NeonCommunicatorResponse* responses; /* for each backend */
static NeonCommunicatorChannel* channels;

#if PG_VERSION_NUM < 170000
int MyProcNumber;
#endif

#ifndef PG_IOV_MAX
#define PG_IOV_MAX 32
#endif

static bool am_communicator = false;

typedef enum PSConnectionState {
	PS_Disconnected,			/* no connection yet */
	PS_Connecting_Startup,		/* connection starting up */
	PS_Connecting_PageStream,	/* negotiating pagestream */
	PS_Connected,				/* connected, pagestream established */
	PS_Expired,					/* connection should be reestablished */
} PSConnectionState;

/* This backend's per-shard connections */
typedef struct
{
	TimestampTz		last_connect_time; /* read-only debug value */
	TimestampTz		last_reconnect_time;
	uint32			delay_us;
	int				n_reconnect_attempts;

	/*---
	 * Pageserver connection state, i.e.
	 *	disconnected: conn == NULL, wes == NULL;
	 *	conn_startup: connection initiated, waiting for connection establishing
	 *	conn_ps:      PageStream query sent, waiting for confirmation
	 *	connected:    PageStream established
	 */
	PSConnectionState state;
	PGconn		   *conn;

	/* request / response counters for debugging */
	uint64			nrequests_sent;
	uint64			nresponses_received;

	/*---
	 * WaitEventSet containing:
	 *	- WL_SOCKET_READABLE on 'conn'
	 *	- WL_LATCH_SET on MyLatch, and
	 *	- WL_EXIT_ON_PM_DEATH.
	 */
	WaitEventSet   *wes_read;
} PageServer;

static PageServer* page_servers;

static void pageserver_disconnect(int chan_no);


static void* communicator_read_loop(void* arg);
static void* communicator_write_loop(void* arg);

/*
 * Produce log message under mutex because it is not thread-safe
 */
static void
log_error_message(NeonErrorResponse* err)
{
	int save_pid = MyProcPid;
	pthread_mutex_lock(&mutex);
	MyProcPid = ProcGlobal->allProcs[err->req.u.recepient.procno].pid;
	neon_log(LOG, "Server returns error for request %d: %s", err->req.tag, err->message);
	MyProcPid = save_pid;
	pthread_mutex_unlock(&mutex);
}

/*
 * We stablish `parallel_connections with each shard to support parallel procerssing of requests at PS
 */
static Size
MaxNumberOfChannels(void)
{
	return MAX_SHARDS * parallel_connections;
}

/*
 * Parse a comma-separated list of connection strings into a ShardMap.
 *
 * If 'result' is NULL, just checks that the input is valid. If the input is
 * not valid, returns false. The contents of *result are undefined in
 * that case, and must not be relied on.
 */
static bool
ParseShardMap(const char *connstr, ShardMap *result)
{
	const char *p;
	int			nshards = 0;

	if (result)
		memset(result, 0, sizeof(ShardMap));

	p = connstr;
	nshards = 0;
	for (;;)
	{
		const char *sep;
		size_t		connstr_len;

		sep = strchr(p, ',');
		connstr_len = sep != NULL ? sep - p : strlen(p);

		if (connstr_len == 0 && sep == NULL)
			break;				/* ignore trailing comma */

		if (nshards >= MAX_SHARDS)
		{
			neon_log(LOG, "Too many shards");
			return false;
		}
		if (connstr_len >= MAX_PAGESERVER_CONNSTRING_SIZE)
		{
			neon_log(LOG, "Connection string too long");
			return false;
		}
		if (result)
		{
			memcpy(result->connstring[nshards], p, connstr_len);
			result->connstring[nshards][connstr_len] = '\0';
		}
		nshards++;

		if (sep == NULL)
			break;
		p = sep + 1;
	}
	if (result)
		result->num_shards = nshards;

	return true;
}

static bool
CheckPageserverConnstring(char **newval, void **extra, GucSource source)
{
	char	   *p = *newval;

	return ParseShardMap(p, NULL);
}

static void
AssignPageserverConnstring(const char *newval, void *extra)
{
	ShardMap	shard_map;
	size_t 		old_num_shards;

	/*
	 * Only communicator background worker estblish connections with page server and need this information
	 */
	if (!am_communicator)
		return;

	old_num_shards = shard_map.num_shards;

	if (!ParseShardMap(newval, &shard_map))
	{
		/*
		 * shouldn't happen, because we already checked the value in
		 * CheckPageserverConnstring
		 */
		elog(ERROR, "could not parse shard map");
	}

	if (page_servers == NULL)
	{
		page_servers = (PageServer*)calloc(MaxNumberOfChannels(), sizeof(PageServer));
	}

	/* Force to reestablish connection with old shards */
	for (size_t i = 0; i < old_num_shards; i++)
	{
		if (page_servers[i].state == PS_Connected)
		{
			/* TODO: race condition */
			page_servers[i].state = PS_Expired;
		}
	}

	/* Start workers for new shards */
	for (size_t i = old_num_shards; i < shard_map.num_shards; i++)
	{
		pthread_t reader, writer;
		void* chan_no = (void*)i;
		pthread_create(&writer, NULL, communicator_write_loop, chan_no);
		pthread_create(&reader, NULL, communicator_read_loop, chan_no);
	}
}

int
get_shard_number(NRelFileInfo rinfo, BlockNumber blocknum)
{
	uint32		hash;

#if PG_MAJORVERSION_NUM < 16
	hash = murmurhash32(rinfo.relNode);
	hash = hash_combine(hash, murmurhash32(blocknum / stripe_size));
#else
	hash = murmurhash32(rinfo.relNumber);
	hash = hash_combine(hash, murmurhash32(blocknum / stripe_size));
#endif

	return hash % shard_map.num_shards;
}

/*
 * Perform disconnect in critical section because it is not thread-safe
 */
static void
cleanup_and_disconnect(PageServer *ps)
{
	pthread_mutex_lock(&mutex);

	if (ps->wes_read)
	{
		FreeWaitEventSet(ps->wes_read);
		ps->wes_read = NULL;
	}
	if (ps->conn)
	{
		MyNeonCounters->pageserver_disconnects_total++;
		PQfinish(ps->conn);
		ps->conn = NULL;
	}

	ps->state = PS_Disconnected;

	pthread_mutex_lock(&mutex);
}

/*
 * Like pchmop but uses malloc instead palloc for thread safety
 */
static char*
chomp(char const* in)
{
	size_t		n;

	n = strlen(in);
	while (n > 0 && in[n - 1] == '\n')
		n--;
	return strndup(in, n);
}

/*
 * Connect to a pageserver, or continue to try to connect if we're yet to
 * complete the connection (e.g. due to receiving an earlier cancellation
 * during connection start).
 * Returns true if successfully connected; false if the connection failed.
 *
 * Throws errors in unrecoverable situations, or when this backend's query
 * is canceled.
 */
static bool
pageserver_connect(int chan_no, int elevel)
{
	PageServer *ps = &page_servers[chan_no];
	char        pagestream_query[MAX_PS_QUERY_LENGTH];
	int			shard_no = CHAN_TO_SHARD(chan_no);
	char*		connstr = shard_map.connstring[shard_no];

	switch (ps->state)
	{
	case PS_Disconnected:
	{
		const char *keywords[3];
		const char *values[3];
		int			n_pgsql_params;
		TimestampTz	now;
		int64		us_since_last_attempt;

		/* Make sure we start with a clean slate */
		cleanup_and_disconnect(ps);

		neon_shard_log_cs(shard_no, DEBUG5, "Connection state: Disconnected");

		now = GetCurrentTimestamp();
		us_since_last_attempt = (int64) (now - ps->last_reconnect_time);
		ps->last_reconnect_time = now;

		/*
		 * Make sure we don't do exponential backoff with a constant multiplier
		 * of 0 us, as that doesn't really do much for timeouts...
		 *
		 * cf. https://github.com/neondatabase/neon/issues/7897
		 */
		if (ps->delay_us == 0)
			ps->delay_us = MIN_RECONNECT_INTERVAL_USEC;

		/*
		 * If we did other tasks between reconnect attempts, then we won't
		 * need to wait as long as a full delay.
		 */
		if (us_since_last_attempt < ps->delay_us)
		{
			pg_usleep(ps->delay_us - us_since_last_attempt);
		}

		/* update the delay metric */
		ps->delay_us = Min(ps->delay_us * 2, MAX_RECONNECT_INTERVAL_USEC);

		/*
		 * Connect using the connection string we got from the
		 * neon.pageserver_connstring GUC. If the NEON_AUTH_TOKEN environment
		 * variable was set, use that as the password.
		 *
		 * The connection options are parsed in the order they're given, so when
		 * we set the password before the connection string, the connection string
		 * can override the password from the env variable. Seems useful, although
		 * we don't currently use that capability anywhere.
		 */
		keywords[0] = "dbname";
		values[0] = connstr;
		n_pgsql_params = 1;

		if (neon_auth_token)
		{
			keywords[1] = "password";
			values[1] = neon_auth_token;
			n_pgsql_params++;
		}

		keywords[n_pgsql_params] = NULL;
		values[n_pgsql_params] = NULL;

		ps->conn = PQconnectStartParams(keywords, values, 1);
		if (PQstatus(ps->conn) == CONNECTION_BAD)
		{
			char	   *msg = chomp(PQerrorMessage(ps->conn));
			cleanup_and_disconnect(ps);
			ereport(elevel,
					(errcode(ERRCODE_SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION),
						errmsg(NEON_TAG "[shard %d] could not establish connection to pageserver", chan_no),
						errdetail_internal("%s", msg)));
			free(msg);
			return false;
		}
		ps->state = PS_Connecting_Startup;
	}
	/* FALLTHROUGH */
	case PS_Connecting_Startup:
	{
		int			ps_send_query_ret;
		bool		connected = false;
		int poll_result = PGRES_POLLING_WRITING;
		neon_shard_log_cs(shard_no, DEBUG5, "Connection state: Connecting_Startup");

		do
		{
			switch (poll_result)
			{
			default: /* unknown/unused states are handled as a failed connection */
			case PGRES_POLLING_FAILED:
				{
					char	   *pqerr = PQerrorMessage(ps->conn);
					char	   *msg = NULL;
					neon_shard_log_cs(shard_no, DEBUG5, "POLLING_FAILED");

					if (pqerr)
						msg = chomp(pqerr);

					cleanup_and_disconnect(ps);

					if (msg)
					{
						neon_shard_log_cs(shard_no, elevel,
										  "could not connect to pageserver: %s",
										  msg);
						free(msg);
					}
					else
						neon_shard_log_cs(shard_no, elevel,
										  "could not connect to pageserver");

					return false;
				}
			case PGRES_POLLING_READING:
				/* Sleep until there's something to do */
				while (true)
				{
					int rc = WaitLatchOrSocket(MyLatch,
											   WL_EXIT_ON_PM_DEATH | WL_LATCH_SET | WL_SOCKET_READABLE,
											   PQsocket(ps->conn),
											   0,
											   WAIT_EVENT_NEON_PS_STARTING);
					if (rc & WL_LATCH_SET)
					{
						ResetLatch(MyLatch);
						/* query cancellation, backend shutdown */
						CHECK_FOR_INTERRUPTS();
					}
					if (rc & WL_SOCKET_READABLE)
						break;
				}
				/* PQconnectPoll() handles the socket polling state updates */

				break;
			case PGRES_POLLING_WRITING:
				/* Sleep until there's something to do */
				while (true)
				{
					int rc = WaitLatchOrSocket(MyLatch,
											   WL_EXIT_ON_PM_DEATH | WL_LATCH_SET | WL_SOCKET_WRITEABLE,
											   PQsocket(ps->conn),
											   0,
											   WAIT_EVENT_NEON_PS_STARTING);
					if (rc & WL_LATCH_SET)
					{
						ResetLatch(MyLatch);
						/* query cancellation, backend shutdown */
						CHECK_FOR_INTERRUPTS();
					}
					if (rc & WL_SOCKET_WRITEABLE)
						break;
				}
				/* PQconnectPoll() handles the socket polling state updates */

				break;
			case PGRES_POLLING_OK:
				neon_shard_log_cs(shard_no, DEBUG5, "POLLING_OK");
				connected = true;
				break;
			}
			poll_result = PQconnectPoll(ps->conn);
		}
		while (!connected);

		/* No more polling needed; connection succeeded */
		ps->last_connect_time = GetCurrentTimestamp();

		/* Allocate wait event set in critical section */
		pthread_mutex_lock(&mutex);
#if PG_MAJORVERSION_NUM >= 17
		ps->wes_read = CreateWaitEventSet(NULL, 3);
#else
		ps->wes_read = CreateWaitEventSet(TopMemoryContext, 3);
#endif
		AddWaitEventToSet(ps->wes_read, WL_LATCH_SET, PGINVALID_SOCKET,
						  MyLatch, NULL);
		AddWaitEventToSet(ps->wes_read, WL_EXIT_ON_PM_DEATH, PGINVALID_SOCKET,
						  NULL, NULL);
		AddWaitEventToSet(ps->wes_read, WL_SOCKET_READABLE, PQsocket(ps->conn), NULL, NULL);
		pthread_mutex_unlock(&mutex);


		switch (neon_protocol_version)
		{
		case 3:
			snprintf(pagestream_query, sizeof pagestream_query, "pagestream_v3 %s %s", neon_tenant, neon_timeline);
			break;
		case 2:
			snprintf(pagestream_query, sizeof pagestream_query, "pagestream_v2 %s %s", neon_tenant, neon_timeline);
			break;
		default:
			neon_shard_log_cs(shard_no, ERROR, "unexpected neon_protocol_version %d", neon_protocol_version);
		}

		if (PQstatus(ps->conn) == CONNECTION_BAD)
		{
			char	   *msg = chomp(PQerrorMessage(ps->conn));

			cleanup_and_disconnect(ps);

			ereport(elevel,
					(errcode(ERRCODE_SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION),
						errmsg(NEON_TAG "[shard %d] could not establish connection to pageserver", chan_no),
						errdetail_internal("%s", msg)));
			free(msg);
			return false;
		}

		ps_send_query_ret = PQsendQuery(ps->conn, pagestream_query);
		if (ps_send_query_ret != 1)
		{
			cleanup_and_disconnect(ps);

			neon_shard_log_cs(shard_no, elevel, "could not send pagestream command to pageserver");
			return false;
		}

		ps->state = PS_Connecting_PageStream;
	}
	/* FALLTHROUGH */
	case PS_Connecting_PageStream:
	{
		neon_shard_log_cs(shard_no, DEBUG5, "Connection state: Connecting_PageStream");

		if (PQstatus(ps->conn) == CONNECTION_BAD)
		{
			char	   *msg = chomp(PQerrorMessage(ps->conn));
			cleanup_and_disconnect(ps);
			ereport(elevel,
					(errcode(ERRCODE_SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION),
						errmsg(NEON_TAG "[shard %d] could not establish connection to pageserver", chan_no),
						errdetail_internal("%s", msg)));
			free(msg);
			return false;
		}

		while (PQisBusy(ps->conn))
		{
			WaitEvent	event;

			/* Sleep until there's something to do */
			(void) WaitEventSetWait(ps->wes_read, -1L, &event, 1,
									WAIT_EVENT_NEON_PS_CONFIGURING);
			ResetLatch(MyLatch);

			CHECK_FOR_INTERRUPTS();

			/* Data available in socket? */
			if (event.events & WL_SOCKET_READABLE)
			{
				if (!PQconsumeInput(ps->conn))
				{
					char	   *msg = chomp(PQerrorMessage(ps->conn));

					cleanup_and_disconnect(ps);
					neon_shard_log_cs(shard_no, elevel, "could not complete handshake with pageserver: %s",
									  msg);
					free(msg);
					return false;
				}
			}
		}

		ps->state = PS_Connected;
		ps->nrequests_sent = 0;
		ps->nresponses_received = 0;
	}
	/* FALLTHROUGH */
	case PS_Connected:
		/*
		 * We successfully connected. Future connections to this PageServer
		 * will do fast retries again, with exponential backoff.
		 */
		ps->delay_us = MIN_RECONNECT_INTERVAL_USEC;

		neon_shard_log_cs(shard_no, LOG, "libpagestore: connected to '%s' with protocol version %d", connstr, neon_protocol_version);
		return true;
	default:
		neon_shard_log_cs(shard_no, ERROR, "libpagestore: invalid connection state %d", ps->state);
	}
	/* This shouldn't be hit */
	Assert(false);
}

/*
 * A wrapper around PQgetCopyData that checks for interrupts while sleeping.
 */
static int
call_PQgetCopyData(int chan_no, char **buffer)
{
	int			ret;
	PageServer *ps = &page_servers[chan_no];
	PGconn	   *pageserver_conn = ps->conn;
	instr_time	now,
				start_ts,
				since_start,
				last_log_ts,
				since_last_log;
	bool		logged = false;
	int			shard_no = CHAN_TO_SHARD(chan_no);

	/*
	 * As a debugging aid, if we don't get a response for a long time, print a
	 * log message.
	 *
	 * 10 s is a very generous threshold, normally we expect a response in a
	 * few milliseconds. We have metrics to track latencies in normal ranges,
	 * but in the cases that take exceptionally long, it's useful to log the
	 * exact timestamps.
	 */
#define LOG_INTERVAL_MS		INT64CONST(10 * 1000)

	INSTR_TIME_SET_CURRENT(now);
	start_ts = last_log_ts = now;
	INSTR_TIME_SET_ZERO(since_last_log);

retry:
	ret = PQgetCopyData(pageserver_conn, buffer, 1 /* async */ );

	if (ret == 0)
	{
		WaitEvent	event;
		long		timeout;

		timeout = Max(0, LOG_INTERVAL_MS - INSTR_TIME_GET_MILLISEC(since_last_log));

		/* Sleep until there's something to do */
		(void) WaitEventSetWait(ps->wes_read, timeout, &event, 1,
								WAIT_EVENT_NEON_PS_READ);
		ResetLatch(MyLatch);

		CHECK_FOR_INTERRUPTS();

		/* Data available in socket? */
		if (event.events & WL_SOCKET_READABLE)
		{
			if (!PQconsumeInput(pageserver_conn))
			{
				char	   *msg = chomp(PQerrorMessage(pageserver_conn));

				neon_shard_log_cs(shard_no, LOG, "could not get response from pageserver: %s", msg);
				free(msg);
				return -1;
			}
		}

		/*
		 * Print a message to the log if a long time has passed with no
		 * response.
		 */
		INSTR_TIME_SET_CURRENT(now);
		since_last_log = now;
		INSTR_TIME_SUBTRACT(since_last_log, last_log_ts);
		if (INSTR_TIME_GET_MILLISEC(since_last_log) >= LOG_INTERVAL_MS)
		{
			int sndbuf = -1;
			int recvbuf = -1;
#ifdef __linux__
			int socketfd;
#endif

			since_start = now;
			INSTR_TIME_SUBTRACT(since_start, start_ts);

#ifdef __linux__
			/*
			 * get kernel's send and recv queue size via ioctl
			 * https://elixir.bootlin.com/linux/v6.1.128/source/include/uapi/linux/sockios.h#L25-L27
			 */
			socketfd = PQsocket(pageserver_conn);
			if (socketfd != -1) {
				int ioctl_err;
				ioctl_err = ioctl(socketfd, SIOCOUTQ, &sndbuf);
				if (ioctl_err!= 0) {
					sndbuf = -errno;
				}
				ioctl_err = ioctl(socketfd, FIONREAD, &recvbuf);
				if (ioctl_err != 0) {
					recvbuf = -errno;
				}
			}
#endif
			neon_shard_log_cs(shard_no, LOG, "no response received from pageserver for %0.3f s, still waiting (sent " UINT64_FORMAT " requests, received " UINT64_FORMAT " responses) (socket sndbuf=%d recvbuf=%d)",
							  INSTR_TIME_GET_DOUBLE(since_start),
							  ps->nrequests_sent, ps->nresponses_received, sndbuf, recvbuf);
			last_log_ts = now;
			logged = true;
		}

		goto retry;
	}

	/*
	 * If we logged earlier that the response is taking a long time, log
	 * another message when the response is finally received.
	 */
	if (logged)
	{
		INSTR_TIME_SET_CURRENT(now);
		since_start = now;
		INSTR_TIME_SUBTRACT(since_start, start_ts);
		neon_shard_log_cs(shard_no, LOG, "received response from pageserver after %0.3f s",
						  INSTR_TIME_GET_DOUBLE(since_start));
	}

	return ret;
}

/*
 * Disconnect from specified shard
 */
static void
pageserver_disconnect(int chan_no)
{
	PageServer *ps = &page_servers[chan_no];
	/*
	 * If anything goes wrong while we were sending a request, it's not clear
	 * what state the connection is in. For example, if we sent the request
	 * but didn't receive a response yet, we might receive the response some
	 * time later after we have already sent a new unrelated request. Close
	 * the connection to avoid getting confused.
	 * Similarly, even when we're in PS_DISCONNECTED, we may have junk to
	 * clean up: It is possible that we encountered an error allocating any
	 * of the wait event sets or the psql connection, or failed when we tried
	 * to attach wait events to the WaitEventSets.
	 */
	cleanup_and_disconnect(ps);
}

static bool
pageserver_send(int chan_no, StringInfo msg)
{
	PageServer *ps = &page_servers[chan_no];
	PGconn	   *pageserver_conn;
	int			shard_no = CHAN_TO_SHARD(chan_no);

	MyNeonCounters->pageserver_requests_sent_total++;

	/* If the connection was lost for some reason, reconnect */
	if (ps->state == PS_Connected && PQstatus(ps->conn) == CONNECTION_BAD)
	{
		neon_shard_log_cs(shard_no, LOG, "pageserver_send disconnect bad connection");
		pageserver_disconnect(chan_no);
		pageserver_conn = NULL;
	}

	/*
	 * If pageserver is stopped, the connections from compute node are broken.
	 * The compute node doesn't notice that immediately, but it will cause the
	 * next request to fail, usually on the next query. That causes
	 * user-visible errors if pageserver is restarted, or the tenant is moved
	 * from one pageserver to another. See
	 * https://github.com/neondatabase/neon/issues/1138 So try to reestablish
	 * connection in case of failure.
	 */
	if (ps->state != PS_Connected)
	{
		if (ps->state == PS_Expired)
		{
			neon_shard_log_cs(shard_no, LOG, "pageserver_send disconnect expired connection");
			pageserver_disconnect(chan_no);
			pageserver_conn = NULL;
		}
		while (!pageserver_connect(chan_no, LOG))
		{
			ps->n_reconnect_attempts += 1;
		}
		ps->n_reconnect_attempts = 0;
	} else {
		Assert(ps->conn != NULL);
	}

	pageserver_conn = ps->conn;

	/*
	 * Send request.
	 *
	 * In principle, this could block if the output buffer is full, and we
	 * should use async mode and check for interrupts while waiting. In
	 * practice, our requests are small enough to always fit in the output and
	 * TCP buffer.
	 *
	 * Note that this also will fail when the connection is in the
	 * PGRES_POLLING_WRITING state. It's kinda dirty to disconnect at this
	 * point, but on the grand scheme of things it's only a small issue.
	 */
	ps->nrequests_sent++;
	if (PQputCopyData(pageserver_conn, msg->data, msg->len) <= 0)
	{
		char	   *msg = chomp(PQerrorMessage(pageserver_conn));

		pageserver_disconnect(chan_no);
		neon_shard_log_cs(shard_no, LOG, "pageserver_send disconnected: failed to send page request (try to reconnect): %s", msg);
		free(msg);
		return false;
	}
	/*
	 * TODO: may be not flush each request?
	 */
	if (PQflush(pageserver_conn))
	{
		char	   *msg = chomp(PQerrorMessage(pageserver_conn));

		pageserver_disconnect(chan_no);
		neon_shard_log_cs(shard_no, LOG, "pageserver_flush disconnect because failed to flush page requests: %s", msg);
		free(msg);
		return false;
	}
	return true;
}

/*
 * Receive request from page server.
 * Returns NULL if there is connection to page server.
 * It is expected that writer thread will restablish connection.
 */
static NeonResponse *
pageserver_receive(int chan_no)
{
	StringInfoData resp_buff;
	NeonResponse *resp;
	PageServer *ps = &page_servers[chan_no];
	PGconn	   *pageserver_conn = ps->conn;
	int			shard_no = CHAN_TO_SHARD(chan_no);
	/* read response */
	int			rc;

	/* TODO: fix race condition between sender and receiver */
	if (ps->state != PS_Connected)
	{
		neon_shard_log_cs(shard_no, LOG,
						  "pageserver_receive: returning NULL for non-connected pageserver connection: 0x%02x",
						  ps->state);
		return NULL;
	}

	Assert(pageserver_conn);

	rc = call_PQgetCopyData(chan_no, &resp_buff.data);
	if (rc >= 0)
	{
		/* call_PQgetCopyData handles rc == 0 */
		Assert(rc > 0);

		/* FIXME: is it thread safe? */
		PG_TRY();
		{
			resp_buff.len = rc;
			resp_buff.cursor = 0;
			resp = nm_unpack_response(&resp_buff);
			PQfreemem(resp_buff.data);
		}
		PG_CATCH();
		{
			neon_shard_log_cs(shard_no, LOG, "pageserver_receive: disconnect due to failure while parsing response");
			pageserver_disconnect(chan_no);
		}
		PG_END_TRY();
	}
	else if (rc == -1)
	{
		neon_shard_log_cs(shard_no, LOG, "pageserver_receive disconnect: psql end of copy data: %s", chomp(PQerrorMessage(pageserver_conn)));
		pageserver_disconnect(chan_no);
		resp = NULL;
	}
	else if (rc == -2)
	{
		char	   *msg = chomp(PQerrorMessage(pageserver_conn));

		pageserver_disconnect(chan_no);
		neon_shard_log_cs(shard_no, ERROR, "pageserver_receive disconnect: could not read COPY data: %s", msg);
	}
	else
	{
		pageserver_disconnect(chan_no);
		neon_shard_log_cs(shard_no, ERROR, "pageserver_receive disconnect: unexpected PQgetCopyData return value: %d", rc);
	}

	ps->nresponses_received++;
	return (NeonResponse *) resp;
}

static bool
check_neon_id(char **newval, void **extra, GucSource source)
{
	uint8		id[16];

	return **newval == '\0' || HexDecodeString(id, *newval, 16);
}


/*
 * Each backend can send up to max_prefetch_distance prefetch requests and one vectored read request.
 * Backends are splitted between parallel conntions so each worker has tpo server at most MaxBackends / parallel_connections
 * backends.
 */
static Size
RequestBufferSize(void)
{
	return (max_prefetch_distance + PG_IOV_MAX) * (MaxBackends + (parallel_connections - 1) / parallel_connections);
}

static Size
CommunicatorShmemSize(void)
{
	return RequestBufferSize() * MaxNumberOfChannels() * sizeof(NeonCommunicatorRequest)
		+ MaxNumberOfChannels() * sizeof(NeonCommunicatorChannel)
		+ sizeof(NeonCommunicatorResponse) * MaxBackends;
}

static Size
PagestoreShmemSize(void)
{
	return CommunicatorShmemSize() + NeonPerfCountersShmemSize();
}

static bool
PagestoreShmemInit(void)
{
	bool		found;

	#if PG_VERSION_NUM < 170000
	MyProcNumber = MyProc->pgprocno;
	#endif

	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);
	channels = ShmemInitStruct("communicator shared state",
							   CommunicatorShmemSize(),
							   &found);
	if (!found)
	{
		size_t n_channels = MaxNumberOfChannels();
		NeonCommunicatorRequest* requests = (NeonCommunicatorRequest*)(channels + n_channels);

		for (size_t i = 0; i < n_channels; i++)
		{
			NeonCommunicatorChannel* chan = &channels[i];
			pg_atomic_init_u64(&chan->write_pos, 0);
			pg_atomic_init_u64(&chan->read_pos, 0);
			InitLatch(&chan->latch);
			chan->requests = requests;
			requests += RequestBufferSize();
		}
		responses = (NeonCommunicatorResponse*)requests;
	}

	NeonPerfCountersShmemInit();

	LWLockRelease(AddinShmemInitLock);
	return found;
}

static void
pagestore_shmem_startup_hook(void)
{
	if (prev_shmem_startup_hook)
		prev_shmem_startup_hook();

	PagestoreShmemInit();
}

static void
pagestore_shmem_request(void)
{
#if PG_VERSION_NUM >= 150000
	if (prev_shmem_request_hook)
		prev_shmem_request_hook();
#endif

	RequestAddinShmemSpace(PagestoreShmemSize());
}

static void
pagestore_prepare_shmem(void)
{
#if PG_VERSION_NUM >= 150000
	prev_shmem_request_hook = shmem_request_hook;
	shmem_request_hook = pagestore_shmem_request;
#else
	pagestore_shmem_request();
#endif
	prev_shmem_startup_hook = shmem_startup_hook;
	shmem_startup_hook = pagestore_shmem_startup_hook;
}

/*
 * Put request in one of communication channels.
 * We have `parallel_connections` channels with each shard.
 * Each backend is assigned to some particular connection (it is done for better
 * batching of subsequent requests at page server side)
 */
void
communicator_send_request(int shard, NeonCommunicatorRequest* req)
{
	/* bind backend to the particular channel */
	NeonCommunicatorChannel* chan = &channels[shard * parallel_connections + (MyProcNumber % parallel_connections)];
	size_t ring_size = RequestBufferSize();
	uint64 write_pos = pg_atomic_add_fetch_u64(&chan->write_pos, 1); /* reserve write position */
	uint64 read_pos;

	Assert(req->hdr.u.reqid == 0); /* ring overflow should not happen */
	req->hdr.u.recepient.procno = MyProcNumber;

	/* copy request */
	chan->requests[(size_t)(write_pos % ring_size)] = *req;

	/* will be overwritten with response code when request will be processed */
	responses[MyProcNumber].tag = req->hdr.tag;

	/* enforce memory battier before pinging communicator */
	pg_write_barrier();

	/* advance read-up-to position */
	do {
		read_pos = write_pos;
	} while (!pg_atomic_compare_exchange_u64(&chan->read_pos, &read_pos, write_pos+1));

	SetLatch(&chan->latch);
}

/*
 * Wait response from page server.
 * We are waiting until request message tag will be replaced with response tag,
 * using backed's latch.
 */
int64
communicator_receive_response(void)
{
	while (responses[MyProcNumber].tag <= T_NeonTestRequest) /* response not yet received */
	{
		(void) WaitLatch(MyLatch,
						 WL_LATCH_SET | WL_EXIT_ON_PM_DEATH,
						 -1L,
						 WAIT_EVENT_NEON_PS_READ);
	}
	if (responses[MyProcNumber].tag == T_NeonErrorResponse)
	{
		elog(ERROR, "Request failed"); /* detailed error message is printed by communicator */
	}
	return responses[MyProcNumber].value;
}

/*
 * Send request and wait for response
 */
int64
communicator_request(int shard, NeonCommunicatorRequest* req)
{
	communicator_send_request(shard, req);
	return communicator_receive_response();
}


void
CommunicatorMain(Datum main_arg)
{
	am_communicator = true;

	/* Establish signal handlers. */
	pqsignal(SIGUSR1, procsignal_sigusr1_handler);
	pqsignal(SIGHUP, SignalHandlerForConfigReload);
	pqsignal(SIGTERM, SignalHandlerForShutdownRequest);
	BackgroundWorkerUnblockSignals();

	while (!ShutdownRequestPending)
	{
		(void) WaitLatch(MyLatch,
						 WL_LATCH_SET | WL_EXIT_ON_PM_DEATH,
						 -1L,
						 PG_WAIT_EXTENSION);
		CHECK_FOR_INTERRUPTS();
	}
}

static void
register_communicator_worker(void)
{
	BackgroundWorker bgw;
	memset(&bgw, 0, sizeof(bgw));
	bgw.bgw_flags = BGWORKER_SHMEM_ACCESS;
	bgw.bgw_start_time = BgWorkerStart_RecoveryFinished;
	snprintf(bgw.bgw_library_name, BGW_MAXLEN, "neon");
	snprintf(bgw.bgw_function_name, BGW_MAXLEN, "CommunicatorMain");
	snprintf(bgw.bgw_name, BGW_MAXLEN, "Page server communicator");
	snprintf(bgw.bgw_type, BGW_MAXLEN, "Page server communicator");
	bgw.bgw_restart_time = 5;
	bgw.bgw_notify_pid = 0;
	bgw.bgw_main_arg = (Datum) 0;

	RegisterBackgroundWorker(&bgw);
}

/*
 * Module initialization function
 */
void
pg_init_libpagestore(void)
{
	pagestore_prepare_shmem();

	DefineCustomStringVariable("neon.pageserver_connstring",
							   "connection string to the page server",
							   NULL,
							   &page_server_connstring,
							   "",
							   PGC_SIGHUP,
							   0,	/* no flags required */
							   CheckPageserverConnstring, AssignPageserverConnstring, NULL);

	DefineCustomStringVariable("neon.timeline_id",
							   "Neon timeline_id the server is running on",
							   NULL,
							   &neon_timeline,
							   "",
							   PGC_POSTMASTER,
							   0,	/* no flags required */
							   check_neon_id, NULL, NULL);

	DefineCustomStringVariable("neon.tenant_id",
							   "Neon tenant_id the server is running on",
							   NULL,
							   &neon_tenant,
							   "",
							   PGC_POSTMASTER,
							   0,	/* no flags required */
							   check_neon_id, NULL, NULL);

	DefineCustomIntVariable("neon.stripe_size",
							"sharding stripe size",
							NULL,
							&stripe_size,
							32768, 1, INT_MAX,
							PGC_SIGHUP,
							GUC_UNIT_BLOCKS,
							NULL, NULL, NULL);

	DefineCustomIntVariable("neon.max_cluster_size",
							"cluster size limit",
							NULL,
							&max_cluster_size,
							-1, -1, INT_MAX,
							PGC_SIGHUP,
							GUC_UNIT_MB,
							NULL, NULL, NULL);

	DefineCustomIntVariable("neon.max_prefetch_distance",
							"Maximal number of prefetch requests",
							"effetive_io_concurrency and maintenance_io_concurrecy should not be larger than sthis value",
							&max_prefetch_distance,
							128, 16, 1024,
							PGC_POSTMASTER,
							0,	/* no flags required */
							NULL, NULL, NULL);

	DefineCustomIntVariable("neon.parallel_connections",
							"number of connections to each shard",
							NULL,
							&parallel_connections,
							10, 1, 16,
							PGC_POSTMASTER,
							0,	/* no flags required */
							NULL, NULL, NULL);

	DefineCustomIntVariable("neon.protocol_version",
							"Version of compute<->page server protocol",
							NULL,
							&neon_protocol_version,
							3,	/* Communicator requires protocol version 3 or higher */
							3,	/* min */
							3,	/* max */
							PGC_SU_BACKEND,
							0,	/* no flags required */
							NULL, NULL, NULL);

	relsize_hash_init();

	register_communicator_worker();

	/*
	 * Retrieve the auth token to use when connecting to pageserver and
	 * safekeepers
	 */
	neon_auth_token = getenv("NEON_AUTH_TOKEN");
	if (neon_auth_token)
		neon_log(LOG, "using storage auth token from NEON_AUTH_TOKEN environment variable");

	if (page_server_connstring && page_server_connstring[0])
	{
		neon_log(PageStoreTrace, "set neon_smgr hook");
		smgr_hook = smgr_neon;
		smgr_init_hook = smgr_init_neon;
		dbsize_hook = neon_dbsize;
	}

	lfc_init();
}

/*
 * Same as initStringInfo but use malloc instead of palloc for thread safety
 */
static void
allocStringInfo(StringInfo s, size_t size)
{
	s->data = (char *)malloc(size);
	s->maxlen = size;
	resetStringInfo(s);
}

/*
 * Main proc of writer thread
 */
static void*
communicator_write_loop(void* arg)
{
	uint64 read_start_pos = 0;
	size_t chan_no = (size_t)arg;
	NeonCommunicatorChannel* chan = &channels[chan_no];
	size_t ring_size = RequestBufferSize();
	StringInfoData s;

	allocStringInfo(&s, MAX_REQUEST_SIZE);

	while (true)
	{
		NeonCommunicatorRequest* req;
		uint64 read_end_pos;

		/* Number of shards is decreased so this worker is not needed any more */
		if (chan_no >= shard_map.num_shards * parallel_connections)
			return NULL;

		read_end_pos = pg_atomic_read_u64(&chan->read_pos);
		Assert(read_start_pos <= read_end_pos);
		while (read_start_pos == read_end_pos)
		{
			int events = WaitLatch(&chan->latch, WL_LATCH_SET|WL_POSTMASTER_DEATH, -1, WAIT_EVENT_NEON_PS_SEND);
			if (events & WL_POSTMASTER_DEATH)
			   return NULL;
		}
		req = &chan->requests[read_start_pos++ % ring_size];
		nm_pack_request(&s, &req->hdr);
		Assert(s.maxlen == MAX_REQUEST_SIZE); /* string buffer was not reallocated */
		pageserver_send(chan_no, &s);
		req->hdr.u.reqid = 0; /* mark requests as processed */
	}
}

/*
 * Main proc of reader thread
 */
static void*
communicator_read_loop(void* arg)
{
	NeonResponse* resp;
	int64	value = 0;
	size_t	chan_no = (size_t)arg;
	int		shard_no = CHAN_TO_SHARD(chan_no);

	while (true)
	{
		/* Number of shards is decreased */
		if (chan_no >= shard_map.num_shards * parallel_connections)
			return NULL;

		resp = pageserver_receive(chan_no);
		if (resp == NULL)
		{
			/* Assume that writer will restablish connection */
			pg_usleep(RECEIVER_RETRY_DELAY_USEC);
			continue;
		}
		switch (resp->tag)
		{
			case T_NeonExistsResponse:
				value = ((NeonExistsResponse*)resp)->exists;
				break;
			case T_NeonNblocksResponse:
				value = ((NeonNblocksResponse*)resp)->n_blocks;
				break;
			case T_NeonDbSizeResponse:
				value = ((NeonDbSizeResponse*)resp)->db_size;
				break;
			case T_NeonGetPageResponse:
			{
				NeonGetPageResponse* page_resp = (NeonGetPageResponse*)resp;
				if (resp->u.recepient.bufid == InvalidBuffer)
				{
					/* result of prefetch */
					(void) lfc_prefetch(page_resp->req.rinfo, page_resp->req.forknum, page_resp->req.blkno, page_resp->page, resp->not_modified_since);
					free(resp);
					continue; /* should not notify backend */
				}
				else
				{
					BufferTag tag;
					Buffer buf = resp->u.recepient.bufid;
					BufferDesc *buf_desc = GetBufferDescriptor(buf - 1);
					InitBufferTag(&tag, &page_resp->req.rinfo, page_resp->req.forknum, page_resp->req.blkno);
					if (!BufferTagsEqual(&buf_desc->tag, &tag))
					{
						neon_shard_log_cs(shard_no, PANIC, "Get page request {rel=%u/%u/%u.%u block=%u} referecing wrpng buffer {rel=%u/%u/%u.%u block=%u}",
										  RelFileInfoFmt(page_resp->req.rinfo), page_resp->req.forknum, page_resp->req.blkno,
										  RelFileInfoFmt(BufTagGetNRelFileInfo(buf_desc->tag)), buf_desc->tag.forkNum, buf_desc->tag.blockNum);
					}
					/* Copy page content to shared buffer */
					memcpy(BufferGetBlock(resp->u.recepient.bufid), page_resp->page, BLCKSZ);
				}
				break;
			}
			case T_NeonErrorResponse:
				log_error_message((NeonErrorResponse *) resp);
				break;
			default:
				break;
		}
		responses[resp->u.recepient.procno].value = value;
		/* enforce write barrier before writing response code which server as received response indicator */
		pg_write_barrier();
		responses[resp->u.recepient.procno].tag = resp->tag;
		SetLatch(&ProcGlobal->allProcs[resp->u.recepient.procno].procLatch);
		free(resp);
	}
}


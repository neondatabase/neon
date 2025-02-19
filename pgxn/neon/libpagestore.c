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
#include <poll.h>

#include "postgres.h"

#include "access/twophase.h"
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

#define MIN_RECONNECT_INTERVAL_USEC	1000
#define MAX_RECONNECT_INTERVAL_USEC	1000000
#define RECEIVER_RETRY_DELAY_USEC	1000000
#define MAX_REQUEST_SIZE			1024
#define MAX_PS_QUERY_LENGTH			256
#define PROCARRAY_MAXPROCS			(MaxBackends + max_prepared_xacts)


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

static bool	am_communicator;

#define CHAN_TO_SHARD(chan_no) ((chan_no) / parallel_connections)

PGDLLEXPORT void CommunicatorMain(Datum main_arg);

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
	size_t		n_shards;
} ShardMap;

static ShardMap shard_map;

#if PG_VERSION_NUM >= 150000
static shmem_request_hook_type prev_shmem_request_hook = NULL;
#endif
static shmem_startup_hook_type prev_shmem_startup_hook;

#if PG_VERSION_NUM < 170000
int MyProcNumber;
#endif

#ifndef PG_IOV_MAX
#define PG_IOV_MAX 32
#endif

typedef enum PSConnectionState {
	PS_Disconnected,			/* no connection yet */
	PS_Connected,				/* connected, pagestream established */
	PS_PendingDisconnect,		/* connection should be reestablished */
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
} PageServer;

static PageServer* page_servers;


typedef struct
{
	size_t n_shards;
	NeonCommunicatorChannel* channels;
	NeonCommunicatorResponse* responses; /* for each backend */
} NeonCommunicatorSharedState;

static NeonCommunicatorSharedState* communicator;

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
	MyProcPid = ProcGlobal->allProcs[err->req.u.recepient.procno-1].pid;
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
	const char *p = connstr;
	int			n_shards = 0;

	if (result)
		memset(result, 0, sizeof(ShardMap));

	for (;;)
	{
		const char *sep;
		size_t		connstr_len;

		sep = strchr(p, ',');
		connstr_len = sep != NULL ? sep - p : strlen(p);

		if (connstr_len == 0 && sep == NULL)
			break;				/* ignore trailing comma */

		if (n_shards >= MAX_SHARDS)
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
			memcpy(result->connstring[n_shards], p, connstr_len);
			result->connstring[n_shards][connstr_len] = '\0';
		}
		n_shards++;

		if (sep == NULL)
			break;
		p = sep + 1;
	}
	if (result)
		communicator->n_shards = result->n_shards = n_shards;

	return true;
}

static bool
CheckPageserverConnstring(char **newval, void **extra, GucSource source)
{
	return ParseShardMap(*newval, NULL);
}

static void
AssignPageserverConnstring(const char *newval, void *extra)
{
	size_t 		old_n_shards;

	/*
	 * Only communicator background worker estblish connections with page server and need this information
	 */
	if (!am_communicator)
		return;

	old_n_shards = shard_map.n_shards;

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
	for (size_t i = 0; i < old_n_shards * parallel_connections; i++)
	{
		if (page_servers[i].state == PS_Connected)
		{
			/* TODO: race condition */
			page_servers[i].state = PS_PendingDisconnect;
		}
	}

	/* Start workers for new shards */
	for (size_t i = old_n_shards * parallel_connections; i < shard_map.n_shards * parallel_connections; i++)
	{
		pthread_t reader, writer;
		void* chan_no = (void*)i;
		pthread_create(&writer, NULL, communicator_write_loop, chan_no);
		pthread_create(&reader, NULL, communicator_read_loop, chan_no);
		pthread_detach(reader);
		pthread_detach(writer);
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

	return hash % communicator->n_shards;
}

/*
 * Perform disconnect in critical section because it is not thread-safe
 */
static void
cleanup_and_disconnect(PageServer *ps)
{
	if (ps->conn)
	{
		MyNeonCounters->pageserver_disconnects_total++;
		PQfinish(ps->conn);
		ps->conn = NULL;
	}

	ps->state = PS_Disconnected;
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
 * TODO: we have to defined this pq_send/pq_poll function to perform raw write to non-blocking socket.
 * Is there and bette/portable/simpler way in :ostgres to do it?
 */
#if PG_VERSION_NUM < 170000
static int
PQsocketPoll(int sock, int forRead, int forWrite, time_t end_time)
{
	struct pollfd input_fd;
	int			timeout_ms;

	if (!forRead && !forWrite)
		return 0;

	input_fd.fd = sock;
	input_fd.events = POLLERR;
	input_fd.revents = 0;

	if (forRead)
		input_fd.events |= POLLIN;
	if (forWrite)
		input_fd.events |= POLLOUT;

	/* Compute appropriate timeout interval */
	if (end_time == ((time_t) -1))
		timeout_ms = -1;
	else
	{
		time_t		now = time(NULL);

		if (end_time > now)
			timeout_ms = (end_time - now) * 1000;
		else
			timeout_ms = 0;
	}

	return poll(&input_fd, 1, timeout_ms);
}
#endif

static int
pq_poll(PGconn *conn, int forRead, int forWrite, time_t end_time)
{
	int result;
	do
	{
		result = PQsocketPoll(PQsocket(conn), forRead, forWrite, end_time);
	}
	while (result < 0 && errno == EINTR);
	return result;
}

static ssize_t
pq_send(PGconn *conn, const void *ptr, size_t len)
{
	size_t offs = 0;
	while (offs < len)
	{
		ssize_t rc = send(PQsocket(conn), (char*)ptr + offs, len - offs, 0);
		if (rc < 0)
		{
			/* Anything except EAGAIN/EWOULDBLOCK/EINTR is trouble */
			switch (errno)
			{
				case EAGAIN:
					break;
				case EINTR:
					continue;
				default:
					return rc;
			}
			rc = pq_poll(conn, false, true, -1);
			if (rc < 0)
			{
				return rc;
			}
		} else {
			offs += rc;
		}
	}
	return offs;
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
	const char *keywords[3];
	const char *values[3];
	int			n_pgsql_params;
	TimestampTz	now;
	int64		us_since_last_attempt;
	PGresult*   res;

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

	ps->conn = PQconnectdbParams(keywords, values, 1);
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
	ps->last_connect_time = GetCurrentTimestamp();
	snprintf(pagestream_query, sizeof pagestream_query, "pagestream_v%d %s %s", neon_protocol_version, neon_tenant, neon_timeline);

	res = PQexec(ps->conn, pagestream_query);
	if (PQresultStatus(res) != PGRES_COPY_BOTH)
	{
		char	   *msg = chomp(PQerrorMessage(ps->conn));
		cleanup_and_disconnect(ps);
		ereport(elevel,
				(errcode(ERRCODE_SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION),
				 errmsg(NEON_TAG "[shard %d] could perform handshake with pageserver", chan_no),
				 errdetail_internal("%s", msg)));
		free(msg);
		return false;
	}
	PQclear(res);

	ps->state = PS_Connected;
	ps->nrequests_sent = 0;
	ps->nresponses_received = 0;
	ps->delay_us = MIN_RECONNECT_INTERVAL_USEC;

	neon_shard_log_cs(shard_no, LOG, "libpagestore: connected to '%s' with protocol version %d", connstr, neon_protocol_version);
	return true;
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
		if (ps->state == PS_PendingDisconnect)
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
	 * We can not use PQputCopyData because it tries to read input messages.
	 * So we have to do it "manually".
	 */
	if (pq_send(pageserver_conn, msg->data, msg->len) < 0)
	{
		char	   *msg = chomp(PQerrorMessage(pageserver_conn));
		pageserver_disconnect(chan_no);
		neon_shard_log_cs(shard_no, LOG, "pageserver_send disconnected: failed to send page request (try to reconnect): %s", msg);
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
	NeonResponse *resp = NULL;
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

	resp_buff.data = NULL;
	rc = PQgetCopyData(pageserver_conn, &resp_buff.data, false);
	if (rc >= 0)
	{
		/* call_PQgetCopyData handles rc == 0 */
		Assert(rc > 0);

		resp_buff.len = rc;
		resp_buff.cursor = 0;
		resp = nm_unpack_response(&resp_buff);
		PQfreemem(resp_buff.data);
		if (resp == NULL)
		{
			neon_shard_log_cs(shard_no, LOG, "pageserver_receive: disconnect due to failure while parsing response");
			pageserver_disconnect(chan_no);
		}
	}
	else if (rc == -1)
	{
		neon_shard_log_cs(shard_no, LOG, "pageserver_receive disconnect: psql end of copy data: %s", chomp(PQerrorMessage(pageserver_conn)));
		ps->state = PS_PendingDisconnect;
	}
	else if (rc == -2)
	{
		char	   *msg = chomp(PQerrorMessage(pageserver_conn));
		ps->state = PS_PendingDisconnect;
		neon_shard_log_cs(shard_no, LOG, "pageserver_receive disconnect: could not read COPY data: %s", msg);
	}
	else
	{
		ps->state = PS_PendingDisconnect;
		neon_shard_log_cs(shard_no, LOG, "pageserver_receive disconnect: unexpected PQgetCopyData return value: %d", rc);
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
 * Backends are splitted between parallel conntions so each worker has tpo server at most PROCARRAY_MAXPROCS / parallel_connections
 * backends.
 */
static Size
RequestBufferSize(void)
{
	return (max_prefetch_distance + PG_IOV_MAX) * (PROCARRAY_MAXPROCS + (parallel_connections - 1) / parallel_connections);
}

static Size
CommunicatorShmemSize(void)
{
	return sizeof(NeonCommunicatorSharedState)
		+ RequestBufferSize() * MaxNumberOfChannels() * sizeof(NeonCommunicatorRequest)
		+ MaxNumberOfChannels() * sizeof(NeonCommunicatorChannel)
		+ sizeof(NeonCommunicatorResponse) * PROCARRAY_MAXPROCS;
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
	communicator = ShmemInitStruct("communicator shared state",
								   CommunicatorShmemSize(),
								   &found);
	if (!found)
	{
		size_t n_channels = MaxNumberOfChannels();
		NeonCommunicatorRequest* requests;
		pthread_condattr_t attrcond;
		pthread_mutexattr_t attrmutex;

		pthread_condattr_init(&attrcond);
		pthread_condattr_setpshared(&attrcond, PTHREAD_PROCESS_SHARED);

		pthread_mutexattr_init(&attrmutex);
		pthread_mutexattr_setpshared(&attrmutex, PTHREAD_PROCESS_SHARED);

		communicator->channels = (NeonCommunicatorChannel*)(communicator + 1);
		requests = (NeonCommunicatorRequest*)(communicator->channels + n_channels);
		for (size_t i = 0; i < n_channels; i++)
		{
			NeonCommunicatorChannel* chan = &communicator->channels[i];
			pg_atomic_init_u64(&chan->write_pos, 0);
			pg_atomic_init_u64(&chan->read_pos, 0);
			pthread_cond_init(&chan->cond, &attrcond);
			pthread_mutex_init(&chan->mutex, &attrmutex);
			chan->requests = requests;
			requests += RequestBufferSize();
		}
		communicator->responses = (NeonCommunicatorResponse*)requests;
		communicator->n_shards = shard_map.n_shards;
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
	NeonCommunicatorChannel* chan = &communicator->channels[shard * parallel_connections + (MyProcNumber % parallel_connections)];
	size_t ring_size = RequestBufferSize();
	uint64 write_pos = pg_atomic_fetch_add_u64(&chan->write_pos, 1); /* reserve write position */
	size_t ring_pos = (size_t)(write_pos % ring_size);
	uint64 read_pos;

	/* ring overflow should not happen */
	Assert(chan->requests[ring_pos].hdr.u.reqid == 0);

	req->hdr.u.recepient.procno = MyProcNumber + 1; /* make it non-zero */
	Assert(req->hdr.u.reqid != 0);

	/* copy request */
	chan->requests[ring_pos] = *req;

	/* will be overwritten with response code when request will be processed */
	communicator->responses[MyProcNumber].tag = req->hdr.tag;

	/* enforce memory barrier before pinging communicator */
	pg_write_barrier();

	/* advance read-up-to position */
	do {
		read_pos = write_pos;
	} while (!pg_atomic_compare_exchange_u64(&chan->read_pos, &read_pos, write_pos+1));

	pthread_cond_signal(&chan->cond);
}

/*
 * Wait response from page server.
 * We are waiting until request message tag will be replaced with response tag,
 * using backed's latch.
 */
int64
communicator_receive_response(void)
{
	while (communicator->responses[MyProcNumber].tag <= T_NeonTestRequest) /* response not yet received */
	{
		(void) WaitLatch(MyLatch,
						 WL_LATCH_SET | WL_EXIT_ON_PM_DEATH,
						 -1L,
						 WAIT_EVENT_NEON_PS_READ);
		ResetLatch(MyLatch);
	}
	if (communicator->responses[MyProcNumber].tag == T_NeonErrorResponse)
	{
		elog(ERROR, "Request failed"); /* detailed error message is printed by communicator */
	}
	return communicator->responses[MyProcNumber].value;
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


PGDLLEXPORT void
CommunicatorMain(Datum main_arg)
{
	am_communicator = true;

	/* Establish signal handlers. */
	pqsignal(SIGUSR1, procsignal_sigusr1_handler);
	pqsignal(SIGHUP, SignalHandlerForConfigReload);
	pqsignal(SIGTERM, SignalHandlerForShutdownRequest);
	BackgroundWorkerUnblockSignals();

	AssignPageserverConnstring(page_server_connstring, NULL);

	while (!ShutdownRequestPending)
	{
		(void) WaitLatch(MyLatch,
						 WL_LATCH_SET | WL_EXIT_ON_PM_DEATH,
						 -1L,
						 PG_WAIT_EXTENSION);
		ResetLatch(MyLatch);
		CHECK_FOR_INTERRUPTS();
		if (ConfigReloadPending)
		{
			ConfigReloadPending = false;
			ProcessConfigFile(PGC_SIGHUP);
		}
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

	/* FIXME: enforce that effective_io_concurrency and maintenance_io_concurrency can not be set larger than max_prefetch_distance */
	DefineCustomIntVariable("neon.max_prefetch_distance",
							"Maximal number of prefetch requests",
							"effetive_io_concurrency and maintenance_io_concurrecy should not be larger than sthis value",
							&max_prefetch_distance,
							128, 0, 1024,
							PGC_POSTMASTER,
							0,	/* no flags required */
							NULL, NULL, NULL);

	DefineCustomIntVariable("neon.parallel_connections",
							"number of connections to each shard",
							NULL,
							&parallel_connections,
							10, 1, 100,
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
	uint64	read_start_pos = 0;
	size_t	chan_no = (size_t)arg;
	int		shard_no = CHAN_TO_SHARD(chan_no);
	NeonCommunicatorChannel* chan = &communicator->channels[chan_no];
	size_t	ring_size = RequestBufferSize();
	StringInfoData s;

	allocStringInfo(&s, MAX_REQUEST_SIZE);

	while (true)
	{
		NeonCommunicatorRequest* req;
		uint64 read_end_pos;

		/* Number of shards is decreased so this worker is not needed any more */
		if (chan_no >= shard_map.n_shards * parallel_connections)
		{
			neon_shard_log_cs(shard_no, LOG, "Shard %d is not online any more (n_shards=%d)", (int)shard_no, (int)shard_map.n_shards);
			return NULL;
		}
		read_end_pos = pg_atomic_read_u64(&chan->read_pos);
		Assert(read_start_pos <= read_end_pos);
		if (read_start_pos == read_end_pos) /* fast path */
		{
			pthread_mutex_lock(&chan->mutex);
			while (!ShutdownRequestPending)
			{
				read_end_pos = pg_atomic_read_u64(&chan->read_pos);
				Assert(read_start_pos <= read_end_pos);
				if (read_start_pos < read_end_pos)
				{
					pthread_mutex_unlock(&chan->mutex);
					break;
				}
				pthread_cond_wait(&chan->cond, &chan->mutex);
			}
			pthread_mutex_unlock(&chan->mutex);
			if (ShutdownRequestPending)
				return NULL;
		}
		req = &chan->requests[read_start_pos++ % ring_size];
		Assert(req->hdr.u.reqid != 0);
		nm_pack_request(&s, &req->hdr);
		Assert(s.maxlen == MAX_REQUEST_SIZE); /* string buffer was not reallocated */
		req->hdr.u.reqid = 0; /* mark requests as processed */
		pageserver_send(chan_no, &s);
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
	bool	notify_backend = false;

	while (true)
	{
		/* Number of shards is decreased */
		if (chan_no >= shard_map.n_shards * parallel_connections)
			return NULL;

		resp = pageserver_receive(chan_no);
		if (resp == NULL)
		{
			/* Assume that writer will restablish connection */
			pg_usleep(RECEIVER_RETRY_DELAY_USEC);
			continue;
		}
		notify_backend = true;
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
					Assert(false);
					(void) lfc_prefetch(page_resp->req.rinfo, page_resp->req.forknum, page_resp->req.blkno, page_resp->page, resp->not_modified_since);
					notify_backend = false;
				}
				else
				{
					BufferTag tag;
					Buffer buf = resp->u.recepient.bufid;
					BufferDesc *buf_desc = GetBufferDescriptor(buf - 1);
					InitBufferTag(&tag, &page_resp->req.rinfo, page_resp->req.forknum, page_resp->req.blkno);
					if (!BufferTagsEqual(&buf_desc->tag, &tag))
					{
						/*
						 * It can happen that backend was terminated before response was received fro page server.
						 * So doesn't treate this as error, just log and ignore response.
						 */
						neon_shard_log_cs(shard_no, LOG, "Get page request {rel=%u/%u/%u.%u block=%u} referencing wrong buffer {rel=%u/%u/%u.%u block=%u}",
										  RelFileInfoFmt(page_resp->req.rinfo), page_resp->req.forknum, page_resp->req.blkno,
										  RelFileInfoFmt(BufTagGetNRelFileInfo(buf_desc->tag)), buf_desc->tag.forkNum, buf_desc->tag.blockNum);
						notify_backend = false;
					}
					else
					{
						/* Copy page content to shared buffer */
						memcpy(BufferGetBlock(resp->u.recepient.bufid), page_resp->page, BLCKSZ);
					}
				}
				break;
			}
			case T_NeonErrorResponse:
				log_error_message((NeonErrorResponse *) resp);
				break;
			default:
				break;
		}
		if (notify_backend)
		{
			communicator->responses[resp->u.recepient.procno-1].value = value;
			/* enforce write barrier before writing response code which is used as received response indicator */
			pg_write_barrier();
			communicator->responses[resp->u.recepient.procno-1].tag = resp->tag;
			SetLatch(&ProcGlobal->allProcs[resp->u.recepient.procno-1].procLatch);
		}
		free(resp);
	}
}


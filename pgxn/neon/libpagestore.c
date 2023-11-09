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
#include "postgres.h"

#include "pagestore_client.h"
#include "fmgr.h"
#include "access/xlog.h"
#include "access/xlogutils.h"
#include "common/hashfn.h"
#include "storage/buf_internals.h"
#include "storage/lwlock.h"
#include "storage/ipc.h"
#include "storage/pg_shmem.h"
#include "c.h"
#include "postmaster/interrupt.h"

#include "libpq-fe.h"
#include "libpq/pqformat.h"
#include "libpq/libpq.h"

#include "miscadmin.h"
#include "pgstat.h"
#include "utils/guc.h"

#include "neon.h"
#include "walproposer.h"
#include "neon_utils.h"
#include "control_plane_connector.h"

#define PageStoreTrace DEBUG5

#define RECONNECT_INTERVAL_USEC 1000000

/* GUCs */
char	   *neon_timeline;
char	   *neon_tenant;
int32		max_cluster_size;
char	   *page_server_connstring;
char	   *neon_auth_token;

int			readahead_buffer_size = 128;
int			flush_every_n_requests = 8;

int			n_reconnect_attempts = 0;
int			max_reconnect_attempts = 60;

bool	(*old_redo_read_buffer_filter) (XLogReaderState *record, uint8 block_id) = NULL;

static bool pageserver_flush(shardno_t shard_no);
static void pageserver_disconnect(shardno_t shard_no);

static shmem_startup_hook_type prev_shmem_startup_hook;
#if PG_VERSION_NUM>=150000
static shmem_request_hook_type prev_shmem_request_hook;
#endif

typedef struct
{
	size_t n_shards;
	size_t update_counter;
	char   shard_connstr[MAX_SHARDS][MAX_PS_CONNSTR_LEN];
} ShardMap;


static ShardMap* shard_map;
static LWLockId  shard_map_lock;
static size_t    shard_map_update_counter;

typedef struct
{
	/*
	 * connection for each shard
	 */
	PGconn	   *conn;
    /*
	 * WaitEventSet containing:
	 * - WL_SOCKET_READABLE on pageserver_conn,
	 * - WL_LATCH_SET on MyLatch, and
	 * - WL_EXIT_ON_PM_DEATH.
	 */
	WaitEventSet    *wes;
} PageServer;

static PageServer page_servers[MAX_SHARDS];
static shardno_t  max_attached_shard_no;

static void
psm_shmem_startup(void)
{
	bool found;
	if (prev_shmem_startup_hook)
	{
		prev_shmem_startup_hook();
	}

	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);

	shard_map = (ShardMap*)ShmemInitStruct("shard_map", sizeof(ShardMap), &found);
	if (!found)
	{
		shard_map_lock = (LWLockId)GetNamedLWLockTranche("shard_map_lock");
		shard_map->n_shards = 0;
		shard_map->update_counter = 0;
	}
	LWLockRelease(AddinShmemInitLock);
}

static void
psm_shmem_request(void)
{
#if PG_VERSION_NUM>=150000
	if (prev_shmem_request_hook)
		prev_shmem_request_hook();
#endif

	RequestAddinShmemSpace(sizeof(ShardMap));
	RequestNamedLWLockTranche("shard_map_lock", 1);
}

static void
psm_init(void)
{
	prev_shmem_startup_hook = shmem_startup_hook;
	shmem_startup_hook = psm_shmem_startup;
#if PG_VERSION_NUM>=150000
	prev_shmem_request_hook = shmem_request_hook;
	shmem_request_hook = psm_shmem_request;
#else
	psm_shmem_request();
#endif
}

/*
 * Reload page map if needed and return number of shards and connection string for the specified shard
 */
static shardno_t
load_shard_map(shardno_t shard_no, char* connstr)
{
	shardno_t n_shards;

	LWLockAcquire(shard_map_lock, LW_SHARED);

	while (shard_map->n_shards == 0 || shard_map_update_counter != shard_map->update_counter)
	{
		/* Close all existed connections */
		for (shardno_t i = 0; i < max_attached_shard_no; i++)
		{
			if (page_servers[i].conn)
				pageserver_disconnect(i);
		}

		/* Request new shard map from control plane under exclusive lock */
		LWLockRelease(shard_map_lock);
		LWLockAcquire(shard_map_lock, LW_EXCLUSIVE);
		if (shard_map->n_shards == 0)
		{
			char const* shard_connstr = page_server_connstring;
			char const* sep;
			size_t connstr_len;
			do
			{
				sep = strchr(shard_connstr, ',');
				connstr_len = sep != NULL ? sep - shard_connstr : strlen(shard_connstr);
				if (shard_map->n_shards >= MAX_SHARDS)
					elog(ERROR, "Too many shards");
				if (connstr_len >= MAX_PS_CONNSTR_LEN)
					elog(ERROR, "Connection  string too long");
				memcpy(shard_map->shard_connstr[shard_map->n_shards++], shard_connstr, connstr_len+1);
				shard_connstr = sep + 1;
			} while (sep != NULL);

			shard_map->update_counter += 1;
		}
		shard_map_update_counter = shard_map->update_counter;
	}
	n_shards = shard_map->n_shards;
	if (shard_no >= n_shards)
		elog(ERROR, "Shard %d is greater or equal than number of shards %d", shard_no, n_shards);

	if (connstr)
		strcpy(connstr, shard_map->shard_connstr[shard_no]);

	LWLockRelease(shard_map_lock);

	return n_shards;
}

shardno_t
get_shard_number(BufferTag* tag)
{
	shardno_t n_shards = load_shard_map(0, NULL);
	uint32	  hash;

#if PG_MAJORVERSION_NUM < 16
	hash = murmurhash32(tag->rnode.spcNode);
	hash_combine(hash, murmurhash32(tag->rnode.dbNode));
	hash_combine(hash, murmurhash32(tag->rnode.relNode));
	hash_combine(hash, murmurhash32(tag->blockNum/STRIPE_SIZE));
#else
	hash = murmurhash32(tag->spcOid);
	hash_combine(hash, murmurhash32(tag->dbOid));
	hash_combine(hash, murmurhash32(tag->relNumber));
	hash_combine(hash, murmurhash32(tag->blockNum/STRIPE_SIZE));
#endif

	return hash % n_shards;
}

static bool
pageserver_connect(shardno_t shard_no, int elevel)
{
	char	   *query;
	int			ret;
	const char *keywords[3];
	const char *values[3];
	int			n;
	PGconn*		conn;
	WaitEventSet *wes;
	char        connstr[MAX_PS_CONNSTR_LEN];

	Assert(page_servers[shard_no].conn == NULL);

	(void)load_shard_map(shard_no, connstr); /* refresh page map if needed */

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
	n = 0;
	if (neon_auth_token)
	{
		keywords[n] = "password";
		values[n] = neon_auth_token;
		n++;
	}
	keywords[n] = "dbname";
	values[n] = connstr;
	n++;
	keywords[n] = NULL;
	values[n] = NULL;
	n++;
	conn = PQconnectdbParams(keywords, values, 1);

	if (PQstatus(conn) == CONNECTION_BAD)
	{
		char	   *msg = pchomp(PQerrorMessage(conn));

		PQfinish(conn);

		ereport(elevel,
				(errcode(ERRCODE_SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION),
				 errmsg(NEON_TAG "could not establish connection to pageserver"),
				 errdetail_internal("%s", msg)));
		return false;
	}
	query = psprintf("pagestream %s %s", neon_tenant, neon_timeline);
	ret = PQsendQuery(conn, query);
	if (ret != 1)
	{
		PQfinish(conn);
		neon_log(elevel, "could not send pagestream command to pageserver");
		return false;
	}

	wes = CreateWaitEventSet(TopMemoryContext, 3);
	AddWaitEventToSet(wes, WL_LATCH_SET, PGINVALID_SOCKET,
					  MyLatch, NULL);
	AddWaitEventToSet(wes, WL_EXIT_ON_PM_DEATH, PGINVALID_SOCKET,
					  NULL, NULL);
	AddWaitEventToSet(wes, WL_SOCKET_READABLE, PQsocket(conn), NULL, NULL);

	while (PQisBusy(conn))
	{
		WaitEvent	event;

		/* Sleep until there's something to do */
		(void) WaitEventSetWait(wes, -1L, &event, 1, PG_WAIT_EXTENSION);
		ResetLatch(MyLatch);

		CHECK_FOR_INTERRUPTS();

		/* Data available in socket? */
		if (event.events & WL_SOCKET_READABLE)
		{
			if (!PQconsumeInput(conn))
			{
				char	   *msg = pchomp(PQerrorMessage(conn));

				PQfinish(conn);
				FreeWaitEventSet(wes);

				neon_log(elevel, "could not complete handshake with pageserver: %s",
						 msg);
				return false;
			}
		}
	}

	neon_log(LOG, "libpagestore: connected to '%s'", shard_map->shard_connstr[shard_no]);
	page_servers[shard_no].conn = conn;
	page_servers[shard_no].wes = wes;
	max_attached_shard_no = Max(shard_no+1, max_attached_shard_no);

	return true;
}

/*
 * A wrapper around PQgetCopyData that checks for interrupts while sleeping.
 */
static int
call_PQgetCopyData(shardno_t shard_no, char **buffer)
{
	int			ret;
	PGconn*     pageserver_conn = page_servers[shard_no].conn;
retry:
	ret = PQgetCopyData(pageserver_conn, buffer, 1 /* async */ );

	if (ret == 0)
	{
		WaitEvent	event;

		/* Sleep until there's something to do */
		(void) WaitEventSetWait(page_servers[shard_no].wes, -1L, &event, 1, PG_WAIT_EXTENSION);
		ResetLatch(MyLatch);

		CHECK_FOR_INTERRUPTS();

		/* Data available in socket? */
		if (event.events & WL_SOCKET_READABLE)
		{
			if (!PQconsumeInput(pageserver_conn))
			{
				char	   *msg = pchomp(PQerrorMessage(pageserver_conn));

				neon_log(LOG, "could not get response from pageserver: %s", msg);
				pfree(msg);
				return -1;
			}
		}

		goto retry;
	}

	return ret;
}


static void
pageserver_disconnect(shardno_t shard_no)
{
	/*
	 * If anything goes wrong while we were sending a request, it's not clear
	 * what state the connection is in. For example, if we sent the request
	 * but didn't receive a response yet, we might receive the response some
	 * time later after we have already sent a new unrelated request. Close
	 * the connection to avoid getting confused.
	 */
	if (page_servers[shard_no].conn)
	{
		neon_log(LOG, "dropping connection to page server due to error");
		PQfinish(page_servers[shard_no].conn);
		page_servers[shard_no].conn = NULL;

		prefetch_on_ps_disconnect();
	}
	if (page_servers[shard_no].wes != NULL)
	{
		FreeWaitEventSet(page_servers[shard_no].wes);
		page_servers[shard_no].wes = NULL;
	}
}

static bool
pageserver_send(shardno_t shard_no, NeonRequest *request)
{
	StringInfoData req_buff;
	PGconn* pageserver_conn = page_servers[shard_no].conn;

	/* If the connection was lost for some reason, reconnect */
	if (pageserver_conn && PQstatus(pageserver_conn) == CONNECTION_BAD)
	{
		neon_log(LOG, "pageserver_send disconnect bad connection");
		pageserver_disconnect(shard_no);
	}

	req_buff = nm_pack_request(request);

	/*
	 * If pageserver is stopped, the connections from compute node are broken.
	 * The compute node doesn't notice that immediately, but it will cause the
	 * next request to fail, usually on the next query. That causes
	 * user-visible errors if pageserver is restarted, or the tenant is moved
	 * from one pageserver to another. See
	 * https://github.com/neondatabase/neon/issues/1138 So try to reestablish
	 * connection in case of failure.
	 */
	if (!page_servers[shard_no].conn)
	{
		while (!pageserver_connect(shard_no, n_reconnect_attempts < max_reconnect_attempts ? LOG : ERROR))
		{
			HandleMainLoopInterrupts();
			n_reconnect_attempts += 1;
			pg_usleep(RECONNECT_INTERVAL_USEC);
		}
		n_reconnect_attempts = 0;
	}

	pageserver_conn = page_servers[shard_no].conn;

    /*
	 * Send request.
	 *
	 * In principle, this could block if the output buffer is full, and we
	 * should use async mode and check for interrupts while waiting. In
	 * practice, our requests are small enough to always fit in the output and
	 * TCP buffer.
	 */
	if (PQputCopyData(pageserver_conn, req_buff.data, req_buff.len) <= 0)
	{
		char	   *msg = pchomp(PQerrorMessage(pageserver_conn));
		pageserver_disconnect(shard_no);
		neon_log(LOG, "pageserver_send disconnect because failed to send page request (try to reconnect): %s", msg);
		pfree(msg);
		pfree(req_buff.data);
		return false;
	}

	pfree(req_buff.data);

	if (message_level_is_interesting(PageStoreTrace))
	{
		char	   *msg = nm_to_string((NeonMessage *) request);

		neon_log(PageStoreTrace, "sent request: %s", msg);
		pfree(msg);
	}
	return true;
}

static NeonResponse *
pageserver_receive(shardno_t shard_no)
{
	StringInfoData resp_buff;
	NeonResponse *resp;
	PGconn* pageserver_conn = page_servers[shard_no].conn;
	if (!pageserver_conn)
		return NULL;

	PG_TRY();
	{
		/* read response */
		int			rc;

		rc = call_PQgetCopyData(shard_no, &resp_buff.data);
		if (rc >= 0)
		{
			resp_buff.len = rc;
			resp_buff.cursor = 0;
			resp = nm_unpack_response(&resp_buff);
			PQfreemem(resp_buff.data);

			if (message_level_is_interesting(PageStoreTrace))
			{
				char	   *msg = nm_to_string((NeonMessage *) resp);

				neon_log(PageStoreTrace, "got response: %s", msg);
				pfree(msg);
			}
		}
		else if (rc == -1)
		{
			neon_log(LOG, "pageserver_receive disconnect because call_PQgetCopyData returns -1: %s", pchomp(PQerrorMessage(pageserver_conn)));
			pageserver_disconnect(shard_no);
			resp = NULL;
		}
		else if (rc == -2)
		{
			char	   *msg = pchomp(PQerrorMessage(pageserver_conn));

			pageserver_disconnect(shard_no);
			neon_log(ERROR, "pageserver_receive disconnect because could not read COPY data: %s", msg);
		}
		else
		{
			pageserver_disconnect(shard_no);
			neon_log(ERROR, "pageserver_receive disconnect because unexpected PQgetCopyData return value: %d", rc);
		}
	}
	PG_CATCH();
	{
		neon_log(LOG, "pageserver_receive disconnect due to caught exception");
		pageserver_disconnect(shard_no);
		PG_RE_THROW();
	}
	PG_END_TRY();

	return (NeonResponse *) resp;
}


static bool
pageserver_flush(shardno_t shard_no)
{
	PGconn* pageserver_conn = page_servers[shard_no].conn;
	if (!pageserver_conn)
	{
		neon_log(WARNING, "Tried to flush while disconnected");
	}
	else
	{
		if (PQflush(pageserver_conn))
		{
			char	   *msg = pchomp(PQerrorMessage(pageserver_conn));

			pageserver_disconnect(shard_no);
			neon_log(LOG, "pageserver_flush disconnect because failed to flush page requests: %s", msg);
			pfree(msg);
			return false;
		}
	}
	return true;
}

page_server_api api =
{
	.send = pageserver_send,
	.flush = pageserver_flush,
	.receive = pageserver_receive
};

static bool
check_neon_id(char **newval, void **extra, GucSource source)
{
	uint8		id[16];

	return **newval == '\0' || HexDecodeString(id, *newval, 16);
}

/*
 * Module initialization function
 */
void
pg_init_libpagestore(void)
{
	DefineCustomStringVariable("neon.pageserver_connstring",
							   "connection string to the page server",
							   NULL,
							   &page_server_connstring,
							   "",
							   PGC_SIGHUP,
							   0,	/* no flags required */
							   NULL, AssignPageserverConnstring, NULL);

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

	DefineCustomIntVariable("neon.max_cluster_size",
							"cluster size limit",
							NULL,
							&max_cluster_size,
							-1, -1, INT_MAX,
							PGC_SIGHUP,
							GUC_UNIT_MB,
							NULL, NULL, NULL);
	DefineCustomIntVariable("neon.flush_output_after",
							"Flush the output buffer after every N unflushed requests",
							NULL,
							&flush_every_n_requests,
							8, -1, INT_MAX,
							PGC_USERSET,
							0,	/* no flags required */
							NULL, NULL, NULL);
	DefineCustomIntVariable("neon.max_reconnect_attempts",
							"Maximal attempts to reconnect to pages server (with 1 second timeout)",
							NULL,
							&max_reconnect_attempts,
							60, 0, INT_MAX,
							PGC_USERSET,
							0,
							NULL, NULL, NULL);
	DefineCustomIntVariable("neon.readahead_buffer_size",
							"number of prefetches to buffer",
							"This buffer is used to hold and manage prefetched "
							"data; so it is important that this buffer is at "
							"least as large as the configured value of all "
							"tablespaces' effective_io_concurrency and "
							"maintenance_io_concurrency, and your sessions' "
							"values for these settings.",
							&readahead_buffer_size,
							128, 16, 1024,
							PGC_USERSET,
							0,	/* no flags required */
							NULL, (GucIntAssignHook) &readahead_buffer_resize, NULL);

	relsize_hash_init();

	if (page_server != NULL)
		neon_log(ERROR, "libpagestore already loaded");

	neon_log(PageStoreTrace, "libpagestore already loaded");
	page_server = &api;

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
		old_redo_read_buffer_filter = redo_read_buffer_filter;
		redo_read_buffer_filter = neon_redo_read_buffer_filter;
	}

	lfc_init();
	psm_init();
}

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

#include "access/xlog.h"
#include "fmgr.h"
#include "libpq-fe.h"
#include "libpq/libpq.h"
#include "libpq/pqformat.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "postmaster/interrupt.h"
#include "storage/buf_internals.h"
#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "storage/pg_shmem.h"
#include "utils/guc.h"

#include "neon.h"
#include "neon_utils.h"
#include "pagestore_client.h"
#include "walproposer.h"

#define PageStoreTrace DEBUG5

#define MIN_RECONNECT_INTERVAL_USEC 100
#define MAX_RECONNECT_INTERVAL_USEC 1000000

bool		connected = false;
PGconn	   *pageserver_conn = NULL;

/*
 * WaitEventSet containing:
 * - WL_SOCKET_READABLE on pageserver_conn,
 * - WL_LATCH_SET on MyLatch, and
 * - WL_EXIT_ON_PM_DEATH.
 */
WaitEventSet *pageserver_conn_wes = NULL;

/* GUCs */
char	   *neon_timeline;
char	   *neon_tenant;
int32		max_cluster_size;
char	   *page_server_connstring;
char	   *neon_auth_token;

int			readahead_buffer_size = 128;
int			flush_every_n_requests = 8;

static int n_reconnect_attempts = 0;
static int max_reconnect_attempts = 60;

#define MAX_PAGESERVER_CONNSTRING_SIZE 256

typedef struct
{
	LWLockId	lock;
	pg_atomic_uint64 update_counter;
	char		pageserver_connstring[MAX_PAGESERVER_CONNSTRING_SIZE];
} PagestoreShmemState;

#if PG_VERSION_NUM >= 150000
static shmem_request_hook_type prev_shmem_request_hook = NULL;
static void walproposer_shmem_request(void);
#endif
static shmem_startup_hook_type prev_shmem_startup_hook;
static PagestoreShmemState *pagestore_shared;
static uint64 pagestore_local_counter = 0;
static char local_pageserver_connstring[MAX_PAGESERVER_CONNSTRING_SIZE];

static bool pageserver_flush(void);
static void pageserver_disconnect(void);

static bool
PagestoreShmemIsValid()
{
	return pagestore_shared && UsedShmemSegAddr;
}

static bool
CheckPageserverConnstring(char **newval, void **extra, GucSource source)
{
	return strlen(*newval) < MAX_PAGESERVER_CONNSTRING_SIZE;
}

static void
AssignPageserverConnstring(const char *newval, void *extra)
{
	if (!PagestoreShmemIsValid())
		return;
	LWLockAcquire(pagestore_shared->lock, LW_EXCLUSIVE);
	strlcpy(pagestore_shared->pageserver_connstring, newval, MAX_PAGESERVER_CONNSTRING_SIZE);
	pg_atomic_fetch_add_u64(&pagestore_shared->update_counter, 1);
	LWLockRelease(pagestore_shared->lock);
}

static bool
CheckConnstringUpdated()
{
	if (!PagestoreShmemIsValid())
		return false;
	return pagestore_local_counter < pg_atomic_read_u64(&pagestore_shared->update_counter);
}

static void
ReloadConnstring()
{
	if (!PagestoreShmemIsValid())
		return;
	LWLockAcquire(pagestore_shared->lock, LW_SHARED);
	strlcpy(local_pageserver_connstring, pagestore_shared->pageserver_connstring, sizeof(local_pageserver_connstring));
	pagestore_local_counter = pg_atomic_read_u64(&pagestore_shared->update_counter);
	LWLockRelease(pagestore_shared->lock);
}

static bool
pageserver_connect(int elevel)
{
	char	   *query;
	int			ret;
	const char *keywords[3];
	const char *values[3];
	int			n;

	static TimestampTz last_connect_time = 0;
	static uint64_t delay_us = MIN_RECONNECT_INTERVAL_USEC;
	TimestampTz now;
        uint64_t us_since_last_connect;

	Assert(!connected);

	if (CheckConnstringUpdated())
	{
		ReloadConnstring();
	}

	now = GetCurrentTimestamp();
        us_since_last_connect = now - last_connect_time;
	if (us_since_last_connect < delay_us)
	{
		pg_usleep(delay_us - us_since_last_connect);
		delay_us *= 2;
		if (delay_us > MAX_RECONNECT_INTERVAL_USEC)
			delay_us = MAX_RECONNECT_INTERVAL_USEC;
		last_connect_time = GetCurrentTimestamp();
	}
	else
	{
		delay_us = MIN_RECONNECT_INTERVAL_USEC;
		last_connect_time = now;
	}

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
	values[n] = local_pageserver_connstring;
	n++;
	keywords[n] = NULL;
	values[n] = NULL;
	n++;
	pageserver_conn = PQconnectdbParams(keywords, values, 1);

	if (PQstatus(pageserver_conn) == CONNECTION_BAD)
	{
		char	   *msg = pchomp(PQerrorMessage(pageserver_conn));

		PQfinish(pageserver_conn);
		pageserver_conn = NULL;

		ereport(elevel,
				(errcode(ERRCODE_SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION),
				 errmsg(NEON_TAG "could not establish connection to pageserver"),
				 errdetail_internal("%s", msg)));
		return false;
	}

	query = psprintf("pagestream %s %s", neon_tenant, neon_timeline);
	ret = PQsendQuery(pageserver_conn, query);
	if (ret != 1)
	{
		PQfinish(pageserver_conn);
		pageserver_conn = NULL;
		neon_log(elevel, "could not send pagestream command to pageserver");
		return false;
	}

	pageserver_conn_wes = CreateWaitEventSet(TopMemoryContext, 3);
	AddWaitEventToSet(pageserver_conn_wes, WL_LATCH_SET, PGINVALID_SOCKET,
					  MyLatch, NULL);
	AddWaitEventToSet(pageserver_conn_wes, WL_EXIT_ON_PM_DEATH, PGINVALID_SOCKET,
					  NULL, NULL);
	AddWaitEventToSet(pageserver_conn_wes, WL_SOCKET_READABLE, PQsocket(pageserver_conn), NULL, NULL);

	while (PQisBusy(pageserver_conn))
	{
		WaitEvent	event;

		/* Sleep until there's something to do */
		(void) WaitEventSetWait(pageserver_conn_wes, -1L, &event, 1, PG_WAIT_EXTENSION);
		ResetLatch(MyLatch);

		CHECK_FOR_INTERRUPTS();

		/* Data available in socket? */
		if (event.events & WL_SOCKET_READABLE)
		{
			if (!PQconsumeInput(pageserver_conn))
			{
				char	   *msg = pchomp(PQerrorMessage(pageserver_conn));

				PQfinish(pageserver_conn);
				pageserver_conn = NULL;
				FreeWaitEventSet(pageserver_conn_wes);
				pageserver_conn_wes = NULL;

				neon_log(elevel, "could not complete handshake with pageserver: %s",
						 msg);
				return false;
			}
		}
	}

	neon_log(LOG, "libpagestore: connected to '%s'", page_server_connstring);

	connected = true;
	return true;
}

/*
 * A wrapper around PQgetCopyData that checks for interrupts while sleeping.
 */
static int
call_PQgetCopyData(char **buffer)
{
	int			ret;

retry:
	ret = PQgetCopyData(pageserver_conn, buffer, 1 /* async */ );

	if (ret == 0)
	{
		WaitEvent	event;

		/* Sleep until there's something to do */
		(void) WaitEventSetWait(pageserver_conn_wes, -1L, &event, 1, PG_WAIT_EXTENSION);
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
pageserver_disconnect(void)
{
	/*
	 * If anything goes wrong while we were sending a request, it's not clear
	 * what state the connection is in. For example, if we sent the request
	 * but didn't receive a response yet, we might receive the response some
	 * time later after we have already sent a new unrelated request. Close
	 * the connection to avoid getting confused.
	 */
	if (connected)
	{
		neon_log(LOG, "dropping connection to page server due to error");
		PQfinish(pageserver_conn);
		pageserver_conn = NULL;
		connected = false;

		prefetch_on_ps_disconnect();
	}
	if (pageserver_conn_wes != NULL)
	{
		FreeWaitEventSet(pageserver_conn_wes);
		pageserver_conn_wes = NULL;
	}
}

static bool
pageserver_send(NeonRequest *request)
{
	StringInfoData req_buff;

	if (CheckConnstringUpdated())
	{
		pageserver_disconnect();
		ReloadConnstring();
	}

	/* If the connection was lost for some reason, reconnect */
	if (connected && PQstatus(pageserver_conn) == CONNECTION_BAD)
	{
		neon_log(LOG, "pageserver_send disconnect bad connection");
		pageserver_disconnect();
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
	if (!connected)
	{
		while (!pageserver_connect(n_reconnect_attempts < max_reconnect_attempts ? LOG : ERROR))
		{
			HandleMainLoopInterrupts();
			n_reconnect_attempts += 1;
		}
		n_reconnect_attempts = 0;
	}

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

		pageserver_disconnect();
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
pageserver_receive(void)
{
	StringInfoData resp_buff;
	NeonResponse *resp;

	if (!connected)
		return NULL;

	PG_TRY();
	{
		/* read response */
		int			rc;

		rc = call_PQgetCopyData(&resp_buff.data);
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
			pageserver_disconnect();
			resp = NULL;
		}
		else if (rc == -2)
		{
			char	   *msg = pchomp(PQerrorMessage(pageserver_conn));

			pageserver_disconnect();
			neon_log(ERROR, "pageserver_receive disconnect because could not read COPY data: %s", msg);
		}
		else
		{
			pageserver_disconnect();
			neon_log(ERROR, "pageserver_receive disconnect because unexpected PQgetCopyData return value: %d", rc);
		}
	}
	PG_CATCH();
	{
		neon_log(LOG, "pageserver_receive disconnect due to caught exception");
		pageserver_disconnect();
		PG_RE_THROW();
	}
	PG_END_TRY();

	return (NeonResponse *) resp;
}


static bool
pageserver_flush(void)
{
	if (!connected)
	{
		neon_log(WARNING, "Tried to flush while disconnected");
	}
	else
	{
		if (PQflush(pageserver_conn))
		{
			char	   *msg = pchomp(PQerrorMessage(pageserver_conn));

			pageserver_disconnect();
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

static Size
PagestoreShmemSize(void)
{
	return sizeof(PagestoreShmemState);
}

static bool
PagestoreShmemInit(void)
{
	bool		found;

	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);
	pagestore_shared = ShmemInitStruct("libpagestore shared state",
									   PagestoreShmemSize(),
									   &found);
	if (!found)
	{
		pagestore_shared->lock = &(GetNamedLWLockTranche("neon_libpagestore")->lock);
		pg_atomic_init_u64(&pagestore_shared->update_counter, 0);
		AssignPageserverConnstring(page_server_connstring, NULL);
	}
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
	RequestNamedLWLockTranche("neon_libpagestore", 1);
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
	}

	lfc_init();
}

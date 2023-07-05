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

#include "access/xact.h"
#include "access/xlog.h"
#include "access/relation.h"
#include "access/xloginsert.h"
#include "storage/buf_internals.h"
#include "storage/bufmgr.h"
#include "catalog/pg_type.h"
#include "catalog/namespace.h"
#include "replication/walsender.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "access/htup_details.h"
#include "utils/pg_lsn.h"
#include "utils/guc.h"
#include "utils/wait_event.h"
#include "utils/rel.h"
#include "utils/varlena.h"
#include "utils/builtins.h"

#include "neon.h"
#include "walproposer.h"
#include "pagestore_client.h"
#include "control_plane_connector.h"

#include "libpq-fe.h"
#include "libpq/pqformat.h"
#include "libpq/libpq.h"

PG_MODULE_MAGIC;
void		_PG_init(void);

void
_PG_init(void)
{
	pg_init_libpagestore();
	pg_init_walproposer();
	InitControlPlaneConnector();

        // Important: This must happen after other parts of the extension
        // are loaded, otherwise any settings to GUCs that were set before
        // the extension was loaded will be removed.
	EmitWarningsOnPlaceholders("neon");
}

PG_FUNCTION_INFO_V1(pg_cluster_size);
PG_FUNCTION_INFO_V1(backpressure_lsns);
PG_FUNCTION_INFO_V1(backpressure_throttling_time);
PG_FUNCTION_INFO_V1(copy_from);

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


#define N_RAW_PAGE_COLUMNS 4
#define COPY_FETCH_COUNT   16


static void
report_error(int elevel, PGresult *res, PGconn *conn,
			 bool clear, const char *sql)
{
	/* If requested, PGresult must be released before leaving this function. */
	PG_TRY();
	{
		char	   *diag_sqlstate = PQresultErrorField(res, PG_DIAG_SQLSTATE);
		char	   *message_primary = PQresultErrorField(res, PG_DIAG_MESSAGE_PRIMARY);
		char	   *message_detail = PQresultErrorField(res, PG_DIAG_MESSAGE_DETAIL);
		char	   *message_hint = PQresultErrorField(res, PG_DIAG_MESSAGE_HINT);
		char	   *message_context = PQresultErrorField(res, PG_DIAG_CONTEXT);
		int			sqlstate;

		if (diag_sqlstate)
			sqlstate = MAKE_SQLSTATE(diag_sqlstate[0],
									 diag_sqlstate[1],
									 diag_sqlstate[2],
									 diag_sqlstate[3],
									 diag_sqlstate[4]);
		else
			sqlstate = ERRCODE_CONNECTION_FAILURE;

		/*
		 * If we don't get a message from the PGresult, try the PGconn.  This
		 * is needed because for connection-level failures, PQexec may just
		 * return NULL, not a PGresult at all.
		 */
		if (message_primary == NULL)
			message_primary = pchomp(PQerrorMessage(conn));

		ereport(elevel,
				(errcode(sqlstate),
				 (message_primary != NULL && message_primary[0] != '\0') ?
				 errmsg_internal("%s", message_primary) :
				 errmsg("could not obtain message string for remote error"),
				 message_detail ? errdetail_internal("%s", message_detail) : 0,
				 message_hint ? errhint("%s", message_hint) : 0,
				 message_context ? errcontext("%s", message_context) : 0,
				 sql ? errcontext("remote SQL command: %s", sql) : 0));
	}
	PG_FINALLY();
	{
		if (clear)
			PQclear(res);
	}
	PG_END_TRY();
}

static PGresult *
get_result(PGconn *conn, const char *query)
{
	PGresult   *volatile last_res = NULL;

	/* In what follows, do not leak any PGresults on an error. */
	PG_TRY();
	{
		for (;;)
		{
			PGresult   *res;

			while (PQisBusy(conn))
			{
				int			wc;

				/* Sleep until there's something to do */
				wc = WaitLatchOrSocket(MyLatch,
									   WL_LATCH_SET | WL_SOCKET_READABLE |
									   WL_EXIT_ON_PM_DEATH,
									   PQsocket(conn),
									   -1L, PG_WAIT_EXTENSION);
				ResetLatch(MyLatch);

				CHECK_FOR_INTERRUPTS();

				/* Data available in socket? */
				if (wc & WL_SOCKET_READABLE)
				{
					if (!PQconsumeInput(conn))
						report_error(ERROR, NULL, conn, false, query);
				}
			}

			res = PQgetResult(conn);
			if (res == NULL)
				break;			/* query is complete */

			PQclear(last_res);
			last_res = res;
		}
	}
	PG_CATCH();
	{
		PQclear(last_res);
		PG_RE_THROW();
	}
	PG_END_TRY();

	return last_res;
}

#define CREATE_COPYDATA_FUNC "\
create or replace function copydata() returns setof record as $$ \
declare \
    relsize integer; \
    total_relsize integer; \
	content bytea; \
	r record; \
	fork text; \
	relname text; \
	pagesize integer; \
begin \
	pagesize = current_setting('block_size'); \
	for r in select oid,reltoastrelid from pg_class where relnamespace not in (select oid from pg_namespace where nspname in ('pg_catalog','pg_toast','information_schema')) \
	loop \
		relname = r.oid::regclass::text; \
        total_relsize = 0; \
	    foreach fork in array array['main','vm','fsm'] \
		loop \
		    relsize = pg_relation_size(r.oid, fork)/pagesize; \
			total_relsize = total_relsize + relsize; \
	        for p in 1..relsize \
		    loop \
			    content = get_raw_page(relname, fork, p-1); \
				return next row(relname,fork,p-1,content); \
			end loop; \
		end loop; \
        if total_relsize <> 0 and r.reltoastrelid <> 0 then \
            foreach relname in array array ['pg_toast.pg_toast_'||r.oid, 'pg_toast.pg_toast_'||r.oid||'_index'] \
			loop \
		    	foreach fork in array array['main','vm','fsm'] \
				loop \
			    	relsize = pg_relation_size(relname, fork)/pagesize; \
	        		for p in 1..relsize \
		    		loop \
			    		content = get_raw_page(relname, fork, p-1); \
						return next row(relname,fork,p-1,content); \
					end loop; \
				end loop; \
			end loop; \
        end if; \
	end loop; \
end; \
$$ language plpgsql"

Datum
copy_from(PG_FUNCTION_ARGS)
{
	char const* conninfo = PG_GETARG_CSTRING(0);
	PGconn* conn;
	char const* declare_cursor = "declare copy_data_cursor no scroll cursor for select * from copydata() as raw_page(relid text, fork text, blkno integer, content bytea)";
	char* fetch_cursor = psprintf("fetch forward %d copy_data_cursor", COPY_FETCH_COUNT);
	char const* close_cursor = "close copy_data_cursor";
	char const* vacuum_freeze = "vacuum freeze";
	char   *content;
	char const* relname;
	BlockNumber blkno;
	ForkNumber forknum;
	BlockNumber prev_blkno = InvalidBlockNumber;
	RangeVar   *relrv;
	Relation rel = NULL;
	BlockNumber rel_size;
	int64_t total = 0;
	PGresult   *res;
	char blkno_buf[4];
	int n_tuples;
	Buffer buf;
	char* toast_rel_name;
	Oid relid = InvalidOid;

	/* Connect to the source database */
	conn = PQconnectdb(conninfo);
	if (!conn || PQstatus(conn) != CONNECTION_OK)
		ereport(ERROR,
				(errcode(ERRCODE_SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION),
				 errmsg("could not connect to server \"%s\"",
						conninfo),
				 errdetail_internal("%s", pchomp(PQerrorMessage(conn)))));

	/* First create store procedure (assumes that pageinspector extension is already installed) */
	res = PQexec(conn, CREATE_COPYDATA_FUNC);
	if (res == NULL || PQresultStatus(res) != PGRES_COMMAND_OK)
		report_error(ERROR, res, conn, true, CREATE_COPYDATA_FUNC);
	PQclear(res);

	/* Freeze all tables to prevent problems with XID mapping */
	res = PQexec(conn, vacuum_freeze);
	if (res == NULL || PQresultStatus(res) != PGRES_COMMAND_OK)
		report_error(ERROR, res, conn, true, vacuum_freeze);
	PQclear(res);

	/* Start transaction to use cursor */
	res = PQexec(conn, "BEGIN");
	if (res == NULL || PQresultStatus(res) != PGRES_COMMAND_OK)
		report_error(ERROR, res, conn, true, "BEGIN");
	PQclear(res);

	/* Declare cursor (we have to use cursor to avoid materializing all database in memory) */
	res = PQexec(conn, declare_cursor);
	if (res == NULL || PQresultStatus(res) != PGRES_COMMAND_OK)
		report_error(ERROR, res, conn, true, declare_cursor);
	PQclear(res);

	/* Get database data */
	while ((res = PQexecParams(conn, fetch_cursor, 0, NULL, NULL, NULL, NULL, 1)) != NULL)
	{
		if (PQresultStatus(res) != PGRES_TUPLES_OK)
			report_error(ERROR, res, conn, true, fetch_cursor);

		n_tuples = PQntuples(res);
		if (PQnfields(res) != 4)
			elog(ERROR, "unexpected result from copydata()");

		for (int i = 0; i < n_tuples; i++)
		{
			relname = PQgetvalue(res, i, 0);
			forknum = forkname_to_number(PQgetvalue(res, i, 1));
			memcpy(&blkno, PQgetvalue(res, i, 2), sizeof(BlockNumber));
			blkno = pg_ntoh32(blkno);
			content = (char*)PQgetvalue(res, i, 3);

			if (blkno <= prev_blkno)
			{
				if (forknum == MAIN_FORKNUM)
				{
					char* dst_rel_name = strncmp(relname, "pg_toast.", 9) == 0
						/* Construct correct TOAST table name */
						? psprintf("pg_toast.pg_toast_%u%s",
								   relid,
								   strcmp(relname + strlen(relname) - 5, "index") == 0 ? "_index" : "")
						: (char*)relname;
					if (rel)
						relation_close(rel, AccessExclusiveLock);
					relrv = makeRangeVarFromNameList(textToQualifiedNameList(cstring_to_text(dst_rel_name)));
					rel = relation_openrv(relrv, AccessExclusiveLock);
					if (dst_rel_name != relname)
						pfree(dst_rel_name);
					else
						relid = RelationGetRelid(rel);
				}
				rel_size = RelationGetNumberOfBlocksInFork(rel, forknum);
			}
			buf = ReadBufferExtended(rel, forknum, blkno < rel_size ? blkno : P_NEW, RBM_ZERO_AND_LOCK, NULL);
			MarkBufferDirty(buf);
			memcpy(BufferGetPage(buf), content, BLCKSZ);
			log_newpage_buffer(buf, forknum == MAIN_FORKNUM);
			UnlockReleaseBuffer(buf);

			total += 1;
			prev_blkno = blkno;
		}
		PQclear(res);
		if (n_tuples < COPY_FETCH_COUNT)
			break;
	}
	res = PQexec(conn, close_cursor);
	if (res == NULL || PQresultStatus(res) != PGRES_COMMAND_OK)
		report_error(ERROR, res, conn, true, close_cursor);
	PQclear(res);

	if (rel)
		relation_close(rel, AccessExclusiveLock);

	/* Complete transaction */
	res = PQexec(conn, "END");
	if (res == NULL || PQresultStatus(res) != PGRES_COMMAND_OK)
		report_error(ERROR, res, conn, true, "END");
	PQclear(res);

	PQfinish(conn);
	PG_RETURN_INT64(total);
}

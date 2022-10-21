/*-------------------------------------------------------------------------
 *
 * neontest.c
 *	  Helpers for neon testing and debugging
 *
 * IDENTIFICATION
 *	 contrib/neon_test_utils/neontest.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/relation.h"
#include "access/xact.h"
#include "access/xlog.h"
#include "catalog/namespace.h"
#include "fmgr.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "storage/buf_internals.h"
#include "storage/bufmgr.h"
#include "utils/builtins.h"
#include "utils/pg_lsn.h"
#include "utils/rel.h"
#include "utils/varlena.h"
#include "utils/wait_event.h"
#include "../neon/pagestore_client.h"

#include "libpq-fe.h"
#include "libpq/pqformat.h"
#include "libpq/libpq.h"

PG_MODULE_MAGIC;

extern void _PG_init(void);

PG_FUNCTION_INFO_V1(test_consume_xids);
PG_FUNCTION_INFO_V1(clear_buffer_cache);
PG_FUNCTION_INFO_V1(get_raw_page_at_lsn);
PG_FUNCTION_INFO_V1(get_raw_page_at_lsn_ex);
PG_FUNCTION_INFO_V1(neon_xlogflush);
PG_FUNCTION_INFO_V1(neon_seqscan_rel);

/*
 * Linkage to functions in neon module.
 * The signature here would need to be updated whenever function parameters change in pagestore_smgr.c
 */
typedef void (*neon_read_at_lsn_type) (RelFileNode rnode, ForkNumber forkNum, BlockNumber blkno,
									   XLogRecPtr request_lsn, bool request_latest, char *buffer);

static neon_read_at_lsn_type neon_read_at_lsn_ptr;

/*
 * Module initialize function: fetch function pointers for cross-module calls.
 */
void
_PG_init(void)
{
	/* Asserts verify that typedefs above match original declarations */
	AssertVariableIsOfType(&neon_read_at_lsn, neon_read_at_lsn_type);
	neon_read_at_lsn_ptr = (neon_read_at_lsn_type)
		load_external_function("$libdir/neon", "neon_read_at_lsn",
							   true, NULL);
}

#define neon_read_at_lsn neon_read_at_lsn_ptr

/*
 * test_consume_xids(int4), for rapidly consuming XIDs, to test wraparound.
 */
Datum
test_consume_xids(PG_FUNCTION_ARGS)
{
	int32		nxids = PG_GETARG_INT32(0);
	TransactionId topxid;
	FullTransactionId fullxid;
	TransactionId xid;
	TransactionId targetxid;

	/* make sure we have a top-XID first */
	topxid = GetTopTransactionId();

	xid = ReadNextTransactionId();

	targetxid = xid + nxids;
	while (targetxid < FirstNormalTransactionId)
		targetxid++;

	while (TransactionIdPrecedes(xid, targetxid))
	{
		fullxid = GetNewTransactionId(true);
		xid = XidFromFullTransactionId(fullxid);
		elog(DEBUG1, "topxid: %u xid: %u", topxid, xid);
	}

	PG_RETURN_VOID();
}

/*
 * Flush the buffer cache, evicting all pages that are not currently pinned.
 */
Datum
clear_buffer_cache(PG_FUNCTION_ARGS)
{
	bool		save_neon_test_evict;

	/*
	 * Temporarily set the zenith_test_evict GUC, so that when we pin and
	 * unpin a buffer, the buffer is evicted. We use that hack to evict all
	 * buffers, as there is no explicit "evict this buffer" function in the
	 * buffer manager.
	 */
	save_neon_test_evict = zenith_test_evict;
	zenith_test_evict = true;
	PG_TRY();
	{
		/* Scan through all the buffers */
		for (int i = 0; i < NBuffers; i++)
		{
			BufferDesc *bufHdr;
			uint32		buf_state;
			Buffer		bufferid;
			bool		isvalid;
			RelFileNode rnode;
			ForkNumber	forknum;
			BlockNumber blocknum;

			/* Peek into the buffer header to see what page it holds. */
			bufHdr = GetBufferDescriptor(i);
			buf_state = LockBufHdr(bufHdr);

			if ((buf_state & BM_VALID) && (buf_state & BM_TAG_VALID))
				isvalid = true;
			else
				isvalid = false;
			bufferid = BufferDescriptorGetBuffer(bufHdr);
			rnode = bufHdr->tag.rnode;
			forknum = bufHdr->tag.forkNum;
			blocknum = bufHdr->tag.blockNum;

			UnlockBufHdr(bufHdr, buf_state);

			/*
			 * Pin the buffer, and release it again. Because we have
			 * zenith_test_evict==true, this will evict the page from the
			 * buffer cache if no one else is holding a pin on it.
			 */
			if (isvalid)
			{
				if (ReadRecentBuffer(rnode, forknum, blocknum, bufferid))
					ReleaseBuffer(bufferid);
			}
		}
	}
	PG_FINALLY();
	{
		/* restore the GUC */
		zenith_test_evict = save_neon_test_evict;
	}
	PG_END_TRY();

	PG_RETURN_VOID();
}

/*
 * Reads the page from page server without buffer cache
 * usage mimics get_raw_page() in pageinspect, but offers reading versions at specific LSN
 * NULL read lsn will result in reading the latest version.
 *
 * Note: reading latest version will result in waiting for latest changes to reach the page server,
 *       if this is undesirable, use pageinspect' get_raw_page that uses buffered access to the latest page
 */
Datum
get_raw_page_at_lsn(PG_FUNCTION_ARGS)
{
	bytea	   *raw_page;
	ForkNumber	forknum;
	RangeVar   *relrv;
	Relation	rel;
	char	   *raw_page_data;
	text	   *relname;
	text	   *forkname;
	uint32		blkno;

	bool		request_latest = PG_ARGISNULL(3);
	uint64		read_lsn = request_latest ? GetXLogInsertRecPtr() : PG_GETARG_INT64(3);

	if (PG_ARGISNULL(0) || PG_ARGISNULL(1) || PG_ARGISNULL(2))
		PG_RETURN_NULL();

	relname = PG_GETARG_TEXT_PP(0);
	forkname = PG_GETARG_TEXT_PP(1);
	blkno = PG_GETARG_UINT32(2);

	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("must be superuser to use raw page functions")));

	relrv = makeRangeVarFromNameList(textToQualifiedNameList(relname));
	rel = relation_openrv(relrv, AccessShareLock);

	/* Check that this relation has storage */
	if (rel->rd_rel->relkind == RELKIND_VIEW)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("cannot get raw page from view \"%s\"",
						RelationGetRelationName(rel))));
	if (rel->rd_rel->relkind == RELKIND_COMPOSITE_TYPE)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("cannot get raw page from composite type \"%s\"",
						RelationGetRelationName(rel))));
	if (rel->rd_rel->relkind == RELKIND_FOREIGN_TABLE)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("cannot get raw page from foreign table \"%s\"",
						RelationGetRelationName(rel))));
	if (rel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("cannot get raw page from partitioned table \"%s\"",
						RelationGetRelationName(rel))));
	if (rel->rd_rel->relkind == RELKIND_PARTITIONED_INDEX)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("cannot get raw page from partitioned index \"%s\"",
						RelationGetRelationName(rel))));

	/*
	 * Reject attempts to read non-local temporary relations; we would be
	 * likely to get wrong data since we have no visibility into the owning
	 * session's local buffers.
	 */
	if (RELATION_IS_OTHER_TEMP(rel))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot access temporary tables of other sessions")));

	forknum = forkname_to_number(text_to_cstring(forkname));

	/* Initialize buffer to copy to */
	raw_page = (bytea *) palloc(BLCKSZ + VARHDRSZ);
	SET_VARSIZE(raw_page, BLCKSZ + VARHDRSZ);
	raw_page_data = VARDATA(raw_page);

	neon_read_at_lsn(rel->rd_node, forknum, blkno, read_lsn, request_latest, raw_page_data);

	relation_close(rel, AccessShareLock);

	PG_RETURN_BYTEA_P(raw_page);
}

/*
 * Another option to read a relation page from page server without cache
 * this version doesn't validate input and allows reading blocks of dropped relations
 *
 * Note: reading latest version will result in waiting for latest changes to reach the page server,
 *  if this is undesirable, use pageinspect' get_raw_page that uses buffered access to the latest page
 */
Datum
get_raw_page_at_lsn_ex(PG_FUNCTION_ARGS)
{
	char	   *raw_page_data;

	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("must be superuser to use raw page functions")));

	if (PG_ARGISNULL(0) || PG_ARGISNULL(1) || PG_ARGISNULL(2) ||
		PG_ARGISNULL(3) || PG_ARGISNULL(4))
		PG_RETURN_NULL();

	{
		RelFileNode rnode = {
			.spcNode = PG_GETARG_OID(0),
			.dbNode = PG_GETARG_OID(1),
		.relNode = PG_GETARG_OID(2)};

		ForkNumber	forknum = PG_GETARG_UINT32(3);

		uint32		blkno = PG_GETARG_UINT32(4);
		bool		request_latest = PG_ARGISNULL(5);
		uint64		read_lsn = request_latest ? GetXLogInsertRecPtr() : PG_GETARG_INT64(5);

		/* Initialize buffer to copy to */
		bytea	   *raw_page = (bytea *) palloc(BLCKSZ + VARHDRSZ);

		SET_VARSIZE(raw_page, BLCKSZ + VARHDRSZ);
		raw_page_data = VARDATA(raw_page);

		neon_read_at_lsn(rnode, forknum, blkno, read_lsn, request_latest, raw_page_data);
		PG_RETURN_BYTEA_P(raw_page);
	}
}


/*
 * A wrapper around PQgetCopyData that checks for interrupts while sleeping.
 */
static int
call_PQgetCopyData(PGconn *conn, char **buffer)
{
	int			ret;

retry:
	ret = PQgetCopyData(conn, buffer, 1 /* async */ );

	if (ret == 0)
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
				elog(ERROR, "could not get response from pageserver: %s",
					 PQerrorMessage(conn));
		}

		goto retry;
	}

	return ret;
}

static void send_getpage_request(PGconn *pageserver_conn, RelFileNode rnode, BlockNumber blkno, XLogRecPtr lsn);

/*
 * Fetch all pages of given relation. This simulates a sequential scan
 * over the table. You can specify the number of blocks to prefetch;
 * the function will try to keep that many requests "in flight" at all
 * times. The fetched pages are simply discarded.
 */
Datum
neon_seqscan_rel(PG_FUNCTION_ARGS)
{
	Oid			relid = PG_GETARG_OID(0);
	Oid			nprefetch = PG_GETARG_INT32(1);
	Relation	rel;
	char	   *raw_page_data;
	BlockNumber nblocks;
	PGconn	   *pageserver_conn;
	XLogRecPtr	read_lsn;

	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("must be superuser to use raw page functions")));

	rel = relation_open(relid, AccessShareLock);

	nblocks = RelationGetNumberOfBlocks(rel);

	pageserver_conn = PQconnectdb(page_server_connstring);
	if (PQstatus(pageserver_conn) == CONNECTION_BAD)
	{
		char	   *msg = pchomp(PQerrorMessage(pageserver_conn));

		PQfinish(pageserver_conn);
		ereport(ERROR,
				(errcode(ERRCODE_SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION),
				 errmsg("could not establish connection to pageserver"),
				 errdetail_internal("%s", msg)));
	}
	PG_TRY();
	{
		char	   *query;
		int			ret;
		StringInfoData resp_buff;

		read_lsn = GetXLogInsertRecPtr();

		query = psprintf("pagestream %s %s", neon_tenant, neon_timeline);
		ret = PQsendQuery(pageserver_conn, query);
		if (ret != 1)
		{
			PQfinish(pageserver_conn);
			pageserver_conn = NULL;
			elog(ERROR, "could not send pagestream command to pageserver");
		}

		while (PQisBusy(pageserver_conn))
		{
			int			wc;

			/* Sleep until there's something to do */
			wc = WaitLatchOrSocket(MyLatch,
								   WL_LATCH_SET | WL_SOCKET_READABLE |
								   WL_EXIT_ON_PM_DEATH,
								   PQsocket(pageserver_conn),
								   -1L, PG_WAIT_EXTENSION);
			ResetLatch(MyLatch);

			CHECK_FOR_INTERRUPTS();

			/* Data available in socket? */
			if (wc & WL_SOCKET_READABLE)
			{
				if (!PQconsumeInput(pageserver_conn))
				{
					char	   *msg = pchomp(PQerrorMessage(pageserver_conn));

					PQfinish(pageserver_conn);
					pageserver_conn = NULL;

					elog(ERROR, "could not complete handshake with pageserver: %s",
							 msg);
				}
			}
		}

		elog(INFO, "scanning %u blocks, prefetch %u", nblocks, nprefetch);

		BlockNumber nsent = 0;
		for (BlockNumber blkno = 0; blkno < nblocks; blkno++)
		{
			NeonGetPageRequest request = {
				.req.tag = T_NeonGetPageRequest,
				.req.latest = true,
				.req.lsn = read_lsn,
				.rnode = rel->rd_node,
				.forknum = MAIN_FORKNUM,
				.blkno = blkno
			};
			NeonResponse *resp;

			if (blkno % 1024 == 0)
				elog(INFO, "blk %u/%u", blkno, nblocks);

			if (nsent < blkno + nprefetch + 1 && nsent < nblocks)
			{
				while (nsent < blkno + nprefetch + 1 && nsent < nblocks)
					send_getpage_request(pageserver_conn, rel->rd_node, nsent++, read_lsn);

				if (PQflush(pageserver_conn))
				{
					char	   *msg = PQerrorMessage(pageserver_conn);

					elog(ERROR, "failed to flush page requests: %s", msg);
				}
			}

			/* read response */
			resp_buff.len = call_PQgetCopyData(pageserver_conn, &resp_buff.data);
			resp_buff.cursor = 0;

			if (resp_buff.len < 0)
			{
				if (resp_buff.len == -1)
					elog(ERROR, "end of COPY");
				else if (resp_buff.len == -2)
					elog(ERROR, "could not read COPY data: %s", PQerrorMessage(pageserver_conn));
			}
			resp = nm_unpack_response(&resp_buff);

			switch (resp->tag)
			{
				case T_NeonGetPageResponse:
					/* ok */
					break;

				case T_NeonErrorResponse:
					ereport(ERROR,
							(errcode(ERRCODE_IO_ERROR),
							 errmsg("could not read block %u", blkno),
							 errdetail("page server returned error: %s",
									   ((NeonErrorResponse *) resp)->message)));
					break;

				default:
					elog(ERROR, "unexpected response from page server with tag 0x%02x", resp->tag);
			}

			PQfreemem(resp_buff.data);
		}
	}
	PG_CATCH();
	{
		PQfinish(pageserver_conn);
		PG_RE_THROW();
	}
	PG_END_TRY();

	relation_close(rel, AccessShareLock);
}

static void
send_getpage_request(PGconn *pageserver_conn, RelFileNode rnode, BlockNumber blkno, XLogRecPtr lsn)
{
	NeonGetPageRequest request = {
		.req.tag = T_NeonGetPageRequest,
		.req.latest = true,
		.req.lsn = lsn,
		.rnode = rnode,
		.forknum = MAIN_FORKNUM,
		.blkno = blkno
	};
	StringInfoData req_buff;

	req_buff = nm_pack_request(&request.req);
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
		char	   *msg = PQerrorMessage(pageserver_conn);

		elog(ERROR, "failed to send page request: %s", msg);
	}
	pfree(req_buff.data);
}

/*
 * Directly calls XLogFlush(lsn) to flush WAL buffers.
 */
Datum
neon_xlogflush(PG_FUNCTION_ARGS)
{
	XLogRecPtr	lsn = PG_GETARG_LSN(0);

	XLogFlush(lsn);
	PG_RETURN_VOID();
}

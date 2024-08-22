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

#include "../neon/neon_pgversioncompat.h"

#include "access/relation.h"
#include "access/xact.h"
#include "access/xlog.h"
#include "access/xlog_internal.h"
#include "catalog/namespace.h"
#include "fmgr.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "storage/buf_internals.h"
#include "storage/bufmgr.h"
#include "storage/fd.h"
#include "utils/builtins.h"
#include "utils/pg_lsn.h"
#include "utils/rel.h"
#include "utils/varlena.h"
#include "utils/wait_event.h"
#include "../neon/pagestore_client.h"

PG_MODULE_MAGIC;

extern void _PG_init(void);

PG_FUNCTION_INFO_V1(test_consume_xids);
PG_FUNCTION_INFO_V1(test_consume_oids);
PG_FUNCTION_INFO_V1(test_consume_cpu);
PG_FUNCTION_INFO_V1(test_consume_memory);
PG_FUNCTION_INFO_V1(test_release_memory);
PG_FUNCTION_INFO_V1(clear_buffer_cache);
PG_FUNCTION_INFO_V1(get_raw_page_at_lsn);
PG_FUNCTION_INFO_V1(get_raw_page_at_lsn_ex);
PG_FUNCTION_INFO_V1(neon_xlogflush);
PG_FUNCTION_INFO_V1(trigger_panic);
PG_FUNCTION_INFO_V1(trigger_segfault);

/*
 * Linkage to functions in neon module.
 * The signature here would need to be updated whenever function parameters change in pagestore_smgr.c
 */
#if PG_MAJORVERSION_NUM < 16
typedef void (*neon_read_at_lsn_type) (NRelFileInfo rinfo, ForkNumber forkNum, BlockNumber blkno,
									   neon_request_lsns request_lsns, char *buffer);
#else
typedef void (*neon_read_at_lsn_type) (NRelFileInfo rinfo, ForkNumber forkNum, BlockNumber blkno,
									   neon_request_lsns request_lsns, void *buffer);
#endif

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
 * test_consume_oids(int4), for rapidly consuming OIDs, to test wraparound.
 * Unlike test_consume_xids which is passed number of xids to be consumed,
 * this function is given the target Oid.
 */
Datum
test_consume_oids(PG_FUNCTION_ARGS)
{
	int32 oid = PG_GETARG_INT32(0);

	while (oid != GetNewObjectId());

	PG_RETURN_VOID();
}

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
 * test_consume_cpu(seconds int). Keeps one CPU busy for the given number of seconds.
 */
Datum
test_consume_cpu(PG_FUNCTION_ARGS)
{
	int32		seconds = PG_GETARG_INT32(0);
	TimestampTz start;
	uint64		total_iterations = 0;

	start = GetCurrentTimestamp();

	for (;;)
	{
		TimestampTz elapsed;

		elapsed = GetCurrentTimestamp() - start;
		if (elapsed > (TimestampTz) seconds * USECS_PER_SEC)
			break;

		/* keep spinning */
		for (int i = 0; i < 1000000; i++)
			total_iterations++;
		elog(DEBUG2, "test_consume_cpu(): %lu iterations in total", total_iterations);

		CHECK_FOR_INTERRUPTS();
	}

	PG_RETURN_VOID();
}

static MemoryContext consume_cxt = NULL;
static slist_head consumed_memory_chunks;
static int64 num_memory_chunks;

/*
 * test_consume_memory(megabytes int).
 *
 * Consume given amount of memory. The allocation is made in TopMemoryContext,
 * so it outlives the function, until you call test_release_memory to
 * explicitly release it, or close the session.
 */
Datum
test_consume_memory(PG_FUNCTION_ARGS)
{
	int32		megabytes = PG_GETARG_INT32(0);

	/*
	 * Consume the memory in a new memory context, so that it's convenient to
	 * release and to display it separately in a possible memory context dump.
	 */
	if (consume_cxt == NULL)
		consume_cxt = AllocSetContextCreate(TopMemoryContext,
											"test_consume_memory",
											ALLOCSET_DEFAULT_SIZES);

	for (int32 i = 0; i < megabytes; i++)
	{
		char	   *p;

		p = MemoryContextAllocZero(consume_cxt, 1024 * 1024);

		/* touch the memory, so that it's really allocated by the kernel */
		for (int j = 0; j < 1024 * 1024; j += 1024)
			p[j] = j % 0xFF;

		slist_push_head(&consumed_memory_chunks, (slist_node *) p);
		num_memory_chunks++;
	}

	PG_RETURN_VOID();
}

/*
 * test_release_memory(megabytes int). NULL releases all
 */
Datum
test_release_memory(PG_FUNCTION_ARGS)
{
	if (PG_ARGISNULL(0))
	{
		if (consume_cxt)
		{
			MemoryContextDelete(consume_cxt);
			consume_cxt = NULL;
			num_memory_chunks = 0;
		}
	}
	else
	{
		int32		chunks_to_release = PG_GETARG_INT32(0);

		if (chunks_to_release > num_memory_chunks)
		{
			elog(WARNING, "only %lu MB is consumed, releasing it all", num_memory_chunks);
			chunks_to_release = num_memory_chunks;
		}

		for (int32 i = 0; i < chunks_to_release; i++)
		{
			slist_node *chunk = slist_pop_head_node(&consumed_memory_chunks);

			pfree(chunk);
			num_memory_chunks--;
		}
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
			NRelFileInfo rinfo;
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
			rinfo = BufTagGetNRelFileInfo(bufHdr->tag);
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
				if (ReadRecentBuffer(rinfo, forknum, blocknum, bufferid))
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
	neon_request_lsns	request_lsns;

	if (PG_NARGS() != 5)
		elog(ERROR, "unexpected number of arguments in SQL function signature");

	if (PG_ARGISNULL(0) || PG_ARGISNULL(1) || PG_ARGISNULL(2))
		PG_RETURN_NULL();

	relname = PG_GETARG_TEXT_PP(0);
	forkname = PG_GETARG_TEXT_PP(1);
	blkno = PG_GETARG_UINT32(2);

	request_lsns.request_lsn = PG_ARGISNULL(3) ? GetXLogInsertRecPtr() : PG_GETARG_LSN(3);
	request_lsns.not_modified_since = PG_ARGISNULL(4) ? request_lsns.request_lsn : PG_GETARG_LSN(4);
	/*
	 * For the time being, use the same LSN for request and
	 * effective request LSN. If any test needed to use UINT64_MAX
	 * as the request LSN, we'd need to add effective_request_lsn
	 * as a new argument.
	 */
	request_lsns.effective_request_lsn = request_lsns.request_lsn;

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

	neon_read_at_lsn(InfoFromRelation(rel), forknum, blkno, request_lsns,
					 raw_page_data);

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

	if (PG_NARGS() != 7)
		elog(ERROR, "unexpected number of arguments in SQL function signature");

	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("must be superuser to use raw page functions")));

	if (PG_ARGISNULL(0) || PG_ARGISNULL(1) || PG_ARGISNULL(2) ||
		PG_ARGISNULL(3) || PG_ARGISNULL(4))
		PG_RETURN_NULL();

	{
		NRelFileInfo rinfo = {
#if PG_MAJORVERSION_NUM < 16
			.spcNode = PG_GETARG_OID(0),
			.dbNode = PG_GETARG_OID(1),
			.relNode = PG_GETARG_OID(2)
#else
			.spcOid = PG_GETARG_OID(0),
			.dbOid = PG_GETARG_OID(1),
			.relNumber = PG_GETARG_OID(2)
#endif
		};

		ForkNumber	forknum = PG_GETARG_UINT32(3);
		uint32		blkno = PG_GETARG_UINT32(4);
		neon_request_lsns	request_lsns;

		/* Initialize buffer to copy to */
		bytea	   *raw_page = (bytea *) palloc(BLCKSZ + VARHDRSZ);

		request_lsns.request_lsn = PG_ARGISNULL(5) ? GetXLogInsertRecPtr() : PG_GETARG_LSN(5);
		request_lsns.not_modified_since = PG_ARGISNULL(6) ? request_lsns.request_lsn : PG_GETARG_LSN(6);
		/*
		 * For the time being, use the same LSN for request
		 * and effective request LSN. If any test needed to
		 * use UINT64_MAX as the request LSN, we'd need to add
		 * effective_request_lsn as a new argument.
		 */
		request_lsns.effective_request_lsn = request_lsns.request_lsn;

		SET_VARSIZE(raw_page, BLCKSZ + VARHDRSZ);
		raw_page_data = VARDATA(raw_page);

		neon_read_at_lsn(rinfo, forknum, blkno, request_lsns, raw_page_data);
		PG_RETURN_BYTEA_P(raw_page);
	}
}

/*
 * Directly calls XLogFlush(lsn) to flush WAL buffers.
 *
 * If 'lsn' is not specified (is NULL), flush all generated WAL.
 */
Datum
neon_xlogflush(PG_FUNCTION_ARGS)
{
	XLogRecPtr	lsn;

	if (RecoveryInProgress())
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("recovery is in progress"),
				 errhint("cannot flush WAL during recovery.")));

	if (!PG_ARGISNULL(0))
		lsn = PG_GETARG_LSN(0);
	else
	{
		lsn = GetXLogInsertRecPtr();

		/*---
		 * The LSN returned by GetXLogInsertRecPtr() is the position where the
		 * next inserted record would begin. If the last record ended just at
		 * the page boundary, the next record will begin after the page header
		 * on the next page, but the next page's page header has not been
		 * written yet. If we tried to flush it, XLogFlush() would throw an
		 * error:
		 *
		 * ERROR : xlog flush request %X/%X is not satisfied --- flushed only to %X/%X
		 *
		 * To avoid that, if the insert position points to just after the page
		 * header, back off to page boundary.
		 */
		if (lsn % XLOG_BLCKSZ == SizeOfXLogShortPHD &&
			XLogSegmentOffset(lsn, wal_segment_size) > XLOG_BLCKSZ)
			lsn -= SizeOfXLogShortPHD;
		else if (lsn % XLOG_BLCKSZ == SizeOfXLogLongPHD &&
				 XLogSegmentOffset(lsn, wal_segment_size) < XLOG_BLCKSZ)
			lsn -= SizeOfXLogLongPHD;
	}

	XLogFlush(lsn);
	PG_RETURN_VOID();
}

/*
 * Function to trigger panic.
 */
Datum
trigger_panic(PG_FUNCTION_ARGS)
{
    elog(PANIC, "neon_test_utils: panic");
    PG_RETURN_VOID();
}

/*
 * Function to trigger a segfault.
 */
Datum
trigger_segfault(PG_FUNCTION_ARGS)
{
    int *ptr = NULL;
    *ptr = 42;
    PG_RETURN_VOID();
}

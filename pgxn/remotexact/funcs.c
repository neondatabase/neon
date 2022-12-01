#include "postgres.h"

#include "access/remotexact.h"
#include "apply.h"
#include "fmgr.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "lib/stringinfo.h"
#include "rwset.h"
#include "storage/block.h"
#include "storage/proc.h"
#include "validate.h"

PG_FUNCTION_INFO_V1(validate_and_apply_xact);
PG_FUNCTION_INFO_V1(lsn_snapshot);

static int relation_comparator(const void *p1, const void *p2)
{
	RWSetRelation r1 = *(const RWSetRelation *)p1;
	RWSetRelation r2 = *(const RWSetRelation *)p2;

	// The initial sort order is based on regions.
	if (r1.region < r2.region)
		return -1;
	else if (r1.region > r2.region)
		return 1;

	/* Both RWSetRelations belong to the same region.
	 * We prioritize in the following order:
	 * 		1. Table scans.
	 * 		2. Index scans.
	 * 		3. Tuple scans.
	 */
	if (r1.is_table_scan && !r2.is_table_scan)
		return -1;
	else if (!r1.is_table_scan && r2.is_table_scan)
		return 1;
	else if (r1.is_index && !r2.is_index)
		return -1;
	else if (!r1.is_index && r2.is_index)
		return 1;

	// Both RWSetRelations are scans of the same type.
	Assert((r1.is_table_scan == r2.is_table_scan) && 
			(r1.is_index == r2.is_index));

	if (r1.relid < r2.relid)
		return -1;
	else if (r1.relid > r2.relid)
		return 1;

	/* 
	 * Ideally, this should never happend because the sender will never send
	 * two different objects for the same relid and relkind.
	 */
	return 0;
}

static int page_comparator(const void *p1, const void *p2)
{
	RWSetPage page1 = *(const RWSetPage *)p1;
	RWSetPage page2 = *(const RWSetPage *)p2;

	if (page1.blkno < page2.blkno)
		return -1;
	else if (page1.blkno > page2.blkno)
		return 1;
	/* Ideally, we should not get the same page twice. */
	return 0;
}

static int tuple_comparator(const void *p1, const void *p2)
{
	RWSetTuple t1 = *(const RWSetTuple *)p1;
	RWSetTuple t2 = *(const RWSetTuple *)p2;

	BlockNumber b1 = ItemPointerGetBlockNumber(&(t1.tid));
	BlockNumber b2 = ItemPointerGetBlockNumber(&(t2.tid));
	OffsetNumber o1 = ItemPointerGetOffsetNumber(&(t1.tid));
	OffsetNumber o2 = ItemPointerGetOffsetNumber(&(t2.tid));

	if (b1 < b2)
		return -1;
	else if (b1 > b2)
		return 1;
	
	/* Both tuples belong to the same block. */
	Assert(b1 == b2);

	if (o1 < o2)
		return -1;
	else if (o1 > o2)
		return 1;

	/* Ideally, we should not get the same tuple twice. */
	return 0;
}

Datum
validate_and_apply_xact(PG_FUNCTION_ARGS)
{
	bytea	   *bytes = PG_GETARG_BYTEA_P(0);
	StringInfoData buf;
	RWSet	   *rwset;
	int i;

	/*
	 * Signify that this is a surrogate transaction. This
	 * variable will be reset on transaction completion.
	 */
	is_surrogate = true;

	/*
	 * Decode the buffer into a rwset and sort the relations.
	 */
	rwset = RWSetAllocate();
	buf.data = VARDATA(bytes);
	buf.len = VARSIZE(bytes) - VARHDRSZ;
	buf.maxlen = buf.len;
	buf.cursor = 0;
	RWSetDecode(rwset, &buf);
	pg_qsort(rwset->relations, rwset->n_relations, sizeof(RWSetRelation), 
			relation_comparator);

	/* 
	 * Mark the xact as remote before starting validation by setting the
	 * isRemoteXact flag in MyProc. We don't lock the ProcArray because its
	 * our own process.
	 */
	MyProc->isRemoteXact = true;
	pg_write_barrier();

	/*
	 * Validate the read set
	 */
	for (i = 0; i < rwset->n_relations; i++)
	{
		RWSetRelation *rel = &(rwset->relations[i]);
		int8 region = rel->region;

		if (region != current_region)
			continue;
		
		/* Prioritize validation on the basis of granularity. */
		if (rel->is_table_scan)
			validate_table_scan(rel);
		else if (rel->is_index)
		{
			pg_qsort(rel->pages, rel->n_pages, sizeof(RWSetPage), page_comparator);
			validate_index_scan(rel);
		} 
		else 
		{
			pg_qsort(rel->tuples, rel->n_tuples, sizeof(RWSetTuple), tuple_comparator);
			validate_tuple_scan(rel);
		}
	}

	/*
	 * Apply the write set
	 */
	apply_writes(rwset);

	/*
	 * Clean up
	 */
	RWSetFree(rwset);

	/*
	 * Mark the xact as local because validation is complete by unsetting the
	 * isRemoteXact flag in MyProc. We don't lock the ProcArray because its
	 * our own process.
	 */
	MyProc->isRemoteXact = false;
	pg_write_barrier();

	PG_RETURN_VOID();
}

Datum
lsn_snapshot(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	MemoryContext oldcontext;
	TupleDesc	tupdesc;
	Tuplestorestate *tupstore;
	AttInMetadata *attinmeta;
	XLogRecPtr *lsns;
	HeapTuple	tuple;
	char	**values;
	int		i;

	/* check to see if caller supports us returning a tuplestore */
	if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("set-valued function called in context that cannot accept a set")));
	if (!(rsinfo->allowedModes & SFRM_Materialize))
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("materialize mode required, but it is not allowed in this context")));

	/* The tupdesc and tuplestore must be created in ecxt_per_query_memory */
	oldcontext = MemoryContextSwitchTo(rsinfo->econtext->ecxt_per_query_memory);

	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	tupstore = tuplestore_begin_heap(true, false, work_mem);
	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = tupstore;
	rsinfo->setDesc = tupdesc;

	MemoryContextSwitchTo(oldcontext);

	lsns = GetAllRegionLsns();
	
	/* Return nothing if the get_all_region_lsns hook is not set */
	if (lsns == NULL)
		return (Datum) 0;

	attinmeta = TupleDescGetAttInMetadata(tupdesc);

	values = (char **) palloc(tupdesc->natts * sizeof(char *));

	for (i = 0; i < MAX_REGIONS; i++)
	{
		if (lsns[i] == InvalidXLogRecPtr)
			continue;

		/* region_id */
		values[0] = psprintf("%d", i);
		/* lsn */
		values[1] = psprintf("%X/%X", LSN_FORMAT_ARGS(lsns[i]));

		/* build the tuple */
		tuple = BuildTupleFromCStrings(attinmeta, values);
		tuplestore_puttuple(tupstore, tuple);
	}

	return (Datum) 0;
}

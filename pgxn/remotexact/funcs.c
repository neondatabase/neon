#include "postgres.h"

#include "access/remotexact.h"
#include "apply.h"
#include "fmgr.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "lib/stringinfo.h"
#include "rwset.h"
#include "storage/proc.h"
#include "validate.h"

PG_FUNCTION_INFO_V1(validate_and_apply_xact);
PG_FUNCTION_INFO_V1(lsn_snapshot);

Datum
validate_and_apply_xact(PG_FUNCTION_ARGS)
{
	bytea	   *bytes = PG_GETARG_BYTEA_P(0);
	StringInfoData buf;
	RWSet	   *rwset;
	dlist_iter	rel_iter;

	/*
	 * Signify that this is a surrogate transaction. This
	 * variable will be reset on transaction completion.
	 */
	is_surrogate = true;

	/*
	 * Decode the buffer into a rwset
	 */
	rwset = RWSetAllocate();
	buf.data = VARDATA(bytes);
	buf.len = VARSIZE(bytes) - VARHDRSZ;
	buf.maxlen = buf.len;
	buf.cursor = 0;
	RWSetDecode(rwset, &buf);

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
	dlist_foreach(rel_iter, &rwset->relations)
	{
		RWSetRelation *rel = dlist_container(RWSetRelation, node, rel_iter.cur);
		Oid relid = rel->relid;
		int8 region = rel->region;
		XidCSN read_csn = rel->csn;

		if (region != current_region)
			continue;

		if (!rel->is_index && rel->is_table_scan)
			validate_table_scan(relid, read_csn);
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

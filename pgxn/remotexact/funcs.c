#include "postgres.h"

#include "access/remotexact.h"
#include "apply.h"
#include "fmgr.h"
#include "lib/stringinfo.h"
#include "rwset.h"
#include "storage/proc.h"
#include "validate.h"

PG_FUNCTION_INFO_V1(validate_and_apply_xact);

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

		if (!rel->is_index)
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

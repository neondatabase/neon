#include "postgres.h"

#include "access/remotexact.h"
#include "apply.h"
#include "fmgr.h"
#include "lib/stringinfo.h"
#include "rwset.h"

PG_FUNCTION_INFO_V1(validate_and_apply_xact);

Datum
validate_and_apply_xact(PG_FUNCTION_ARGS)
{
	bytea	   *bytes = PG_GETARG_BYTEA_P(0);
	StringInfoData buf;
	RWSet	   *rwset;

	// Signify that this is a surrogate transaction. This
	// variable will be reset on transaction completion.
	is_surrogate = true;

	// Extract the buffer from the function argument
	buf.data = VARDATA(bytes);
	buf.len = VARSIZE(bytes) - VARHDRSZ;
	buf.maxlen = buf.len;
	buf.cursor = 0;

	// Decode the buffer into a rwset
	rwset = RWSetAllocate();
	RWSetDecode(rwset, &buf);

	ereport(LOG, errmsg("%s", RWSetToString(rwset)));

	// Apply the writes
	apply_writes(rwset);

	RWSetFree(rwset);

	PG_RETURN_BOOL(true);
}

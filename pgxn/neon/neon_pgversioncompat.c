/*
 * Support functions for the compatibility macros in neon_pgversioncompat.h
 */
#include "postgres.h"

#include "funcapi.h"
#include "miscadmin.h"
#include "utils/tuplestore.h"

#include "neon_pgversioncompat.h"

#if PG_MAJORVERSION_NUM < 15
void
InitMaterializedSRF(FunctionCallInfo fcinfo, bits32 flags)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	Tuplestorestate *tupstore;
	MemoryContext old_context,
				per_query_ctx;
	TupleDesc	stored_tupdesc;

	/* check to see if caller supports returning a tuplestore */
	if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("set-valued function called in context that cannot accept a set")));

	/*
	 * Store the tuplestore and the tuple descriptor in ReturnSetInfo.  This
	 * must be done in the per-query memory context.
	 */
	per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
	old_context = MemoryContextSwitchTo(per_query_ctx);

	if (get_call_result_type(fcinfo, NULL, &stored_tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	tupstore = tuplestore_begin_heap(false, false, work_mem);
	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = tupstore;
	rsinfo->setDesc = stored_tupdesc;
	MemoryContextSwitchTo(old_context);
}
#endif

#if PG_MAJORVERSION_NUM < 16
/*
 * Some infrastructure for checking malloc/strdup/realloc calls
 */
void *
guc_malloc(int elevel, size_t size)
{
	void	   *data;

	/* Avoid unportable behavior of malloc(0) */
	if (size == 0)
		size = 1;
	data = malloc(size);
	if (data == NULL)
		ereport(elevel,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("out of memory")));
	return data;
}

void *
guc_realloc(int elevel, void *old, size_t size)
{
	void	   *data;

	/* Avoid unportable behavior of realloc(NULL, 0) */
	if (old == NULL && size == 0)
		size = 1;
	data = realloc(old, size);
	if (data == NULL)
		ereport(elevel,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("out of memory")));
	return data;
}

char *
guc_strdup(int elevel, const char *src)
{
	char	   *data;

	data = strdup(src);
	if (data == NULL)
		ereport(elevel,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("out of memory")));
	return data;
}

void
guc_free(void *ptr)
{
	free(ptr);
}
#endif

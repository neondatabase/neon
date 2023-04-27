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
#include "commands/prepare.h"
#include "executor/spi.h"
#include "storage/buf_internals.h"
#include "storage/bufmgr.h"
#include "catalog/pg_type.h"
#include "replication/walsender.h"
#include "funcapi.h"
#include "access/htup_details.h"
#include "utils/builtins.h"
#include "utils/pg_lsn.h"
#include "utils/guc.h"

#include "neon.h"
#include "walproposer.h"

PG_MODULE_MAGIC;
void		_PG_init(void);

static char* neon_load_prepared_statement(char const* stmt_name, bool* from_sql);
static void  neon_save_prepared_statement(char const* stmt_name, char const* stmt_body, bool from_sql);
static bool  neon_drop_prepared_statement(char const* stmt_name);

static bool  save_parepared_statememts;

void
_PG_init(void)
{
	pg_init_libpagestore();
	pg_init_walproposer();
	DefineCustomBoolVariable("neon.save_prepared_statements",
							 "Support prepared statements in case of using connetion pooler",
							 NULL,
							 &save_parepared_statememts,
							 false, /* disabled by default */
							 PGC_POSTMASTER,
							 0,
							 NULL,
							 NULL,
							 NULL);

	save_prepared_statement_hook = neon_save_prepared_statement;
	load_prepared_statement_hook = neon_load_prepared_statement;
	drop_prepared_statement_hook = neon_drop_prepared_statement;
	EmitWarningsOnPlaceholders("neon");
}

PG_FUNCTION_INFO_V1(pg_cluster_size);
PG_FUNCTION_INFO_V1(backpressure_lsns);
PG_FUNCTION_INFO_V1(backpressure_throttling_time);

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

static char*
neon_load_prepared_statement(char const* stmt_name, bool* from_sql)
{
	char* stmt_body = NULL;
	if (save_parepared_statememts)
	{
		int rc;
		Oid param_types[2] = {TEXTOID, TEXTOID};
		Datum param_values[2] = {CStringGetTextDatum(application_name), CStringGetTextDatum(stmt_name)};
		bool is_null;
		MemoryContext call_ctx = CurrentMemoryContext;

		SPI_connect();
		rc = SPI_execute_with_args("select stmt_body,from_sql from neon_prepared_statements where client_id=$1 and stmt_name=$2",
								   2, param_types, param_values, NULL, true, 1);
		if (rc != SPI_OK_SELECT || SPI_processed != 1) {
			SPI_finish();
			elog(LOG, "Prepared statement %s not found for client %s", stmt_name, application_name);
			return NULL;
		}
		stmt_body = SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1);
		stmt_body = MemoryContextStrdup(call_ctx, stmt_body);
		*from_sql = DatumGetBool(SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 2, &is_null));
		SPI_finish();
	}
	return stmt_body;
}

static void
neon_save_prepared_statement(char const* stmt_name, char const* stmt_body, bool from_sql)
{
	if (save_parepared_statememts)
	{
		int rc;
		Oid param_types[4] = {TEXTOID, TEXTOID, TEXTOID, BOOLOID};
		Datum param_values[4] = {CStringGetTextDatum(application_name), CStringGetTextDatum(stmt_name), CStringGetTextDatum(stmt_body), BoolGetDatum(from_sql)};

		SPI_connect();
		rc = SPI_execute_with_args("insert into neon_prepared_statements values($1,$2,$3,$4) on conflict (client_id,stmt_name) do update set stmt_body=EXCLUDED.stmt_body, from_sql=EXCLUDED.from_sql",
								   4, param_types, param_values, NULL, false, 1);
		if (rc != SPI_OK_INSERT && rc != SPI_OK_UPDATE)
			elog(LOG, "Failed to persist prepared statement %s for client %s", stmt_name, application_name);
		SPI_finish();
	}
}

static bool
neon_drop_prepared_statement(char const* stmt_name)
{
	if (save_parepared_statememts)
	{
		int rc;
		Oid param_types[2] = {TEXTOID, TEXTOID};
		Datum param_values[2] = {CStringGetTextDatum(application_name), CStringGetTextDatum(stmt_name)};

		SPI_connect();
		rc = SPI_execute_with_args("delete from neon_prepared_statements where client_id=$1 and stmt_name=$2",
								   2, param_types, param_values, NULL, false, 1);
		if (rc != SPI_OK_DELETE || SPI_processed != 1) {
			SPI_finish();
			elog(LOG, "Prepared statement %s not found for client %s", stmt_name, application_name);
			return false;
		}
		SPI_finish();
		return true;
	}
	return false;
}

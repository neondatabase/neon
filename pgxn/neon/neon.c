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

#include <sys/stat.h>

#include "access/table.h"
#include "access/xact.h"
#include "access/xlog.h"
#include "commands/copy.h"
#include "nodes/makefuncs.h"
#include "nodes/value.h"
#include "storage/buf_internals.h"
#include "storage/bufmgr.h"
#include "catalog/pg_type.h"
#include "replication/walsender.h"
#include "funcapi.h"
#include "access/htup_details.h"
#include "utils/pg_lsn.h"
#include "utils/rel.h"
#include "utils/guc.h"

#include "neon.h"
#include "walproposer.h"

PG_MODULE_MAGIC;
void		_PG_init(void);

void
_PG_init(void)
{
	pg_init_libpagestore();
	pg_init_walproposer();

	EmitWarningsOnPlaceholders("neon");
}

PG_FUNCTION_INFO_V1(pg_cluster_size);
PG_FUNCTION_INFO_V1(backpressure_lsns);
PG_FUNCTION_INFO_V1(backpressure_throttling_time);
PG_FUNCTION_INFO_V1(read_postgres_log);

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


#define PG_LOG_DIR 			"log"
#define POSTGRES_LOG 		"postgres_log"
#define LOG_TABLE_N_COLUMS	26

typedef struct {
	char*  path;
	time_t ctime;
} LogFile;

typedef struct
{
	Relation    log_table;
	List*       log_files;
	CopyFromState copy_state;
	ListCell*   curr_log;
} LogfileContext;

static int cmp_log_ctime(const ListCell *a, const ListCell *b)
{
	LogFile* la = (LogFile*)lfirst(a);
	LogFile* lb = (LogFile*)lfirst(b);
	return la->ctime < lb->ctime ? -1 : la->ctime == lb->ctime ? 0 : 1;
}



Datum
read_postgres_log(PG_FUNCTION_ARGS)
{
	FuncCallContext *funcctx;
	MemoryContext oldcontext;
	LogfileContext *fctx;	/* User function context. */
	List*       log_files = NULL;

	if (SRF_IS_FIRSTCALL())
	{
		struct dirent *dent;
		DIR* dir;
		struct stat statbuf;
		char* path;

		funcctx = SRF_FIRSTCALL_INIT();

		/* Switch context when allocating stuff to be used in later calls */
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		/* Create a user function context for cross-call persistence */
		fctx = (LogfileContext *) palloc(sizeof(LogfileContext));
		fctx->log_files = NULL;
		fctx->copy_state = NULL;

		if ((dir = AllocateDir(PG_LOG_DIR)) != NULL)
		{
			while ((dent = ReadDirExtended(dir, PG_LOG_DIR, LOG)) != NULL)
			{
				/* Ignore non-csv files */
				if (strcmp(dent->d_name + strlen(dent->d_name) - 4, ".csv") != 0)
					continue;

				path = psprintf("%s/%s", PG_LOG_DIR, dent->d_name);
				if (stat(path, &statbuf) == 0)
				{
					LogFile* log = (LogFile*)palloc(sizeof(LogFile));
					log->ctime = statbuf.st_ctime;
					log->path = path;
					fctx->log_files = lappend(fctx->log_files, log);
				}
				else if (errno != ENOENT) /* file can be concurrently removed */
				{
					elog(LOG, "Failed to access log file %s", path);
					pfree(path);
				}
			}
			FreeDir(dir);
		}
		list_sort(fctx->log_files, cmp_log_ctime);
		fctx->log_table = table_openrv(makeRangeVar(NULL, POSTGRES_LOG, -1), AccessShareLock);
		fctx->curr_log = list_head(fctx->log_files);

		/* Remember the user function context. */
		funcctx->user_fctx = fctx;

		/* Return to original context when allocating transient memory */
		MemoryContextSwitchTo(oldcontext);
	}

	funcctx = SRF_PERCALL_SETUP();

	/* Get the saved state */
	fctx = funcctx->user_fctx;
	while (fctx->curr_log != NULL)
	{
		if (fctx->copy_state == NULL)
		{
			oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);
			fctx->copy_state = BeginCopyFrom(NULL,
											 fctx->log_table,
											 NULL,
											 ((LogFile*)lfirst(fctx->curr_log))->path,
											 false,
											 NULL,
											 NIL,
											 list_make1(makeDefElem("format", (Node *) makeString("csv"), -1)));
			MemoryContextSwitchTo(oldcontext);
		}
		if (fctx->copy_state != NULL)
		{
			Datum values[LOG_TABLE_N_COLUMS];
			bool nulls[LOG_TABLE_N_COLUMS];
			if (NextCopyFrom(fctx->copy_state, NULL,
							 values, nulls))
			{
				HeapTuple tuple = heap_form_tuple(RelationGetDescr(fctx->log_table), values, nulls);
				Datum result = HeapTupleGetDatum(tuple);
				SRF_RETURN_NEXT(funcctx, result);
			}
			EndCopyFrom(fctx->copy_state);
			fctx->copy_state = NULL;
		}
		fctx->curr_log = lnext(fctx->log_files, fctx->curr_log);
	}
	table_close(fctx->log_table, AccessShareLock);
	SRF_RETURN_DONE(funcctx);
}

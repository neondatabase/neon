/*-------------------------------------------------------------------------
 *
 * lfc_prewarm.c
 *		Functions related to LFC prewarming
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "bitmap.h"
#include "communicator.h"
#include "communicator_new.h"
#include "file_cache.h"
#include "lfc_prewarm.h"
#include "neon.h"
#include "pagestore_client.h"

#include "funcapi.h"
#include "miscadmin.h"
#include "postmaster/bgworker.h"
#include "storage/dsm.h"
#include "tcop/tcopprot.h"
#include "utils/timestamp.h"

#define MAX_PREWARM_WORKERS 8

typedef struct PrewarmWorkerState
{
	uint32		prewarmed_pages;
	uint32		skipped_pages;
	TimestampTz completed;
} PrewarmWorkerState;

typedef struct PrewarmControl
{
	/* -1 when not using workers, 0 when no prewarm has been performed */
	size_t		n_prewarm_workers;
	size_t		total_prewarm_pages;
	bool		prewarm_active;
	bool		prewarm_canceled;

	/* These are used in the non-worker mode */
	uint32		prewarmed_pages;
	uint32		skipped_pages;
	TimestampTz completed;

	/* These are used with workers */
	PrewarmWorkerState prewarm_workers[MAX_PREWARM_WORKERS];
	dsm_handle	prewarm_lfc_state_handle;
	size_t		prewarm_batch;
	size_t		n_prewarm_entries;
} PrewarmControl;

static PrewarmControl *prewarm_ctl;

static int	lfc_prewarm_limit;
static int	lfc_prewarm_batch;

static LWLockId prewarm_lock;

bool AmPrewarmWorker;

static void lfc_prewarm_with_workers(FileCacheState *fcs, uint32 n_workers);
static void lfc_prewarm_with_async_requests(FileCacheState *fcs);
PGDLLEXPORT void lfc_prewarm_main(Datum main_arg);

void
pg_init_prewarm(void)
{
	DefineCustomIntVariable("neon.file_cache_prewarm_limit",
							"Maximal number of prewarmed chunks",
							NULL,
							&lfc_prewarm_limit,
							INT_MAX,	/* no limit by default */
							0,
							INT_MAX,
							PGC_SIGHUP,
							0,
							NULL,
							NULL,
							NULL);

	DefineCustomIntVariable("neon.file_cache_prewarm_batch",
							"Number of pages retrivied by prewarm from page server",
							NULL,
							&lfc_prewarm_batch,
							64,
							1,
							INT_MAX,
							PGC_SIGHUP,
							0,
							NULL,
							NULL,
							NULL);
}

static size_t
PrewarmShmemSize(void)
{
	return sizeof(PrewarmControl);
}

void
PrewarmShmemRequest(void)
{
	RequestAddinShmemSpace(PrewarmShmemSize());
	RequestNamedLWLockTranche("prewarm_lock", 1);
}

void
PrewarmShmemInit(void)
{
	bool		found;

	prewarm_ctl = (PrewarmControl *) ShmemInitStruct("Prewarmer shmem state",
								PrewarmShmemSize(),
								&found);
	if (!found)
	{
		/* it's zeroed already */

		prewarm_lock = (LWLockId) GetNamedLWLockTranche("prewarm_lock");
	}
}

static void
validate_fcs(FileCacheState *fcs)
{
	size_t fcs_size;
#if 0
	size_t fcs_chunk_size_log;
#endif

	if (fcs->magic != FILE_CACHE_STATE_MAGIC)
	{
		elog(ERROR, "LFC: Invalid file cache state magic: %X", fcs->magic);
	}

	fcs_size = VARSIZE(fcs);
	if (FILE_CACHE_STATE_SIZE(fcs) != fcs_size)
	{
		elog(ERROR, "LFC: Invalid file cache state size: %u vs. %u", (unsigned)FILE_CACHE_STATE_SIZE(fcs), VARSIZE(fcs));
	}

	/* FIXME */
#if 0
	fcs_chunk_size_log = fcs->chunk_size_log;
	if (fcs_chunk_size_log > MAX_BLOCKS_PER_CHUNK_LOG)
	{
		elog(ERROR, "LFC: Invalid chunk size log: %u", fcs->chunk_size_log);
	}
#endif
}

/*
 * Prewarm LFC cache to the specified state. It uses lfc_prefetch function to
 * load prewarmed page without hoilding shared buffer lock and avoid race
 * conditions with other backends.
 */
void
lfc_prewarm_with_workers(FileCacheState *fcs, uint32 n_workers)
{
	size_t n_entries;
	size_t prewarm_batch = Min(lfc_prewarm_batch, readahead_buffer_size);
	size_t fcs_size = VARSIZE(fcs);
	dsm_segment *seg;
	BackgroundWorkerHandle* bgw_handle[MAX_PREWARM_WORKERS];

	Assert(!neon_use_communicator_worker);

	if (prewarm_batch == 0 || lfc_prewarm_limit == 0 || n_workers == 0)
	{
		elog(LOG, "LFC: prewarm is disabled");
		return;
	}

	if (n_workers > MAX_PREWARM_WORKERS)
	{
		elog(ERROR, "LFC: too many prewarm workers, maximum is %d", MAX_PREWARM_WORKERS);
	}

	if (fcs == NULL || fcs->n_chunks == 0)
	{
		elog(LOG, "LFC: nothing to prewarm");
		return;
	}

	n_entries = Min(fcs->n_chunks, lfc_prewarm_limit);
	Assert(n_entries != 0);

	LWLockAcquire(prewarm_lock, LW_EXCLUSIVE);

	/* Do not prewarm more entries than LFC limit */
	/* FIXME */
#if 0
	if (prewarm_ctl->limit <= prewarm_ctl->size)
	{
		elog(LOG, "LFC: skip prewarm because LFC is already filled");
		LWLockRelease(prewarm_lock);
		return;
	}
#endif
	
	if (prewarm_ctl->prewarm_active)
	{
		LWLockRelease(prewarm_lock);
		elog(ERROR, "LFC: skip prewarm because another prewarm is still active");
	}
	prewarm_ctl->n_prewarm_entries = n_entries;
	prewarm_ctl->n_prewarm_workers = n_workers;
	prewarm_ctl->prewarm_active = true;
	prewarm_ctl->prewarm_canceled = false;
	prewarm_ctl->prewarm_batch = prewarm_batch;
	memset(prewarm_ctl->prewarm_workers, 0, n_workers*sizeof(PrewarmWorkerState));

	/* Calculate total number of pages to be prewarmed */
	prewarm_ctl->total_prewarm_pages = fcs->n_pages;

	LWLockRelease(prewarm_lock);

	seg = dsm_create(fcs_size, 0);
	memcpy(dsm_segment_address(seg), fcs, fcs_size);
	prewarm_ctl->prewarm_lfc_state_handle = dsm_segment_handle(seg);

	/* Spawn background workers */
	for (uint32 i = 0; i < n_workers; i++)
	{
		BackgroundWorker worker = {0};

		worker.bgw_flags = BGWORKER_SHMEM_ACCESS;
		worker.bgw_start_time = BgWorkerStart_ConsistentState;
		worker.bgw_restart_time = BGW_NEVER_RESTART;
		strcpy(worker.bgw_library_name, "neon");
		strcpy(worker.bgw_function_name, "lfc_prewarm_main");
		snprintf(worker.bgw_name, BGW_MAXLEN, "LFC prewarm worker %d", i+1);
		strcpy(worker.bgw_type, "LFC prewarm worker");
		worker.bgw_main_arg = Int32GetDatum(i);
		/* must set notify PID to wait for shutdown */
		worker.bgw_notify_pid = MyProcPid;

		if (!RegisterDynamicBackgroundWorker(&worker, &bgw_handle[i]))
		{
			ereport(LOG,
					(errcode(ERRCODE_INSUFFICIENT_RESOURCES),
					 errmsg("LFC: registering dynamic bgworker prewarm failed"),
					 errhint("Consider increasing the configuration parameter \"%s\".", "max_worker_processes")));
			n_workers = i;
			prewarm_ctl->prewarm_canceled = true;
			break;
		}
	}

	for (uint32 i = 0; i < n_workers; i++)
	{
		bool interrupted;
		do
		{
			interrupted = false;
			PG_TRY();
			{
				BgwHandleStatus status = WaitForBackgroundWorkerShutdown(bgw_handle[i]);
				if (status != BGWH_STOPPED && status != BGWH_POSTMASTER_DIED)
				{
					elog(LOG, "LFC: Unexpected status of prewarm worker termination: %d", status);
				}
			}
			PG_CATCH();
			{
				elog(LOG, "LFC: cancel prewarm");
				prewarm_ctl->prewarm_canceled = true;
				interrupted = true;
			}
			PG_END_TRY();
		} while (interrupted);

		if (!prewarm_ctl->prewarm_workers[i].completed)
		{
			/* Background worker doesn't set completion time: it means that it was abnormally terminated */
			elog(LOG, "LFC: prewarm worker %d failed", i+1);
			/* Set completion time to prevent get_prewarm_info from considering this worker as active */
			prewarm_ctl->prewarm_workers[i].completed = GetCurrentTimestamp();
		}
	}
	dsm_detach(seg);

	LWLockAcquire(prewarm_lock, LW_EXCLUSIVE);
	prewarm_ctl->prewarm_active = false;
	LWLockRelease(prewarm_lock);
}


void
lfc_prewarm_main(Datum main_arg)
{
	size_t snd_idx = 0, rcv_idx = 0;
	size_t n_sent = 0, n_received = 0;
	size_t fcs_chunk_size_log;
	size_t max_prefetch_pages;
	size_t prewarm_batch;
	size_t n_workers;
	dsm_segment *seg;
	FileCacheState* fcs;
	uint8* bitmap;
	BufferTag tag;
	PrewarmWorkerState* ws;
	uint32 worker_id = DatumGetInt32(main_arg);

	Assert(!neon_use_communicator_worker);

	AmPrewarmWorker = true;

	pqsignal(SIGTERM, die);
	BackgroundWorkerUnblockSignals();

	seg = dsm_attach(prewarm_ctl->prewarm_lfc_state_handle);
	if (seg == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("could not map dynamic shared memory segment")));

	fcs = (FileCacheState*) dsm_segment_address(seg);
	prewarm_batch = prewarm_ctl->prewarm_batch;
	fcs_chunk_size_log = fcs->chunk_size_log;
	n_workers = prewarm_ctl->n_prewarm_workers;
	max_prefetch_pages = prewarm_ctl->n_prewarm_entries << fcs_chunk_size_log;
	ws = &prewarm_ctl->prewarm_workers[worker_id];
	bitmap = FILE_CACHE_STATE_BITMAP(fcs);

	/* enable prefetch in LFC */
	lfc_store_prefetch_result = true;
	lfc_do_prewarm = true; /* Flag for lfc_prefetch preventing replacement of existed entries if LFC cache is full */

	elog(LOG, "LFC: worker %d start prewarming", worker_id);
	while (!prewarm_ctl->prewarm_canceled)
	{
		if (snd_idx < max_prefetch_pages)
		{
			if ((snd_idx >> fcs_chunk_size_log) % n_workers != worker_id)
			{
				/* If there are multiple workers, split chunks between them */
				snd_idx += 1 << fcs_chunk_size_log;
			}
			else
			{
				if (BITMAP_ISSET(bitmap, snd_idx))
				{
					tag = fcs->chunks[snd_idx >> fcs_chunk_size_log];
					tag.blockNum += snd_idx & ((1 << fcs_chunk_size_log) - 1);
					if (!lfc_cache_contains(BufTagGetNRelFileInfo(tag), tag.forkNum, tag.blockNum))
					{
						(void) communicator_prefetch_register_bufferv(tag, NULL, 1, NULL);
						n_sent += 1;
					}
					else
					{
						ws->skipped_pages += 1;
						BITMAP_CLR(bitmap, snd_idx);
					}
				}
				snd_idx += 1;
			}
		}
		if (n_sent >= n_received + prewarm_batch || snd_idx == max_prefetch_pages)
		{
			if (n_received == n_sent && snd_idx == max_prefetch_pages)
			{
				break;
			}
			if ((rcv_idx >> fcs_chunk_size_log) % n_workers != worker_id)
			{
				/* Skip chunks processed by other workers */
				rcv_idx += 1 << fcs_chunk_size_log;
				continue;
			}

			/* Locate next block to prefetch */
			while (!BITMAP_ISSET(bitmap, rcv_idx))
			{
				rcv_idx += 1;
			}
			tag = fcs->chunks[rcv_idx >> fcs_chunk_size_log];
			tag.blockNum += rcv_idx & ((1 << fcs_chunk_size_log) - 1);
			if (communicator_prefetch_receive(tag))
			{
				ws->prewarmed_pages += 1;
			}
			else
			{
				ws->skipped_pages += 1;
			}
			rcv_idx += 1;
			n_received += 1;
		}
	}
	/* No need to perform prefetch cleanup here because prewarm worker will be terminated and
	 * connection to PS dropped just after return from this function.
	 */
	Assert(n_sent == n_received || prewarm_ctl->prewarm_canceled);
	elog(LOG, "LFC: worker %d complete prewarming: loaded %ld pages", worker_id, (long)n_received);
	prewarm_ctl->prewarm_workers[worker_id].completed = GetCurrentTimestamp();
}

/*
 * Prewarm LFC cache to the specified state. Uses the new communicator
 *
 * FIXME: Is there a race condition because we're not holding Postgres
 * buffer manager locks?
 */
static void
lfc_prewarm_with_async_requests(FileCacheState *fcs)
{
	size_t n_entries;
	uint8	   *bitmap;
	uint64		bitno;
	int			blocks_per_chunk;

	Assert(neon_use_communicator_worker);

	if (lfc_prewarm_limit == 0)
	{
		elog(LOG, "LFC: prewarm is disabled");
		return;
	}

	if (fcs == NULL || fcs->n_chunks == 0)
	{
		elog(LOG, "LFC: nothing to prewarm");
		return;
	}

	n_entries = Min(fcs->n_chunks, lfc_prewarm_limit);
	Assert(n_entries != 0);

	LWLockAcquire(prewarm_lock, LW_EXCLUSIVE);

	/* Do not prewarm more entries than LFC limit */
	/* FIXME */
#if 0
	if (prewarm_ctl->limit <= prewarm_ctl->size)
	{
		elog(LOG, "LFC: skip prewarm because LFC is already filled");
		LWLockRelease(prewarm_lock);
		return;
	}
#endif

	if (prewarm_ctl->prewarm_active)
	{
		LWLockRelease(prewarm_lock);
		elog(ERROR, "LFC: skip prewarm because another prewarm is still active");
	}
	prewarm_ctl->n_prewarm_entries = n_entries;
	prewarm_ctl->n_prewarm_workers = -1;
	prewarm_ctl->prewarm_active = true;
	prewarm_ctl->prewarm_canceled = false;

	/* Calculate total number of pages to be prewarmed */
	prewarm_ctl->total_prewarm_pages = fcs->n_pages;

	LWLockRelease(prewarm_lock);

	elog(LOG, "LFC: start prewarming");
	lfc_do_prewarm = true;
	lfc_prewarm_cancel = false;

	bitmap = FILE_CACHE_STATE_BITMAP(fcs);

	blocks_per_chunk = 1 << fcs->chunk_size_log;

	bitno = 0;
	for (uint32 chunkno = 0; chunkno < fcs->n_chunks; chunkno++)
	{
		BufferTag *chunk_tag = &fcs->chunks[chunkno];
		BlockNumber request_startblkno = InvalidBlockNumber;
		BlockNumber request_endblkno;

		if (lfc_prewarm_cancel)
		{
			prewarm_ctl->prewarm_canceled = true;
			break;
		}

		/* take next chunk */
		for (int j = 0; j < blocks_per_chunk; j++)
		{
			BlockNumber blkno = chunk_tag->blockNum + j;

			if (BITMAP_ISSET(bitmap, bitno))
			{
				if (request_startblkno != InvalidBlockNumber)
				{
					if (request_endblkno == blkno)
					{
						/* append this block to the request */
						request_endblkno++;
					}
					else
					{
						/* flush this request, and start new one */
						communicator_new_prefetch_register_bufferv(
							BufTagGetNRelFileInfo(*chunk_tag),
							chunk_tag->forkNum,
							request_startblkno,
							request_endblkno - request_startblkno
							);
						request_startblkno = blkno;
						request_endblkno = blkno + 1;
					}
				}
				else
				{
					/* flush this request, if any, and start new one */
					if (request_startblkno != InvalidBlockNumber)
					{
						communicator_new_prefetch_register_bufferv(
							BufTagGetNRelFileInfo(*chunk_tag),
							chunk_tag->forkNum,
							request_startblkno,
							request_endblkno - request_startblkno
							);
					}
					request_startblkno = blkno;
					request_endblkno = blkno + 1;
				}
				prewarm_ctl->prewarmed_pages += 1;
			}
			bitno++;
		}

		/* flush this request */
		communicator_new_prefetch_register_bufferv(
			BufTagGetNRelFileInfo(*chunk_tag),
			chunk_tag->forkNum,
			request_startblkno,
			request_endblkno - request_startblkno
			);
		request_startblkno = request_endblkno = InvalidBlockNumber;
	}

	Assert(n_sent == n_received || prewarm_ctl->prewarm_canceled);
	elog(LOG, "LFC: complete prewarming: loaded %lu pages", (unsigned long) prewarm_ctl->prewarmed_pages);
	prewarm_ctl->completed = GetCurrentTimestamp();

	LWLockAcquire(prewarm_lock, LW_EXCLUSIVE);
	prewarm_ctl->prewarm_active = false;
	LWLockRelease(prewarm_lock);
}

PG_FUNCTION_INFO_V1(get_local_cache_state);

Datum
get_local_cache_state(PG_FUNCTION_ARGS)
{
	size_t max_entries = PG_ARGISNULL(0) ? lfc_prewarm_limit : PG_GETARG_INT32(0);
	FileCacheState* fcs;

	if (neon_use_communicator_worker)
		fcs = communicator_new_get_lfc_state(max_entries);
	else
		fcs = lfc_get_state(max_entries);

	if (fcs != NULL)
		PG_RETURN_BYTEA_P((bytea*)fcs);
	else
		PG_RETURN_NULL();
}

PG_FUNCTION_INFO_V1(prewarm_local_cache);

Datum
prewarm_local_cache(PG_FUNCTION_ARGS)
{
	bytea* state = PG_GETARG_BYTEA_PP(0);
	uint32 n_workers =  PG_GETARG_INT32(1);
	FileCacheState* fcs;

	fcs = (FileCacheState *)state;
	validate_fcs(fcs);

	if (neon_use_communicator_worker)
		lfc_prewarm_with_async_requests(fcs);
	else
		lfc_prewarm_with_workers(fcs, n_workers);

	PG_RETURN_NULL();
}

PG_FUNCTION_INFO_V1(get_prewarm_info);

Datum
get_prewarm_info(PG_FUNCTION_ARGS)
{
	Datum		values[4];
	bool		nulls[4];
	TupleDesc	tupdesc;
	uint32		prewarmed_pages = 0;
	uint32		skipped_pages = 0;
	uint32		active_workers = 0;
	uint32		total_pages;

	if (lfc_size_limit == 0)
		PG_RETURN_NULL();

	LWLockAcquire(prewarm_lock, LW_SHARED);
	if (!prewarm_ctl || prewarm_ctl->n_prewarm_workers == 0)
	{
		LWLockRelease(prewarm_lock);
		PG_RETURN_NULL();
	}

	if (prewarm_ctl->n_prewarm_workers == -1)
	{
		total_pages = prewarm_ctl->total_prewarm_pages;
		prewarmed_pages = prewarm_ctl->prewarmed_pages;
		skipped_pages = prewarm_ctl->prewarmed_pages;
		active_workers = 1;
	}
	else
	{
		size_t		n_workers;

		n_workers = prewarm_ctl->n_prewarm_workers;
		total_pages = prewarm_ctl->total_prewarm_pages;
		for (size_t i = 0; i < n_workers; i++)
		{
			PrewarmWorkerState *ws = &prewarm_ctl->prewarm_workers[i];

			prewarmed_pages += ws->prewarmed_pages;
			skipped_pages += ws->skipped_pages;
			active_workers += ws->completed != 0;
		}
	}
	LWLockRelease(prewarm_lock);

	tupdesc = CreateTemplateTupleDesc(4);
	TupleDescInitEntry(tupdesc, (AttrNumber) 1, "total_pages", INT4OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 2, "prewarmed_pages", INT4OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 3, "skipped_pages", INT4OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 4, "active_workers", INT4OID, -1, 0);
	tupdesc = BlessTupleDesc(tupdesc);

	MemSet(nulls, 0, sizeof(nulls));

	values[0] = Int32GetDatum(total_pages);
	values[1] = Int32GetDatum(prewarmed_pages);
	values[2] = Int32GetDatum(skipped_pages);
	values[3] = Int32GetDatum(active_workers);

	PG_RETURN_DATUM(HeapTupleGetDatum(heap_form_tuple(tupdesc, values, nulls)));
}

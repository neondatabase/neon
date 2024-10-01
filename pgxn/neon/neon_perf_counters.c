/*-------------------------------------------------------------------------
 *
 * neon_perf_counters.c
 *	  Collect statistics about Neon I/O
 *
 * Each backend has its own set of counters in shared memory.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <math.h>

#include "funcapi.h"
#include "miscadmin.h"
#include "storage/proc.h"
#include "storage/shmem.h"
#include "utils/builtins.h"

#include "neon_perf_counters.h"
#include "neon_pgversioncompat.h"

neon_per_backend_counters *neon_per_backend_counters_shared;

Size
NeonPerfCountersShmemSize(void)
{
	Size		size = 0;

	size = add_size(size, mul_size(MaxBackends, sizeof(neon_per_backend_counters)));

	return size;
}

void
NeonPerfCountersShmemInit(void)
{
	bool		found;

	neon_per_backend_counters_shared =
		ShmemInitStruct("Neon perf counters",
						mul_size(MaxBackends,
								 sizeof(neon_per_backend_counters)),
						&found);
	Assert(found == IsUnderPostmaster);
	if (!found)
	{
		/* shared memory is initialized to zeros, so nothing to do here */
	}
}

/*
 * Count a GetPage wait operation.
 */
void
inc_getpage_wait(uint64 latency_us)
{
	int			lo = 0;
	int			hi = NUM_GETPAGE_WAIT_BUCKETS - 1;

	/* Find the right bucket with binary search */
	while (lo < hi)
	{
		int			mid = (lo + hi) / 2;

		if (latency_us < getpage_wait_bucket_thresholds[mid])
			hi = mid;
		else
			lo = mid + 1;
	}
	MyNeonCounters->getpage_wait_us_bucket[lo]++;
	MyNeonCounters->getpage_wait_us_sum += latency_us;
	MyNeonCounters->getpage_wait_us_count++;
}

/*
 * Support functions for the views, neon_backend_perf_counters and
 * neon_perf_counters.
 */

typedef struct
{
	char	   *name;
	bool		is_bucket;
	double		bucket_le;
	double		value;
} metric_t;

static metric_t *
neon_perf_counters_to_metrics(neon_per_backend_counters *counters)
{
#define NUM_METRICS (2 + NUM_GETPAGE_WAIT_BUCKETS + 8)
	metric_t   *metrics = palloc((NUM_METRICS + 1) * sizeof(metric_t));
	uint64		bucket_accum;
	int			i = 0;
	Datum		getpage_wait_str;

	metrics[i].name = "getpage_wait_seconds_count";
	metrics[i].is_bucket = false;
	metrics[i].value = (double) counters->getpage_wait_us_count;
	i++;
	metrics[i].name = "getpage_wait_seconds_sum";
	metrics[i].is_bucket = false;
	metrics[i].value = ((double) counters->getpage_wait_us_sum) / 1000000.0;
	i++;

	bucket_accum = 0;
	for (int bucketno = 0; bucketno < NUM_GETPAGE_WAIT_BUCKETS; bucketno++)
	{
		uint64		threshold = getpage_wait_bucket_thresholds[bucketno];

		bucket_accum += counters->getpage_wait_us_bucket[bucketno];

		metrics[i].name = "getpage_wait_seconds_bucket";
		metrics[i].is_bucket = true;
		metrics[i].bucket_le = (threshold == UINT64_MAX) ? INFINITY : ((double) threshold) / 1000000.0;
		metrics[i].value = (double) bucket_accum;
		i++;
	}
	metrics[i].name = "getpage_prefetch_requests_total";
	metrics[i].is_bucket = false;
	metrics[i].value = (double) counters->getpage_prefetch_requests_total;
	i++;
	metrics[i].name = "getpage_sync_requests_total";
	metrics[i].is_bucket = false;
	metrics[i].value = (double) counters->getpage_sync_requests_total;
	i++;
	metrics[i].name = "getpage_prefetch_misses_total";
	metrics[i].is_bucket = false;
	metrics[i].value = (double) counters->getpage_prefetch_misses_total;
	i++;
	metrics[i].name = "getpage_prefetch_discards_total";
	metrics[i].is_bucket = false;
	metrics[i].value = (double) counters->getpage_prefetch_discards_total;
	i++;
	metrics[i].name = "pageserver_requests_sent_total";
	metrics[i].is_bucket = false;
	metrics[i].value = (double) counters->pageserver_requests_sent_total;
	i++;
	metrics[i].name = "pageserver_disconnects_total";
	metrics[i].is_bucket = false;
	metrics[i].value = (double) counters->pageserver_disconnects_total;
	i++;
	metrics[i].name = "pageserver_send_flushes_total";
	metrics[i].is_bucket = false;
	metrics[i].value = (double) counters->pageserver_send_flushes_total;
	i++;
	metrics[i].name = "file_cache_hits_total";
	metrics[i].is_bucket = false;
	metrics[i].value = (double) counters->file_cache_hits_total;
	i++;

	Assert(i == NUM_METRICS);

	/* NULL entry marks end of array */
	metrics[i].name = NULL;
	metrics[i].value = 0;

	return metrics;
}

/*
 * Write metric to three output Datums
 */
static void
metric_to_datums(metric_t *m, Datum *values, bool *nulls)
{
	values[0] = CStringGetTextDatum(m->name);
	nulls[0] = false;
	if (m->is_bucket)
	{
		values[1] = Float8GetDatum(m->bucket_le);
		nulls[1] = false;
	}
	else
	{
		values[1] = (Datum) 0;
		nulls[1] = true;
	}
	values[2] = Float8GetDatum(m->value);
	nulls[2] = false;
}

PG_FUNCTION_INFO_V1(neon_get_backend_perf_counters);
Datum
neon_get_backend_perf_counters(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	Datum		values[5];
	bool		nulls[5];

	/* We put all the tuples into a tuplestore in one go. */
	InitMaterializedSRF(fcinfo, 0);

	for (int procno = 0; procno < MaxBackends; procno++)
	{
		PGPROC	   *proc = GetPGProcByNumber(procno);
		int			pid = proc->pid;
		neon_per_backend_counters *counters = &neon_per_backend_counters_shared[procno];
		metric_t   *metrics = neon_perf_counters_to_metrics(counters);

		values[0] = Int32GetDatum(procno);
		nulls[0] = false;
		values[1] = Int32GetDatum(pid);
		nulls[1] = false;

		for (int i = 0; metrics[i].name != NULL; i++)
		{
			metric_to_datums(&metrics[i], &values[2], &nulls[2]);
			tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
		}

		pfree(metrics);
	}

	return (Datum) 0;
}

PG_FUNCTION_INFO_V1(neon_get_perf_counters);
Datum
neon_get_perf_counters(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	Datum		values[3];
	bool		nulls[3];
	Datum		getpage_wait_str;
	neon_per_backend_counters totals = {0};
	metric_t   *metrics;

	/* We put all the tuples into a tuplestore in one go. */
	InitMaterializedSRF(fcinfo, 0);

	/* Aggregate the counters across all backends */
	for (int procno = 0; procno < MaxBackends; procno++)
	{
		neon_per_backend_counters *counters = &neon_per_backend_counters_shared[procno];

		totals.getpage_wait_us_count += counters->getpage_wait_us_count;
		totals.getpage_wait_us_sum += counters->getpage_wait_us_sum;
		for (int bucketno = 0; bucketno < NUM_GETPAGE_WAIT_BUCKETS; bucketno++)
			totals.getpage_wait_us_bucket[bucketno] += counters->getpage_wait_us_bucket[bucketno];
		totals.getpage_prefetch_requests_total += counters->getpage_prefetch_requests_total;
		totals.getpage_sync_requests_total += counters->getpage_sync_requests_total;
		totals.getpage_prefetch_misses_total += counters->getpage_prefetch_misses_total;
		totals.getpage_prefetch_discards_total += counters->getpage_prefetch_discards_total;
		totals.pageserver_requests_sent_total += counters->pageserver_requests_sent_total;
		totals.pageserver_disconnects_total += counters->pageserver_disconnects_total;
		totals.pageserver_send_flushes_total += counters->pageserver_send_flushes_total;
		totals.file_cache_hits_total += counters->file_cache_hits_total;
	}

	metrics = neon_perf_counters_to_metrics(&totals);
	for (int i = 0; metrics[i].name != NULL; i++)
	{
		metric_to_datums(&metrics[i], &values[0], &nulls[0]);
		tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
	}
	pfree(metrics);

	return (Datum) 0;
}

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

bool
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
	double		value;
} metric_t;

static metric_t *
neon_perf_counters_to_metrics(neon_per_backend_counters *counters)
{
#define NUM_METRICS (2 + NUM_GETPAGE_WAIT_BUCKETS + 8)
	metric_t   *metrics = palloc((NUM_METRICS + 1) * sizeof(metric_t));
	uint64		bucket_accum;
	int			i = 0;

	metrics[i].name = "getpage_wait_seconds_count";
	metrics[i].value = (double) counters->getpage_wait_us_count;
	i++;
	metrics[i].name = "getpage_wait_seconds_sum";
	metrics[i].value = ((double) counters->getpage_wait_us_sum) / 1000000.0;
	i++;

	bucket_accum = 0;
	for (int bucketno = 0; bucketno < NUM_GETPAGE_WAIT_BUCKETS; bucketno++)
	{
		uint64		threshold = getpage_wait_bucket_thresholds[bucketno];

		if (threshold == UINT64_MAX)
			metrics[i].name = "getpage_wait_seconds_bucket{le=\"+Inf\"}";
		else if (threshold >= 1000000)
		{
			Assert(threshold % 1000000 == 0);
			metrics[i].name = psprintf("getpage_wait_seconds_bucket{le=\"%u\"}", threshold / 1000000);
		}
		else if (threshold >= 1000)
		{
			Assert(threshold % 1000 == 0);
			metrics[i].name = psprintf("getpage_wait_seconds_bucket{le=\"0.%03u\"}", threshold / 1000);
		}
		else
			metrics[i].name = psprintf("getpage_wait_seconds_bucket{le=\"0.%06u\"}", threshold);

		bucket_accum += counters->getpage_wait_us_bucket[bucketno];
		metrics[i].value = (double) bucket_accum;
		i++;
	}
	metrics[i].name = "prefetch_requests_total";
	metrics[i].value = (double) counters->prefetch_requests_total;
	i++;
	metrics[i].name = "sync_requests_total";
	metrics[i].value = (double) counters->sync_requests_total;
	i++;
	metrics[i].name = "pageserver_requests_sent_total";
	metrics[i].value = (double) counters->pageserver_requests_sent_total;
	i++;
	metrics[i].name = "pageserver_requests_disconnects_total";
	metrics[i].value = (double) counters->pageserver_disconnects_total;
	i++;
	metrics[i].name = "pageserver_send_flushes_total";
	metrics[i].value = (double) counters->pageserver_send_flushes_total;
	i++;
	metrics[i].name = "prefetch_misses_total";
	metrics[i].value = (double) counters->prefetch_misses_total;
	i++;
	metrics[i].name = "prefetch_discards_total";
	metrics[i].value = (double) counters->prefetch_discards_total;
	i++;
	metrics[i].name = "file_cache_hits_total";
	metrics[i].value = (double) counters->file_cache_hits_total;
	i++;

	Assert(i == NUM_METRICS);

	/* NULL entry marks end of array */
	metrics[i].name = NULL;
	metrics[i].value = 0;

	return metrics;
}

PG_FUNCTION_INFO_V1(neon_get_backend_perf_counters);
Datum
neon_get_backend_perf_counters(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	Datum		values[4];
	bool		nulls[4];
	Datum		getpage_wait_str;

	/* We put all the tuples into a tuplestore in one go. */
	InitMaterializedSRF(fcinfo, 0);

	getpage_wait_str = CStringGetTextDatum("getpage_wait_us_bucket");
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
			values[2] = CStringGetTextDatum(metrics[i].name);
			nulls[2] = false;
			values[3] = Float8GetDatum(metrics[i].value);
			nulls[3] = false;
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
	Datum		values[2];
	bool		nulls[2];
	Datum		getpage_wait_str;
	neon_per_backend_counters totals = { 0 };
	metric_t   *metrics;

	/* We put all the tuples into a tuplestore in one go. */
	InitMaterializedSRF(fcinfo, 0);

	/* Aggregate the counters across all backends */
	getpage_wait_str = CStringGetTextDatum("getpage_wait_us_bucket");
	for (int procno = 0; procno < MaxBackends; procno++)
	{
		neon_per_backend_counters *counters = &neon_per_backend_counters_shared[procno];

		totals.getpage_wait_us_count += counters->getpage_wait_us_count;
		totals.getpage_wait_us_sum += counters->getpage_wait_us_sum;
		for (int bucketno = 0; bucketno < NUM_GETPAGE_WAIT_BUCKETS; bucketno++)
			totals.getpage_wait_us_bucket[bucketno] += counters->getpage_wait_us_bucket[bucketno];
		totals.prefetch_requests_total += counters->prefetch_requests_total;
		totals.sync_requests_total += counters->sync_requests_total;
		totals.pageserver_requests_sent_total += counters->pageserver_requests_sent_total;
		totals.pageserver_disconnects_total += counters->pageserver_disconnects_total;
		totals.pageserver_send_flushes_total += counters->pageserver_send_flushes_total;
		totals.prefetch_misses_total += counters->prefetch_misses_total;
		totals.prefetch_discards_total += counters->prefetch_discards_total;
		totals.file_cache_hits_total += counters->file_cache_hits_total;
	}

	metrics = neon_perf_counters_to_metrics(&totals);
	for (int i = 0; metrics[i].name != NULL; i++)
	{
		values[0] = CStringGetTextDatum(metrics[i].name);
		nulls[0] = false;
		values[1] = Float8GetDatum(metrics[i].value);
		nulls[1] = false;
		tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
	}
	pfree(metrics);

	return (Datum) 0;
}

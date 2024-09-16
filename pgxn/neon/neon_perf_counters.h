/*-------------------------------------------------------------------------
 *
 * neon_perf_counters.h
 *	  Performance counters for neon storage requests
 *-------------------------------------------------------------------------
 */

#ifndef NEON_PERF_COUNTERS_H
#define NEON_PERF_COUNTERS_H

#if PG_VERSION_NUM >= 170000
#include "storage/procnumber.h"
#else
#include "storage/backendid.h"
#include "storage/proc.h"
#endif

static const uint64 getpage_wait_bucket_thresholds[] = {
	      20,       30,       60,       100,  /* 0      -  100 us */
	     200,      300,      600,	   1000,  /* 100 us - 1 ms */
	    2000,     3000,     6000,     10000,  /* 1 ms   - 10 ms */
	   20000,    30000,    60000,    100000,  /* 10 ms  - 100 ms */
	  200000,   300000,   600000,   1000000,  /* 100 ms - 1 s */
	 2000000,  3000000,  6000000,  10000000,  /* 1 s - 10 s */
    20000000, 30000000, 60000000, 100000000,  /* 10 s - 100 s */
	UINT64_MAX,
};
#define NUM_GETPAGE_WAIT_BUCKETS (lengthof(getpage_wait_bucket_thresholds))

typedef struct
{
	/*
	 * Histogram for how long an smgrread() request needs to wait for response
	 * from pageserver. When prefetching is effective, these wait times can be
	 * lower than the network latency to the pageserver, even zero, if the
	 * page is already readily prefetched whenever we need to read a page.
	 *
	 * Note: we accumulate these in microseconds, because that's convenient in
	 * the backend, but the 'neon_backend_perf_counters' view will convert
	 * them to seconds, to make them more idiomatic as prometheus metrics.
	 */
	uint64		getpage_wait_us_count;
	uint64		getpage_wait_us_sum;
	uint64		getpage_wait_us_bucket[NUM_GETPAGE_WAIT_BUCKETS];

	/*
	 * Total number of speculative prefetch Getpage requests and synchronous
	 * GetPage requests sent.
	 */
	uint64		prefetch_requests_total;
	uint64		sync_requests_total;

	/*
	 * Total number of requests send to pageserver. (prefetch_requests_total
	 * and sync_request_total count only GetPage requests, this counts all
	 * request types.)
	 */
	uint64		pageserver_requests_sent_total;

	/*
	 * Number of times the connection to the pageserver was lost and the
	 * backend had to reconnect. Note that this doesn't count the first
	 * connection in each backend, only reconnects.
	 */
	uint64		pageserver_disconnects_total;

	/*
	 * Number of network flushes to the pageserver. Synchronous requests are
	 * flushed immediately, but when prefetching requests are sent in batches,
	 * this can be smaller than pageserver_requests_sent_total.
	 */
	uint64		pageserver_send_flushes_total;

	/* XXX: It's not clear to me when these misses happen. */
	uint64		prefetch_misses_total;

	/*
	 * Number of prefetched responses that were discarded becuase the
	 * prefetched page was not needed or because it was concurrently fetched /
	 * modified by another backend.
	 */
	uint64		prefetch_discards_total;

	/*
	 * Number of requests satisfied from the LFC.
	 *
	 * This is redundant with the server-wide file_cache_hits, but this gives
	 * per-backend granularity, and it's handy to have this in the same place
	 * as counters for requests that went to the pageserver. Maybe move all
	 * the LFC stats to this struct in the future?
	 */
	uint64		file_cache_hits_total;

} neon_per_backend_counters;

/* Pointer to the shared memory array of neon_per_backend_counters structs */
extern neon_per_backend_counters *neon_per_backend_counters_shared;

#if PG_VERSION_NUM >= 170000
#define MyNeonCounters (&neon_per_backend_counters_shared[MyProcNumber])
#else
#define MyNeonCounters (&neon_per_backend_counters_shared[MyProc->pgprocno])
#endif

extern void inc_getpage_wait(uint64 latency);

extern Size NeonPerfCountersShmemSize(void);
extern bool NeonPerfCountersShmemInit(void);


#endif							/* NEON_PERF_COUNTERS_H */

{
  metric_name: 'getpage_prefetch_misses_total',
  type: 'counter',
  help: "Total number of readahead misses; consisting of either prefetches that don't satisfy the LSN bounds once the prefetch got read by the backend, or cases where somehow no readahead was issued for the read",
  values: [
    'getpage_prefetch_misses_total',
  ],
  query_ref: 'neon_perf_counters',
}

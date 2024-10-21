{
  metric_name: 'pageserver_open_requests',
  type: 'gauge',
  help: 'Number of open requests to PageServer',
  values: [
    'pageserver_open_requests',
  ],
  query_ref: 'neon_perf_counters',
}

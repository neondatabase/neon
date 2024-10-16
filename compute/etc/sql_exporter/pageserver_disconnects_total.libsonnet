{
  metric_name: 'pageserver_disconnects_total',
  type: 'counter',
  help: 'Number of times that the connection to the pageserver was lost',
  values: [
    'pageserver_disconnects_total',
  ],
  query_ref: 'neon_perf_counters',
}

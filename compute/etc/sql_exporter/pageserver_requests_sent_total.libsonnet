{
  metric_name: 'pageserver_requests_sent_total',
  type: 'counter',
  help: 'Number of all requests sent to the pageserver (not just GetPage requests)',
  values: [
    'pageserver_requests_sent_total',
  ],
  query_ref: 'neon_perf_counters',
}

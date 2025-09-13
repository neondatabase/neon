{
  metric_name: 'compute_getpage_stuck_requests_total',
  type: 'counter',
  help: 'Total number of Getpage requests left without an answer for more than pageserver_response_log_timeout but less than pageserver_response_disconnect_timeout',
  values: [
    'compute_getpage_stuck_requests_total',
  ],
  query_ref: 'neon_perf_counters',
}

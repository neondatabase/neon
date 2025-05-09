{
  metric_name: 'compute_getpage_max_inflight_stuck_time_ms',
  type: 'gauge',
  help: 'Max wait time for stuck requests among all backends. Includes only active stuck requests, terminated or disconnected ones are not accounted for',
  values: [
    'compute_getpage_max_inflight_stuck_time_ms',
  ],
  query_ref: 'neon_perf_counters',
}

{
  metric_name: 'compute_backpressure_throttling_ms',
  type: 'gauge',
  help: 'Time compute has spent throttled',
  key_labels: null,
  values: [
    'throttled',
  ],
  query: importstr 'sql_exporter/compute_backpressure_throttling_ms.sql',
}

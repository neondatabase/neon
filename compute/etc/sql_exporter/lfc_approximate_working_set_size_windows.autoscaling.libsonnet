{
  metric_name: 'lfc_approximate_working_set_size_windows',
  type: 'gauge',
  help: 'Approximate working set size in pages of 8192 bytes',
  key_labels: [
    'duration_seconds',
  ],
  values: [
    'size',
  ],
  query: importstr 'sql_exporter/lfc_approximate_working_set_size_windows.autoscaling.sql',
}

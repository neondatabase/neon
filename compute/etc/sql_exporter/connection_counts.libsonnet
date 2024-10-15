{
  metric_name: 'connection_counts',
  type: 'gauge',
  help: 'Connection counts',
  key_labels: [
    'datname',
    'state',
  ],
  values: [
    'count',
  ],
  query: importstr 'sql_exporter/connection_counts.sql',
}

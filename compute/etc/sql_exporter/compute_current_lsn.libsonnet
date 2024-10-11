{
  metric_name: 'compute_current_lsn',
  type: 'gauge',
  help: 'Current LSN of the database',
  key_labels: null,
  values: [
    'lsn',
  ],
  query: importstr 'sql_exporter/compute_current_lsn.sql',
}

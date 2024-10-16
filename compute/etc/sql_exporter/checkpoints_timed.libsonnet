{
  metric_name: 'checkpoints_timed',
  type: 'gauge',
  help: 'Number of scheduled checkpoints',
  key_labels: null,
  values: [
    'checkpoints_timed',
  ],
  query: importstr 'sql_exporter/checkpoints_timed.sql',
}

{
  metric_name: 'replication_delay_seconds',
  type: 'gauge',
  help: 'Time since last LSN was replayed',
  key_labels: null,
  values: [
    'replication_delay_seconds',
  ],
  query: importstr 'sql_exporter/replication_delay_seconds.sql',
}

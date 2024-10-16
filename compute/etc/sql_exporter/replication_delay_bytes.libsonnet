{
  metric_name: 'replication_delay_bytes',
  type: 'gauge',
  help: 'Bytes between received and replayed LSN',
  key_labels: null,
  values: [
    'replication_delay_bytes',
  ],
  query: importstr 'sql_exporter/replication_delay_bytes.sql',
}

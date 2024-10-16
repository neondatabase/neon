{
  metric_name: 'compute_receive_lsn',
  type: 'gauge',
  help: 'Returns the last write-ahead log location that has been received and synced to disk by streaming replication',
  key_labels: null,
  values: [
    'lsn',
  ],
  query: importstr 'sql_exporter/compute_receive_lsn.sql',
}

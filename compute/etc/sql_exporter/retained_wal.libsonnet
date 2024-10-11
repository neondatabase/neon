{
  metric_name: 'retained_wal',
  type: 'gauge',
  help: 'Retained WAL in inactive replication slots',
  key_labels: [
    'slot_name',
  ],
  values: [
    'retained_wal',
  ],
  query: importstr 'sql_exporter/retained_wal.sql',
}

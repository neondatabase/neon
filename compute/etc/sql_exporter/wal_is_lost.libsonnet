{
  metric_name: 'wal_is_lost',
  type: 'gauge',
  help: 'Whether or not the replication slot wal_status is lost',
  key_labels: [
    'slot_name',
  ],
  values: [
    'wal_is_lost',
  ],
  query: importstr 'sql_exporter/wal_is_lost.sql',
}

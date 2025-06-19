{
  metric_name: 'pg_autovacuum_stats',
  type: 'gauge',
  help: 'Autovacuum statistics for all tables across databases',
  key_labels: [
    'database_name',
  ],
  value_label: 'metric',
  values: [
    'oldest_mxid',
    'oldest_frozen_xid',
  ],
  query: importstr 'sql_exporter/autovacuum_stats.sql',
}

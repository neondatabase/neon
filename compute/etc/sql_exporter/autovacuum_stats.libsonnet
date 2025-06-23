{
  metric_name: 'pg_autovacuum_stats',
  type: 'gauge',
  help: 'Autovacuum statistics for all tables across databases',
  key_labels: [
    'database_name',
  ],
  value_label: 'metric',
  values: [
    'min_mxid_age',
    'frozen_xid_age',
  ],
  query: importstr 'sql_exporter/autovacuum_stats.sql',
}

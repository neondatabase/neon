{
  metric_name: 'pg_autovacuum_stats',
  type: 'gauge',
  help: 'Autovacuum statistics for all tables across databases',
  key_labels: [
    'dbname',
  ],
  value_label: 'metric',
  values: [
    'autovacuum_count',
  ],
  query: importstr 'sql_exporter/autovacuum_stats.sql',
}

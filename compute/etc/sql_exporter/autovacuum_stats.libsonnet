{
  metric_name: 'pg_autovacuum_stats',
  type: 'gauge',
  help: 'Autovacuum statistics for all tables across databases',
  key_labels: [
    'datname',
    'schemaname',
    'relname',
  ],
  value_label: 'metric',
  values: [
    'autovacuum_count',
    'autoanalyze_count',
    'last_autovacuum',
  ],
  query: importstr 'sql_exporter/autovacuum_stats.sql',
} 

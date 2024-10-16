{
  metric_name: 'pg_stats_userdb',
  type: 'gauge',
  help: 'Stats for several oldest non-system dbs',
  key_labels: [
    'datname',
  ],
  value_label: 'kind',
  values: [
    'db_size',
    'deadlocks',
    // Rows
    'inserted',
    'updated',
    'deleted',
  ],
  query: importstr 'sql_exporter/pg_stats_userdb.sql',
}

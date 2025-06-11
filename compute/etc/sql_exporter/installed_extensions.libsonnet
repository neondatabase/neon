{
  metric_name: 'installed_extensions',
  type: 'gauge',
  help: 'List of extensions installed across databases',
  key_labels: [
    'extname',
    'extversion',
    'owned_by_superuser',
  ],
  values: [
    'n_databases',
  ],
  query: importstr 'sql_exporter/installed_extensions.sql',
}

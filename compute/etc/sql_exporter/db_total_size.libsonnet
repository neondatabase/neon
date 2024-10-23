{
  metric_name: 'db_total_size',
  type: 'gauge',
  help: 'Size of all databases',
  key_labels: null,
  values: [
    'total',
  ],
  query: importstr 'sql_exporter/db_total_size.sql',
}

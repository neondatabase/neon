{
  metric_name: 'max_cluster_size',
  type: 'gauge',
  help: 'neon.max_cluster_size setting',
  key_labels: null,
  values: [
    'max_cluster_size',
  ],
  query: importstr 'sql_exporter/max_cluster_size.sql',
}

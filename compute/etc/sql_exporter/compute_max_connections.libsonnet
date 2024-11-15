{
  metric_name: 'compute_max_connections',
  type: 'gauge',
  help: 'Max connections allowed for Postgres',
  key_labels: null,
  values: [
    'max_connections',
  ],
  query: importstr 'sql_exporter/compute_max_connections.sql',
}

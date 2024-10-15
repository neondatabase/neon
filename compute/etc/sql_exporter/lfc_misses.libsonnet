{
  metric_name: 'lfc_misses',
  type: 'gauge',
  help: 'lfc_misses',
  key_labels: null,
  values: [
    'lfc_misses',
  ],
  query: importstr 'sql_exporter/lfc_misses.sql',
}

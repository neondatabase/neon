{
  metric_name: 'lfc_hits',
  type: 'gauge',
  help: 'lfc_hits',
  key_labels: null,
  values: [
    'lfc_hits',
  ],
  query: importstr 'sql_exporter/lfc_hits.sql',
}

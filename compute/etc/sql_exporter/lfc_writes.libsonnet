{
  metric_name: 'lfc_writes',
  type: 'gauge',
  help: 'lfc_writes',
  key_labels: null,
  values: [
    'lfc_writes',
  ],
  query: importstr 'sql_exporter/lfc_writes.sql',
}

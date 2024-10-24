{
  metric_name: 'lfc_used',
  type: 'gauge',
  help: 'LFC chunks used (chunk = 1MB)',
  key_labels: null,
  values: [
    'lfc_used',
  ],
  query: importstr 'sql_exporter/lfc_used.sql',
}

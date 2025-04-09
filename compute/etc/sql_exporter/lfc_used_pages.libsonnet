{
  metric_name: 'lfc_used_pages',
  type: 'gauge',
  help: 'LFC pages used',
  key_labels: null,
  values: [
    'lfc_used_pages',
  ],
  query: importstr 'sql_exporter/lfc_used_pages.sql',
}

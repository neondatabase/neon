// DEPRECATED

{
  metric_name: 'lfc_approximate_working_set_size',
  type: 'gauge',
  help: 'Approximate working set size in pages of 8192 bytes',
  key_labels: null,
  values: [
    'approximate_working_set_size',
  ],
  query: importstr 'sql_exporter/lfc_approximate_working_set_size.sql',
}

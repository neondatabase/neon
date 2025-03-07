{
  metric_name: 'lfc_chunk_size',
  type: 'gauge',
  help: 'LFC chunk size, measured in 8KiB pages',
  key_labels: null,
  values: [
    'lfc_chunk_size_pages',
  ],
  query: importstr 'sql_exporter/lfc_chunk_size.sql',
}

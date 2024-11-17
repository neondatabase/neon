{
  metric_name: 'lfc_cache_size_limit',
  type: 'gauge',
  help: 'LFC cache size limit in bytes',
  key_labels: null,
  values: [
    'lfc_cache_size_limit',
  ],
  query: importstr 'sql_exporter/lfc_cache_size_limit.sql',
}

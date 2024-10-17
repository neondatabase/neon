{
  metric_name: 'file_cache_write_wait_seconds_bucket',
  type: 'counter',
  help: 'Histogram buckets of LFC write operation latencies',
  key_labels: [
    'bucket_le',
  ],
  values: [
    'value',
  ],
  query: importstr 'sql_exporter/file_cache_write_wait_seconds_bucket.sql',
}

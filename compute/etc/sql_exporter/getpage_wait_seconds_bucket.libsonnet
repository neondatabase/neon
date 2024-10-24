{
  metric_name: 'getpage_wait_seconds_bucket',
  type: 'counter',
  help: 'Histogram buckets of getpage request latency',
  key_labels: [
    'bucket_le',
  ],
  values: [
    'value',
  ],
  query: importstr 'sql_exporter/getpage_wait_seconds_bucket.sql',
}

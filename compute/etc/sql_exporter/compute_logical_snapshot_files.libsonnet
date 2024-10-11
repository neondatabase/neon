{
  metric_name: 'compute_logical_snapshot_files',
  type: 'gauge',
  help: 'Number of snapshot files in pg_logical/snapshot',
  key_labels: [
    'timeline_id',
  ],
  values: [
    'num_logical_snapshot_files',
  ],
  query: importstr 'sql_exporter/compute_logical_snapshot_files.sql',
}

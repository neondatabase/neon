{
  metric_name: 'compute_min_mxid_age',
  type: 'gauge',
  help: 'Age of oldest MXIDs that have not been replaced by VACUUM. An indicator of how long it has been since VACUUM last ran.',
  key_labels: [
    'database_name',
  ],
  value_label: 'metric',
  values: [
    'min_mxid_age',
  ],
  query: importstr 'sql_exporter/compute_autovacuum_metrics.sql',
}

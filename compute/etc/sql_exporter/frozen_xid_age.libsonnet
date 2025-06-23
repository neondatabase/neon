{
  metric_name: 'frozen_xid_age',
  type: 'gauge',
  help: 'Age of oldest XIDs that have not been frozen by VACUUM. An indicator of how long it has been since AUTO VACUUM/VACUUM last ran.',
  key_labels: [
    'database_name',
  ],
  value_label: 'metric',
  values: [
    'frozen_xid_age',
  ],
  query: importstr 'sql_exporter/frozen_xid_age.sql',
}

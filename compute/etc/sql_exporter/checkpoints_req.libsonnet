{
  metric_name: 'checkpoints_req',
  type: 'gauge',
  help: 'Number of requested checkpoints',
  key_labels: null,
  values: [
    'checkpoints_req',
  ],
  query: importstr 'sql_exporter/checkpoints_req.sql',
}

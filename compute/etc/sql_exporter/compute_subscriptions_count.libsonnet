{
  metric_name: 'compute_subscriptions_count',
  type: 'gauge',
  help: 'Number of logical replication subscriptions grouped by enabled/disabled',
  key_labels: [
    'enabled',
  ],
  values: [
    'subscriptions_count',
  ],
  query: importstr 'sql_exporter/compute_subscriptions_count.sql',
}

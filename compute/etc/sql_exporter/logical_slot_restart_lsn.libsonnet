// Number of slots is limited by max_replication_slots, so collecting position
// for all of them shouldn't be bad.

{
  metric_name: 'logical_slot_restart_lsn',
  type: 'gauge',
  help: 'restart_lsn of logical slots',
  key_labels: [
    'slot_name',
  ],
  values: [
    'restart_lsn',
  ],
  query: importstr 'sql_exporter/logical_slot_restart_lsn.sql',
}

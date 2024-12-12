local neon = import 'neon.libsonnet';

local pg_ls_logicalsnapdir = importstr 'sql_exporter/compute_logical_snapshots_bytes.15.sql';
local pg_ls_dir = importstr 'sql_exporter/compute_logical_snapshots_bytes.sql';

{
  metric_name: 'compute_logical_snapshots_bytes',
  type: 'gauge',
  help: 'Size of the pg_logical/snapshots directory, not including temporary files',
  key_labels: [
    'timeline_id',
  ],
  values: [
    'logical_snapshots_bytes',
  ],
  query: if neon.PG_MAJORVERSION_NUM < 15 then pg_ls_dir else pg_ls_logicalsnapdir,
}

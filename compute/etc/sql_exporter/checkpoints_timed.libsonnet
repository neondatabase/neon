local neon = import 'neon.libsonnet';

local pg_stat_bgwriter = importstr 'sql_exporter/checkpoints_timed.sql';
local pg_stat_checkpointer = importstr 'sql_exporter/checkpoints_timed.17.sql';

{
  metric_name: 'checkpoints_timed',
  type: 'gauge',
  help: 'Number of scheduled checkpoints',
  key_labels: null,
  values: [
    'checkpoints_timed',
  ],
  query: if neon.PG_MAJORVERSION_NUM < 17 then pg_stat_bgwriter else pg_stat_checkpointer,
}

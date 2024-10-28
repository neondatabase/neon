local neon = import 'neon.libsonnet';

local pg_stat_bgwriter = importstr 'sql_exporter/checkpoints_req.sql';
local pg_stat_checkpointer = importstr 'sql_exporter/checkpoints_req.17.sql';

{
  metric_name: 'checkpoints_req',
  type: 'gauge',
  help: 'Number of requested checkpoints',
  key_labels: null,
  values: [
    'checkpoints_req',
  ],
  query: if neon.PG_MAJORVERSION_NUM < 17 then pg_stat_bgwriter else pg_stat_checkpointer,
}

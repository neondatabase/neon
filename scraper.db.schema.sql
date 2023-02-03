
CREATE TABLE layer_map (
    scrape_ts timestamp with time zone,
    pageserver_id text,
    launch_id text,
    tenant_id text,
    timeline_id text,
    layer_map jsonb
);

CREATE VIEW last_access_ts AS
 SELECT layer_map.scrape_ts,
    layer_map.tenant_id,
    layer_map.timeline_id,
    layer_info.remote,
    layer_info.layer_file_name,
    layer_info.layer_file_size,
    to_timestamp(((max(most_recent_rec.when_millis_since_epoch) / (1000)::numeric))::double precision) AS last_access
   FROM layer_map,
    LATERAL jsonb_to_recordset(layer_map.layer_map['historic_layers'::text]) layer_info(kind text, remote boolean, layer_file_name text, layer_file_size numeric, access_stats jsonb),
    LATERAL jsonb_to_record(layer_info.access_stats) access_stats_rec(most_recent jsonb),
    LATERAL jsonb_to_recordset(
        CASE
            WHEN (jsonb_array_length(access_stats_rec.most_recent) > 0) THEN access_stats_rec.most_recent
            ELSE '[{"when_millis_since_epoch": 0}]'::jsonb
        END) most_recent_rec(when_millis_since_epoch numeric)
  GROUP BY layer_map.scrape_ts, layer_map.tenant_id, layer_map.timeline_id, layer_info.layer_file_name, layer_info.remote, layer_info.layer_file_size;


CREATE VIEW last_access AS
 SELECT last_access_ts.tenant_id,
    last_access_ts.timeline_id,
    last_access_ts.layer_file_name,
    max(last_access_ts.last_access) AS last_access,
    max(last_access_ts.layer_file_size) AS layer_file_size
   FROM last_access_ts
  GROUP BY last_access_ts.tenant_id, last_access_ts.timeline_id, last_access_ts.layer_file_name;


CREATE VIEW layer_files AS
 SELECT DISTINCT last_access_ts.tenant_id,
    last_access_ts.timeline_id,
    last_access_ts.layer_file_name
   FROM last_access_ts;



CREATE VIEW most_recent_scrape AS
 SELECT last_access_ts.tenant_id,
    last_access_ts.timeline_id,
    last_access_ts.layer_file_name,
    max(last_access_ts.scrape_ts) AS most_recent_scrape_ts
   FROM last_access_ts
  GROUP BY last_access_ts.tenant_id, last_access_ts.timeline_id, last_access_ts.layer_file_name;


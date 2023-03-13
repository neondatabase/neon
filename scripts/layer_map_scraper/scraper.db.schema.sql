CREATE TABLE scrapes (
    scrape_ts timestamp with time zone,
    pageserver_id text,
    pageserver_launch_timestamp timestamp with time zone,
    tenant_id text,
    timeline_id text,
    layer_map_dump jsonb
);

create index scrapes_tenant_id_idx on scrapes (tenant_id);
create index scrapes_timeline_id_idx on scrapes (timeline_id);
create index scrapes_scrape_ts_idx on scrapes (scrape_ts);
create index scrapes_tenant_timeline_id_idx on scrapes (tenant_id, timeline_id); -- this can probably go
create index timeline_scrape_ts_idx on scrapes (timeline_id, scrape_ts);
create index tenant_timeline_scrape_ts_idx on scrapes (tenant_id, timeline_id, scrape_ts);
create index pageserver_id_idx on scrapes (pageserver_id);

-- add statistics so that query planner selects the tenant_timeline_scrape_ts idx
create statistics scrapes_tenant_timeline_stats on tenant_id, timeline_id from scrapes;

--- what follows are example queries ---

--- how many layer accesses did we have per layers/timeline/tenant in the last 30 seconds
with flattened_to_access_count as (
    select *
    from scrapes as scrapes
             cross join jsonb_to_recordset(scrapes.layer_map_dump -> 'historic_layers') historic_layer(layer_file_name text, access_stats jsonb)
             cross join jsonb_to_record(historic_layer.access_stats) access_stats(access_count_by_access_kind jsonb)
             cross join LATERAL (select key as access_kind, value::numeric as access_count from jsonb_each(access_count_by_access_kind)) access_count
)
select tenant_id, timeline_id, layer_file_name, access_kind, SUM(access_count) access_count_sum
from flattened_to_access_count
where scrape_ts > (clock_timestamp() - '30 second'::interval)
group by rollup(tenant_id, timeline_id, layer_file_name, access_kind)
having SUM(access_count) > 0
order by access_count_sum desc, tenant_id desc, timeline_id desc, layer_file_name, access_kind;

--- residence change events in the last 30 minutes
-- (precise, unless more residence changes happen between scrapes than layer access stats buffer)
with flattened_to_residence_changes as (select *
    from scrapes as scrapes
             cross join jsonb_to_recordset(scrapes.layer_map_dump -> 'historic_layers') historic_layer(layer_file_name text, access_stats jsonb)
             cross join jsonb_to_record(historic_layer.access_stats) access_stats(residence_events_history jsonb)
            cross join jsonb_to_record(access_stats.residence_events_history) residence_events_history(buffer jsonb, drop_count numeric)
            cross join jsonb_to_recordset(residence_events_history.buffer) residence_events_buffer(status text, reason text, timestamp_millis_since_epoch numeric)
        )
, renamed as (
    select
        scrape_ts,
        pageserver_launch_timestamp,
        layer_file_name,
        tenant_id,
        timeline_id,
        to_timestamp(timestamp_millis_since_epoch/1000) as residence_change_ts,
        status,
        reason
    from flattened_to_residence_changes
)
select distinct residence_change_ts, status, reason, tenant_id, timeline_id, layer_file_name
from renamed
where residence_change_ts > (clock_timestamp() - '30 min'::interval)
order by residence_change_ts desc, layer_file_name;

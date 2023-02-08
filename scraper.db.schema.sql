CREATE TABLE scrapes (
    scrape_ts timestamp with time zone,
    pageserver_id text,
    pageserver_launch_timestamp timestamp with time zone,
    tenant_id text,
    timeline_id text,
    layer_map_dump jsonb
);

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

--- layer map changes in the last hour, for a given tenant and timeline
with layer_file_names_ts as (
    select scrape_ts, array_agg(layer_file_name ORDER BY layer_file_name) as layer_file_names from scrapes
             cross join jsonb_to_recordset(layer_map_dump->'historic_layers') historic_layers(layer_file_name text)
    where tenant_id = '8c9520708d8cce74f072a867f141c1b9' and timeline_id = 'f15ae0cf21cce2ba27e4d80c6709a6cd'
    and scrape_ts > (clock_timestamp() - '1 hour'::interval)
    group by scrape_ts
), layer_map_changes as (
    select MIN(scrape_ts) as ts, layer_file_names from layer_file_names_ts  group by layer_file_names
    order by ts
)
, layer_map_changes_with_prev as (
    select ts,
        layer_file_names,
        lag(layer_file_names) over (order by ts)  as prev
    from layer_map_changes
)
-- select * from layer_file_names_ts limit 10;
-- select * from layer_map_changes;
select ts, layer_file_names,
       array((select unnest(layer_file_names) except  select unnest(prev))) as diff_previous_scrape,
       array((select unnest(prev) except  select unnest(layer_file_names))) as diff_next_scrape
from layer_map_changes_with_prev;

--- downsampling pattern. This query here picks the earliest scrape in 20 minute buckets
---- XXX: buckets keep moving because clock_timestamp(), better divide up the calendar into fixed buckets
with points(point) as (
    select generate_series(clock_timestamp() - '24 hours'::interval, clock_timestamp(), '20 minute'::interval)
), ranges(lower, upper) as (
    select point, lead(point) over (order by point) from points
), data as (
    (select * from scrapes
    where tenant_id = '8c9520708d8cce74f072a867f141c1b9' and timeline_id = 'f15ae0cf21cce2ba27e4d80c6709a6cd')
), first_scrape_ts_in_range(lower, upper, scrape_ts) as (
    select lower, upper, min(scrape_ts) from ranges
        LEFT JOIN data on
            scrape_ts >= lower and scrape_ts < upper
    group by  lower, upper
), downsampled_data as (
    select data.* from first_scrape_ts_in_range LEFT JOIN data using (scrape_ts) order by lower
)
select scrape_ts, jsonb_array_length(layer_map_dump->'historic_layers') num_layers from downsampled_data;

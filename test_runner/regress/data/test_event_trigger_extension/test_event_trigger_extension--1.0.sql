\echo Use "CREATE EXTENSION test_event_trigger_extension" to load this file. \quit

CREATE SCHEMA event_trigger;

create sequence if not exists event_trigger.seq_schema_version as int cycle;

create or replace function event_trigger.increment_schema_version()
    returns event_trigger
    security definer
    language plpgsql
as $$
begin
    perform pg_catalog.nextval('event_trigger.seq_schema_version');
end;
$$;

create or replace function event_trigger.get_schema_version()
    returns int
    security definer
    language sql
as $$
    select last_value from event_trigger.seq_schema_version;
$$;

-- On DDL event, increment the schema version number
create event trigger event_trigger_watch_ddl
    on ddl_command_end
    execute procedure event_trigger.increment_schema_version();

create event trigger event_trigger_watch_drop
    on sql_drop
    execute procedure event_trigger.increment_schema_version();

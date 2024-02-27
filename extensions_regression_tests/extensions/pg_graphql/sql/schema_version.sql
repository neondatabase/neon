-- Is updated every time the schema changes
create sequence if not exists graphql.seq_schema_version as int cycle;

create or replace function graphql.increment_schema_version()
    returns event_trigger
    security definer
    language plpgsql
as $$
begin
    perform nextval('graphql.seq_schema_version');
end;
$$;

create or replace function graphql.get_schema_version()
    returns int
    security definer
    language sql
as $$
    select last_value from graphql.seq_schema_version;
$$;

-- On DDL event, increment the schema version number
create event trigger graphql_watch_ddl
    on ddl_command_end
    execute procedure graphql.increment_schema_version();

create event trigger graphql_watch_drop
    on sql_drop
    execute procedure graphql.increment_schema_version();

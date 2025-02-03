create or replace function who_am_i()
    returns event_trigger
    language plpgsql as
$$
begin
    raise notice 'the event trigger is executed for %', current_user;
end;
$$;

create role neon_superuser;
grant create on schema public to neon_superuser;

create role neon_normaluser;
grant create on schema public to neon_normaluser;

set role neon_normaluser;

-- Non-priveleged neon user should not be able to create event trigers
create event trigger on_ddl on ddl_command_end
execute procedure who_am_i();

set role neon_superuser;

-- Neon superuser should be able to create event triggers
create event trigger on_ddl on ddl_command_end
execute procedure who_am_i();

-- Check that event trigger is fired for neon_superuser
create table t1(x integer);

set role cloud_admin;
-- Check that event trigger is also by default fired for superuser
create table t2(x integer);

-- Now disabe event triggers execution for superuser
set neon.disable_event_triggers_for_superuser=on;

-- Check that even trigger is not fired in this case
create table t3(x integer);

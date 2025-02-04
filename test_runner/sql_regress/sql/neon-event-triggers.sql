create or replace function admin_proc()
    returns event_trigger
    language plpgsql as
$$
begin
    raise notice 'admin event trigger is executed for %', current_user;
end;
$$;

create role neon_superuser;
grant create on schema public to neon_superuser;

create role neon_normaluser;
grant create on schema public to neon_normaluser;

create event trigger on_ddl1 on ddl_command_end
execute procedure admin_proc();

set role neon_normaluser;

-- Non-priveleged neon user should not be able to create event trigers
create event trigger on_ddl2 on ddl_command_end
execute procedure admin_proc();

set role neon_superuser;

create or replace function neon_proc()
    returns event_trigger
    language plpgsql as
$$
begin
    raise notice 'neon event trigger is executed for %', current_user;
end;
$$ security definer;

-- Neon superuser should be able to create event triggers
create event trigger on_ddl3 on ddl_command_end
execute procedure neon_proc();

-- Check that event trigger is fired for neon_superuser
create table t1(x integer);

set role cloud_admin;
-- Check that event trigger is not fired for superuser
create table t2(x integer);

-- Now enaabe event triggers execution for superuser
set neon.enable_event_triggers_for_superuser=on;

-- Check that even trigger is fired in this case
create table t3(x integer);

set role neon_superuser;

-- Check that neon_superuser can drop it's event trigger
drop event trigger on_ddl1;
drop event trigger on_ddl3;

SELECT datname, application_name, state, count(*) AS count FROM pg_stat_activity WHERE state <> '' GROUP BY datname, state;

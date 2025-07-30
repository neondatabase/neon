SELECT datname, state, pg_catalog.count(*) AS count FROM pg_catalog.pg_stat_activity WHERE state <> '' GROUP BY datname, state;

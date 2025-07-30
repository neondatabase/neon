SELECT CASE
  WHEN pg_catalog.pg_is_in_recovery() THEN (pg_catalog.pg_last_wal_replay_lsn() - '0/0')::pg_catalog.FLOAT8
  ELSE (pg_catalog.pg_current_wal_lsn() - '0/0')::pg_catalog.FLOAT8
END AS lsn;

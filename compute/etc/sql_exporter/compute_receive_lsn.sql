SELECT CASE
  WHEN pg_catalog.pg_is_in_recovery() THEN (pg_catalog.pg_last_wal_receive_lsn() - '0/0')::pg_catalog.FLOAT8
  ELSE 0
END AS lsn;

SELECT CASE
  WHEN pg_catalog.pg_is_in_recovery() THEN (pg_last_wal_receive_lsn() - '0/0')::FLOAT8
  ELSE 0
END AS lsn;

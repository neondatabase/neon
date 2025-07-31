SELECT
  slot_name,
  pg_catalog.pg_wal_lsn_diff(
    CASE
      WHEN pg_catalog.pg_is_in_recovery() THEN pg_catalog.pg_last_wal_replay_lsn()
      ELSE pg_catalog.pg_current_wal_lsn()
    END,
    restart_lsn)::pg_catalog.FLOAT8 AS retained_wal
FROM pg_catalog.pg_replication_slots
WHERE active = false;

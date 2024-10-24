SELECT
  slot_name,
  pg_wal_lsn_diff(
    CASE
      WHEN pg_is_in_recovery() THEN pg_last_wal_replay_lsn()
      ELSE pg_current_wal_lsn()
    END,
    restart_lsn)::FLOAT8 AS retained_wal
FROM pg_replication_slots
WHERE active = false;

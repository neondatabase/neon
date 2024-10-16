SELECT
  slot_name,
  pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)::FLOAT8 AS retained_wal
FROM pg_replication_slots
WHERE active = false;

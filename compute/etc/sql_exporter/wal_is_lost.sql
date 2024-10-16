SELECT
  slot_name,
  CASE
    WHEN wal_status = 'lost' THEN 1
    ELSE 0
  END AS wal_is_lost
FROM pg_replication_slots;

SELECT
  CASE
    WHEN pg_catalog.pg_last_wal_receive_lsn() = pg_catalog.pg_last_wal_replay_lsn() THEN 0
    ELSE GREATEST(0, EXTRACT (EPOCH FROM pg_catalog.now() - pg_catalog.pg_last_xact_replay_timestamp()))
  END AS replication_delay_seconds;

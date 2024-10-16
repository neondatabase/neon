-- We use a GREATEST call here because this calculation can be negative. The
-- calculation is not atomic, meaning after we've gotten the receive LSN, the
-- replay LSN may have advanced past the receive LSN we are using for the
-- calculation.

SELECT GREATEST(0, pg_wal_lsn_diff(pg_last_wal_receive_lsn(), pg_last_wal_replay_lsn())) AS replication_delay_bytes;

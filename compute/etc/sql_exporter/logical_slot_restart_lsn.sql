SELECT slot_name, (restart_lsn - '0/0')::FLOAT8 as restart_lsn
FROM pg_replication_slots
WHERE slot_type = 'logical';

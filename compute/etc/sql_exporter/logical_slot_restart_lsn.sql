SELECT slot_name, (restart_lsn - '0/0')::pg_catalog.FLOAT8 AS restart_lsn
FROM pg_catalog.pg_replication_slots
WHERE slot_type = 'logical';

SELECT
  (SELECT setting FROM pg_settings WHERE name = 'neon.timeline_id') AS timeline_id,
  -- Postgres creates temporary snapshot files of the form %X-%X.snap.%d.tmp.
  -- These temporary snapshot files are renamed to the actual snapshot files
  -- after they are completely built. We only WAL-log the completely built
  -- snapshot files
  (SELECT COUNT(*) FROM pg_ls_dir('pg_logical/snapshots') AS name WHERE name LIKE '%.snap') AS num_logical_snapshot_files;

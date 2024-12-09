SELECT
  (SELECT setting FROM pg_settings WHERE name = 'neon.timeline_id') AS timeline_id,
  -- Postgres creates temporary snapshot files of the form %X-%X.snap.%d.tmp.
  -- These temporary snapshot files are renamed to the actual snapshot files
  -- after they are completely built. We only WAL-log the completely built
  -- snapshot files
  (SELECT COALESCE(sum((pg_stat_file('pg_logical/snapshots/' || name, missing_ok => true)).size), 0)
    FROM (SELECT * FROM pg_ls_dir('pg_logical/snapshots') WHERE pg_ls_dir LIKE '%.snap') AS name
  ) AS logical_snapshots_bytes;

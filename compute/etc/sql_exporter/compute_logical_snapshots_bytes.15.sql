SELECT
  (SELECT pg_catalog.current_setting('neon.timeline_id')) AS timeline_id,
  -- Postgres creates temporary snapshot files of the form %X-%X.snap.%d.tmp.
  -- These temporary snapshot files are renamed to the actual snapshot files
  -- after they are completely built. We only WAL-log the completely built
  -- snapshot files
  (SELECT pg_catalog.COALESCE(pg_catalog.sum(size), 0) FROM pg_catalog.pg_ls_logicalsnapdir() WHERE name LIKE '%.snap') AS logical_snapshots_bytes;

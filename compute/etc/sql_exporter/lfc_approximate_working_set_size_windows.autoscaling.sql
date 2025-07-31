-- NOTE: This is the "internal" / "machine-readable" version. This outputs the
-- working set size looking back 1..60 minutes, labeled with the number of
-- minutes.

SELECT
  x::pg_catalog.text AS duration_seconds,
  neon.approximate_working_set_size_seconds(x) AS size
FROM (SELECT generate_series * 60 AS x FROM generate_series(1, 60)) AS t (x);

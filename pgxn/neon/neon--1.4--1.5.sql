\echo Use "ALTER EXTENSION neon UPDATE TO '1.5'" to load this file. \quit


CREATE FUNCTION get_backend_perf_counters()
RETURNS SETOF RECORD
AS 'MODULE_PATHNAME', 'neon_get_backend_perf_counters'
LANGUAGE C PARALLEL SAFE;

CREATE FUNCTION get_perf_counters()
RETURNS SETOF RECORD
AS 'MODULE_PATHNAME', 'neon_get_perf_counters'
LANGUAGE C PARALLEL SAFE;

-- Show various metrics, for each backend. Note that the values are not reset
-- when a backend exits. When a new backend starts with the backend ID, it will
-- continue accumulating the values from where the old backend left. If you are
-- only interested in the changes from your own session, store the values at the
-- beginning of the session somewhere, and subtract them on subsequent calls.
--
-- For histograms, 'bucket_le' is the upper bound of the histogram bucket.
CREATE VIEW neon_backend_perf_counters AS
  SELECT P.procno, P.pid, P.metric, P.bucket_le, P.value
  FROM get_backend_perf_counters() AS P (
    procno integer,
    pid integer,
    metric text,
    bucket_le float8,
    value float8
  );

-- Summary across all backends. (This could also be implemented with
-- an aggregate query over neon_backend_perf_counters view.)
CREATE VIEW neon_perf_counters AS
  SELECT P.metric, P.bucket_le, P.value
  FROM get_perf_counters() AS P (
    metric text,
    bucket_le float8,
    value float8
  );

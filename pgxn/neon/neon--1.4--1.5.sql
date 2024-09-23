\echo Use "ALTER EXTENSION neon UPDATE TO '1.5'" to load this file. \quit

-- returns minimal LFC cache size (in 8kb pages) provided specified hit rate
CREATE FUNCTION approximate_optimal_cache_size(duration_sec integer default null, min_hit_ration float8 default null)
RETURNS integer
AS 'MODULE_PATHNAME', 'approximate_optimal_cache_size'
LANGUAGE C PARALLEL SAFE;

GRANT EXECUTE ON FUNCTION approximate_optimal_cache_size(integer,float8) TO pg_monitor;


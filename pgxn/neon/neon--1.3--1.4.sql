\echo Use "ALTER EXTENSION neon UPDATE TO '1.4'" to load this file. \quit

CREATE FUNCTION approximate_working_set_size_seconds(duration integer default null)
RETURNS integer
AS 'MODULE_PATHNAME', 'approximate_working_set_size_seconds'
LANGUAGE C PARALLEL SAFE;

GRANT EXECUTE ON FUNCTION approximate_working_set_size_seconds(integer) TO pg_monitor;


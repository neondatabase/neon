\echo Use "ALTER EXTENSION neon UPDATE TO '1.6'" to load this file. \quit

CREATE FUNCTION save_local_cache_state()
RETURNS void
AS 'MODULE_PATHNAME', 'save_local_cache_state'
LANGUAGE C STRICT
PARALLEL UNSAFE;


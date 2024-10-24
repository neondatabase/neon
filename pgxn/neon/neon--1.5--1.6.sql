\echo Use "ALTER EXTENSION neon UPDATE TO '1.6'" to load this file. \quit

CREATE FUNCTION save_local_cache_state()
RETURNS void
AS 'MODULE_PATHNAME', 'save_local_cache_state'
LANGUAGE C STRICT
PARALLEL UNSAFE;

CREATE FUNCTION get_prewarm_info(out total_chunks integer, out curr_chunk integer, out prewarmed_pages integer, out skipped_pages integer)
RETURNS record
AS 'MODULE_PATHNAME', 'get_prewarm_info'
LANGUAGE C STRICT
PARALLEL SAFE;




-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION neon_test_utils" to load this file. \quit

CREATE FUNCTION test_consume_xids(nxids int)
RETURNS VOID
AS 'MODULE_PATHNAME', 'test_consume_xids'
LANGUAGE C STRICT
PARALLEL UNSAFE;

CREATE FUNCTION test_consume_oids(oid int)
RETURNS VOID
AS 'MODULE_PATHNAME', 'test_consume_oids'
LANGUAGE C STRICT
PARALLEL UNSAFE;

CREATE FUNCTION test_consume_cpu(seconds int)
RETURNS VOID
AS 'MODULE_PATHNAME', 'test_consume_cpu'
LANGUAGE C STRICT
PARALLEL UNSAFE;

CREATE FUNCTION test_consume_memory(megabytes int)
RETURNS VOID
AS 'MODULE_PATHNAME', 'test_consume_memory'
LANGUAGE C STRICT
PARALLEL UNSAFE;

CREATE FUNCTION test_release_memory(megabytes int DEFAULT NULL)
RETURNS VOID
AS 'MODULE_PATHNAME', 'test_release_memory'
LANGUAGE C
PARALLEL UNSAFE;

CREATE FUNCTION clear_buffer_cache()
RETURNS VOID
AS 'MODULE_PATHNAME', 'clear_buffer_cache'
LANGUAGE C STRICT
PARALLEL UNSAFE;

CREATE FUNCTION get_raw_page_at_lsn(relname text, forkname text, blocknum int8, request_lsn pg_lsn, not_modified_since pg_lsn)
RETURNS bytea
AS 'MODULE_PATHNAME', 'get_raw_page_at_lsn'
LANGUAGE C PARALLEL UNSAFE;

CREATE FUNCTION get_raw_page_at_lsn(tbspc oid, db oid, relfilenode oid, forknum int8, blocknum int8, request_lsn pg_lsn, not_modified_since pg_lsn)
RETURNS bytea
AS 'MODULE_PATHNAME', 'get_raw_page_at_lsn_ex'
LANGUAGE C PARALLEL UNSAFE;

CREATE FUNCTION neon_xlogflush(lsn pg_lsn DEFAULT NULL)
RETURNS VOID
AS 'MODULE_PATHNAME', 'neon_xlogflush'
LANGUAGE C PARALLEL UNSAFE;

CREATE FUNCTION trigger_panic()
RETURNS VOID
AS 'MODULE_PATHNAME', 'trigger_panic'
LANGUAGE C PARALLEL UNSAFE;

CREATE FUNCTION trigger_segfault()
RETURNS VOID
AS 'MODULE_PATHNAME', 'trigger_segfault'
LANGUAGE C PARALLEL UNSAFE;

-- Alias for `trigger_segfault`, just because `SELECT 💣()` looks fun
CREATE OR REPLACE FUNCTION 💣() RETURNS void
LANGUAGE plpgsql AS $$
BEGIN
    PERFORM trigger_segfault();
END;
$$;

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION neon_test_utils" to load this file. \quit

CREATE FUNCTION test_consume_xids(nxids int)
RETURNS VOID
AS 'MODULE_PATHNAME', 'test_consume_xids'
LANGUAGE C STRICT
PARALLEL UNSAFE;

CREATE FUNCTION clear_buffer_cache()
RETURNS VOID
AS 'MODULE_PATHNAME', 'clear_buffer_cache'
LANGUAGE C STRICT
PARALLEL UNSAFE;

CREATE FUNCTION get_raw_page_at_lsn(relname text, forkname text, blocknum int8, lsn pg_lsn)
RETURNS bytea
AS 'MODULE_PATHNAME', 'get_raw_page_at_lsn'
LANGUAGE C PARALLEL UNSAFE;

CREATE FUNCTION get_raw_page_at_lsn(tbspc oid, db oid, relfilenode oid, forknum int8, blocknum int8, lsn pg_lsn)
RETURNS bytea
AS 'MODULE_PATHNAME', 'get_raw_page_at_lsn_ex'
LANGUAGE C PARALLEL UNSAFE;

CREATE FUNCTION neon_xlogflush(lsn pg_lsn)
RETURNS VOID
AS 'MODULE_PATHNAME', 'neon_xlogflush'
LANGUAGE C PARALLEL UNSAFE;

/* contrib/remotexact/remotexact--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION remotexact" to load this file. \quit

CREATE FUNCTION validate_and_apply_xact(IN bytes bytea)
RETURNS void
AS 'MODULE_PATHNAME', 'validate_and_apply_xact'
LANGUAGE C STRICT;

CREATE FUNCTION lsn_snapshot(OUT region smallint, OUT lsn pg_lsn)
RETURNS SETOF record
AS 'MODULE_PATHNAME', 'lsn_snapshot'
LANGUAGE C STRICT;
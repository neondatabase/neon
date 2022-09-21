/* contrib/remotexact/remotexact--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION remotexact" to load this file. \quit

CREATE FUNCTION validate_and_apply_xact(IN bytes bytea)
RETURNS bool
AS 'MODULE_PATHNAME', 'validate_and_apply_xact'
LANGUAGE C STRICT;

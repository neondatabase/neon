\echo Use "ALTER EXTENSION neon UPDATE TO '1.1'" to load this file. \quit

CREATE FUNCTION neon_get_stat(text) RETURNS bigint
AS 'MODULE_PATHNAME', 'neon_get_stat'
LANGUAGE C PARALLEL SAFE STRICT;

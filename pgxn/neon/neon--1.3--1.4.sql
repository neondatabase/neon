\echo Use "ALTER EXTENSION neon UPDATE TO '1.4'" to load this file. \quit

CREATE FUNCTION neon_pgdump_schema(dbname text)
RETURNS text
AS 'MODULE_PATHNAME', 'neon_pgdump_schema'
LANGUAGE C PARALLEL SAFE;


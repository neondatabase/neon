\echo Use "ALTER EXTENSION test_extension_with_lib UPDATE TO '1.1'" to load this file. \quit

CREATE FUNCTION test_extension_with_lib.fun_fact()
RETURNS void
IMMUTABLE LEAKPROOF PARALLEL SAFE
AS 'MODULE_PATHNAME', 'fun_fact' LANGUAGE C;

\echo Use "CREATE EXTENSION test_extension_with_lib" to load this file. \quit

CREATE SCHEMA test_extension_with_lib;

CREATE FUNCTION test_extension_with_lib.motd()
RETURNS void
IMMUTABLE LEAKPROOF PARALLEL SAFE
AS 'MODULE_PATHNAME', 'motd' LANGUAGE C;

\echo Use "CREATE EXTENSION test_extension_sql_only" to load this file. \quit

CREATE SCHEMA test_extension_sql_only;

CREATE FUNCTION test_extension_sql_only.motd()
RETURNS void
IMMUTABLE LEAKPROOF PARALLEL SAFE
AS $$
BEGIN
    RAISE NOTICE 'Have a great day';
END;
$$ LANGUAGE 'plpgsql';

\echo Use "CREATE EXTENSION test_extension" to load this file. \quit

CREATE SCHEMA test_extension;

CREATE FUNCTION test_extension.motd()
RETURNS void
IMMUTABLE LEAKPROOF PARALLEL SAFE
AS $$
BEGIN
    RAISE NOTICE 'Have a great day';
END;
$$ LANGUAGE 'plpgsql';

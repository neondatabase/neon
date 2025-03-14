\echo Use "ALTER EXTENSION test_extension UPDATE TO '1.1'" to load this file. \quit

CREATE FUNCTION test_extension.fun_fact()
RETURNS void
IMMUTABLE LEAKPROOF PARALLEL SAFE
AS $$
BEGIN
    RAISE NOTICE 'Neon has a melting point of -246.08 C';
END;
$$ LANGUAGE 'plpgsql';

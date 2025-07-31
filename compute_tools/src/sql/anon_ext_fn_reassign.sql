DO $$
DECLARE
    query varchar;
BEGIN
    FOR query IN
    SELECT pg_catalog.format('ALTER FUNCTION %I(%s) OWNER TO {db_owner};', p.oid::regproc, pg_catalog.pg_get_function_identity_arguments(p.oid))
    FROM pg_catalog.pg_proc p
        WHERE p.pronamespace OPERATOR(pg_catalog.=) 'anon'::regnamespace::oid
    LOOP
        EXECUTE query;
    END LOOP;
END
$$;

DO $$
DECLARE
    query varchar;
BEGIN
    FOR query IN SELECT 'ALTER FUNCTION '||nsp.nspname||'.'||p.proname||'('||pg_get_function_identity_arguments(p.oid)||') OWNER TO {db_owner};'
    FROM pg_proc p
        JOIN pg_namespace nsp ON p.pronamespace = nsp.oid
    WHERE nsp.nspname = 'anon' LOOP
        EXECUTE query;
    END LOOP;
END
$$;

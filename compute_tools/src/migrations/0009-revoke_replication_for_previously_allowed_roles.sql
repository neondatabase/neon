-- SKIP: The original goal of this migration was to prevent creating
--       subscriptions, but this migration was insufficient.

DO $$
DECLARE
    role_name TEXT;
BEGIN
    FOR role_name IN SELECT rolname FROM pg_roles WHERE rolreplication IS TRUE
    LOOP
        RAISE NOTICE 'EXECUTING ALTER ROLE % NOREPLICATION', quote_ident(role_name);
        EXECUTE 'ALTER ROLE ' || quote_ident(role_name) || ' NOREPLICATION';
    END LOOP;
END $$;

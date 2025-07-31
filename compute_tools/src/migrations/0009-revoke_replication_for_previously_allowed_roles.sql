-- SKIP: The original goal of this migration was to prevent creating
--       subscriptions, but this migration was insufficient.

DO $$
DECLARE
    role_name TEXT;
BEGIN
    FOR role_name IN SELECT rolname FROM pg_catalog.pg_roles WHERE rolreplication IS TRUE
    LOOP
        RAISE NOTICE 'EXECUTING ALTER ROLE % NOREPLICATION', pg_catalog.quote_ident(role_name);
        EXECUTE pg_catalog.format('ALTER ROLE %I NOREPLICATION;', role_name);
    END LOOP;
END $$;

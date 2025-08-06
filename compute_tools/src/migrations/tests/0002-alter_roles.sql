DO $$
DECLARE
    role record;
BEGIN
    FOR role IN
        SELECT rolname AS name, rolinherit AS inherit
        FROM pg_catalog.pg_roles
        WHERE pg_catalog.pg_has_role(rolname, 'neon_superuser', 'member')
    LOOP
        IF NOT role.inherit THEN
            RAISE EXCEPTION '% cannot inherit', quote_ident(role.name);
        END IF;
    END LOOP;

    FOR role IN
        SELECT rolname AS name, rolbypassrls AS bypassrls
        FROM pg_catalog.pg_roles
        WHERE NOT pg_catalog.pg_has_role(rolname, 'neon_superuser', 'member')
            AND NOT pg_catalog.starts_with(rolname, 'pg_')
    LOOP
        IF role.bypassrls THEN
            RAISE EXCEPTION  '% can bypass RLS', pg_catalog.quote_ident(role.name);
        END IF;
    END LOOP;
END $$;

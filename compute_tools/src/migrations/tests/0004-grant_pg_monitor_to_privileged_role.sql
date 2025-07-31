DO $$
DECLARE
    monitor record;
BEGIN
    SELECT pg_catalog.pg_has_role('neon_superuser', 'pg_monitor', 'member') AS member,
            admin_option AS admin
        INTO monitor
        FROM pg_catalog.pg_auth_members
        WHERE roleid = 'pg_monitor'::pg_catalog.regrole
            AND member = 'neon_superuser'::pg_catalog.regrole;

    IF monitor IS NULL THEN
        RAISE EXCEPTION 'no entry in pg_auth_members for neon_superuser and pg_monitor';
    END IF;

    IF monitor.admin IS NULL OR NOT monitor.member THEN
        RAISE EXCEPTION 'neon_superuser is not a member of pg_monitor';
    END IF;

    IF monitor.admin IS NULL OR NOT monitor.admin THEN
        RAISE EXCEPTION 'neon_superuser cannot grant pg_monitor';
    END IF;
END $$;

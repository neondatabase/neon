DO $$
DECLARE
    signal_backend record;
BEGIN
    SELECT pg_catalog.pg_has_role('neon_superuser', 'pg_signal_backend', 'member') AS member,
            admin_option AS admin
        INTO signal_backend
        FROM pg_catalog.pg_auth_members
        WHERE roleid = 'pg_signal_backend'::regrole
            AND member = 'neon_superuser'::regrole;

    IF signal_backend IS NULL THEN
        RAISE EXCEPTION 'no entry in pg_auth_members for neon_superuser and pg_signal_backend';
    END IF;

    IF signal_backend.member IS NULL OR NOT signal_backend.member THEN
        RAISE EXCEPTION 'neon_superuser is not a member of pg_signal_backend';
    END IF;

    IF signal_backend.admin IS NULL OR NOT signal_backend.admin THEN
        RAISE EXCEPTION 'neon_superuser cannot grant pg_signal_backend';
    END IF;
END $$;

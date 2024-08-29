DO $$
DECLARE
    role_name text;
BEGIN
    FOR role_name IN SELECT rolname FROM pg_roles WHERE pg_has_role(rolname, 'neon_superuser', 'member')
    LOOP
        RAISE NOTICE 'EXECUTING ALTER ROLE % INHERIT', quote_ident(role_name);
        EXECUTE 'ALTER ROLE ' || quote_ident(role_name) || ' INHERIT';
    END LOOP;

    FOR role_name IN SELECT rolname FROM pg_roles
        WHERE
            NOT pg_has_role(rolname, 'neon_superuser', 'member') AND NOT starts_with(rolname, 'pg_')
    LOOP
        RAISE NOTICE 'EXECUTING ALTER ROLE % NOBYPASSRLS', quote_ident(role_name);
        EXECUTE 'ALTER ROLE ' || quote_ident(role_name) || ' NOBYPASSRLS';
    END LOOP;
END $$;

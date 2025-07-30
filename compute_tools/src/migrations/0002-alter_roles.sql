-- On December 8th, 2023, an engineering escalation (INC-110) was opened after
-- it was found that BYPASSRLS was being applied to all roles.
--
-- PR that introduced the issue: https://github.com/neondatabase/neon/pull/5657
-- Subsequent commit on main: https://github.com/neondatabase/neon/commit/ad99fa5f0393e2679e5323df653c508ffa0ac072
--
-- NOBYPASSRLS and INHERIT are the defaults for a Postgres role, but because it
-- isn't easy to know if a Postgres cluster is affected by the issue, we need to
-- keep the migration around for a long time, if not indefinitely, so any
-- cluster can be fixed.
--
-- Branching is the gift that keeps on giving...

DO $$
DECLARE
    role_name text;
BEGIN
    FOR role_name IN SELECT rolname FROM pg_catalog.pg_roles WHERE pg_catalog.pg_has_role(rolname, '{privileged_role_name}', 'member')
    LOOP
        RAISE NOTICE 'EXECUTING ALTER ROLE % INHERIT', pg_catalog.quote_ident(role_name);
        EXECUTE pg_catalog.format('ALTER ROLE %I INHERIT;', role_name);
    END LOOP;

    FOR role_name IN SELECT rolname FROM pg_catalog.pg_roles
        WHERE
            NOT pg_catalog.pg_has_role(rolname, '{privileged_role_name}', 'member') AND NOT pg_catalog.starts_with(rolname, 'pg_')
    LOOP
        RAISE NOTICE 'EXECUTING ALTER ROLE % NOBYPASSRLS', pg_catalog.quote_ident(role_name);
        EXECUTE pg_catalog.format('ALTER ROLE %I NOBYPASSRLS;', role_name);
    END LOOP;
END $$;

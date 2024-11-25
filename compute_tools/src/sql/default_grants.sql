DO
$$
    BEGIN
        IF EXISTS(
            SELECT nspname
            FROM pg_catalog.pg_namespace
            WHERE nspname = 'public'
        ) AND
           current_setting('server_version_num')::int / 10000 >= 15
        THEN
            IF EXISTS(
                SELECT rolname
                FROM pg_catalog.pg_roles
                WHERE rolname = 'web_access'
            )
            THEN
                GRANT CREATE ON SCHEMA public TO web_access;
            END IF;
        END IF;
        IF EXISTS(
            SELECT nspname
            FROM pg_catalog.pg_namespace
            WHERE nspname = 'public'
        )
        THEN
            ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO neon_superuser WITH GRANT OPTION;
            ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO neon_superuser WITH GRANT OPTION;
        END IF;
    END
$$;
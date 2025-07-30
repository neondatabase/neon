DO
$$
    BEGIN
        IF EXISTS(
            SELECT nspname
            FROM pg_catalog.pg_namespace
            WHERE nspname OPERATOR(pg_catalog.=) 'public'
        ) AND
           pg_catalog.current_setting('server_version_num')::int OPERATOR(pg_catalog./) 10000 OPERATOR(pg_catalog.>=) 15
        THEN
            IF EXISTS(
                SELECT rolname
                FROM pg_catalog.pg_roles
                WHERE rolname OPERATOR(pg_catalog.=) 'web_access'
            )
            THEN
                GRANT CREATE ON SCHEMA public TO web_access;
            END IF;
        END IF;
        IF EXISTS(
            SELECT nspname
            FROM pg_catalog.pg_namespace
            WHERE nspname OPERATOR(pg_catalog.=) 'public'
        )
        THEN
            ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO neon_superuser WITH GRANT OPTION;
            ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO neon_superuser WITH GRANT OPTION;
        END IF;
    END
$$;
DO
$$
    DECLARE
        schema_owner TEXT;
    BEGIN
        IF EXISTS(
            SELECT nspname
            FROM pg_catalog.pg_namespace
            WHERE nspname = 'public'
        )
        THEN
            SELECT nspowner::regrole::text
            FROM pg_catalog.pg_namespace
            WHERE nspname = 'public'
            INTO schema_owner;

            IF schema_owner = 'cloud_admin' OR schema_owner = 'zenith_admin'
            THEN
                ALTER SCHEMA public OWNER TO {db_owner};
            END IF;
        END IF;
    END
$$;
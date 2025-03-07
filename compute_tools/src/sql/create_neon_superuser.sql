DO $$
    BEGIN
        IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'neon_superuser')
        THEN
            CREATE ROLE neon_superuser CREATEDB CREATEROLE NOLOGIN REPLICATION BYPASSRLS IN ROLE pg_read_all_data, pg_write_all_data;
        END IF;
    END
$$;

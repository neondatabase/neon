DO $$
    BEGIN
        IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname OPERATOR(pg_catalog.=) '{privileged_role_name}'::pg_catalog.name)
        THEN
            CREATE ROLE {privileged_role_name} CREATEDB CREATEROLE NOLOGIN REPLICATION BYPASSRLS IN ROLE pg_read_all_data, pg_write_all_data;
        END IF;
    END
$$;

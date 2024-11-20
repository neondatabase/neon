DO $$
    BEGIN
        IF EXISTS(
            SELECT 1
            FROM pg_catalog.pg_database
            WHERE datname = {datname_str}
        )
        THEN
            ALTER DATABASE {datname} is_template false;
        END IF;
    END
$$;
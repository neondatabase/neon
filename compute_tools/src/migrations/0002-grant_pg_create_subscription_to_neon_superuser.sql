DO $$
BEGIN
    IF (SELECT setting::numeric >= 160000 FROM pg_settings WHERE name = 'server_version_num') THEN
        EXECUTE 'GRANT pg_create_subscription TO neon_superuser';
    END IF;
END $$;

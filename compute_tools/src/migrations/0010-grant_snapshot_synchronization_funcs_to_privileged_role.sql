DO $$
BEGIN
    IF (SELECT setting::numeric >= 160000 FROM pg_settings WHERE name = 'server_version_num') THEN
       EXECUTE 'GRANT EXECUTE ON FUNCTION pg_export_snapshot TO {privileged_role_name}';
       EXECUTE 'GRANT EXECUTE ON FUNCTION pg_log_standby_snapshot TO {privileged_role_name}';
    END IF;
END $$;

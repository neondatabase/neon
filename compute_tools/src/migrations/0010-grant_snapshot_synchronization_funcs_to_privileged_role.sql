DO $$
BEGIN
    IF (SELECT setting::pg_catalog.numeric >= 160000 FROM pg_catalog.pg_settings WHERE name OPERATOR(pg_catalog.=) 'server_version_num'::pg_catalog.text) THEN
       EXECUTE 'GRANT EXECUTE ON FUNCTION pg_export_snapshot TO {privileged_role_name}';
       EXECUTE 'GRANT EXECUTE ON FUNCTION pg_log_standby_snapshot TO {privileged_role_name}';
    END IF;
END $$;

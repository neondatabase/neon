ALTER ROLE databricks_monitor SET statement_timeout = '60s';

CREATE OR REPLACE FUNCTION health_check_write_succeeds()
RETURNS INTEGER AS $$
BEGIN
INSERT INTO health_check VALUES (1, now())
ON CONFLICT (id) DO UPDATE
    SET updated_at = now();

RETURN 1;
EXCEPTION WHEN OTHERS THEN
RAISE EXCEPTION '[DATABRICKS_SMGR] health_check failed: [%] %', SQLSTATE, SQLERRM;
RETURN 0;
END;
$$ LANGUAGE plpgsql;

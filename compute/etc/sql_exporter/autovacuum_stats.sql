WITH dbs AS (
  SELECT datname 
  FROM pg_database 
  WHERE datconnlimit != -2
    AND datname NOT IN ('postgres', 'template0', 'template1')
    AND NOT datistemplate
)
SELECT 
  d.datname,
  t.schemaname,
  t.relname,
  t.last_autovacuum,
  t.autovacuum_count,
  t.autoanalyze_count
FROM dbs d,
LATERAL (
  SELECT * FROM dblink(
    'dbname=' || d.datname || ' user=' || current_user,
    'SELECT schemaname, relname, last_autovacuum, autovacuum_count, autoanalyze_count 
     FROM pg_stat_all_tables 
     WHERE schemaname NOT IN (''pg_catalog'', ''information_schema'')
     ORDER BY last_autovacuum DESC limit 10'
  ) AS t(schemaname name, relname name, last_autovacuum timestamptz, 
         autovacuum_count bigint, autoanalyze_count bigint)
) t;

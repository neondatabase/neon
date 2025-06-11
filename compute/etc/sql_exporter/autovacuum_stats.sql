WITH dbs AS (
  SELECT datname 
  FROM pg_database 
  WHERE datconnlimit != -2
    AND datname NOT IN ('postgres')
    AND NOT datistemplate
)
SELECT 
  d.datname as dbname,
  t.autovacuum_count as autovacuum_count
FROM dbs d,
LATERAL (
  SELECT * FROM dblink(
    'dbname=' || quote_ident(d.datname) || ' user=' || quote_ident(current_user) || ' connect_timeout=5',
    'SELECT sum(autovacuum_count) as autovacuum_count
     FROM pg_stat_all_tables 
     WHERE schemaname NOT IN (''pg_catalog'', ''information_schema'')'
  ) AS t(autovacuum_count bigint)
) t;

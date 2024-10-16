-- We export stats for 10 non-system databases. Without this limit it is too
-- easy to abuse the system by creating lots of databases.

SELECT pg_database_size(datname) AS db_size, deadlocks, tup_inserted AS inserted,
  tup_updated AS updated, tup_deleted AS deleted, datname
FROM pg_stat_database
WHERE datname IN (
  SELECT datname FROM pg_database
  WHERE datname <> 'postgres' AND NOT datistemplate ORDER BY oid LIMIT 10
);

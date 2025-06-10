select schemaname, relname, last_autovacuum, autovacuum_count, autoanalyze_count from pg_stat_all_tables order by last_autovacuum desc;

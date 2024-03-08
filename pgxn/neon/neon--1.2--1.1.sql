-- the order of operations is important here
-- because the view depends on the function

DROP VIEW IF EXISTS neon_stat_file_cache CASCADE;

DROP FUNCTION IF EXISTS neon_get_lfc_stats CASCADE;

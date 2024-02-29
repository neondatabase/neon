\echo Use "ALTER EXTENSION neon UPDATE TO '1.2'" to load this file. \quit

-- Create a convenient view similar to pg_stat_database
-- that exposes all lfc stat values in one row.
CREATE OR REPLACE VIEW NEON_STAT_FILE_CACHE AS 
   WITH lfc_stats AS (
   SELECT 
     stat_name, 
     count
   FROM neon_get_lfc_stats() AS t(stat_name text, count bigint)
   ),
   lfc_values AS (
   SELECT 
     MAX(CASE WHEN stat_name = 'file_cache_misses' THEN count ELSE NULL END) AS file_cache_misses,
     MAX(CASE WHEN stat_name = 'file_cache_hits'   THEN count ELSE NULL END) AS file_cache_hits,
     MAX(CASE WHEN stat_name = 'file_cache_used'   THEN count ELSE NULL END) AS file_cache_used,
     MAX(CASE WHEN stat_name = 'file_cache_writes' THEN count ELSE NULL END) AS file_cache_writes,
     -- Calculate the file_cache_hit_ratio within the same CTE for simplicity
     CASE 
        WHEN MAX(CASE WHEN stat_name = 'file_cache_misses' THEN count ELSE 0 END) + MAX(CASE WHEN stat_name = 'file_cache_hits' THEN count ELSE 0 END) = 0 THEN NULL
        ELSE ROUND((MAX(CASE WHEN stat_name = 'file_cache_hits' THEN count ELSE 0 END)::DECIMAL / 
        (MAX(CASE WHEN stat_name = 'file_cache_hits' THEN count ELSE 0 END) + MAX(CASE WHEN stat_name = 'file_cache_misses' THEN count ELSE 0 END))) * 100, 2)
     END AS file_cache_hit_ratio
   FROM lfc_stats
   )
SELECT file_cache_misses, file_cache_hits, file_cache_used, file_cache_writes, file_cache_hit_ratio from lfc_values;

-- externalize the view to all users in role pg_monitor
GRANT SELECT ON NEON_STAT_FILE_CACHE TO PG_MONITOR;
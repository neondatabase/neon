-- run with pooled connection
-- pgbench -T 300 -c 100 -j20 -f pgbench_halfvec_queries.sql -postgresql://neondb_owner:<secret>@ep-floral-thunder-w1gzhaxi-pooler.eu-west-1.aws.neon.build/neondb?sslmode=require"

with x (x) as (
  select "embeddings" as x
  from halfvec_test_table 
  TABLESAMPLE SYSTEM (1) 
  LIMIT 1
)
SELECT title, "embeddings" <=> (select x from x) as distance
FROM halfvec_test_table
ORDER BY 2
LIMIT 30;
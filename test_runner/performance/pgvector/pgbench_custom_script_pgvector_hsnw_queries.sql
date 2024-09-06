with x (x) as (
  select "embeddings" as x
  from hnsw_test_table 
  TABLESAMPLE SYSTEM (1) 
  LIMIT 1
)
SELECT title, "embeddings" <=> (select x from x) as distance
FROM hnsw_test_table
ORDER BY 2
LIMIT 30;

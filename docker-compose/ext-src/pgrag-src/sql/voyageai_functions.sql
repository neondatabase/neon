-- Test VoyageAI API key functions
SELECT 'voyageai_api_key_test' AS test_name, 
       (SELECT rag.voyageai_set_api_key('test_key') IS NULL) AS result;

SELECT 'voyageai_get_api_key_test' AS test_name,
       (SELECT rag.voyageai_get_api_key() = 'test_key') AS result;

-- Test VoyageAI embedding functions exist
SELECT 'voyageai_embedding_exists' AS test_name,
       count(*) > 0 AS result
FROM pg_proc
WHERE proname = 'voyageai_embedding'
  AND pronamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'rag');

SELECT 'voyageai_embedding_3_exists' AS test_name,
       count(*) > 0 AS result
FROM pg_proc
WHERE proname = 'voyageai_embedding_3'
  AND pronamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'rag');

SELECT 'voyageai_embedding_3_lite_exists' AS test_name,
       count(*) > 0 AS result
FROM pg_proc
WHERE proname = 'voyageai_embedding_3_lite'
  AND pronamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'rag');

SELECT 'voyageai_embedding_code_2_exists' AS test_name,
       count(*) > 0 AS result
FROM pg_proc
WHERE proname = 'voyageai_embedding_code_2'
  AND pronamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'rag');

SELECT 'voyageai_embedding_finance_2_exists' AS test_name,
       count(*) > 0 AS result
FROM pg_proc
WHERE proname = 'voyageai_embedding_finance_2'
  AND pronamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'rag');

SELECT 'voyageai_embedding_law_2_exists' AS test_name,
       count(*) > 0 AS result
FROM pg_proc
WHERE proname = 'voyageai_embedding_law_2'
  AND pronamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'rag');

SELECT 'voyageai_embedding_multilingual_2_exists' AS test_name,
       count(*) > 0 AS result
FROM pg_proc
WHERE proname = 'voyageai_embedding_multilingual_2'
  AND pronamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'rag');

-- Test VoyageAI reranking functions exist
SELECT 'voyageai_rerank_distance_exists' AS test_name,
       count(*) > 0 AS result
FROM pg_proc
WHERE proname = 'voyageai_rerank_distance'
  AND pronamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'rag');

SELECT 'voyageai_rerank_score_exists' AS test_name,
       count(*) > 0 AS result
FROM pg_proc
WHERE proname = 'voyageai_rerank_score'
  AND pronamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'rag');

-- Test VoyageAI function signatures
SELECT 'voyageai_embedding_signature' AS test_name,
       count(*) > 0 AS result
FROM pg_proc
WHERE proname = 'voyageai_embedding'
  AND pronamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'rag')
  AND pronargs = 3;

SELECT 'voyageai_rerank_distance_signature' AS test_name,
       count(*) > 0 AS result
FROM pg_proc
WHERE proname = 'voyageai_rerank_distance'
  AND pronamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'rag')
  AND pronargs IN (3, 4);

SELECT 'voyageai_rerank_score_signature' AS test_name,
       count(*) > 0 AS result
FROM pg_proc
WHERE proname = 'voyageai_rerank_score'
  AND pronamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'rag')
  AND pronargs IN (3, 4);

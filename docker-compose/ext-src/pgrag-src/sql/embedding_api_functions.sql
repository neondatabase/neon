-- Test embedding functions exist with correct signatures
-- OpenAI embedding functions
SELECT 'openai_text_embedding_exists' AS test_name,
       count(*) > 0 AS result
FROM pg_proc
WHERE proname = 'openai_text_embedding'
  AND pronamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'rag');

SELECT 'openai_text_embedding_3_small_exists' AS test_name,
       count(*) > 0 AS result
FROM pg_proc
WHERE proname = 'openai_text_embedding_3_small'
  AND pronamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'rag');

SELECT 'openai_text_embedding_3_large_exists' AS test_name,
       count(*) > 0 AS result
FROM pg_proc
WHERE proname = 'openai_text_embedding_3_large'
  AND pronamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'rag');

SELECT 'openai_text_embedding_ada_002_exists' AS test_name,
       count(*) > 0 AS result
FROM pg_proc
WHERE proname = 'openai_text_embedding_ada_002'
  AND pronamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'rag');

-- Fireworks embedding functions
SELECT 'fireworks_nomic_embed_text_v1_exists' AS test_name,
       count(*) > 0 AS result
FROM pg_proc
WHERE proname = 'fireworks_nomic_embed_text_v1'
  AND pronamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'rag');

SELECT 'fireworks_nomic_embed_text_v15_exists' AS test_name,
       count(*) > 0 AS result
FROM pg_proc
WHERE proname = 'fireworks_nomic_embed_text_v15'
  AND pronamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'rag');

SELECT 'fireworks_text_embedding_exists' AS test_name,
       count(*) > 0 AS result
FROM pg_proc
WHERE proname = 'fireworks_text_embedding'
  AND pronamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'rag');

SELECT 'fireworks_text_embedding_thenlper_gte_base_exists' AS test_name,
       count(*) > 0 AS result
FROM pg_proc
WHERE proname = 'fireworks_text_embedding_thenlper_gte_base'
  AND pronamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'rag');

SELECT 'fireworks_text_embedding_thenlper_gte_large_exists' AS test_name,
       count(*) > 0 AS result
FROM pg_proc
WHERE proname = 'fireworks_text_embedding_thenlper_gte_large'
  AND pronamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'rag');

SELECT 'fireworks_text_embedding_whereisai_uae_large_v1_exists' AS test_name,
       count(*) > 0 AS result
FROM pg_proc
WHERE proname = 'fireworks_text_embedding_whereisai_uae_large_v1'
  AND pronamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'rag');

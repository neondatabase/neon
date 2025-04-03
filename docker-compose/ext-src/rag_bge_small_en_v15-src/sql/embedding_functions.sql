-- Embedding function tests
SELECT 'embedding_for_passage_test' AS test_name, 
       vector_dims(rag_bge_small_en_v15.embedding_for_passage('the cat sat on the mat')) > 0 AS result;

SELECT 'embedding_for_query_test' AS test_name,
       vector_dims(rag_bge_small_en_v15.embedding_for_query('the cat sat on the mat')) > 0 AS result;

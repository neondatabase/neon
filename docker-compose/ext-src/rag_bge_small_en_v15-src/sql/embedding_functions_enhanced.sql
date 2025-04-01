-- Embedding function tests
SELECT 'embedding_for_passage_test_1' AS test_name, 
       vector_dims(rag_bge_small_en_v15.embedding_for_passage('the cat sat on the mat')) > 0 AS result;

SELECT 'embedding_for_passage_test_2' AS test_name,
       vector_dims(rag_bge_small_en_v15.embedding_for_passage('Lorem ipsum dolor sit amet')) > 0 AS result;

SELECT 'embedding_for_passage_test_3' AS test_name,
       vector_dims(rag_bge_small_en_v15.embedding_for_passage('')) > 0 AS result;

SELECT 'embedding_for_query_test_1' AS test_name,
       vector_dims(rag_bge_small_en_v15.embedding_for_query('the cat sat on the mat')) > 0 AS result;

SELECT 'embedding_for_query_test_2' AS test_name,
       vector_dims(rag_bge_small_en_v15.embedding_for_query('Lorem ipsum dolor sit amet')) > 0 AS result;

SELECT 'embedding_for_query_test_3' AS test_name,
       vector_dims(rag_bge_small_en_v15.embedding_for_query('')) > 0 AS result;

-- Test that passage and query embeddings have the same dimensions
SELECT 'embedding_dimensions_match' AS test_name,
       vector_dims(rag_bge_small_en_v15.embedding_for_passage('test')) = 
       vector_dims(rag_bge_small_en_v15.embedding_for_query('test')) AS result;

-- Basic function tests for chunks_by_token_count
SELECT rag_bge_small_en_v15.chunks_by_token_count('the cat sat on the mat', 3, 2);

SELECT rag_bge_small_en_v15.chunks_by_token_count('Lorem ipsum dolor sit amet, consectetur adipiscing elit. Sed do eiusmod tempor incididunt ut labore et dolore magna aliqua.', 5, 2);

SELECT (rag_bge_small_en_v15.chunks_by_token_count('the cat', 5, 0))[1];

SELECT rag_bge_small_en_v15.chunks_by_token_count('', 5, 2);

SELECT rag_bge_small_en_v15.chunks_by_token_count('a b c d e f g h i j k l m n o p', 3, 1);

-- Reranking function tests
SELECT rag_jina_reranker_v1_tiny_en.rerank_distance('the cat sat on the mat', 'the baboon played with the balloon');

SELECT rag_jina_reranker_v1_tiny_en.rerank_distance('the cat sat on the mat', ARRAY['the baboon played with the balloon', 'the tanks fired at the buildings']);

SELECT rag_jina_reranker_v1_tiny_en.rerank_score('the cat sat on the mat', 'the baboon played with the balloon');

SELECT rag_jina_reranker_v1_tiny_en.rerank_score('the cat sat on the mat', ARRAY['the baboon played with the balloon', 'the tanks fired at the buildings']);
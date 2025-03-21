-- Reranking function tests - single passage
SELECT rag_jina_reranker_v1_tiny_en.rerank_distance('the cat sat on the mat', 'the baboon played with the balloon');

SELECT rag_jina_reranker_v1_tiny_en.rerank_distance('the cat sat on the mat', 'the tanks fired at the buildings');

SELECT rag_jina_reranker_v1_tiny_en.rerank_distance('query about cats', 'information about felines');

SELECT rag_jina_reranker_v1_tiny_en.rerank_distance('', 'empty query test');

-- Reranking function tests - array of passages
SELECT rag_jina_reranker_v1_tiny_en.rerank_distance('the cat sat on the mat',
    ARRAY['the baboon played with the balloon', 'the tanks fired at the buildings']);

SELECT rag_jina_reranker_v1_tiny_en.rerank_distance('query about programming',
    ARRAY['Python is a programming language', 'Java is also a programming language', 'SQL is used for databases']);

SELECT rag_jina_reranker_v1_tiny_en.rerank_distance('empty array test', ARRAY[]::text[]);

-- Reranking score function tests - single passage
SELECT rag_jina_reranker_v1_tiny_en.rerank_score('the cat sat on the mat', 'the baboon played with the balloon');

SELECT rag_jina_reranker_v1_tiny_en.rerank_score('the cat sat on the mat', 'the tanks fired at the buildings');

SELECT rag_jina_reranker_v1_tiny_en.rerank_score('query about cats', 'information about felines');

SELECT rag_jina_reranker_v1_tiny_en.rerank_score('', 'empty query test');

-- Reranking score function tests - array of passages
SELECT rag_jina_reranker_v1_tiny_en.rerank_score('the cat sat on the mat',
    ARRAY['the baboon played with the balloon', 'the tanks fired at the buildings']);

SELECT rag_jina_reranker_v1_tiny_en.rerank_score('query about programming',
    ARRAY['Python is a programming language', 'Java is also a programming language', 'SQL is used for databases']);

SELECT rag_jina_reranker_v1_tiny_en.rerank_score('empty array test', ARRAY[]::text[]);

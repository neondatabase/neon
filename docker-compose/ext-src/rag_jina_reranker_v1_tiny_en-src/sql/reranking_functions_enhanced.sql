-- Reranking function tests - single passage
SELECT ROUND(rag_jina_reranker_v1_tiny_en.rerank_distance('the cat sat on the mat', 'the baboon played with the balloon')::NUMERIC,4);

SELECT ROUND(rag_jina_reranker_v1_tiny_en.rerank_distance('the cat sat on the mat', 'the tanks fired at the buildings')::NUMERIC,4);

SELECT ROUND(rag_jina_reranker_v1_tiny_en.rerank_distance('query about cats', 'information about felines')::NUMERIC,4);

SELECT ROUND(rag_jina_reranker_v1_tiny_en.rerank_distance('', 'empty query test')::NUMERIC,4);

-- Reranking function tests - array of passages
SELECT ARRAY(SELECT ROUND(x::NUMERIC,4) FROM unnest(rag_jina_reranker_v1_tiny_en.rerank_distance('the cat sat on the mat',
    ARRAY['the baboon played with the balloon', 'the tanks fired at the buildings'])) AS x);

SELECT ARRAY(SELECT ROUND(x::NUMERIC,4) FROM unnest(rag_jina_reranker_v1_tiny_en.rerank_distance('query about programming',
    ARRAY['Python is a programming language', 'Java is also a programming language', 'SQL is used for databases'])) AS x);

SELECT rag_jina_reranker_v1_tiny_en.rerank_distance('empty array test', ARRAY[]::text[]);

-- Reranking score function tests - single passage
SELECT ROUND(rag_jina_reranker_v1_tiny_en.rerank_score('the cat sat on the mat', 'the baboon played with the balloon')::NUMERIC,4);

SELECT ROUND(rag_jina_reranker_v1_tiny_en.rerank_score('the cat sat on the mat', 'the tanks fired at the buildings')::NUMERIC,4);

SELECT ROUND(rag_jina_reranker_v1_tiny_en.rerank_score('query about cats', 'information about felines')::NUMERIC,4);

SELECT ROUND(rag_jina_reranker_v1_tiny_en.rerank_score('', 'empty query test')::NUMERIC,4);

-- Reranking score function tests - array of passages
SELECT ARRAY(SELECT ROUND(x::NUMERIC,4) FROM unnest(rag_jina_reranker_v1_tiny_en.rerank_score('the cat sat on the mat',
    ARRAY['the baboon played with the balloon', 'the tanks fired at the buildings'])) AS x);

SELECT ARRAY(SELECT ROUND(x::NUMERIC,4) FROM unnest(rag_jina_reranker_v1_tiny_en.rerank_score('query about programming',
    ARRAY['Python is a programming language', 'Java is also a programming language', 'SQL is used for databases'])) AS x);

SELECT rag_jina_reranker_v1_tiny_en.rerank_score('empty array test', ARRAY[]::text[]);

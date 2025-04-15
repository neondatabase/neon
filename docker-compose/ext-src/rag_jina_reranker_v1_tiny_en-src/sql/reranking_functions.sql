-- Reranking function tests
SELECT ROUND(rag_jina_reranker_v1_tiny_en.rerank_distance('the cat sat on the mat', 'the baboon played with the balloon')::NUMERIC,4);

SELECT ARRAY(SELECT ROUND(x::NUMERIC,4) FROM unnest(rag_jina_reranker_v1_tiny_en.rerank_distance('the cat sat on the mat',
    ARRAY['the baboon played with the balloon', 'the tanks fired at the buildings'])) AS x);

SELECT ROUND(rag_jina_reranker_v1_tiny_en.rerank_score('the cat sat on the mat', 'the baboon played with the balloon')::NUMERIC,4);

SELECT ARRAY(SELECT ROUND(x::NUMERIC,4) FROM unnest(rag_jina_reranker_v1_tiny_en.rerank_score('the cat sat on the mat',
    ARRAY['the baboon played with the balloon', 'the tanks fired at the buildings'])) as x);
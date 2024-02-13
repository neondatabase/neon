SET enable_seqscan = off;

CREATE UNLOGGED TABLE t (val vector(3));
INSERT INTO t (val) VALUES ('[0,0,0]'), ('[1,2,3]'), ('[1,1,1]'), (NULL);
CREATE INDEX ON t USING hnsw (val vector_l2_ops);

SELECT * FROM t ORDER BY val <-> '[3,3,3]';

DROP TABLE t;

SET enable_seqscan = off;

CREATE TABLE t (val real[]);
INSERT INTO t (val) VALUES ('{0,0,0}'), ('{1,2,3}'), ('{1,1,1}'), (NULL);
CREATE INDEX ON t USING hnsw (val) WITH (maxelements = 10, dims=3, m=3);

INSERT INTO t (val) VALUES (array[1,2,4]);

explain SELECT * FROM t ORDER BY val <-> array[3,3,3];
SELECT * FROM t ORDER BY val <-> array[3,3,3];
SELECT COUNT(*) FROM t;

DROP TABLE t;

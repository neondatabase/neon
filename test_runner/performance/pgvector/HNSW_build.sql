
\set ECHO queries
\timing

-- prepare test table
DROP TABLE IF EXISTS hnsw_test_table;
CREATE TABLE hnsw_test_table AS TABLE documents WITH NO DATA;
INSERT INTO hnsw_test_table SELECT * FROM documents;
CREATE INDEX ON hnsw_test_table (_id); -- needed later for random tuple queries
-- tune index build params
SET max_parallel_maintenance_workers = 7; 
SET maintenance_work_mem = '8GB';
-- create HNSW index for the supported distance metrics
CREATE INDEX ON hnsw_test_table USING hnsw (embeddings vector_cosine_ops);
CREATE INDEX ON hnsw_test_table USING hnsw (embeddings vector_ip_ops);
CREATE INDEX ON hnsw_test_table USING hnsw (embeddings vector_l1_ops);
CREATE INDEX ON hnsw_test_table USING hnsw ((binary_quantize(embeddings)::bit(1536)) bit_hamming_ops);
CREATE INDEX ON hnsw_test_table USING hnsw ((binary_quantize(embeddings)::bit(1536)) bit_jaccard_ops);
-- note: in a second psql session we can monitor the progress of the index build phases using
-- the following query:
-- SELECT phase, round(100.0 * blocks_done / nullif(blocks_total, 0), 1) AS "%" FROM pg_stat_progress_create_index;

-- show all indexes built on the table
SELECT 
    idx.relname AS index_name,
    tbl.relname AS table_name,
    am.amname AS access_method,
    a.attname AS column_name,
    opc.opcname AS operator_class
FROM 
    pg_index i
JOIN 
    pg_class idx ON idx.oid = i.indexrelid
JOIN 
    pg_class tbl ON tbl.oid = i.indrelid
JOIN 
    pg_am am ON am.oid = idx.relam
JOIN 
    pg_attribute a ON a.attrelid = tbl.oid AND a.attnum = ANY(i.indkey)
JOIN 
    pg_opclass opc ON opc.oid = i.indclass[0]
WHERE 
    tbl.relname = 'hnsw_test_table' 
    AND a.attname = 'embeddings';

-- show table sizes
\dt+

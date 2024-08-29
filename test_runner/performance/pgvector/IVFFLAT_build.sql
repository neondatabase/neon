
\set ECHO queries
\timing

-- prepare test table
DROP TABLE IF EXISTS ivfflat_test_table;
CREATE TABLE ivfflat_test_table AS TABLE documents WITH NO DATA;
INSERT INTO ivfflat_test_table SELECT * FROM documents;
CREATE INDEX ON ivfflat_test_table (_id); -- needed later for random tuple queries
-- tune index build params
SET max_parallel_maintenance_workers = 7; 
SET maintenance_work_mem = '8GB';
-- create ivfflat index for the supported distance metrics
-- the formulat for lists is # rows / 1000 or sqrt(# rows) if # rows > 1 million
-- we have 1 million embeddings of vector size 1536 in column embeddings of table documents
-- so we use 1000 lists
CREATE INDEX ON ivfflat_test_table USING ivfflat (embeddings vector_l2_ops) WITH (lists = 1000);
CREATE INDEX ON ivfflat_test_table USING ivfflat (embeddings vector_ip_ops) WITH (lists = 1000);
CREATE INDEX ON ivfflat_test_table USING ivfflat (embeddings vector_cosine_ops) WITH (lists = 1000);
CREATE INDEX ON ivfflat_test_table USING ivfflat (embeddings::halfvec(1536) halfvec_l2_ops) WITH (lists = 1000);
CREATE INDEX ON ivfflat_test_table
    USING ivfflat ((binary_quantize(embeddings)::bit(1536)) bit_hamming_ops) WITH (lists = 1000);

\d ivfflat_test_table


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
    tbl.relname = 'ivfflat_test_table' 
    AND a.attname = 'embeddings';
-- show table sizes
\dt+



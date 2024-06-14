DROP TABLE IF EXISTS halfvec_test_table;

CREATE TABLE halfvec_test_table (
    _id text NOT NULL,
    title text,
    text text,
    embeddings halfvec(1536),
    PRIMARY KEY (_id)
);

INSERT INTO halfvec_test_table (_id, title, text, embeddings)
SELECT _id, title, text, embeddings::halfvec
FROM documents;

CREATE INDEX documents_half_precision_hnsw_idx ON halfvec_test_table USING hnsw (embeddings halfvec_cosine_ops) WITH (m = 64, ef_construction = 128);
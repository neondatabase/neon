CREATE EXTENSION IF NOT EXISTS vector;

DROP TABLE documents;

CREATE TABLE documents (
    _id TEXT PRIMARY KEY,
    title TEXT,
    text TEXT,
    embeddings vector(1536) -- text-embedding-3-large-1536-embedding (OpenAI)
);

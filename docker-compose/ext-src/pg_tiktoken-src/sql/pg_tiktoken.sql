-- Load the extension
CREATE EXTENSION IF NOT EXISTS pg_tiktoken;

-- Test encoding function
SELECT tiktoken_encode('cl100k_base', 'Hello world!');

-- Test token count function
SELECT tiktoken_count('cl100k_base', 'Hello world!');

-- Test encoding function with a different model
SELECT tiktoken_encode('r50k_base', 'PostgreSQL is amazing!');

-- Test token count function with the same model
SELECT tiktoken_count('r50k_base', 'PostgreSQL is amazing!');

-- Edge cases: Empty string
SELECT tiktoken_encode('cl100k_base', '');
SELECT tiktoken_count('cl100k_base', '');

-- Edge cases: Long text
SELECT tiktoken_count('cl100k_base', repeat('word ', 100));

-- Edge case: Invalid encoding
SELECT tiktoken_encode('invalid_model', 'Test') AS should_fail;
-- API key function tests
SELECT rag.anthropic_set_api_key('test_key');

SELECT rag.anthropic_get_api_key();

SELECT rag.openai_set_api_key('test_key');

SELECT rag.openai_get_api_key();

SELECT rag.fireworks_set_api_key('test_key');

SELECT rag.fireworks_get_api_key();

SELECT rag.voyageai_set_api_key('test_key');

SELECT rag.voyageai_get_api_key();

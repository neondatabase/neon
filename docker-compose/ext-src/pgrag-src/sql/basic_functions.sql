-- Basic function tests
SELECT rag.markdown_from_html('<p>Hello</p>');

SELECT array_length(rag.chunks_by_character_count('the cat sat on the mat', 10, 5), 1);

-- Text processing function tests
SELECT rag.markdown_from_html('<p>Hello <i>world</i></p>');

SELECT rag.chunks_by_character_count('the cat sat on the mat', 10, 5);

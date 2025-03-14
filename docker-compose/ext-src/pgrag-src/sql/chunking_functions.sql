-- Chunking function tests
SELECT rag.chunks_by_character_count('the cat sat on the mat', 10, 5);

SELECT rag.chunks_by_character_count('Lorem ipsum dolor sit amet, consectetur adipiscing elit. Sed do eiusmod tempor incididunt ut labore et dolore magna aliqua.', 20, 10);

SELECT (rag.chunks_by_character_count('the cat', 10, 0))[1];

SELECT rag.chunks_by_character_count('', 10, 5);

SELECT rag.chunks_by_character_count('a b c d e f g h i j k l m n o p', 5, 2);


-- HTML to Markdown conversion tests
SELECT rag.markdown_from_html('<p>Hello</p>');

SELECT rag.markdown_from_html('<p>Hello <i>world</i></p>');

SELECT rag.markdown_from_html('<h1>Title</h1><p>Paragraph</p>');

SELECT rag.markdown_from_html('<ul><li>Item 1</li><li>Item 2</li></ul>');

SELECT rag.markdown_from_html('<a href="https://example.com">Link</a>');

-- Note: text_from_pdf and text_from_docx require binary input which is harder to test in regression tests
-- We'll test that the functions exist and have the right signature
SELECT 'text_from_pdf_exists' AS test_name,
       count(*) > 0 AS result
FROM pg_proc
WHERE proname = 'text_from_pdf'
  AND pronamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'rag');

SELECT 'text_from_docx_exists' AS test_name,
       count(*) > 0 AS result
FROM pg_proc
WHERE proname = 'text_from_docx'
  AND pronamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'rag');

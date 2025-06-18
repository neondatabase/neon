-- add 100000 rows or approximately 20 MB to the ocr_pipeline_results_version table
-- takes about 1 second
INSERT INTO ocr.ocr_pipeline_results_version (
    id, transaction_id, operation_type, created_at, updated_at, s3_filename, completed_at, result,
    end_transaction_id, pipeline_type, is_async, callback, callback_kwargs, input, error, file_type, s3_bucket_name, pipeline_kwargs
)
SELECT
    gs.aid,  -- id
    gs.aid,  -- transaction_id (same as id for simplicity)
    (gs.aid % 5)::smallint + 1,  -- operation_type (cyclic values from 1 to 5)
    now() - interval '1 day' * (random() * 30),  -- created_at (random timestamp within the last 30 days)
    now() - interval '1 day' * (random() * 30),  -- updated_at (random timestamp within the last 30 days)
    's3_file_' || gs.aid || '.txt',  -- s3_filename (synthetic filename)
    now() - interval '1 day' * (random() * 30),  -- completed_at (random timestamp within the last 30 days)
    '{}'::jsonb,  -- result (empty JSON object)
    NULL,  -- end_transaction_id (NULL)
    CASE (gs.aid % 3)  -- pipeline_type (cyclic text values)
        WHEN 0 THEN 'OCR'
        WHEN 1 THEN 'PDF'
        ELSE 'Image'
    END,
    gs.aid % 2 = 0,  -- is_async (alternating between true and false)
    'http://callback/' || gs.aid,  -- callback (synthetic URL)
    '{}'::jsonb,  -- callback_kwargs (empty JSON object)
    'Input text ' || gs.aid,  -- input (synthetic input text)
    NULL,  -- error (NULL)
    'pdf',  -- file_type (default to 'pdf')
    'bucket_' || gs.aid % 10,  -- s3_bucket_name (synthetic bucket names)
    '{}'::jsonb  -- pipeline_kwargs (empty JSON object)
FROM
    generate_series(1, 100000) AS gs(aid);
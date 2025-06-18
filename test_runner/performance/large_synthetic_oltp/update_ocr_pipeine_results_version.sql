-- update approximately 2000 rows or 400 kb in the ocr_pipeline_results_version table
-- takes about 1 second
UPDATE   ocr.ocr_pipeline_results_version 
SET is_async = NOT is_async
WHERE ctid in (
    SELECT ctid
    FROM   ocr.ocr_pipeline_results_version 
    TABLESAMPLE SYSTEM (0.0005) 
);

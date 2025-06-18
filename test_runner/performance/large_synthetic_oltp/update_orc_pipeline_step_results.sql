-- update approximately 3000 rows or 1 MB in the ocr_pipeline_step_results table
-- takes about 11 seconds
UPDATE     ocr.ocr_pipeline_step_results 
SET created_at = now()
WHERE ctid in (
    SELECT ctid
    FROM    ocr.ocr_pipeline_step_results 
    TABLESAMPLE SYSTEM (0.0005) 
);

-- update approximately 5000 rows or 1 MB in the ocr_pipeline_step_results_version table
-- takes about 40 seconds
UPDATE    ocr.ocr_pipeline_step_results_version  
SET created_at = now()
WHERE ctid in (
    SELECT ctid
    FROM    ocr.ocr_pipeline_step_results_version  
    TABLESAMPLE SYSTEM (0.0005) 
);

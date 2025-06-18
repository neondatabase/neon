-- update approximately 2500 rows or 1 MB in the values table
-- takes about 3 minutes
UPDATE workflows.values
SET bool_value = NOT bool_value
WHERE ctid in (
    SELECT ctid
    FROM workflows.values
    TABLESAMPLE SYSTEM (0.0002) 
) AND bool_value IS NOT NULL;

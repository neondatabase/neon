-- update approximately 8000 rows or 1 MB in the state_values table
-- takes about 2 minutes
UPDATE workflows.state_values
SET state_type = now()::text
WHERE ctid in (
    SELECT ctid
    FROM workflows.state_values
    TABLESAMPLE SYSTEM (0.0002) 
);
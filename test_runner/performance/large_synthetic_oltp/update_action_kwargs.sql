-- update approximately 5000 rows or 1 MB in the action_kwargs table
-- takes about 1 second
UPDATE workflows.action_kwargs
SET created_at = now()
WHERE ctid in (
    SELECT ctid
    FROM workflows.action_kwargs
    TABLESAMPLE SYSTEM (0.0002) 
);

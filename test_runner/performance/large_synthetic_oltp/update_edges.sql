-- update approximately 4000 rows or 600 kb in the edges table
-- takes about 1 second
UPDATE workflows.edges
SET created_at = now()
WHERE ctid in (
    SELECT ctid
    FROM workflows.edges
    TABLESAMPLE SYSTEM (0.0005) 
);

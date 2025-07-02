-- update approximately 10000 rows or 2 MB in the vertices table
-- takes about 1 minute
UPDATE workflows.vertices
SET has_been_visited = NOT has_been_visited
WHERE ctid in (
    SELECT ctid
    FROM workflows.vertices
    TABLESAMPLE SYSTEM (0.0002) 
);
-- update approximately 9000 rows or 1 MB in the action_blocks table
-- takes about 1 second
UPDATE  workflows.action_blocks 
SET run_synchronously = NOT run_synchronously
WHERE ctid in (
    SELECT ctid
    FROM  workflows.action_blocks 
    TABLESAMPLE SYSTEM (0.001) 
);

-- update approximately 3000 rows or 500 KB in the denormalized_approval_workflow table
-- takes about 1 second
UPDATE  approvals_v2.denormalized_approval_workflow 
SET created_at = now()
WHERE ctid in (
    SELECT ctid
    FROM  approvals_v2.denormalized_approval_workflow 
    TABLESAMPLE SYSTEM (0.0005) 
);


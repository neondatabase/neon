-- update approximately 8000 rows or 1 MB in the relabeled_transactions table
-- takes about 1 second
UPDATE heron.relabeled_transactions
SET created_at = now()
WHERE ctid in (
    SELECT ctid
    FROM heron.relabeled_transactions
    TABLESAMPLE SYSTEM (0.0005) 
);
